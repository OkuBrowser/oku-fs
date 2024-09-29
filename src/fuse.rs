use crate::error::OkuFuseError;
use crate::fs::OkuFs;
use chrono::TimeZone;
use fuse_mt::CallbackResult;
use fuse_mt::CreatedEntry;
use fuse_mt::DirectoryEntry;
use fuse_mt::FileAttr;
use fuse_mt::FilesystemMT;
use fuse_mt::RequestInfo;
use fuse_mt::ResultCreate;
use fuse_mt::ResultData;
use fuse_mt::ResultEmpty;
use fuse_mt::ResultEntry;
use fuse_mt::ResultOpen;
use fuse_mt::ResultReaddir;
use fuse_mt::ResultSlice;
use fuse_mt::ResultStatfs;
use fuse_mt::ResultWrite;
#[cfg(target_os = "macos")]
use fuse_mt::ResultXTimes;
use fuse_mt::ResultXattr;
use fuse_mt::Statfs;
use iroh::client::docs::Entry;
use iroh::docs::NamespaceId;
use log::debug;
use log::error;
use log::info;
use log::trace;
use log::warn;
use miette::IntoDiagnostic;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Seek;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::SystemTime;

/// Parse a FUSE path to retrieve the replica and path.
///
/// # Arguments
///
/// * `path` - The FUSE path.
///
/// # Returns
///
/// A replica ID, if the FUSE path is not the root directory, and a path in the optional replica.
pub fn parse_fuse_path(path: &Path) -> miette::Result<Option<(NamespaceId, PathBuf)>> {
    let mut components = path.components();
    if let Some(_root) = components.next() {
        if let Some(replica_id) = components.next() {
            let replica_id_string = replica_id.as_os_str().to_str().unwrap_or_default();
            let namespace_id = NamespaceId::from_str(replica_id_string)
                .map_err(|_| OkuFuseError::NoReplica(replica_id_string.to_string()))?;
            let replica_path = PathBuf::from("/").join(components.as_path()).to_path_buf();
            return Ok(Some((namespace_id, replica_path)));
        } else {
            return Ok(None);
        }
    }
    Err(OkuFuseError::NoRoot.into())
}

/// Determines the immediate contents of a directory.
///
/// # Arguments
///
/// * `prefix_path` - The path to the directory.
///
/// * `files` - The recursive contents of the directory.
///
/// # Returns
///
/// The file system entries in the directory.
pub fn get_immediate_children(
    prefix_path: PathBuf,
    files: Vec<Entry>,
) -> miette::Result<Vec<DirectoryEntry>> {
    let mut directory_set: HashSet<OsString> = HashSet::new();
    let mut directory_entries: Vec<DirectoryEntry> = vec![
        DirectoryEntry {
            name: std::ffi::OsString::from("."),
            kind: fuse_mt::FileType::Directory,
        },
        DirectoryEntry {
            name: std::ffi::OsString::from(".."),
            kind: fuse_mt::FileType::Directory,
        },
    ];
    // For all descending files …
    for file in files {
        let file_path = PathBuf::from(std::str::from_utf8(file.key()).unwrap_or_default());
        let stripped_file_path = file_path
            .strip_prefix(prefix_path.clone())
            .into_diagnostic()?;
        let number_of_components = stripped_file_path.components().count();
        if let Some(first_component) = stripped_file_path.components().next() {
            // Check if this file is a direct child of the prefix path
            // If the file isn't a direct child, it must be in a folder under the prefix path
            if number_of_components == 1 {
                directory_entries.push(DirectoryEntry {
                    name: stripped_file_path
                        .file_name()
                        .unwrap_or_default()
                        .to_os_string(),
                    kind: fuse_mt::FileType::RegularFile,
                })
            } else {
                directory_set.insert(first_component.as_os_str().to_os_string());
            }
        }
    }
    for directory in directory_set {
        directory_entries.push(DirectoryEntry {
            name: directory,
            kind: fuse_mt::FileType::Directory,
        })
    }
    Ok(directory_entries)
}

impl OkuFs {
    /// Translates a file system handle into a path.
    ///
    /// # Arguments
    ///
    /// * `fh` - The file system handle for a file system entry.
    ///
    /// # Returns
    ///
    /// The path to the corresponding file system entry.
    pub fn fs_handle_to_path(&self, fh: u64) -> miette::Result<Option<PathBuf>> {
        match self.fs_handles.read() {
            Ok(fs_handles) => {
                let path = fs_handles.get(&fh).map(|p| p.to_path_buf());
                Ok(path)
            }
            Err(_e) => Err(OkuFuseError::NoFileWithHandle(fh).into()),
        }
    }

    /// Creates a file system handle for the file system entry at a path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the file system entry for which the handle should be made.
    ///
    /// # Returns
    ///
    /// The file system handle for the file system entry at the path.
    pub fn add_fs_handle(&self, path: &Path) -> miette::Result<u64> {
        match self.fs_handles.write() {
            Ok(mut fs_handles) => match self.newest_handle.write() {
                Ok(mut newest_handle) => {
                    fs_handles.insert(*newest_handle + 1, path.to_path_buf());
                    *newest_handle += 1;
                    Ok(*newest_handle)
                }
                Err(_e) => Err(OkuFuseError::FsHandlesFailedUpdate.into()),
            },
            Err(_e) => Err(OkuFuseError::FsHandlesFailedUpdate.into()),
        }
    }

    /// Deletes a file system handle for a file system entry.
    ///
    /// # Arguments
    ///
    /// * `fh` - The file system handle to be dropped.
    ///
    /// # Returns
    ///
    /// The path of the file system entry whose handle was released.
    pub fn remove_fs_handle(&self, fh: u64) -> miette::Result<Option<PathBuf>> {
        match self.fs_handles.write() {
            Ok(mut fs_handles) => Ok(fs_handles.remove(&fh)),
            Err(_e) => Err(OkuFuseError::FsHandlesFailedUpdate.into()),
        }
    }

    /// Determines if the file system entry at a path is a file or a directory.
    ///
    /// # Arguments
    ///
    /// * `path` - The path pointing to the file system entry.
    ///
    /// # Returns
    ///
    /// The file system entry type, being either a file or a directory.
    pub async fn is_file_or_directory(&self, path: &Path) -> miette::Result<fuse_mt::FileType> {
        let parsed_path = parse_fuse_path(path)?;
        if let Some((namespace_id, replica_path)) = parsed_path {
            if self
                .get_entry(namespace_id, replica_path.clone())
                .await
                .is_ok()
                && replica_path != PathBuf::from("/")
            {
                Ok(fuse_mt::FileType::RegularFile)
            } else {
                match path.parent() {
                    Some(parent_path) => {
                        let parent_path_buf = parent_path.to_path_buf();
                        if parent_path_buf == PathBuf::from("/") {
                            // The children of the root are the replica directories
                            Ok(fuse_mt::FileType::Directory)
                        } else {
                            match parse_fuse_path(&parent_path_buf.clone())? {
                                Some((namespace_id, parsed_parent_path)) => {
                                    let parent_children = self
                                        .list_files(namespace_id, Some(parsed_parent_path.clone()))
                                        .await?;
                                    let parent_immediate_children = get_immediate_children(
                                        parsed_parent_path.clone(),
                                        parent_children,
                                    )?;
                                    if parent_immediate_children
                                        .iter()
                                        .find(|immediate_child| {
                                            immediate_child.name
                                                == path.file_name().unwrap_or_default()
                                        })
                                        .is_some()
                                    {
                                        Ok(fuse_mt::FileType::Directory)
                                    } else {
                                        Err(OkuFuseError::NoFileAtPath(path.to_path_buf()).into())
                                    }
                                }
                                None => Err(OkuFuseError::NoFileAtPath(path.to_path_buf()).into()),
                            }
                        }
                    }
                    None => Err(OkuFuseError::NoFileAtPath(path.to_path_buf()).into()),
                }
            }
        } else {
            Ok(fuse_mt::FileType::Directory)
        }
    }

    /// Determines the attributes of a file system entry.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file system entry.
    ///
    /// # Returns
    ///
    /// The attributes of the file system entry.
    pub async fn get_fs_entry_attributes(&self, path: &Path) -> miette::Result<FileAttr> {
        let parsed_path = parse_fuse_path(path)?;
        if let Some((namespace_id, replica_path)) = parsed_path {
            let fs_entry_permission = match self.get_replica_capability(namespace_id).await? {
                iroh::docs::CapabilityKind::Read => 0o444u16,
                iroh::docs::CapabilityKind::Write => 0o777u16,
            };
            let fs_entry_type = self.is_file_or_directory(path).await?;
            match fs_entry_type {
                fuse_mt::FileType::RegularFile => {
                    let file_entry = self.get_entry(namespace_id, replica_path.clone()).await?;
                    let estimated_creation_time = SystemTime::from(
                        chrono::Utc.timestamp_nanos(
                            self.get_oldest_entry_timestamp(namespace_id, replica_path)
                                .await? as i64,
                        ),
                    );
                    Ok(FileAttr {
                        size: file_entry.content_len(),
                        blocks: 0,
                        atime: SystemTime::now(),
                        mtime: SystemTime::from(chrono::Utc.timestamp_nanos(
                            (file_entry.timestamp() * 1000).try_into().unwrap_or(0),
                        )),
                        ctime: estimated_creation_time,
                        crtime: estimated_creation_time,
                        kind: fs_entry_type,
                        perm: fs_entry_permission,
                        nlink: 0,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        flags: 0,
                    })
                }
                fuse_mt::FileType::Directory => {
                    let directory_creation_time_estimate = self
                        .get_oldest_timestamp_in_folder(namespace_id, replica_path.clone())
                        .await?;
                    let directory_modification_time_estimate = self
                        .get_newest_timestamp_in_folder(namespace_id, replica_path.clone())
                        .await?;
                    let directory_size_estimate = self
                        .get_folder_size(namespace_id, replica_path.clone())
                        .await?;
                    Ok(FileAttr {
                        size: directory_size_estimate,
                        blocks: 0,
                        atime: SystemTime::now(),
                        mtime: SystemTime::from(
                            chrono::Utc.timestamp_nanos(
                                (directory_modification_time_estimate * 1000)
                                    .try_into()
                                    .unwrap_or(0),
                            ),
                        ),
                        ctime: SystemTime::from(
                            chrono::Utc.timestamp_nanos(
                                (directory_creation_time_estimate * 1000)
                                    .try_into()
                                    .unwrap_or(0),
                            ),
                        ),
                        crtime: SystemTime::from(
                            chrono::Utc.timestamp_nanos(
                                (directory_creation_time_estimate * 1000)
                                    .try_into()
                                    .unwrap_or(0),
                            ),
                        ),
                        kind: fuse_mt::FileType::Directory,
                        perm: fs_entry_permission,
                        nlink: 0,
                        uid: 0,
                        gid: 0,
                        rdev: 0,
                        flags: 0,
                    })
                }
                _ => unreachable!(),
            }
        } else {
            if path.to_path_buf() == PathBuf::from("/") {
                let root_creation_time_estimate = self.get_oldest_timestamp().await?;
                let root_modification_time_estimate = self.get_newest_timestamp().await?;
                let root_size_estimate = self.get_size().await?;
                Ok(FileAttr {
                    size: root_size_estimate,
                    blocks: 0,
                    atime: SystemTime::now(),
                    mtime: SystemTime::from(
                        chrono::Utc.timestamp_nanos(
                            (root_modification_time_estimate * 1000)
                                .try_into()
                                .unwrap_or(0),
                        ),
                    ),
                    ctime: SystemTime::from(chrono::Utc.timestamp_nanos(
                        (root_creation_time_estimate * 1000).try_into().unwrap_or(0),
                    )),
                    crtime: SystemTime::from(chrono::Utc.timestamp_nanos(
                        (root_creation_time_estimate * 1000).try_into().unwrap_or(0),
                    )),
                    kind: fuse_mt::FileType::Directory,
                    perm: 0o444u16,
                    nlink: 0,
                    uid: 0,
                    gid: 0,
                    rdev: 0,
                    flags: 0,
                })
            } else {
                Err(OkuFuseError::NoFileAtPath(path.to_path_buf()).into())
            }
        }
    }
}

impl FilesystemMT for OkuFs {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        trace!("init() called");
        return Ok(());
    }

    fn destroy(&self) {
        let _ = self
            .handle
            .block_on(async move { self.clone().shutdown().await });
        info!("Node unmounting and shutting down … ");
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, fh: Option<u64>) -> ResultEntry {
        debug!("[getattr] path = {:?}, fh = {:?}", path, fh);
        // Potential improvement: spawn a new thread to block on.
        let fs_entry_attr_result = self
            .handle
            .block_on(async { self.get_fs_entry_attributes(&path).await });
        match fs_entry_attr_result {
            Ok(fs_entry_attr) => Ok((std::time::Duration::from_secs(0), fs_entry_attr)),
            Err(e) => {
                error!("[getattr]: {}", e);
                Err(libc::ENOENT)
            }
        }
    }

    fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        debug!("[open] path = {:?}, flags = {:#06o}", path, flags);
        match self.add_fs_handle(path) {
            Ok(fs_handle) => Ok((fs_handle, flags)),
            Err(e) => {
                error!("[open]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn read(
        &self,
        _req: RequestInfo,
        path: &Path,
        fh: u64,
        offset: u64,
        size: u32,
        callback: impl FnOnce(ResultSlice<'_>) -> CallbackResult,
    ) -> CallbackResult {
        debug!(
            "[read] path = {:?}, fh = {}, offset = {}, size = {}",
            path, fh, offset, size
        );

        match parse_fuse_path(&path) {
            Ok(parsed_path) => match parsed_path {
                Some((namespace_id, replica_path)) => {
                    // Potential improvement: spawn a new thread to block on.
                    let bytes_result = self
                        .handle
                        .block_on(async { self.read_file(namespace_id, replica_path).await });
                    match bytes_result {
                        Ok(bytes) => {
                            if offset > bytes.len() as u64 {
                                return callback(Ok(&[]));
                            }
                            if size as u64 + offset > bytes.len() as u64 {
                                return callback(Ok(&bytes[offset as usize..]));
                            } else {
                                return callback(Ok(
                                    &bytes[offset as usize..offset as usize + size as usize]
                                ));
                            }
                        }
                        Err(e) => {
                            error!("[read]: {}", e);
                            callback(Err(libc::ENOSYS))
                        }
                    }
                }
                None => {
                    error!(
                        "[read] failed on: path = {:?}, fh = {}, offset = {}, size = {}",
                        path, fh, offset, size
                    );
                    callback(Err(libc::ENOSYS))
                }
            },
            Err(e) => {
                error!("[read]: {}", e);
                callback(Err(libc::ENOSYS))
            }
        }
    }

    fn flush(&self, _req: RequestInfo, path: &Path, fh: u64, _lock_owner: u64) -> ResultEmpty {
        trace!("[flush] path = {:?}, fh = {}", path, fh);
        return Ok(());
    }

    fn fsync(&self, _req: RequestInfo, path: &Path, fh: u64, _datasync: bool) -> ResultEmpty {
        trace!("[fsync] path = {:?}, fh = {}", path, fh);
        return Ok(());
    }

    fn fsyncdir(&self, _req: RequestInfo, path: &Path, fh: u64, _datasync: bool) -> ResultEmpty {
        trace!("[fsyncdir] path = {:?}, fh = {}", path, fh);
        return Ok(());
    }

    fn release(
        &self,
        _req: RequestInfo,
        path: &Path,
        fh: u64,
        flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> ResultEmpty {
        debug!(
            "[release] path = {:?}, fh = {}, flags = {:#06o}",
            path, fh, flags
        );

        match self.remove_fs_handle(fh) {
            Ok(_path) => Ok(()),
            Err(e) => {
                error!("[release]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        debug!("[opendir] path = {:?}, flags = {:#06o}", path, flags);

        match self.add_fs_handle(path) {
            Ok(fs_handle) => Ok((fs_handle, flags)),
            Err(e) => {
                error!("[opendir]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, fh: u64) -> ResultReaddir {
        debug!("[readdir] path = {:?}, fh = {}", path, fh);

        let mut directory_entries: Vec<DirectoryEntry> = vec![
            DirectoryEntry {
                name: std::ffi::OsString::from("."),
                kind: fuse_mt::FileType::Directory,
            },
            DirectoryEntry {
                name: std::ffi::OsString::from(".."),
                kind: fuse_mt::FileType::Directory,
            },
        ];
        match parse_fuse_path(&path) {
            Ok(parsed_path) => match parsed_path {
                Some((namespace_id, replica_path)) => {
                    let files_result = self.handle.block_on(async {
                        self.list_files(namespace_id, Some(replica_path.clone()))
                            .await
                    });
                    match files_result {
                        Ok(files) => match get_immediate_children(replica_path, files) {
                            Ok(immediate_children) => {
                                directory_entries.extend(immediate_children);
                                Ok(directory_entries)
                            }
                            Err(e) => {
                                error!("[readdir]: {}", e);
                                Err(libc::ENOSYS)
                            }
                        },
                        Err(e) => {
                            error!("[readdir]: {}", e);
                            Err(libc::ENOSYS)
                        }
                    }
                }
                None => {
                    let replicas_result =
                        self.handle.block_on(async { self.list_replicas().await });
                    match replicas_result {
                        Ok(replicas) => {
                            for (replica, _capability_kind) in replicas {
                                directory_entries.push(DirectoryEntry {
                                    name: replica.to_string().into(),
                                    kind: fuse_mt::FileType::Directory,
                                });
                            }
                            Ok(directory_entries)
                        }
                        Err(e) => {
                            error!("[readdir]: {}", e);
                            Err(libc::ENOSYS)
                        }
                    }
                }
            },
            Err(e) => {
                error!("[readdir]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn releasedir(&self, _req: RequestInfo, path: &Path, fh: u64, flags: u32) -> ResultEmpty {
        debug!(
            "[releasedir] path = {:?}, fh = {}, flags = {:#06o}",
            path, fh, flags
        );

        match self.remove_fs_handle(fh) {
            Ok(_path) => Ok(()),
            Err(e) => {
                error!("[releasedir]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn statfs(&self, _req: RequestInfo, path: &Path) -> ResultStatfs {
        trace!("[statfs] path = {:?}", path);

        let file_count = self.handle.block_on(async {
            let mut file_count = 0u64;
            if let Ok(replicas) = self.list_replicas().await {
                for (replica, _capability_kind) in replicas {
                    if let Ok(files) = self.list_files(replica, None).await {
                        file_count += files.len().try_into().unwrap_or(0);
                    }
                }
            }
            file_count
        });

        let statfs = Statfs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: file_count,
            ffree: 0,
            bsize: 0,
            namelen: 256,
            frsize: 0,
        };

        return Ok(statfs);
    }

    fn rmdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
        debug!("[rmdir] parent = {:?}, name = {:?}", parent, name);

        let path = parent.join(name);

        match parse_fuse_path(&path) {
            Ok(parsed_path) => match parsed_path {
                Some((namespace_id, replica_path)) => self.handle.block_on(async {
                    if replica_path == PathBuf::from("/") {
                        match self.delete_replica(namespace_id).await {
                            Ok(_) => {
                                info!("Replica {} deleted", namespace_id);
                                Ok(())
                            }
                            Err(e) => {
                                error!("[rmdir]: {}", e);
                                Err(libc::ENOSYS)
                            }
                        }
                    } else {
                        match self.delete_directory(namespace_id, replica_path).await {
                            Ok(entries_deleted) => {
                                info!("{} entries deleted in {:?}", entries_deleted, path);
                                Ok(())
                            }
                            Err(e) => {
                                error!("[rmdir]: {}", e);
                                Err(libc::ENOSYS)
                            }
                        }
                    }
                }),
                None => Err(libc::ENOSYS),
            },
            Err(e) => {
                error!("[rmdir]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn create(
        &self,
        _req: RequestInfo,
        parent: &Path,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> ResultCreate {
        debug!(
            "[create] parent = {:?}, name = {:?}, mode = {}, flags = {:#06o}",
            parent, name, mode, flags
        );

        let path = parent.join(name);

        match parse_fuse_path(&path) {
            Ok(parsed_path) => match parsed_path {
                Some((namespace_id, replica_path)) => self.handle.block_on(async {
                    match self
                        .create_or_modify_file(namespace_id, replica_path, b"\0".to_vec())
                        .await
                    {
                        Ok(file_hash) => {
                            info!("File created at {:?} with hash {}", path, file_hash);
                            match self.get_fs_entry_attributes(&path).await {
                                Ok(file_attr) => match self.add_fs_handle(&path) {
                                    Ok(file_handle) => Ok(CreatedEntry {
                                        ttl: std::time::Duration::from_secs(0),
                                        attr: file_attr,
                                        fh: file_handle,
                                        flags: flags,
                                    }),
                                    Err(e) => {
                                        error!("[create]: {}", e);
                                        Err(libc::ENOSYS)
                                    }
                                },
                                Err(e) => {
                                    error!("[create]: {}", e);
                                    Err(libc::ENOSYS)
                                }
                            }
                        }
                        Err(e) => {
                            error!("[create]: {}", e);
                            Err(libc::ENOSYS)
                        }
                    }
                }),
                None => Err(libc::ENOSYS),
            },
            Err(e) => {
                error!("[create]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn rename(
        &self,
        _req: RequestInfo,
        parent: &Path,
        name: &OsStr,
        newparent: &Path,
        newname: &OsStr,
    ) -> ResultEmpty {
        debug!(
            "[rename] parent = {:?}, name = {:?}, newparent = {:?}, newname = {:?}",
            parent, name, newparent, newname
        );

        let old_path = parent.join(name);
        let new_path = newparent.join(newname);

        match parse_fuse_path(&old_path) {
            Ok(parsed_path) => {
                match parsed_path {
                    Some((old_namespace_id, old_replica_path)) => {
                        self.handle.block_on(async {
                            match self.is_file_or_directory(&old_path).await {
                                Ok(fs_entry_type) => {
                                    match parse_fuse_path(&new_path) {
                                        Ok(parsed_path) => {
                                            match parsed_path {
                                                Some((new_namespace_id, new_replica_path)) => {
                                                    match fs_entry_type {
                                                        fuser::FileType::RegularFile => {
                                                            match self.move_file(old_namespace_id, old_replica_path, new_namespace_id, new_replica_path).await {
                                                                Ok(file_move_info) => {
                                                                    info!("File with hash {} moved from {:?} to {:?} ({} entries deleted)", file_move_info.0, old_path, new_path, file_move_info.1);
                                                                    Ok(())
                                                                },
                                                                Err(e) => {
                                                                    error!("[rename]: {}", e);
                                                                    Err(libc::ENOSYS)
                                                                }
                                                            }
                                                        },
                                                        fuser::FileType::Directory => {
                                                            match self.move_directory(old_namespace_id, old_replica_path, new_namespace_id, new_replica_path).await {
                                                                Ok(directory_move_info) => {
                                                                    info!("Directory moved from {:?} to {:?} ({} entries deleted, new file hashes: {:#?})", old_path, new_path, directory_move_info.1, directory_move_info.0);
                                                                    Ok(())
                                                                },
                                                                Err(e) => {
                                                                    error!("[rename]: {}", e);
                                                                    Err(libc::ENOSYS)
                                                                }
                                                            }
                                                        },
                                                        _ => Err(libc::ENOSYS)
                                                    }
                                                },
                                                None => Err(libc::ENOSYS)
                                            }
                                        },
                                        Err(e) => {
                                            error!("[rename]: {}", e);
                                            Err(libc::ENOSYS)
                                        }
                                    }
                                },
                                Err(e) => {
                                    error!("[rename]: {}", e);
                                    Err(libc::ENOSYS)
                                }
                            }
                        })
                    },
                    None => Err(libc::ENOSYS)
                }
            },
            Err(e) => {
                error!("[rename]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn access(&self, _req: RequestInfo, path: &Path, mask: u32) -> ResultEmpty {
        trace!("[access] path = {:?}, mask = {:#06o}", path, mask);

        let fs_entry_attr_result = self
            .handle
            .block_on(async { self.get_fs_entry_attributes(&path).await });
        match fs_entry_attr_result {
            Ok(fs_entry_attr) => {
                trace!("[access] permission: {:#06o}", fs_entry_attr.perm);
                trace!(
                    "[access] mask applied: {:#06o}",
                    fs_entry_attr.perm | mask.try_into().unwrap_or(u16::MIN)
                );
                if fs_entry_attr.perm | mask.try_into().unwrap_or(u16::MIN) != 0 {
                    Ok(())
                } else {
                    Err(libc::EACCES)
                }
            }
            Err(e) => {
                error!("[access]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn write(
        &self,
        _req: RequestInfo,
        path: &Path,
        fh: u64,
        offset: u64,
        data: Vec<u8>,
        flags: u32,
    ) -> ResultWrite {
        debug!(
            "[write] path = {:?}, fh = {}, offset = {}, data = {:#?}, flags = {:#06o}",
            path,
            fh,
            offset,
            std::str::from_utf8(&data),
            flags
        );

        match parse_fuse_path(&path) {
            Ok(parsed_path) => match parsed_path {
                Some((namespace_id, replica_path)) => self.handle.block_on(async {
                    match self.read_file(namespace_id, replica_path.clone()).await {
                        Ok(file_bytes) => {
                            let mut writer = BufWriter::new(Cursor::new(file_bytes.to_vec()));
                            match writer.seek(std::io::SeekFrom::Start(offset)) {
                                Ok(_new_position) => match writer.write(&data) {
                                    Ok(_) => match writer.into_inner() {
                                        Ok(inner_cursor) => {
                                            match self
                                                .create_or_modify_file(
                                                    namespace_id,
                                                    replica_path,
                                                    inner_cursor.into_inner(),
                                                )
                                                .await
                                            {
                                                Ok(file_hash) => {
                                                    info!(
                                                        "File at {:?} updated (hash: {})",
                                                        path, file_hash
                                                    );
                                                    Ok(data.len().try_into().unwrap_or(u32::MAX))
                                                }
                                                Err(e) => {
                                                    error!("[write]: {}", e);
                                                    Err(libc::ENOSYS)
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            error!("[write]: {}", e);
                                            Err(libc::ENOSYS)
                                        }
                                    },
                                    Err(e) => {
                                        error!("[write]: {}", e);
                                        Err(libc::ENOSYS)
                                    }
                                },
                                Err(e) => {
                                    error!("[write]: {}", e);
                                    Err(libc::ENOSYS)
                                }
                            }
                        }
                        Err(e) => {
                            error!("[write]: {}", e);
                            Err(libc::ENOSYS)
                        }
                    }
                }),
                None => Err(libc::ENOSYS),
            },
            Err(e) => {
                error!("[write]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn chmod(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, mode: u32) -> ResultEmpty {
        warn!(
            "[chmod] path = {:?}, fh = {:?}, mode = {:#06o}",
            path, fh, mode
        );

        Err(libc::ENOSYS)
    }

    fn chown(
        &self,
        _req: RequestInfo,
        path: &Path,
        fh: Option<u64>,
        uid: Option<u32>,
        gid: Option<u32>,
    ) -> ResultEmpty {
        warn!(
            "[chown] path = {:?}, fh = {:?}, uid = {:?}, gid = {:?}",
            path, fh, uid, gid
        );

        Err(libc::ENOSYS)
    }

    fn truncate(&self, _req: RequestInfo, path: &Path, fh: Option<u64>, size: u64) -> ResultEmpty {
        debug!(
            "[truncate] path = {:?}, fh = {:?}, size = {}",
            path, fh, size
        );

        match parse_fuse_path(&path) {
            Ok(parsed_path) => match parsed_path {
                Some((namespace_id, replica_path)) => self.handle.block_on(async {
                    match self.read_file(namespace_id, replica_path).await {
                        Ok(file_bytes) => match file_bytes.try_into_mut() {
                            Ok(mut file_bytes_mut) => {
                                file_bytes_mut.resize(size.try_into().unwrap_or(usize::MIN), 0);
                                Ok(())
                            }
                            Err(_bytes) => Err(libc::ENOSYS),
                        },
                        Err(e) => {
                            error!("[truncate]: {}", e);
                            Err(libc::ENOSYS)
                        }
                    }
                }),
                None => Err(libc::ENOSYS),
            },
            Err(e) => {
                error!("[truncate]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn utimens(
        &self,
        _req: RequestInfo,
        path: &Path,
        fh: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> ResultEmpty {
        warn!(
            "[utimens] path = {:?}, fh = {:?}, atime = {:?}, mtime = {:?}",
            path, fh, atime, mtime
        );

        Ok(())
    }

    fn utimens_macos(
        &self,
        _req: RequestInfo,
        path: &Path,
        fh: Option<u64>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> ResultEmpty {
        warn!(
            "[utimens_macos] path = {:?}, fh = {:?}, crtime = {:?}, chgtime = {:?}, bkuptime = {:?}, flags = {:?}",
            path,
            fh,
            crtime,
            chgtime,
            bkuptime,
            flags
        );

        Ok(())
    }

    fn readlink(&self, _req: RequestInfo, path: &Path) -> ResultData {
        warn!("[readlink] path = {:?}", path,);

        Err(libc::ENOSYS)
    }

    fn mknod(
        &self,
        _req: RequestInfo,
        parent: &Path,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> ResultEntry {
        warn!(
            "[mknod] parent = {:?}, name = {:?}, mode = {:#06o}, rdev = {:#06o}",
            parent, name, mode, rdev
        );

        Err(libc::ENOSYS)
    }

    fn unlink(&self, _req: RequestInfo, parent: &Path, name: &OsStr) -> ResultEmpty {
        debug!("[unlink] parent = {:?}, name = {:?}", parent, name);

        let path = parent.join(name);
        match parse_fuse_path(&path) {
            Ok(parsed_path) => match parsed_path {
                Some((namespace_id, replica_path)) => self.handle.block_on(async {
                    match self.delete_file(namespace_id, replica_path).await {
                        Ok(entries_deleted) => {
                            info!(
                                "File {:?} deleted ({} entries removed)",
                                path, entries_deleted
                            );
                            Ok(())
                        }
                        Err(e) => {
                            error!("[unlink]: {}", e);
                            Err(libc::ENOSYS)
                        }
                    }
                }),
                None => Err(libc::ENOSYS),
            },
            Err(e) => {
                error!("[unlink]: {}", e);
                Err(libc::ENOSYS)
            }
        }
    }

    fn symlink(
        &self,
        _req: RequestInfo,
        parent: &Path,
        name: &OsStr,
        target: &Path,
    ) -> ResultEntry {
        warn!(
            "[symlink] parent = {:?}, name = {:?}, target = {:?}",
            parent, name, target
        );

        Err(libc::ENOSYS)
    }

    fn link(
        &self,
        _req: RequestInfo,
        path: &Path,
        newparent: &Path,
        newname: &OsStr,
    ) -> ResultEntry {
        warn!(
            "[link] path = {:?}, newparent = {:?}, newname = {:?}",
            path, newparent, newname
        );

        Err(libc::ENOSYS)
    }

    fn setxattr(
        &self,
        _req: RequestInfo,
        path: &Path,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> ResultEmpty {
        warn!(
            "[setxattr] path = {:?}, name = {:?}, value = {:#?}, flags = {:#06o}, position = {}",
            path,
            name,
            std::str::from_utf8(&value),
            flags,
            position
        );

        Err(libc::ENOSYS)
    }

    fn getxattr(&self, _req: RequestInfo, path: &Path, name: &OsStr, size: u32) -> ResultXattr {
        warn!(
            "[getxattr] path = {:?}, name = {:?}, size = {}",
            path, name, size
        );

        Err(libc::ENOSYS)
    }

    fn listxattr(&self, _req: RequestInfo, path: &Path, size: u32) -> ResultXattr {
        warn!("[listxattr] path = {:?}, size = {}", path, size);

        Err(libc::ENOSYS)
    }

    fn removexattr(&self, _req: RequestInfo, path: &Path, name: &OsStr) -> ResultEmpty {
        warn!("[removexattr] path = {:?}, name = {:?}", path, name);

        Err(libc::ENOSYS)
    }

    #[cfg(target_os = "macos")]
    fn setvolname(&self, _req: RequestInfo, name: &OsStr) -> ResultEmpty {
        warn!("[setvolname] name = {:?}", name);

        Err(libc::ENOSYS)
    }

    #[cfg(target_os = "macos")]
    fn getxtimes(&self, _req: RequestInfo, path: &Path) -> ResultXTimes {
        warn!("[getxtimes] path = {:?}", path,);

        Err(libc::ENOSYS)
    }

    fn mkdir(&self, _req: RequestInfo, parent: &Path, name: &OsStr, mode: u32) -> ResultEntry {
        debug!(
            "[mkdir] parent = {:?}, name = {:?}, mode = {:#06o}",
            parent, name, mode
        );

        if parent == PathBuf::from("/") {
            self.handle.block_on(async {
                match self.create_replica().await {
                    Ok(namespace_id) => {
                        info!("Replica {} created", namespace_id);
                        match self
                            .get_fs_entry_attributes(&PathBuf::from(format!("/{}", namespace_id)))
                            .await
                        {
                            Ok(dir_attr) => Ok((std::time::Duration::from_secs(0), dir_attr)),
                            Err(e) => {
                                error!("[mkdir]: {}", e);
                                Err(libc::ENOSYS)
                            }
                        }
                    }
                    Err(e) => {
                        error!("[mkdir]: {}", e);
                        Err(libc::ENOSYS)
                    }
                }
            })
        } else {
            let path = parent.join(name);
            match parse_fuse_path(&path) {
                Ok(parsed_path) => match parsed_path {
                    Some((namespace_id, replica_path)) => self.handle.block_on(async {
                        // Folders must have a single entry (ie, must have at least one file); a hidden file (`.folder`) is necessary
                        match self
                            .create_or_modify_file(
                                namespace_id,
                                replica_path.join(".folder"),
                                b"\0".to_vec(),
                            )
                            .await
                        {
                            Ok(file_hash) => {
                                info!("Directory created at {:?} (hash: {})", path, file_hash);
                                match self.get_fs_entry_attributes(&path).await {
                                    Ok(dir_attr) => {
                                        Ok((std::time::Duration::from_secs(0), dir_attr))
                                    }
                                    Err(e) => {
                                        error!("[mkdir]: {}", e);
                                        Err(libc::ENOSYS)
                                    }
                                }
                            }
                            Err(e) => {
                                error!("[mkdir]: {}", e);
                                Err(libc::ENOSYS)
                            }
                        }
                    }),
                    None => Err(libc::ENOSYS),
                },
                Err(e) => {
                    error!("[mkdir]: {}", e);
                    Err(libc::ENOSYS)
                }
            }
        }
    }
}
