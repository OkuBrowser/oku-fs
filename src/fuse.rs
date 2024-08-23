use crate::error::OkuFuseError;
use crate::fs::OkuFs;
use chrono::TimeZone;
use fuse_mt::CallbackResult;
use fuse_mt::DirectoryEntry;
use fuse_mt::FileAttr;
use fuse_mt::FilesystemMT;
use fuse_mt::RequestInfo;
use fuse_mt::ResultEmpty;
use fuse_mt::ResultEntry;
use fuse_mt::ResultOpen;
use fuse_mt::ResultReaddir;
use fuse_mt::ResultSlice;
use fuse_mt::ResultStatfs;
use fuse_mt::Statfs;
use iroh::docs::NamespaceId;
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
            let namespace_id =
                NamespaceId::from_str(replica_id.as_os_str().to_str().unwrap_or_default())
                    .map_err(|_e| OkuFuseError::NoRoot)?;
            let replica_path = components.as_path().to_path_buf();
            return Ok(Some((namespace_id, replica_path)));
        } else {
            return Ok(None);
        }
    }
    Err(OkuFuseError::NoRoot.into())
}

impl OkuFs {
    pub fn fs_handle_to_path(&self, fh: u64) -> miette::Result<Option<PathBuf>> {
        match self.fs_handles.read() {
            Ok(fs_handles) => {
                let path = fs_handles.get(&fh).map(|p| p.to_path_buf());
                Ok(path)
            }
            Err(_e) => Err(OkuFuseError::NoFileWithHandle(fh).into()),
        }
    }
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
    pub fn remove_fs_handle(&self, fh: u64) -> miette::Result<Option<PathBuf>> {
        match self.fs_handles.write() {
            Ok(mut fs_handles) => Ok(fs_handles.remove(&fh)),
            Err(_e) => Err(OkuFuseError::FsHandlesFailedUpdate.into()),
        }
    }
    // pub async fn get_file_entry(&self, path: &Path) -> miette::Result<Entry> {
    //     let parsed_path = parse_fuse_path(path)?;
    //     if let Some((namespace_id, replica_path)) = parsed_path {
    //         Ok(self.get_entry(namespace_id, replica_path.clone()).await?)
    //     } else {
    //         Err(OkuFuseError::NoFileAtPath(path.to_path_buf()).into())
    //     }
    // }
    pub async fn get_file_attributes(&self, path: &Path) -> miette::Result<FileAttr> {
        let parsed_path = parse_fuse_path(path)?;
        if let Some((namespace_id, replica_path)) = parsed_path {
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
                mtime: SystemTime::from(chrono::Utc.timestamp_nanos(file_entry.timestamp() as i64)),
                ctime: estimated_creation_time,
                crtime: estimated_creation_time,
                kind: fuse_mt::FileType::RegularFile,
                perm: 0,
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

impl FilesystemMT for OkuFs {
    fn init(&self, _req: RequestInfo) -> ResultEmpty {
        println!("init() called");
        return Ok(());
    }

    fn destroy(&self) {
        let _ = self
            .handle
            .block_on(async move { self.clone().shutdown().await });
    }

    fn getattr(&self, _req: RequestInfo, path: &Path, fh: Option<u64>) -> ResultEntry {
        println!("[getattr] path = {:?}, fh = {:?}", path, fh);
        // Potential improvement: spawn a new thread to block on.
        let file_attr_result = self
            .handle
            .block_on(async { self.get_file_attributes(&path).await });
        match file_attr_result {
            Ok(file_attr) => Ok((std::time::Duration::from_secs(1), file_attr)),
            Err(_e) => Err(libc::ENOSYS),
        }
    }

    fn open(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        println!("[open] path = {:?}, flags = {}", path, flags);
        match self.add_fs_handle(path) {
            Ok(fs_handle) => Ok((fs_handle, flags)),
            Err(_e) => Err(libc::ENOSYS),
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
        println!(
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
                        Err(_e) => callback(Err(libc::ENOSYS)),
                    }
                }
                None => callback(Err(libc::ENOSYS)),
            },
            Err(_e) => callback(Err(libc::ENOSYS)),
        }
    }

    fn flush(&self, _req: RequestInfo, path: &Path, fh: u64, _lock_owner: u64) -> ResultEmpty {
        println!("[flush] path = {:?}, fh = {}", path, fh);
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
        println!(
            "[release] path = {:?}, fh = {}, flags = {}",
            path, fh, flags
        );

        if let Ok(_path) = self.remove_fs_handle(fh) {
            Ok(())
        } else {
            Err(libc::ENOSYS)
        }
    }

    fn opendir(&self, _req: RequestInfo, path: &Path, flags: u32) -> ResultOpen {
        println!("[opendir] path = {:?}, flags = {}", path, flags);
        match self.add_fs_handle(path) {
            Ok(fs_handle) => Ok((fs_handle, flags)),
            Err(_e) => Err(libc::ENOSYS),
        }
    }

    fn readdir(&self, _req: RequestInfo, path: &Path, fh: u64) -> ResultReaddir {
        println!("[readdir] path = {:?}, fh = {}", path, fh);

        match parse_fuse_path(&path) {
            Ok(parsed_path) => match parsed_path {
                Some((namespace_id, replica_path)) => {
                    let files_result = self.handle.block_on(async {
                        self.list_files(namespace_id, Some(replica_path)).await
                    });
                    match files_result {
                        Ok(files) => {
                            let mut directory_entries: Vec<DirectoryEntry> = Vec::new();
                            // This says all descending files are the immediate children of this directory.
                            // TODO: Figure out what the subdirectories of a directory are.
                            for file in files {
                                directory_entries.push(DirectoryEntry {
                                    name: PathBuf::from(
                                        std::str::from_utf8(file.key()).unwrap_or_default(),
                                    )
                                    .file_name()
                                    .unwrap_or_default()
                                    .to_os_string(),
                                    kind: fuse_mt::FileType::RegularFile,
                                });
                            }
                            Ok(directory_entries)
                        }
                        Err(_e) => Err(libc::ENOSYS),
                    }
                }
                None => {
                    let replicas_result =
                        self.handle.block_on(async { self.list_replicas().await });
                    match replicas_result {
                        Ok(replicas) => {
                            let mut directory_entries: Vec<DirectoryEntry> = Vec::new();
                            for replica in replicas {
                                directory_entries.push(DirectoryEntry {
                                    name: replica.to_string().into(),
                                    kind: fuse_mt::FileType::Directory,
                                });
                            }
                            Ok(directory_entries)
                        }
                        Err(_e) => Err(libc::ENOSYS),
                    }
                }
            },
            Err(_e) => Err(libc::ENOSYS),
        }
    }

    fn releasedir(&self, _req: RequestInfo, path: &Path, fh: u64, flags: u32) -> ResultEmpty {
        println!(
            "[releasedir] path = {:?}, fh = {}, flags = {}",
            path, fh, flags
        );

        if let Ok(_path) = self.remove_fs_handle(fh) {
            Ok(())
        } else {
            Err(libc::ENOSYS)
        }
    }

    fn statfs(&self, _req: RequestInfo, path: &Path) -> ResultStatfs {
        println!("[statfs] path = {:?}", path);

        let file_count = self.handle.block_on(async {
            let mut file_count = 0u64;
            if let Ok(replicas) = self.list_replicas().await {
                for replica in replicas {
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
}
