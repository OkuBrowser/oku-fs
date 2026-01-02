use crate::fs::OkuFs;
use crate::fuse::util::*;
use easy_fuser::prelude::FileKind::Directory;
use easy_fuser::prelude::*;
use log::info;
use miette::IntoDiagnostic;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::io::Read;
use std::io::Seek;
use std::path::PathBuf;

impl OkuFs {
    pub(super) fn getattr(&self, file_id: PathBuf) -> miette::Result<FileAttribute> {
        // Potential improvement: spawn a new thread to block on.
        self.handle
            .block_on(async { self.get_fs_entry_attributes(&file_id).await })
    }

    pub(super) fn read(&self, file_id: PathBuf, seek: SeekFrom) -> miette::Result<Vec<u8>> {
        let (namespace_id, replica_path) = parse_fuse_path(&file_id)
            .map(|x| x.ok_or(miette::miette!("Cannot read root directory as file")))??;
        let mut bytes = std::io::Cursor::new(
            self.handle
                .block_on(async { self.read_file(&namespace_id, &replica_path).await })?,
        );
        let mut buf = Vec::new();
        bytes.seek(seek).into_diagnostic()?;
        bytes.read_to_end(&mut buf).into_diagnostic()?;
        Ok(buf)
    }

    pub(super) fn readdir(
        &self,
        file_id: PathBuf,
    ) -> miette::Result<Vec<(OsString, <PathBuf as FileIdType>::MinimalMetadata)>> {
        let mut directory_entries: Vec<(OsString, <PathBuf as FileIdType>::MinimalMetadata)> = vec![
            (std::ffi::OsString::from("."), Directory),
            (std::ffi::OsString::from(".."), Directory),
        ];
        let parsed_path = parse_fuse_path(&file_id)?;
        match parsed_path {
            None => {
                let replicas = self.handle.block_on(async { self.list_replicas().await })?;
                for (replica, _capability_kind) in replicas {
                    directory_entries.push((crate::fs::util::fmt(replica).into(), Directory));
                }
                Ok(directory_entries)
            }
            Some((namespace_id, replica_path)) => {
                let files = self.handle.block_on(async {
                    self.list_files(&namespace_id, &Some(replica_path.clone()))
                        .await
                })?;
                let immediate_children = get_immediate_children(replica_path, files)?;
                directory_entries.extend(immediate_children);
                Ok(directory_entries)
            }
        }
    }

    pub(super) fn rmdir(&self, parent_id: PathBuf, name: &OsStr) -> miette::Result<()> {
        let path = parent_id.join(name);
        let (namespace_id, replica_path) = parse_fuse_path(&path)
            .map(|x| x.ok_or(miette::miette!("Cannot remove root directory")))??;
        match replica_path == PathBuf::from("/") {
            true => {
                self.handle
                    .block_on(async { self.delete_replica(&namespace_id).await })?;
                info!("Replica {namespace_id} deleted");
            }
            false => {
                let entries_deleted = self.handle.block_on(async {
                    self.delete_directory(&namespace_id, &replica_path).await
                })?;
                info!("{entries_deleted} entries deleted in {path:?}");
            }
        }
        Ok(())
    }

    pub(super) fn create(
        &self,
        _req: &RequestInfo,
        parent_id: PathBuf,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: OpenFlags,
    ) -> miette::Result<(
        OwnedFileHandle,
        <PathBuf as FileIdType>::Metadata,
        FUSEOpenResponseFlags,
    )> {
        let path = parent_id.join(name);
        let (namespace_id, replica_path) = parse_fuse_path(&path)
            .map(|x| x.ok_or(miette::miette!("Cannot create file at root path")))??;
        let file_hash = self.handle.block_on(async {
            self.create_or_modify_file(&namespace_id, &replica_path, b"\0".as_slice())
                .await
        })?;
        info!("File created at {path:?} with hash {file_hash}");
        let file_attr = self.getattr(path)?;
        Ok((
            unsafe { OwnedFileHandle::from_raw(0) },
            file_attr,
            FUSEOpenResponseFlags::empty(),
        ))
    }
}
