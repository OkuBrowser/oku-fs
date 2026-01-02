use crate::fs::OkuFs;
use easy_fuser::prelude::*;
#[cfg(target_os = "macos")]
use log::debug;
use log::error;
use log::info;
use log::trace;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::path::PathBuf;

impl FuseHandler<PathBuf> for OkuFs {
    fn get_inner(&self) -> &dyn FuseHandler<PathBuf> {
        &**self.fuse_handler
    }

    fn destroy(&self) {
        self.handle
            .block_on(async move { self.clone().shutdown().await });
        info!("Node unmounting and shutting down … ");
    }

    fn statfs(&self, _req: &RequestInfo, file_id: PathBuf) -> FuseResult<StatFs> {
        trace!("[statfs] file_id = {file_id:?}");
        self.handle
            .block_on(async { self.get_fs_entry_stats(&file_id).await })
            .map_err(|e| {
                error!("[statfs]: {e}");
                PosixError::new(ErrorKind::FileNotFound, e.to_string())
            })
    }

    fn getattr(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        file_handle: Option<BorrowedFileHandle>,
    ) -> FuseResult<FileAttribute> {
        debug!("[getattr] file_id = {file_id:?}, file_handle = {file_handle:?}");
        self.getattr(file_id).map_err(|e| {
            error!("[getattr]: {e}");
            PosixError::new(ErrorKind::FileNotFound, e.to_string())
        })
    }

    fn read(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        file_handle: BorrowedFileHandle,
        seek: SeekFrom,
        size: u32,
        _flags: FUSEOpenFlags,
        _lock_owner: Option<u64>,
    ) -> FuseResult<Vec<u8>> {
        debug!(
            "[read] file_id = {file_id:?}, file_handle = {file_handle:?}, seek = {seek:?}, size = {size}"
        );
        self.read(file_id, seek).map_err(|e| {
            error!("[read]: {e}");
            PosixError::new(ErrorKind::FileNotFound, e.to_string())
        })
    }

    fn readdir(
        &self,
        _req: &RequestInfo,
        file_id: PathBuf,
        file_handle: BorrowedFileHandle,
    ) -> FuseResult<Vec<(OsString, <PathBuf as FileIdType>::MinimalMetadata)>> {
        debug!("[readdir] file_id = {file_id:?}, file_handle = {file_handle:?}");
        self.readdir(file_id).map_err(|e| {
            error!("[readdir]: {e}");
            PosixError::new(ErrorKind::FileNotFound, e.to_string())
        })
    }

    fn rmdir(&self, _req: &RequestInfo, parent_id: PathBuf, name: &OsStr) -> FuseResult<()> {
        debug!("[rmdir] parent_id = {parent_id:?}, name = {name:?}");
        self.rmdir(parent_id, name).map_err(|e| {
            error!("[rmdir]: {e}");
            PosixError::new(ErrorKind::FileNotFound, e.to_string())
        })
    }

    fn create(
        &self,
        req: &RequestInfo,
        parent_id: PathBuf,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: OpenFlags,
    ) -> FuseResult<(
        OwnedFileHandle,
        <PathBuf as FileIdType>::Metadata,
        FUSEOpenResponseFlags,
    )> {
        debug!("[create] parent_id = {parent_id:?}, name = {name:?}, mode = {mode}, umask = {umask}, flags = {flags:?}");
        self.create(req, parent_id, name, mode, umask, flags)
            .map_err(|e| {
                error!("[create]: {e}");
                PosixError::new(ErrorKind::FileNotFound, e.to_string())
            })
    }
}
