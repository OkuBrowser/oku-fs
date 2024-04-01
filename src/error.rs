use miette::Diagnostic;
use thiserror::Error;

#[derive(Error, Debug, Diagnostic)]
pub enum OkuFsError {
    #[error("File system entry not found.")]
    #[diagnostic(
        code(fs::fs_entry_not_found),
        url(docsrs),
        help("Please ensure that the file system entry exists before attempting to read it.")
    )]
    FsEntryNotFound,
    #[error("File has no name.")]
    #[diagnostic(
        code(fs::file_no_name),
        url(docsrs),
        help("Please ensure that the file you are trying to create has a name.")
    )]
    FileNoName,
    #[error("Cannot get lock on file system entry.")]
    #[diagnostic(code(fs::cannot_get_lock), url(docsrs))]
    CannotGetLock,
    #[error("A file already exists at this path.")]
    #[diagnostic(code(fs::file_already_exists), url(docsrs))]
    FileAlreadyExists,
    #[error("A directory already exists at this path.")]
    #[diagnostic(code(fs::directory_already_exists), url(docsrs))]
    DirectoryAlreadyExists,
    #[error("Cannot delete a directory that has children.")]
    #[diagnostic(code(fs::directory_not_empty), url(docsrs))]
    DirectoryNotEmpty,
}
