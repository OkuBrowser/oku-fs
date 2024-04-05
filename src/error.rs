use miette::Diagnostic;
use thiserror::Error;

#[derive(Error, Debug, Diagnostic)]
/// File system errors.
pub enum OkuFsError {
    #[error("File system entry not found.")]
    #[diagnostic(
        code(fs::fs_entry_not_found),
        url(docsrs),
        help("Please ensure that the file system entry exists before attempting to read it.")
    )]
    /// File system entry not found.
    FsEntryNotFound,
    #[error("File has no name.")]
    #[diagnostic(
        code(fs::file_no_name),
        url(docsrs),
        help("Please ensure that the file you are trying to create has a name.")
    )]
    /// File has no name.
    FileNoName,
    #[error("Cannot get lock on file system entry.")]
    #[diagnostic(code(fs::cannot_get_lock), url(docsrs))]
    /// Cannot get lock on file system entry.
    CannotGetLock,
    #[error("A file already exists at this path.")]
    #[diagnostic(code(fs::file_already_exists), url(docsrs))]
    /// A file already exists at this path.
    FileAlreadyExists,
    #[error("A directory already exists at this path.")]
    #[diagnostic(code(fs::directory_already_exists), url(docsrs))]
    /// A directory already exists at this path.
    DirectoryAlreadyExists,
    #[error("Cannot remove a directory that has children.")]
    #[diagnostic(code(fs::directory_not_empty), url(docsrs))]
    /// Cannot remove a directory that has children.
    DirectoryNotEmpty,
    #[error("File system root has not been loaded.")]
    #[diagnostic(code(fs::root_not_loaded), url(docsrs))]
    /// File system root has not been loaded.
    RootNotLoaded,
}

#[derive(Error, Debug, Diagnostic)]
pub enum OkuDiscoveryError {
    #[error("Invalid hash and format.")]
    #[diagnostic(code(discovery::invalid_hash_and_format), url(docsrs))]
    InvalidHashAndFormat,
    #[error("Unable to discover node address for node ID.")]
    #[diagnostic(code(discovery::node_address_discovery_failed), url(docsrs))]
    NodeAddressDiscoveryFailed,
    #[error("Unable to find nodes able to satisfy query")]
    #[diagnostic(code(discovery::no_nodes_found), url(docsrs))]
    NoNodesFound,
    #[error("Unsupported protocol identifier: {0}")]
    #[diagnostic(code(discovery::unsupported_alpn), url(docsrs))]
    UnsupportedALPN(String),
}
