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
}

#[derive(Error, Debug, Diagnostic)]
/// Content discovery errors.
pub enum OkuDiscoveryError {
    #[error("Invalid hash and format.")]
    #[diagnostic(code(discovery::invalid_hash_and_format), url(docsrs))]
    /// Invalid hash and format.
    InvalidHashAndFormat,
    #[error("Unable to find nodes able to satisfy query.")]
    #[diagnostic(code(discovery::no_nodes_found), url(docsrs))]
    /// Unable to find nodes able to satisfy query.
    NoNodesFound,
    #[error("Unsupported protocol identifier: {0}")]
    #[diagnostic(code(discovery::unsupported_alpn), url(docsrs))]
    /// Unsupported protocol identifier.
    UnsupportedALPN(String),
}
