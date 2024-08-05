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
    #[error("Problem announcing {0} ({1}).")]
    #[diagnostic(code(discovery::problem_announcing_content), url(docsrs))]
    /// Problem announcing content.
    ProblemAnnouncingContent(String, String),
}

#[derive(Error, Debug, Diagnostic)]
/// Relay errors.
pub enum OkuRelayError {
    #[error("No connected node can satisfy {0}.")]
    #[diagnostic(code(relay::cannot_satisfy_request), url(docsrs))]
    /// No connected node can satisfy request.
    CannotSatisfyRequest(String),
    #[error("Problem connecting to {0}.")]
    #[diagnostic(code(relay::problem_connecting), url(docsrs))]
    /// Problem connecting to node.
    ProblemConnecting(String),
}
