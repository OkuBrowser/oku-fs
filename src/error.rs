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
