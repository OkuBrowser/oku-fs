use bytes::Bytes;
use iroh_docs::DocTicket;
use miette::IntoDiagnostic;
use path_clean::PathClean;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use std::ffi::CString;
use std::path::PathBuf;

pub(super) fn normalise_path(path: PathBuf) -> PathBuf {
    PathBuf::from("/").join(path).clean()
}

/// Converts a path to a key for an entry in a file system replica.
///
/// # Arguments
///
/// * `path` - The path to convert to a key.
///
/// # Returns
///
/// A null-terminated byte string representing the path.
pub fn path_to_entry_key(path: PathBuf) -> Bytes {
    let path = normalise_path(path.clone());
    let mut path_bytes = path.into_os_string().into_encoded_bytes();
    path_bytes.push(b'\0');
    path_bytes.into()
}

/// Converts a key of a replica entry into a path within a replica.
///
/// # Arguments
///
/// * `key` - The replica entry key, being a null-terminated byte string.
///
/// # Returns
///
/// A path pointing to the file with the key.
pub fn entry_key_to_path(key: &[u8]) -> miette::Result<PathBuf> {
    Ok(PathBuf::from(
        CString::from_vec_with_nul(key.to_vec())
            .into_diagnostic()?
            .into_string()
            .into_diagnostic()?,
    ))
}

/// Converts a path to a key prefix for entries in a file system replica.
///
/// # Arguments
///
/// * `path` - The path to convert to a key prefix.
///
/// # Returns
///
/// A byte string representing the path, without a null byte at the end.
pub fn path_to_entry_prefix(path: PathBuf) -> Bytes {
    let path = normalise_path(path.clone());
    let path_bytes = path.into_os_string().into_encoded_bytes();
    path_bytes.into()
}

/// Merge multiple tickets into one, returning `None` if no tickets were given.
///
/// # Arguments
///
/// * `tickets` - A vector of tickets to merge.
///
/// # Returns
///
/// `None` if no tickets were given, or a ticket with a merged capability and merged list of nodes.
pub fn merge_tickets(tickets: Vec<DocTicket>) -> Option<DocTicket> {
    let ticket_parts: Vec<_> = tickets
        .par_iter()
        .map(|ticket| ticket.capability.clone())
        .zip(tickets.par_iter().map(|ticket| ticket.nodes.clone()))
        .collect();
    ticket_parts
        .into_iter()
        .reduce(|mut merged_tickets, next_ticket| {
            let _ = merged_tickets.0.merge(next_ticket.0);
            merged_tickets.1.extend_from_slice(&next_ticket.1);
            merged_tickets
        })
        .map(|mut merged_tickets| {
            merged_tickets.1.sort_unstable();
            merged_tickets.1.dedup();
            DocTicket {
                capability: merged_tickets.0,
                nodes: merged_tickets.1,
            }
        })
}
