#![doc = include_str!("../README.md")]
// #![feature(doc_auto_cfg)]
#![warn(missing_docs)]

/// Configuration of an Oku file system node.
pub(crate) mod config;
/// Database used by the Oku file system.
pub mod database;
/// Content discovery and retrieval.
pub mod discovery;
/// Errors originating in the Oku file system implementation.
pub mod error;
/// An instance of an Oku file system.
pub mod fs;
#[cfg(feature = "fuse")]
/// FUSE implementation.
pub mod fuse;
/// Authorisation utilities.
pub mod ucan;

#[cfg(feature = "fuse")]
pub use fuser;
pub use iroh;
