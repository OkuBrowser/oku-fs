#![doc = include_str!("../README.md")]
#![feature(doc_auto_cfg)]
#![warn(missing_docs)]

/// Content discovery and retrieval.
pub mod discovery;
/// Errors originating in the Oku file system implementation.
pub mod error;
/// An instance of an Oku file system.
pub mod fs;

/// FUSE implementation.
#[cfg(feature = "fuse")]
pub mod fuse;
