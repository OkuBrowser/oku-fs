use iroh_blobs::net_protocol::Blobs;
use iroh_blobs::util::local_pool::LocalPool;
use iroh_docs::protocol::Docs;
#[cfg(feature = "fuse")]
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
#[cfg(feature = "fuse")]
use std::sync::RwLock;
#[cfg(feature = "fuse")]
use tokio::runtime::Handle;
use tokio::sync::watch::Sender;

/// Core functionality of an Oku file system.
pub mod core;
/// Directory-related functionality of an Oku file system.
pub mod directory;
/// File-related functionality of an Oku file system.
pub mod file;
/// Implementation of OkuNet.
pub mod net;
/// Replica-related functionality of an Oku file system.
pub mod replica;
/// Useful functions for implementing the Oku file system.
pub mod util;

/// The path on disk where the file system is stored.
pub const FS_PATH: &str = ".oku";
pub(crate) static NODE_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| PathBuf::from(FS_PATH).join("node"));

/// An instance of an Oku file system.
///
/// The `OkuFs` struct is the primary interface for interacting with an Oku file system.
#[derive(Clone, Debug)]
pub struct OkuFs {
    pub(crate) local_pool: Arc<LocalPool>,
    pub(crate) endpoint: iroh::Endpoint,
    pub(crate) blobs: Blobs<iroh_blobs::store::fs::Store>,
    pub(crate) docs: Docs<iroh_blobs::store::fs::Store>,
    pub(crate) router: iroh::protocol::Router,
    /// An Iroh node responsible for storing replicas on the local machine, as well as joining swarms to fetch replicas from other nodes.
    /// A watcher for when replicas are created, deleted, or imported.
    pub replica_sender: Sender<()>,
    /// A watcher for whether or not content is being fetched from the OkuNet.
    pub okunet_fetch_sender: Sender<bool>,
    #[cfg(feature = "fuse")]
    /// The handles pointing to paths within the file system; used by FUSE.
    pub(crate) fs_handles: Arc<RwLock<HashMap<u64, PathBuf>>>,
    #[cfg(feature = "fuse")]
    /// The latest file system handle created.
    pub(crate) newest_handle: Arc<RwLock<u64>>,
    #[cfg(feature = "fuse")]
    /// A Tokio runtime handle to perform asynchronous operations with.
    pub(crate) handle: Handle,
    pub(crate) dht: mainline::async_dht::AsyncDht,
}
