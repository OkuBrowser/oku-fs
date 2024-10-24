use iroh::client::Iroh;
use iroh::node::FsNode;
#[cfg(feature = "fuse")]
use std::collections::HashMap;
use std::path::PathBuf;
#[cfg(feature = "fuse")]
use std::sync::Arc;
use std::sync::LazyLock;
#[cfg(feature = "fuse")]
use std::sync::RwLock;
#[cfg(feature = "fuse")]
use tokio::runtime::Handle;
use tokio::sync::watch::Sender;

/// Core functionality of an Oku file system.
mod core;
/// Directory-related functionality of an Oku file system.
mod directory;
/// File-related functionality of an Oku file system.
mod file;
/// Implementation of OkuNet.
mod net;
/// Replica-related functionality of an Oku file system.
mod replica;
/// Useful functions for implementing the Oku file system.
mod util;
#[allow(unused_imports)]
pub use self::core::*;
#[allow(unused_imports)]
pub use self::directory::*;
#[allow(unused_imports)]
pub use self::file::*;
#[allow(unused_imports)]
pub use self::net::*;
#[allow(unused_imports)]
pub use self::replica::*;
#[allow(unused_imports)]
pub use self::util::*;

/// The path on disk where the file system is stored.
pub const FS_PATH: &str = ".oku";
pub(crate) static NODE_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| PathBuf::from(FS_PATH).join("node"));

/// An instance of an Oku file system.
///
/// The `OkuFs` struct is the primary interface for interacting with an Oku file system.
#[derive(Clone, Debug)]
pub struct OkuFs {
    running_node: Option<FsNode>,
    /// An Iroh node responsible for storing replicas on the local machine, as well as joining swarms to fetch replicas from other nodes.
    pub(crate) node: Iroh,
    /// A watcher for when replicas are created, deleted, or imported.
    pub replica_sender: Sender<()>,
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
