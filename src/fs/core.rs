use super::*;
use crate::discovery::{INITIAL_PUBLISH_DELAY, REPUBLISH_DELAY};
use bytes::Bytes;
#[cfg(feature = "fuse")]
use fuse_mt::spawn_mount;
use iroh::protocol::ProtocolHandler;
use iroh_blobs::store::Store;
use iroh_docs::Author;
use log::{error, info};
#[cfg(feature = "fuse")]
use miette::IntoDiagnostic;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
#[cfg(feature = "fuse")]
use std::collections::HashMap;
use std::path::PathBuf;
#[cfg(feature = "fuse")]
use std::sync::Arc;
#[cfg(feature = "fuse")]
use std::sync::RwLock;
#[cfg(feature = "fuse")]
use tokio::runtime::Handle;
use tokio::sync::watch::{self};

impl OkuFs {
    /// Obtain the private key of the node's authorship credentials.
    ///
    /// # Return
    ///
    /// The private key of the node's authorship credentials.
    pub async fn get_author(&self) -> anyhow::Result<Author> {
        let default_author_id = self.default_author().await;

        self.docs
            .client()
            .authors()
            .export(default_author_id)
            .await
            .ok()
            .flatten()
            .ok_or(anyhow::anyhow!(
                "Missing private key for default author ({}).",
                crate::fs::util::fmt_short(default_author_id)
            ))
    }

    /// Starts an instance of an Oku file system.
    /// In the background, an Iroh node is started if none is running, or is connected to if one is already running.
    ///
    /// # Arguments
    ///
    /// * `handle` - If compiling with the `fuse` feature, a Tokio runtime handle is required.
    ///
    /// # Returns
    ///
    /// A running instance of an Oku file system.
    pub async fn start(#[cfg(feature = "fuse")] handle: &Handle) -> anyhow::Result<Self> {
        let local_pool = Arc::new(iroh_blobs::util::local_pool::LocalPool::default());
        let endpoint = iroh::Endpoint::builder()
            .discovery_n0()
            .discovery_dht()
            .discovery_local_network()
            .bind()
            .await?;
        let blobs = iroh_blobs::net_protocol::Blobs::persistent(NODE_PATH.clone())
            .await?
            .build(local_pool.handle(), &endpoint);
        let gossip = iroh_gossip::net::Gossip::builder()
            .spawn(endpoint.clone())
            .await?;
        let docs = iroh_docs::protocol::Docs::persistent(NODE_PATH.clone())
            .spawn(&blobs, &gossip)
            .await?;

        let router = iroh::protocol::Router::builder(endpoint.clone())
            .accept(iroh_blobs::ALPN, blobs.clone())
            .accept(iroh_gossip::ALPN, gossip.clone())
            .accept(iroh_docs::ALPN, docs.clone())
            .spawn()
            .await?;
        info!(
            "Default author ID is {} … ",
            crate::fs::util::fmt_short(docs.client().authors().default().await.unwrap_or_default())
        );

        let (replica_sender, _replica_receiver) = watch::channel(());
        let (okunet_fetch_sender, _okunet_fetch_receiver) = watch::channel(false);

        let oku_fs = Self {
            local_pool,
            endpoint,
            blobs,
            docs,
            router,
            replica_sender,
            okunet_fetch_sender,
            #[cfg(feature = "fuse")]
            fs_handles: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "fuse")]
            newest_handle: Arc::new(RwLock::new(0)),
            #[cfg(feature = "fuse")]
            handle: handle.clone(),
            dht: mainline::Dht::server()?.as_async(),
        };
        let oku_fs_clone = oku_fs.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(INITIAL_PUBLISH_DELAY).await;
                match oku_fs_clone.announce_replicas().await {
                    Ok(_) => info!("Announced all replicas … "),
                    Err(e) => error!("{}", e),
                }
                match oku_fs_clone.refresh_users().await {
                    Ok(_) => info!("Refreshed OkuNet database … "),
                    Err(e) => error!("{}", e),
                }
                tokio::time::sleep(REPUBLISH_DELAY - INITIAL_PUBLISH_DELAY).await;
            }
        });
        Ok(oku_fs.clone())
    }

    /// Shuts down the Oku file system.
    pub async fn shutdown(self) {
        let _ = self.endpoint.close().await;
        let _ = self.router.shutdown().await;
        self.docs.shutdown().await;
        self.blobs.shutdown().await;
        self.blobs.store().shutdown().await;
        if let Some(local_pool) = Arc::into_inner(self.local_pool) {
            local_pool.shutdown().await
        }
    }

    /// Retrieve the content of a document entry.
    ///
    /// # Arguments
    ///
    /// * `entry` - An entry in an Iroh document.
    ///
    /// # Returns
    ///
    /// The content of the entry, as raw bytes.
    pub async fn content_bytes(&self, entry: &iroh_docs::Entry) -> anyhow::Result<Bytes> {
        self.content_bytes_by_hash(&entry.content_hash()).await
    }

    /// Retrieve the content of a document entry by its hash.
    ///
    /// # Arguments
    ///
    /// * `hash` - The content hash of an Iroh document.
    ///
    /// # Returns
    ///
    /// The content of the entry, as raw bytes.
    pub async fn content_bytes_by_hash(&self, hash: &iroh_blobs::Hash) -> anyhow::Result<Bytes> {
        self.blobs.client().read_to_bytes(*hash).await
    }

    /// Determines the oldest timestamp of a file entry in any replica stored locally.
    ///
    /// # Returns
    ///
    /// The oldest timestamp in any local replica, in microseconds from the Unix epoch.
    pub async fn get_oldest_timestamp(&self) -> miette::Result<u64> {
        let replicas = self.list_replicas().await?;
        let mut timestamps: Vec<u64> = Vec::new();
        for (replica, _capability_kind) in replicas {
            timestamps.push(
                self.get_oldest_timestamp_in_folder(&replica, &PathBuf::from("/"))
                    .await?,
            );
        }
        Ok(*timestamps.par_iter().min().unwrap_or(&u64::MIN))
    }

    /// Determines the latest timestamp of a file entry in any replica stored locally.
    ///
    /// # Returns
    ///
    /// The latest timestamp in any local replica, in microseconds from the Unix epoch.
    pub async fn get_newest_timestamp(&self) -> miette::Result<u64> {
        let replicas = self.list_replicas().await?;
        let mut timestamps: Vec<u64> = Vec::new();
        for (replica, _capability_kind) in replicas {
            timestamps.push(
                self.get_newest_timestamp_in_folder(&replica, &PathBuf::from("/"))
                    .await?,
            );
        }
        Ok(*timestamps.par_iter().max().unwrap_or(&u64::MIN))
    }

    /// Determines the size of the file system.
    ///
    /// # Returns
    ///
    /// The total size, in bytes, of the files in every replica stored locally.
    pub async fn get_size(&self) -> miette::Result<u64> {
        let replicas = self.list_replicas().await?;
        let mut size = 0;
        for (replica, _capability_kind) in replicas {
            size += self.get_folder_size(&replica, &PathBuf::from("/")).await?;
        }
        Ok(size)
    }

    #[cfg(feature = "fuse")]
    /// Mount the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the file system mount point.
    ///
    /// # Returns
    ///
    /// A handle referencing the mounted file system; joining or dropping the handle will unmount the file system and shutdown the node.
    pub fn mount(&self, path: PathBuf) -> miette::Result<fuser::BackgroundSession> {
        spawn_mount(fuse_mt::FuseMT::new(self.clone(), 1), path, &[]).into_diagnostic()
    }
}
