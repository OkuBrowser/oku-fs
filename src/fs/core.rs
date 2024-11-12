use super::*;
use crate::discovery::{INITIAL_PUBLISH_DELAY, REPUBLISH_DELAY};
use crate::error::OkuFsError;
use anyhow::anyhow;
#[cfg(feature = "fuse")]
use fuse_mt::spawn_mount;
use futures::StreamExt;
use iroh::docs::Author;
use iroh::net::discovery::dns::DnsDiscovery;
use iroh::net::discovery::pkarr::PkarrPublisher;
use iroh::{
    net::discovery::{ConcurrentDiscovery, Discovery},
    node::FsNode,
};
use log::{error, info};
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
        let default_author_id = self.node.authors().default().await?;
        self.node
            .authors()
            .export(default_author_id)
            .await?
            .ok_or(anyhow!(
                "Missing private key for default author ({}).",
                default_author_id.fmt_short()
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
    pub async fn start(#[cfg(feature = "fuse")] handle: &Handle) -> miette::Result<Self> {
        let (running_node, node) = match iroh::client::Iroh::connect_path(NODE_PATH.clone()).await {
            Ok(node) => (None, node),
            Err(e) => {
                error!("{}", e);
                let node = FsNode::persistent(&*NODE_PATH)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        OkuFsError::CannotStartNode
                    })?
                    .enable_docs()
                    .enable_rpc()
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        OkuFsError::CannotStartNode
                    })?
                    .spawn()
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        OkuFsError::CannotStartNode
                    })?;
                let node_addr = node.net().node_addr().await.map_err(|e| {
                    error!("{}", e);
                    OkuFsError::CannotRetrieveNodeAddress
                })?;
                let addr_info = node_addr.info;
                let magic_endpoint = node.endpoint();
                let secret_key = magic_endpoint.secret_key();
                let mut discovery_service = ConcurrentDiscovery::empty();
                let pkarr = PkarrPublisher::n0_dns(secret_key.clone());
                let dns = DnsDiscovery::n0_dns();
                discovery_service.add(pkarr);
                discovery_service.add(dns);
                discovery_service.publish(&addr_info);
                (Some(node.clone()), node.client().clone())
            }
        };
        let authors = node.authors().list().await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotRetrieveAuthors
        })?;
        futures::pin_mut!(authors);
        let authors_count = authors.as_mut().count().await.to_owned();
        let default_author_id = if authors_count == 0 {
            let new_author_id = node.authors().create().await.map_err(|e| {
                error!("{}", e);
                OkuFsError::AuthorCannotBeCreated
            })?;
            node.authors()
                .set_default(new_author_id)
                .await
                .map_err(|e| {
                    error!("{}", e);
                    OkuFsError::AuthorCannotBeCreated
                })?;
            new_author_id
        } else {
            node.authors().default().await.map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotRetrieveDefaultAuthor
            })?
        };
        info!("Default author ID is {} … ", default_author_id.fmt_short());

        let (replica_sender, _replica_receiver) = watch::channel(());

        let oku_fs = Self {
            running_node,
            node,
            replica_sender,
            #[cfg(feature = "fuse")]
            fs_handles: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "fuse")]
            newest_handle: Arc::new(RwLock::new(0)),
            #[cfg(feature = "fuse")]
            handle: handle.clone(),
            dht: mainline::Dht::server().into_diagnostic()?.as_async(),
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
    pub async fn shutdown(self) -> miette::Result<()> {
        match self.running_node {
            Some(running_node) => Ok(running_node.shutdown().await.map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotStopNode
            })?),
            None => Ok(self.node.shutdown(false).await.map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotStopNode
            })?),
        }
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
                self.get_oldest_timestamp_in_folder(replica, PathBuf::from("/"))
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
                self.get_newest_timestamp_in_folder(replica, PathBuf::from("/"))
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
            size += self.get_folder_size(replica, PathBuf::from("/")).await?;
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
        Ok(spawn_mount(fuse_mt::FuseMT::new(self.clone(), 1), path, &[]).into_diagnostic()?)
    }
}
