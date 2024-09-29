use crate::config::OkuFsConfig;
use crate::discovery::{announce_replica, INITIAL_PUBLISH_DELAY, REPUBLISH_DELAY};
use crate::discovery::{
    PeerContentRequest, PeerContentResponse, PeerTicketResponse, DISCOVERY_PORT,
};
use crate::error::{OkuDiscoveryError, OkuFuseError};
use crate::{discovery::ContentRequest, error::OkuFsError};
use anyhow::anyhow;
use async_recursion::async_recursion;
use bytes::Bytes;
#[cfg(feature = "fuse")]
use fuse_mt::spawn_mount;
use futures::{pin_mut, StreamExt};
use iroh::base::node_addr::AddrInfoOptions;
use iroh::client::docs::Entry;
use iroh::client::docs::LiveEvent::SyncFinished;
use iroh::client::Iroh;
use iroh::docs::store::FilterKind;
use iroh::docs::{CapabilityKind, DocTicket};
use iroh::net::discovery::dns::DnsDiscovery;
use iroh::net::discovery::pkarr::PkarrPublisher;
use iroh::{
    base::hash::Hash,
    client::docs::ShareMode,
    docs::NamespaceId,
    net::discovery::{ConcurrentDiscovery, Discovery},
    node::FsNode,
};
use iroh_mainline_content_discovery::protocol::{Query, QueryFlags};
use iroh_mainline_content_discovery::to_infohash;
use log::{error, info};
use miette::IntoDiagnostic;
use path_clean::PathClean;
#[cfg(feature = "fuse")]
use std::collections::HashMap;
use std::ffi::CString;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, ToSocketAddrs};
use std::sync::Arc;
#[cfg(feature = "fuse")]
use std::sync::RwLock;
use std::{error::Error, path::PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
#[cfg(feature = "fuse")]
use tokio::runtime::Handle;
use tokio::sync::watch::{self, Sender};
use tokio::sync::Mutex;

/// The path on disk where the file system is stored.
pub const FS_PATH: &str = ".oku";

/// The protocol identifier for exchanging document tickets.
pub const ALPN_DOCUMENT_TICKET_FETCH: &[u8] = b"oku/document-ticket/fetch/v0";

/// The protocol identifier for initially connecting to relays.
pub const ALPN_INITIAL_RELAY_CONNECTION: &[u8] = b"oku/relay/connect/v0";

/// The protocol identifier for fetching its list of replicas.
pub const ALPN_RELAY_FETCH: &[u8] = b"oku/relay/fetch/v0";

fn normalise_path(path: PathBuf) -> PathBuf {
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
    let ticket_parts = tickets
        .iter()
        .map(|ticket| ticket.capability.clone())
        .zip(tickets.iter().map(|ticket| ticket.nodes.clone()));
    ticket_parts
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

/// An instance of an Oku file system.
///
/// The `OkuFs` struct is the primary interface for interacting with an Oku file system.
#[derive(Clone, Debug)]
pub struct OkuFs {
    running_node: Option<FsNode>,
    /// An Iroh node responsible for storing replicas on the local machine, as well as joining swarms to fetch replicas from other nodes.
    pub(crate) node: Iroh,
    /// The configuration of the file system.
    pub config: OkuFsConfig,
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
}

impl OkuFs {
    /// Starts an instance of an Oku file system.
    /// In the background, an Iroh node is started if none is running, or is connected to if one is already running.
    /// If not connected to an Oku relay, the node's address is periodically announced to the mainline DHT.
    ///
    /// # Arguments
    ///
    /// * `handle` - If compiling with the `fuse` feature, a Tokio runtime handle is required.
    ///
    /// # Returns
    ///
    /// A running instance of an Oku file system.
    pub async fn start(#[cfg(feature = "fuse")] handle: &Handle) -> miette::Result<Self> {
        let node_path = PathBuf::from(FS_PATH).join("node");
        let (running_node, node) = match iroh::client::Iroh::connect_path(node_path.clone()).await {
            Ok(node) => (None, node),
            Err(e) => {
                error!("{}", e);
                let node = FsNode::persistent(node_path)
                    .await
                    .map_err(|e| {
                        error!("{}", e);
                        OkuFsError::CannotStartNode
                    })?
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
        let config = OkuFsConfig::load_or_create_config()?;

        let (replica_sender, _replica_receiver) = watch::channel(());

        let oku_fs = Self {
            running_node,
            node,
            config,
            replica_sender,
            #[cfg(feature = "fuse")]
            fs_handles: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "fuse")]
            newest_handle: Arc::new(RwLock::new(0)),
            #[cfg(feature = "fuse")]
            handle: handle.clone(),
        };
        let docs_client = oku_fs.node.docs().clone();
        oku_fs.start_relay_connection(0).await?;
        if oku_fs.running_node.is_some() {
            let oku_fs_clone = oku_fs.clone();
            tokio::spawn(async move {
                oku_fs_clone
                    .listen_for_document_ticket_fetch_requests()
                    .await
                    .unwrap()
            });
        }
        if oku_fs.config.relay_connection_config()?.is_none() {
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(INITIAL_PUBLISH_DELAY).await;
                    let replicas = docs_client.list().await.unwrap();
                    pin_mut!(replicas);
                    while let Some(replica) = replicas.next().await {
                        let (namespace_id, _) = replica.unwrap();
                        announce_replica(namespace_id).await.unwrap();
                    }
                    tokio::time::sleep(REPUBLISH_DELAY - INITIAL_PUBLISH_DELAY).await;
                }
            });
        }
        Ok(oku_fs.clone())
    }

    #[async_recursion]
    /// Attempts to open a connection to a relay node, re-trying if unable to or if the connection fails at some point.
    ///
    /// # Arguments
    ///
    /// * `restarts` - The current number of attempts at connection; should be zero, as this is used for recursive calls.
    pub async fn start_relay_connection(&self, restarts: i64) -> miette::Result<()> {
        if let Some(relay_connection_config) = self.config.relay_connection_config()? {
            let node = self.clone();
            let relay_address = relay_connection_config.relay_address()?;
            info!(
                "Attempting to connect to relay ({}, attempt: {})",
                relay_address, restarts
            );
            tokio::spawn(async move {
                match node.connect_to_relay(relay_address).await {
                    Ok(_) => Ok::<(), Box<dyn Error + Send + Sync>>(()),
                    Err(e) => {
                        error!("{}", e);
                        if restarts < relay_connection_config.relay_connection_attempts()? {
                            Ok(node.start_relay_connection(restarts + 1).await?)
                        } else {
                            Err(e.into())
                        }
                    }
                }
            });
        }
        Ok(())
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

    /// Creates a new replica in the file system.
    ///
    /// # Returns
    ///
    /// The ID of the new replica, being its public key.
    pub async fn create_replica(&self) -> miette::Result<NamespaceId> {
        let docs_client = &self.node.docs();
        let new_document = docs_client.create().await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotCreateReplica
        })?;
        let document_id = new_document.id();
        new_document.close().await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotExitReplica
        })?;
        self.replica_sender.send_replace(());
        Ok(document_id)
    }

    /// Deletes a replica from the file system.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to delete.
    pub async fn delete_replica(&self, namespace_id: NamespaceId) -> miette::Result<()> {
        let docs_client = &self.node.docs();
        self.replica_sender.send_replace(());
        Ok(docs_client.drop_doc(namespace_id).await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotDeleteReplica
        })?)
    }

    /// Lists all replicas in the file system.
    ///
    /// # Returns
    ///
    /// A list of all replicas in the file system.
    pub async fn list_replicas(&self) -> miette::Result<Vec<(NamespaceId, CapabilityKind)>> {
        let docs_client = &self.node.docs();
        let replicas = docs_client.list().await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotListReplicas
        })?;
        pin_mut!(replicas);
        let replica_ids: Vec<(NamespaceId, CapabilityKind)> =
            replicas.map(|replica| replica.unwrap()).collect().await;
        Ok(replica_ids)
    }

    /// Retrieves the permissions for a local replica.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica.
    ///
    /// # Returns
    ///
    /// If either the replica can be read from & written to, or if it can only be read from.
    pub async fn get_replica_capability(
        &self,
        namespace_id: NamespaceId,
    ) -> miette::Result<CapabilityKind> {
        let replicas_vec = self.list_replicas().await?;
        match replicas_vec
            .iter()
            .find(|replica| replica.0 == namespace_id)
        {
            Some(replica) => Ok(replica.1),
            None => Err(OkuFuseError::NoReplica(namespace_id.to_string()).into()),
        }
    }

    /// Lists files in a replica.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to list files in.
    ///
    /// * `path` - An optional path within the replica.
    ///
    /// # Returns
    ///
    /// A list of files in the replica.
    pub async fn list_files(
        &self,
        namespace_id: NamespaceId,
        path: Option<PathBuf>,
    ) -> miette::Result<Vec<Entry>> {
        let docs_client = &self.node.docs();
        let document = docs_client
            .open(namespace_id)
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotOpenReplica
            })?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let query = if let Some(path) = path {
            let file_key = path_to_entry_prefix(path);
            iroh::docs::store::Query::single_latest_per_key()
                .key_prefix(file_key)
                .build()
        } else {
            iroh::docs::store::Query::single_latest_per_key().build()
        };
        let entries = document.get_many(query).await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotListFiles
        })?;
        pin_mut!(entries);
        let files: Vec<Entry> = entries.map(|entry| entry.unwrap()).collect().await;
        Ok(files)
    }

    /// Creates a file (if it does not exist) or modifies an existing file.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the file to create or modify.
    ///
    /// * `path` - The path of the file to create or modify.
    ///
    /// * `data` - The data to write to the file.
    ///
    /// # Returns
    ///
    /// The hash of the file.
    pub async fn create_or_modify_file(
        &self,
        namespace_id: NamespaceId,
        path: PathBuf,
        data: impl Into<Bytes>,
    ) -> miette::Result<Hash> {
        let file_key = path_to_entry_key(path);
        let data_bytes = data.into();
        let docs_client = &self.node.docs();
        let document = docs_client
            .open(namespace_id)
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotOpenReplica
            })?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entry_hash = document
            .set_bytes(
                self.node.authors().default().await.map_err(|e| {
                    error!("{}", e);
                    OkuFsError::CannotRetrieveDefaultAuthor
                })?,
                file_key,
                data_bytes,
            )
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotCreateOrModifyFile
            })?;

        Ok(entry_hash)
    }

    /// Deletes a file.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the file to delete.
    ///
    /// * `path` - The path of the file to delete.
    ///
    /// # Returns
    ///
    /// The number of entries deleted in the replica, which should be 1 if the file was successfully deleted.
    pub async fn delete_file(
        &self,
        namespace_id: NamespaceId,
        path: PathBuf,
    ) -> miette::Result<usize> {
        let file_key = path_to_entry_key(path);
        let docs_client = &self.node.docs();
        let document = docs_client
            .open(namespace_id)
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotOpenReplica
            })?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let query = iroh::docs::store::Query::single_latest_per_key()
            .key_exact(file_key.clone())
            .build();
        let entry = document
            .get_one(query)
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotReadFile
            })?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entries_deleted = document.del(entry.author(), file_key).await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotDeleteFile
        })?;
        Ok(entries_deleted)
    }

    /// Gets an Iroh entry for a file.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the file.
    ///
    /// * `path` - The path of the file.
    ///
    /// # Returns
    ///
    /// The entry representing the file.
    pub async fn get_entry(
        &self,
        namespace_id: NamespaceId,
        path: PathBuf,
    ) -> miette::Result<Entry> {
        let file_key = path_to_entry_key(path);
        let docs_client = &self.node.docs();
        let document = docs_client
            .open(namespace_id)
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotOpenReplica
            })?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let query = iroh::docs::store::Query::single_latest_per_key()
            .key_exact(file_key)
            .build();
        let entry = document
            .get_one(query)
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotReadFile
            })?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        Ok(entry)
    }

    /// Determines the oldest timestamp of a file.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the file.
    ///
    /// * `path` - The path to the file.
    ///
    /// # Returns
    ///
    /// The timestamp, in microseconds from the Unix epoch, of the oldest entry in the file.
    pub async fn get_oldest_entry_timestamp(
        &self,
        namespace_id: NamespaceId,
        path: PathBuf,
    ) -> miette::Result<u64> {
        let file_key = path_to_entry_key(path);
        let docs_client = &self.node.docs();
        let document = docs_client
            .open(namespace_id)
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotOpenReplica
            })?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let query = iroh::docs::store::Query::all().key_exact(file_key).build();
        let entries = document.get_many(query).await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotListFiles
        })?;
        pin_mut!(entries);
        let timestamps: Vec<u64> = entries
            .map(|entry| entry.unwrap().timestamp())
            .collect()
            .await;
        Ok(*timestamps.iter().min().unwrap_or(&u64::MIN))
    }

    /// Determines the oldest timestamp of a file entry in a folder.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the folder.
    ///
    /// * `path` - The folder whose oldest timestamp is to be determined.
    ///
    /// # Returns
    ///
    /// The oldest timestamp of any file descending from this folder, in microseconds from the Unix epoch.
    pub async fn get_oldest_timestamp_in_folder(
        &self,
        namespace_id: NamespaceId,
        path: PathBuf,
    ) -> miette::Result<u64> {
        let files = self.list_files(namespace_id, Some(path)).await?;
        let mut timestamps: Vec<u64> = Vec::new();
        for file in files {
            timestamps.push(
                self.get_oldest_entry_timestamp(namespace_id, entry_key_to_path(file.key())?)
                    .await?,
            );
        }
        Ok(*timestamps.iter().min().unwrap_or(&u64::MIN))
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
        Ok(*timestamps.iter().min().unwrap_or(&u64::MIN))
    }

    /// Determines the latest timestamp of a file entry in a folder.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the folder.
    ///
    /// * `path` - The folder whose latest timestamp is to be determined.
    ///
    /// # Returns
    ///
    /// The latest timestamp of any file descending from this folder, in microseconds from the Unix epoch.
    pub async fn get_newest_timestamp_in_folder(
        &self,
        namespace_id: NamespaceId,
        path: PathBuf,
    ) -> miette::Result<u64> {
        let files = self.list_files(namespace_id, Some(path)).await?;
        let mut timestamps: Vec<u64> = Vec::new();
        for file in files {
            timestamps.push(file.timestamp());
        }
        Ok(*timestamps.iter().max().unwrap_or(&u64::MIN))
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
        Ok(*timestamps.iter().max().unwrap_or(&u64::MIN))
    }

    /// Determines the size of a folder.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the folder.
    ///
    /// * `path` - The path to the folder within the replica.
    ///
    /// # Returns
    ///
    /// The total size, in bytes, of the files descending from this folder.
    pub async fn get_folder_size(
        &self,
        namespace_id: NamespaceId,
        path: PathBuf,
    ) -> miette::Result<u64> {
        let files = self.list_files(namespace_id, Some(path)).await?;
        let mut size = 0;
        for file in files {
            size += file.content_len();
        }
        Ok(size)
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

    /// Reads a file.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the file to read.
    ///
    /// * `path` - The path of the file to read.
    ///
    /// # Returns
    ///
    /// The data read from the file.
    pub async fn read_file(
        &self,
        namespace_id: NamespaceId,
        path: PathBuf,
    ) -> miette::Result<Bytes> {
        let entry = self.get_entry(namespace_id, path).await?;
        Ok(entry.content_bytes(&self.node).await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotReadFile
        })?)
    }

    /// Moves a file by copying it to a new location and deleting the original.
    ///
    /// # Arguments
    ///
    /// * `from_namespace_id` - The ID of the replica containing the file to move.
    ///
    /// * `to_namespace_id` - The ID of the replica to move the file to.
    ///
    /// * `from_path` - The path of the file to move.
    ///
    /// * `to_path` - The path to move the file to.
    ///
    /// # Returns
    ///
    /// A tuple containing the hash of the file at the new destination and the number of replica entries deleted during the operation, which should be 1 if the file at the original path was deleted.
    pub async fn move_file(
        &self,
        from_namespace_id: NamespaceId,
        from_path: PathBuf,
        to_namespace_id: NamespaceId,
        to_path: PathBuf,
    ) -> miette::Result<(Hash, usize)> {
        let data = self.read_file(from_namespace_id, from_path.clone()).await?;
        let hash = self
            .create_or_modify_file(to_namespace_id, to_path.clone(), data)
            .await?;
        let entries_deleted = self.delete_file(from_namespace_id, from_path).await?;
        Ok((hash, entries_deleted))
    }

    /// Moves a directory by copying it to a new location and deleting the original.
    ///
    /// # Arguments
    ///
    /// * `from_namespace_id` - The ID of the replica containing the directory to move.
    ///
    /// * `to_namespace_id` - The ID of the replica to move the directory to.
    ///
    /// * `from_path` - The path of the directory to move.
    ///
    /// * `to_path` - The path to move the directory to.
    ///
    /// # Returns
    ///
    /// A tuple containing the list of file hashes for files at their new destinations, and the total number of replica entries deleted during the operation.
    pub async fn move_directory(
        &self,
        from_namespace_id: NamespaceId,
        from_path: PathBuf,
        to_namespace_id: NamespaceId,
        to_path: PathBuf,
    ) -> miette::Result<(Vec<Hash>, usize)> {
        let mut entries_deleted = 0;
        let mut moved_file_hashes = Vec::new();
        let old_directory_files = self.list_files(from_namespace_id, Some(from_path)).await?;
        for old_directory_file in old_directory_files {
            let old_file_path = entry_key_to_path(old_directory_file.key())?;
            let new_file_path = to_path.join(old_file_path.file_name().unwrap_or_default());
            let file_move_info = self
                .move_file(
                    from_namespace_id,
                    old_file_path,
                    to_namespace_id,
                    new_file_path,
                )
                .await?;
            moved_file_hashes.push(file_move_info.0);
            entries_deleted += file_move_info.1;
        }
        Ok((moved_file_hashes, entries_deleted))
    }

    /// Deletes a directory and all its contents.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the directory to delete.
    ///
    /// * `path` - The path of the directory to delete.
    ///
    /// # Returns
    ///
    /// The number of entries deleted.
    pub async fn delete_directory(
        &self,
        namespace_id: NamespaceId,
        path: PathBuf,
    ) -> miette::Result<usize> {
        let path = normalise_path(path).join(""); // Ensure path ends with a slash
        let file_key = path_to_entry_prefix(path);
        let docs_client = &self.node.docs();
        let document = docs_client
            .open(namespace_id)
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotOpenReplica
            })?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let mut entries_deleted = 0;
        let query = iroh::docs::store::Query::single_latest_per_key()
            .key_prefix(file_key)
            .build();
        let entries = document.get_many(query).await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotListFiles
        })?;
        pin_mut!(entries);
        let files: Vec<Entry> = entries.map(|entry| entry.unwrap()).collect().await;
        for file in files {
            entries_deleted += document
                .del(
                    file.author(),
                    format!(
                        "{}",
                        std::str::from_utf8(&path_to_entry_prefix(entry_key_to_path(file.key())?))
                            .into_diagnostic()?
                    ),
                )
                .await
                .map_err(|e| {
                    error!("{}", e);
                    OkuFsError::CannotDeleteDirectory
                })?;
        }
        Ok(entries_deleted)
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

    /// Create a sharing ticket for a given replica.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to share.
    ///
    /// * `share_mode` - Whether the replica should be shared as read-only, or if read & write permissions are to be shared.
    ///
    /// # Returns
    ///
    /// A ticket to retrieve the given replica with the requested permissions.
    pub async fn create_document_ticket(
        &self,
        namespace_id: NamespaceId,
        share_mode: ShareMode,
    ) -> miette::Result<DocTicket> {
        if matches!(share_mode, ShareMode::Write)
            && matches!(
                self.get_replica_capability(namespace_id.clone()).await?,
                CapabilityKind::Read
            )
        {
            Err(OkuFsError::CannotShareReplicaWriteable(namespace_id).into())
        } else {
            let docs_client = &self.node.docs();
            let document = docs_client
                .open(namespace_id)
                .await
                .map_err(|e| {
                    error!("{}", e);
                    OkuFsError::CannotOpenReplica
                })?
                .ok_or(OkuFsError::FsEntryNotFound)?;
            Ok(document
                .share(share_mode, AddrInfoOptions::RelayAndAddresses)
                .await
                .map_err(|e| {
                    error!("{}", e);
                    OkuDiscoveryError::CannotGenerateSharingTicket
                })?)
        }
    }

    /// Respond to requests for content from peers.
    ///
    /// # Arguments
    ///
    /// * `request` - A request for content.
    ///
    /// # Returns
    ///
    /// A response containing a ticket for the content.
    pub async fn respond_to_content_request(
        &self,
        request: PeerContentRequest,
    ) -> miette::Result<PeerContentResponse> {
        let docs_client = &self.node.docs();
        let document = docs_client
            .open(request.namespace_id)
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuFsError::CannotOpenReplica
            })?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let document_ticket = document
            .share(ShareMode::Read, AddrInfoOptions::RelayAndAddresses)
            .await
            .map_err(|e| {
                error!("{}", e);
                OkuDiscoveryError::CannotGenerateSharingTicket
            })?;
        let query = iroh::docs::store::Query::single_latest_per_key().build();
        let entries = document.get_many(query).await.map_err(|e| {
            error!("{}", e);
            OkuFsError::CannotListFiles
        })?;
        pin_mut!(entries);
        let file_sizes: Vec<u64> = entries
            .map(|entry| entry.unwrap().content_len())
            .collect()
            .await;
        let content_length = file_sizes.iter().sum();
        Ok(PeerContentResponse {
            ticket_response: PeerTicketResponse::Document(document_ticket),
            content_size: content_length,
        })
    }

    /// Handles incoming requests for document tickets.
    /// This function listens for incoming connections from peers and responds to requests for document tickets.
    pub async fn listen_for_document_ticket_fetch_requests(&self) -> miette::Result<()> {
        let socket = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, DISCOVERY_PORT);
        let listener = TcpListener::bind(socket).await.into_diagnostic()?;
        loop {
            let (mut stream, _) = listener.accept().await.into_diagnostic()?;
            let self_clone = self.clone();
            tokio::spawn(async move {
                let mut buf_reader = BufReader::new(&mut stream);
                let received: Vec<u8> = buf_reader.fill_buf().await?.to_vec();
                buf_reader.consume(received.len());
                let mut incoming_lines = received.split(|x| *x == 10);
                if let Some(first_line) = incoming_lines.next() {
                    if first_line == ALPN_DOCUMENT_TICKET_FETCH {
                        let remaining_lines: Vec<Vec<u8>> =
                            incoming_lines.map(|x| x.to_owned()).collect();
                        let peer_content_request_bytes = remaining_lines.concat();
                        let peer_content_request_str =
                            String::from_utf8_lossy(&peer_content_request_bytes).to_string();
                        let peer_content_request = serde_json::from_str(&peer_content_request_str)?;
                        let peer_content_response = self_clone
                            .respond_to_content_request(peer_content_request)
                            .await?;
                        let peer_content_response_string =
                            serde_json::to_string(&peer_content_response)?;
                        stream
                            .write_all(peer_content_response_string.as_bytes())
                            .await?;
                        stream.flush().await?;
                    }
                }
                Ok::<(), Box<dyn Error + Send + Sync>>(())
            });
        }
    }

    /// Use the mainline DHT to obtain a ticket for the replica with the given ID.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to fetch.
    ///
    /// * `partial` - Whether to discover peers who claim to only have a partial copy of the replica.
    ///
    /// * `verified` - Whether to discover peers who have been verified to have the replica.
    ///
    /// # Returns
    ///
    /// A ticket for the replica with the given ID.
    pub async fn resolve_namespace_id(
        &self,
        namespace_id: NamespaceId,
        partial: bool,
        verified: bool,
    ) -> anyhow::Result<DocTicket> {
        let content = ContentRequest::Hash(Hash::new(namespace_id));
        let dht = mainline::Dht::server()?;
        let q = Query {
            content: content.hash_and_format(),
            flags: QueryFlags {
                complete: !partial,
                verified,
            },
        };
        let info_hash = to_infohash(q.content);
        let peer_content_request = PeerContentRequest { namespace_id };
        let peer_content_request_string = serde_json::to_string(&peer_content_request)?;

        let mut addrs = dht.get_peers(info_hash)?;
        let tickets: Arc<Mutex<Vec<DocTicket>>> = Arc::new(Mutex::new(Vec::new()));
        for peer_response in &mut addrs {
            for peer in peer_response {
                let peer_content_request_string = peer_content_request_string.clone();
                let tickets_clone = tickets.clone();
                tokio::spawn(async move {
                    let mut stream = TcpStream::connect(peer).await?;
                    let mut request = Vec::new();
                    request.write_all(ALPN_DOCUMENT_TICKET_FETCH).await?;
                    request.write_all(b"\n").await?;
                    request
                        .write_all(peer_content_request_string.as_bytes())
                        .await?;
                    request.flush().await?;
                    stream.write_all(&request).await?;
                    stream.flush().await?;
                    let mut response_bytes = Vec::new();
                    stream.read_to_end(&mut response_bytes).await?;
                    let response: PeerContentResponse =
                        serde_json::from_str(String::from_utf8_lossy(&response_bytes).as_ref())?;
                    match response.ticket_response {
                        PeerTicketResponse::Document(document_ticket) => {
                            if document_ticket.capability.id() != namespace_id {
                                return Ok::<(), Box<dyn Error + Send + Sync>>(());
                            }
                            tickets_clone.lock().await.push(document_ticket);
                            Ok::<(), Box<dyn Error + Send + Sync>>(())
                        }
                    }
                });
            }
        }
        let tickets = tickets.lock_owned().await.to_vec();
        merge_tickets(tickets).ok_or(anyhow!(
            "Could not find tickets for {}",
            namespace_id.to_string()
        ))
    }

    /// Retrieve a file locally after attempting to retrieve the latest version from the Internet.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the file to retrieve.
    ///
    /// * `path` - The path to the file to retrieve.
    ///
    /// * `partial` - If retrieving externally, whether to discover peers who claim to only have a partial copy of the replica.
    ///
    /// * `verified` - If retrieving externally, whether to discover peers who have been verified to have the replica.
    ///
    /// # Returns
    ///
    /// The data read from the file.
    pub async fn fetch_file(
        &self,
        namespace_id: NamespaceId,
        path: PathBuf,
        partial: bool,
        verified: bool,
    ) -> anyhow::Result<Bytes> {
        match self
            .resolve_namespace_id(namespace_id.clone(), partial, verified)
            .await
        {
            Ok(ticket) => match self.fetch_file_with_ticket(ticket, path.clone()).await {
                Ok(bytes) => Ok(bytes),
                Err(e) => {
                    error!("{}", e);
                    Ok(self
                        .read_file(namespace_id, path)
                        .await
                        .map_err(|e| anyhow!("{}", e))?)
                }
            },
            Err(e) => {
                error!("{}", e);
                Ok(self
                    .read_file(namespace_id, path)
                    .await
                    .map_err(|e| anyhow!("{}", e))?)
            }
        }
    }

    /// Join a swarm to fetch the latest version of a file and save it to the local machine.
    ///
    /// # Arguments
    ///
    /// * `ticket` - A ticket for the replica containing the file to retrieve.
    ///
    /// * `path` - The path to the file to retrieve.
    ///
    /// # Returns
    ///
    /// The data read from the file.
    pub async fn fetch_file_with_ticket(
        &self,
        ticket: DocTicket,
        path: PathBuf,
    ) -> anyhow::Result<Bytes> {
        let docs_client = &self.node.docs();
        let replica = docs_client
            .import_namespace(ticket.capability.clone())
            .await?;
        let filter = FilterKind::Exact(path_to_entry_key(path.clone()));
        replica
            .set_download_policy(iroh::docs::store::DownloadPolicy::NothingExcept(vec![
                filter,
            ]))
            .await?;
        replica.start_sync(ticket.nodes).await?;
        let namespace_id = ticket.capability.id();
        let mut events = replica.subscribe().await?;
        let sync_start = std::time::Instant::now();
        while let Some(event) = events.next().await {
            if matches!(event?, SyncFinished { .. }) {
                let elapsed = sync_start.elapsed();
                info!(
                    "Synchronisation took {elapsed:?} for {} … ",
                    namespace_id.to_string(),
                );
                break;
            }
        }
        Ok(self
            .read_file(namespace_id, path)
            .await
            .map_err(|e| anyhow!("{}", e))?)
    }

    /// Join a swarm to fetch the latest version of a replica and save it to the local machine.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to fetch.
    ///
    /// * `path` - An optional path of requested files within the replica.
    ///
    /// * `partial` - Whether to discover peers who claim to only have a partial copy of the replica.
    ///
    /// * `verified` - Whether to discover peers who have been verified to have the replica.
    pub async fn fetch_replica_by_id(
        &self,
        namespace_id: NamespaceId,
        path: Option<PathBuf>,
        partial: bool,
        verified: bool,
    ) -> anyhow::Result<()> {
        let ticket = self
            .resolve_namespace_id(namespace_id.clone(), partial, verified)
            .await?;
        let docs_client = self.node.docs();
        let replica_sender = self.replica_sender.clone();
        match path.clone() {
            Some(path) => {
                let replica = docs_client.import_namespace(ticket.capability).await?;
                let filter = FilterKind::Prefix(path_to_entry_prefix(path));
                replica
                    .set_download_policy(iroh::docs::store::DownloadPolicy::NothingExcept(vec![
                        filter,
                    ]))
                    .await?;
                replica.start_sync(ticket.nodes).await?;
                let mut events = replica.subscribe().await?;
                let sync_start = std::time::Instant::now();
                while let Some(event) = events.next().await {
                    if matches!(event?, SyncFinished { .. }) {
                        let elapsed = sync_start.elapsed();
                        info!(
                            "Synchronisation took {elapsed:?} for {} … ",
                            namespace_id.to_string(),
                        );
                        break;
                    }
                }
            }
            None => {
                if let Some(replica) = docs_client.open(namespace_id.clone()).await.unwrap_or(None)
                {
                    replica
                        .set_download_policy(iroh::docs::store::DownloadPolicy::default())
                        .await?;
                    replica.start_sync(ticket.nodes).await?;
                    let mut events = replica.subscribe().await?;
                    let sync_start = std::time::Instant::now();
                    while let Some(event) = events.next().await {
                        if matches!(event?, SyncFinished { .. }) {
                            let elapsed = sync_start.elapsed();
                            info!(
                                "Synchronisation took {elapsed:?} for {} … ",
                                namespace_id.to_string(),
                            );
                            break;
                        }
                    }
                } else {
                    let (_replica, mut events) = docs_client.import_and_subscribe(ticket).await?;
                    let sync_start = std::time::Instant::now();
                    while let Some(event) = events.next().await {
                        if matches!(event?, SyncFinished { .. }) {
                            let elapsed = sync_start.elapsed();
                            info!(
                                "Synchronisation took {elapsed:?} for {} … ",
                                namespace_id.to_string(),
                            );
                            break;
                        }
                    }
                }
            }
        }
        replica_sender.send_replace(());
        Ok(())
    }

    /// Join a swarm to fetch the latest version of a replica and save it to the local machine.
    ///
    /// # Arguments
    ///
    /// * `ticket` - A ticket for the replica to fetch.
    ///
    /// * `path` - An optional path of requested files within the replica.
    pub async fn fetch_replica_by_ticket(
        &self,
        ticket: DocTicket,
        path: Option<PathBuf>,
    ) -> anyhow::Result<()> {
        let namespace_id = ticket.capability.id();
        let docs_client = self.node.docs();
        let replica_sender = self.replica_sender.clone();
        match path.clone() {
            Some(path) => {
                let replica = docs_client.import_namespace(ticket.capability).await?;
                let filter = FilterKind::Prefix(path_to_entry_prefix(path));
                replica
                    .set_download_policy(iroh::docs::store::DownloadPolicy::NothingExcept(vec![
                        filter,
                    ]))
                    .await?;
                replica.start_sync(ticket.nodes).await?;
                let mut events = replica.subscribe().await?;
                let sync_start = std::time::Instant::now();
                while let Some(event) = events.next().await {
                    if matches!(event?, SyncFinished { .. }) {
                        let elapsed = sync_start.elapsed();
                        info!(
                            "Synchronisation took {elapsed:?} for {} … ",
                            namespace_id.to_string(),
                        );
                        break;
                    }
                }
            }
            None => {
                if let Some(replica) = docs_client.open(namespace_id.clone()).await.unwrap_or(None)
                {
                    replica
                        .set_download_policy(iroh::docs::store::DownloadPolicy::default())
                        .await?;
                    replica.start_sync(ticket.nodes).await?;
                    let mut events = replica.subscribe().await?;
                    let sync_start = std::time::Instant::now();
                    while let Some(event) = events.next().await {
                        if matches!(event?, SyncFinished { .. }) {
                            let elapsed = sync_start.elapsed();
                            info!(
                                "Synchronisation took {elapsed:?} for {} … ",
                                namespace_id.to_string(),
                            );
                            break;
                        }
                    }
                } else {
                    let (_replica, mut events) = docs_client.import_and_subscribe(ticket).await?;
                    let sync_start = std::time::Instant::now();
                    while let Some(event) = events.next().await {
                        if matches!(event?, SyncFinished { .. }) {
                            let elapsed = sync_start.elapsed();
                            info!(
                                "Synchronisation took {elapsed:?} for {} … ",
                                namespace_id.to_string(),
                            );
                            break;
                        }
                    }
                }
            }
        }
        replica_sender.send_replace(());
        Ok(())
    }

    /// Join a swarm to fetch the latest version of a replica and save it to the local machine.
    ///
    /// If a version of the replica already exists locally, only the last-fetched paths will be fetched.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to fetch.
    ///
    /// * `partial` - Whether to discover peers who claim to only have a partial copy of the replica.
    ///
    /// * `verified` - Whether to discover peers who have been verified to have the replica.
    pub async fn sync_replica(
        &self,
        namespace_id: NamespaceId,
        partial: bool,
        verified: bool,
    ) -> anyhow::Result<()> {
        let ticket = self
            .resolve_namespace_id(namespace_id, partial, verified)
            .await?;
        let docs_client = self.node.docs();
        let replica_sender = self.replica_sender.clone();
        let (_replica, mut events) = docs_client.import_and_subscribe(ticket).await?;
        let sync_start = std::time::Instant::now();
        while let Some(event) = events.next().await {
            if matches!(event?, SyncFinished { .. }) {
                let elapsed = sync_start.elapsed();
                info!(
                    "Synchronisation took {elapsed:?} for {} … ",
                    namespace_id.to_string(),
                );
                break;
            }
        }
        replica_sender.send_replace(());
        Ok(())
    }

    /// Connects to a relay to facilitate communication behind NAT.
    /// Upon connecting, the file system will send a list of all replicas to the relay. Periodically, the relay will request the list of replicas again using the same connection.
    ///
    /// # Arguments
    ///
    /// * `relay_address` - The address of the relay to connect to.
    pub async fn connect_to_relay(&self, relay_address: String) -> miette::Result<()> {
        let relay_addr = relay_address
            .to_socket_addrs()
            .into_diagnostic()?
            .next()
            .ok_or(miette::miette!(
                "Unable to resolve relay address: {} … ",
                relay_address
            ))?;
        let mut stream = TcpStream::connect(relay_addr).await.into_diagnostic()?;
        let all_replicas = self.list_replicas().await?;
        let all_replicas_str = serde_json::to_string(&all_replicas).into_diagnostic()?;
        let mut request = Vec::new();
        request
            .write_all(ALPN_INITIAL_RELAY_CONNECTION)
            .await
            .into_diagnostic()?;
        request.write_all(b"\n").await.into_diagnostic()?;
        request
            .write_all(all_replicas_str.as_bytes())
            .await
            .into_diagnostic()?;
        request.flush().await.into_diagnostic()?;
        stream.write_all(&request).await.into_diagnostic()?;
        stream.flush().await.into_diagnostic()?;
        loop {
            let mut response_bytes = Vec::new();
            stream
                .read_to_end(&mut response_bytes)
                .await
                .into_diagnostic()?;
            if response_bytes == ALPN_RELAY_FETCH {
                let all_replicas = self.list_replicas().await?;
                let all_replicas_str = serde_json::to_string(&all_replicas).into_diagnostic()?;
                stream
                    .write_all(all_replicas_str.as_bytes())
                    .await
                    .into_diagnostic()?;
                stream.flush().await.into_diagnostic()?;
            }
        }
        #[allow(unreachable_code)]
        Ok(())
    }
}
