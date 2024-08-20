use crate::discovery::{announce_replica, INITIAL_PUBLISH_DELAY, REPUBLISH_DELAY};
use crate::discovery::{
    PeerContentRequest, PeerContentResponse, PeerTicketResponse, DISCOVERY_PORT,
};
use crate::error::{OkuDiscoveryError, OkuRelayError};
use crate::{discovery::ContentRequest, error::OkuFsError};
use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use iroh::base::node_addr::AddrInfoOptions;
use iroh::base::ticket::BlobTicket;
use iroh::client::docs::Entry;
use iroh::net::discovery::dns::DnsDiscovery;
use iroh::net::discovery::pkarr::PkarrPublisher;
use iroh::{
    base::hash::Hash,
    client::docs::ShareMode,
    docs::{Author, AuthorId, NamespaceId},
    net::discovery::{ConcurrentDiscovery, Discovery},
    node::FsNode,
};
use iroh_mainline_content_discovery::protocol::{Query, QueryFlags};
use iroh_mainline_content_discovery::to_infohash;
// use iroh_pkarr_node_discovery::PkarrNodeDiscovery;
use miette::IntoDiagnostic;
use path_clean::PathClean;
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{error::Error, path::PathBuf};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::TcpStream;

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

#[derive(Clone, Debug, Serialize, Deserialize)]
///  The configuration of the file system.
pub struct OkuFsConfig {
    /// An optional address to facilitate communication behind NAT.
    pub relay_address: Option<String>,
}

/// An instance of an Oku file system.
///
/// The `OkuFs` struct is the primary interface for interacting with an Oku file system.
#[derive(Clone, Debug)]
pub struct OkuFs {
    /// An Iroh node responsible for storing replicas on the local machine, as well as joining swarms to fetch replicas from other nodes.
    node: FsNode,
    /// The public key of the author of the file system.
    author_id: AuthorId,
    /// The configuration of the file system.
    config: OkuFsConfig,
}

impl OkuFs {
    /// Starts an instance of an Oku file system.
    /// In the background, an Iroh node is started, and the node's address is periodically announced to the mainline DHT.
    /// If no author credentials are found on disk, new credentials are generated.
    ///
    /// # Returns
    ///
    /// A running instance of an Oku file system.
    pub async fn start() -> miette::Result<OkuFs> {
        let node_path = PathBuf::from(FS_PATH).join("node");
        let node = FsNode::persistent(node_path)
            .await
            .map_err(|_e| OkuFsError::CannotStartNode)?
            .spawn()
            .await
            .map_err(|_e| OkuFsError::CannotStartNode)?;
        let authors = node
            .authors()
            .list()
            .await
            .map_err(|_e| OkuFsError::CannotRetrieveAuthors)?;
        futures::pin_mut!(authors);
        let authors_count = authors.as_mut().count().await.to_owned();
        let author_id = if authors_count == 0 {
            node.authors()
                .create()
                .await
                .map_err(|_e| OkuFsError::AuthorCannotBeCreated)?
        } else {
            let authors = node
                .authors()
                .list()
                .await
                .map_err(|_e| OkuFsError::CannotRetrieveAuthors)?;
            futures::pin_mut!(authors);
            let authors_list: Vec<AuthorId> = authors.map(|author| author.unwrap()).collect().await;
            authors_list[0]
        };
        let config = load_or_create_config()?;
        let oku_fs = OkuFs {
            node,
            author_id,
            config,
        };
        let node_addr = oku_fs
            .node
            .node_addr()
            .await
            .map_err(|_e| OkuFsError::CannotRetrieveNodeAddress)?;
        let addr_info = node_addr.info;
        let magic_endpoint = oku_fs.node.endpoint();
        let secret_key = magic_endpoint.secret_key();
        let mut discovery_service = ConcurrentDiscovery::empty();
        let pkarr = PkarrPublisher::n0_dns(secret_key.clone());
        let dns = DnsDiscovery::n0_dns();
        discovery_service.add(pkarr);
        discovery_service.add(dns);
        discovery_service.publish(&addr_info);
        let docs_client = oku_fs.node.docs();
        let docs_client = docs_client.clone();
        let oku_fs_clone = oku_fs.clone();
        if let Some(relay_address) = oku_fs_clone.config.relay_address {
            let oku_fs_clone = oku_fs.clone();
            tokio::spawn(async move {
                oku_fs_clone
                    .connect_to_relay(relay_address.to_string())
                    .await
                    .map_err(|_e| OkuRelayError::ProblemConnecting(relay_address.to_string()))
                    .unwrap();
            });
        }
        let oku_fs_clone = oku_fs.clone();
        tokio::spawn(async move {
            oku_fs_clone
                .listen_for_document_ticket_fetch_requests()
                .await
                .unwrap()
        });
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
        Ok(oku_fs.clone())
    }

    /// Create a mechanism for discovering other nodes on the network given their IDs.
    ///
    /// # Returns
    ///
    /// A discovery service for finding other node's addresses given their IDs.
    pub async fn create_discovery_service(&self) -> miette::Result<ConcurrentDiscovery> {
        let node_addr = self
            .node
            .node_addr()
            .await
            .map_err(|_e| OkuFsError::CannotRetrieveNodeAddress)?;
        let addr_info = node_addr.info;
        let magic_endpoint = self.node.endpoint();
        let secret_key = magic_endpoint.secret_key();
        let mut discovery_service = ConcurrentDiscovery::empty();
        let pkarr = PkarrPublisher::n0_dns(secret_key.clone());
        let dns = DnsDiscovery::n0_dns();
        discovery_service.add(pkarr);
        discovery_service.add(dns);
        discovery_service.publish(&addr_info);
        Ok(discovery_service)
    }

    /// Shuts down the Oku file system.
    pub async fn shutdown(self) -> miette::Result<()> {
        Ok(self
            .node
            .shutdown()
            .await
            .map_err(|_e| OkuFsError::CannotStopNode)?)
    }

    /// Creates a new replica in the file system.
    ///
    /// # Returns
    ///
    /// The ID of the new replica, being its public key.
    pub async fn create_replica(&self) -> miette::Result<NamespaceId> {
        let docs_client = &self.node.docs();
        let new_document = docs_client
            .create()
            .await
            .map_err(|_e| OkuFsError::CannotCreateReplica)?;
        let document_id = new_document.id();
        new_document
            .close()
            .await
            .map_err(|_e| OkuFsError::CannotExitReplica)?;
        Ok(document_id)
    }

    /// Deletes a replica from the file system.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to delete.
    pub async fn delete_replica(&self, namespace_id: NamespaceId) -> miette::Result<()> {
        let docs_client = &self.node.docs();
        Ok(docs_client
            .drop_doc(namespace_id)
            .await
            .map_err(|_e| OkuFsError::CannotDeleteReplica)?)
    }

    /// Lists all replicas in the file system.
    ///
    /// # Returns
    ///
    /// A list of all replicas in the file system.
    pub async fn list_replicas(&self) -> miette::Result<Vec<NamespaceId>> {
        let docs_client = &self.node.docs();
        let replicas = docs_client
            .list()
            .await
            .map_err(|_e| OkuFsError::CannotListReplicas)?;
        pin_mut!(replicas);
        let replica_ids: Vec<NamespaceId> =
            replicas.map(|replica| replica.unwrap().0).collect().await;
        Ok(replica_ids)
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
            .map_err(|_e| OkuFsError::CannotOpenReplica)?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let query = if let Some(path) = path {
            let file_key = path_to_entry_prefix(path);
            iroh::docs::store::Query::single_latest_per_key()
                .key_prefix(file_key)
                .build()
        } else {
            iroh::docs::store::Query::single_latest_per_key().build()
        };
        let entries = document
            .get_many(query)
            .await
            .map_err(|_e| OkuFsError::CannotListFiles)?;
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
            .map_err(|_e| OkuFsError::CannotOpenReplica)?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entry_hash = document
            .set_bytes(self.author_id, file_key, data_bytes)
            .await
            .map_err(|_e| OkuFsError::CannotCreateOrModifyFile)?;

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
            .map_err(|_e| OkuFsError::CannotOpenReplica)?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entries_deleted = document
            .del(self.author_id, file_key)
            .await
            .map_err(|_e| OkuFsError::CannotDeleteFile)?;
        Ok(entries_deleted)
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
        let file_key = path_to_entry_key(path);
        let docs_client = &self.node.docs();
        let document = docs_client
            .open(namespace_id)
            .await
            .map_err(|_e| OkuFsError::CannotOpenReplica)?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let query = iroh::docs::store::Query::single_latest_per_key()
            .key_exact(file_key)
            .build();
        let entry = document
            .get_one(query)
            .await
            .map_err(|_e| OkuFsError::CannotReadFile)?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        Ok(entry
            .content_bytes(self.node.client())
            .await
            .map_err(|_e| OkuFsError::CannotReadFile)?)
    }

    /// Moves a file by copying it to a new location and deleting the original.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica containing the file to move.
    ///
    /// * `from` - The path of the file to move.
    ///
    /// * `to` - The path to move the file to.
    ///
    /// # Returns
    ///
    /// A tuple containing the hash of the file at the new destination and the number of replica entries deleted during the operation, which should be 1 if the file at the original path was deleted.
    pub async fn move_file(
        &self,
        namespace_id: NamespaceId,
        from: PathBuf,
        to: PathBuf,
    ) -> miette::Result<(Hash, usize)> {
        let data = self.read_file(namespace_id, from.clone()).await?;
        let hash = self
            .create_or_modify_file(namespace_id, to.clone(), data)
            .await?;
        let entries_deleted = self.delete_file(namespace_id, from).await?;
        Ok((hash, entries_deleted))
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
        let docs_client = &self.node.docs();
        let document = docs_client
            .open(namespace_id)
            .await
            .map_err(|_e| OkuFsError::CannotOpenReplica)?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entries_deleted = document
            .del(self.author_id, format!("{}", path.display()))
            .await
            .map_err(|_e| OkuFsError::CannotDeleteDirectory)?;
        Ok(entries_deleted)
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
            .map_err(|_e| OkuFsError::CannotOpenReplica)?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        match request.path {
            None => {
                let document_ticket = document
                    .share(ShareMode::Read, AddrInfoOptions::RelayAndAddresses)
                    .await
                    .map_err(|_e| OkuDiscoveryError::CannotGenerateSharingTicket)?;
                let query = iroh::docs::store::Query::single_latest_per_key().build();
                let entries = document
                    .get_many(query)
                    .await
                    .map_err(|_e| OkuFsError::CannotListFiles)?;
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
            Some(blob_path) => {
                let blobs_client = &self.node.blobs();
                let entry_prefix = path_to_entry_key(blob_path);
                let query = iroh::docs::store::Query::single_latest_per_key()
                    .key_prefix(entry_prefix)
                    .build();
                let entries = document
                    .get_many(query)
                    .await
                    .map_err(|_e| OkuFsError::CannotListFiles)?;
                pin_mut!(entries);
                let entry_hashes_and_sizes: Vec<(Hash, u64)> = entries
                    .map(|entry| {
                        (
                            entry.as_ref().unwrap().content_hash(),
                            entry.unwrap().content_len(),
                        )
                    })
                    .collect()
                    .await;
                let entry_tickets: Vec<BlobTicket> =
                    futures::future::try_join_all(entry_hashes_and_sizes.iter().map(|entry| {
                        blobs_client.share(
                            entry.0,
                            iroh::base::hash::BlobFormat::Raw,
                            iroh::base::node_addr::AddrInfoOptions::RelayAndAddresses,
                        )
                    }))
                    .await
                    .map_err(|_e| OkuDiscoveryError::CannotGenerateSharingTicketForFiles)?;
                let content_length = entry_hashes_and_sizes
                    .iter()
                    .map(|entry| entry.1)
                    .collect::<Vec<u64>>()
                    .iter()
                    .sum();
                Ok(PeerContentResponse {
                    ticket_response: PeerTicketResponse::Entries(entry_tickets),
                    content_size: content_length,
                })
            }
        }
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

    /// Joins a swarm to fetch the latest version of a replica and save it to the local machine.
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
    pub async fn get_external_replica(
        &self,
        namespace_id: NamespaceId,
        path: Option<PathBuf>,
        partial: bool,
        verified: bool,
    ) -> miette::Result<()> {
        let content = ContentRequest::Hash(Hash::new(namespace_id));
        let dht = mainline::Dht::server().into_diagnostic()?;
        let q = Query {
            content: content.hash_and_format(),
            flags: QueryFlags {
                complete: !partial,
                verified,
            },
        };
        let info_hash = to_infohash(q.content);
        let peer_content_request = PeerContentRequest { namespace_id, path };
        let peer_content_request_string =
            serde_json::to_string(&peer_content_request).into_diagnostic()?;
        let docs_client = self.node.docs();

        let mut addrs = dht.get_peers(info_hash).into_diagnostic()?;
        for peer_response in &mut addrs {
            for peer in peer_response {
                if docs_client.open(namespace_id).await.is_ok() {
                    break;
                }
                let peer_content_request_string = peer_content_request_string.clone();
                let docs_client = docs_client.clone();
                let self_clone = self.clone();
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
                            // let docs_client = &self.node.docs;
                            docs_client.import(document_ticket).await?;
                            Ok::<(), Box<dyn Error + Send + Sync>>(())
                        }
                        PeerTicketResponse::Entries(entry_tickets) => {
                            let blobs_client = &self_clone.node.blobs();
                            for blob_ticket in entry_tickets {
                                let ticket_parts = blob_ticket.into_parts();
                                // let blob_download_request = BlobDownloadRequest {
                                //     hash: ticket_parts.1,
                                //     format: ticket_parts.2,
                                //     peer: ticket_parts.0,
                                //     tag: iroh::blobs::util::SetTagOption::Auto,
                                // };
                                blobs_client
                                    .download(ticket_parts.1, ticket_parts.0)
                                    .await?;
                                // break;
                            }
                            Ok::<(), Box<dyn Error + Send + Sync>>(())
                        }
                    }
                });
            }
        }

        Ok(())
    }

    /// Connects to a relay to facilitate communication behind NAT.
    /// Upon connecting, the file system will send a list of all replicas to the relay. Periodically, the relay will request the list of replicas again using the same connection.
    ///
    /// # Arguments
    ///
    /// * `relay_address` - The address of the relay to connect to.
    pub async fn connect_to_relay(&self, relay_address: String) -> miette::Result<()> {
        let relay_addr = relay_address.parse::<SocketAddr>().into_diagnostic()?;
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
        Ok(())
    }
}

/// Imports the author credentials of the file system from disk, or creates new credentials if none exist.
///
/// # Arguments
///
/// * `path` - The path on disk of the file holding the author's credentials.
///
/// # Returns
///
/// The author credentials.
pub fn load_or_create_author() -> miette::Result<Author> {
    let path = PathBuf::from(FS_PATH).join("author");
    let author_file = std::fs::read(path.clone());
    match author_file {
        Ok(bytes) => Ok(Author::from_bytes(
            &bytes[..32].try_into().into_diagnostic()?,
        )),
        Err(_) => {
            let mut rng = OsRng;
            let author = Author::new(&mut rng);
            let author_bytes = author.to_bytes();
            std::fs::write(path, author_bytes).into_diagnostic()?;
            Ok(author)
        }
    }
}

/// Loads the configuration of the file system from disk, or creates a new configuration if none exists.
///
/// # Returns
///
/// The configuration of the file system.
pub fn load_or_create_config() -> miette::Result<OkuFsConfig> {
    let path = PathBuf::from(FS_PATH).join("config.toml");
    let config_file_contents = std::fs::read_to_string(path.clone());
    match config_file_contents {
        Ok(config_file_toml) => Ok(toml::from_str(&config_file_toml).into_diagnostic()?),
        Err(_) => {
            let config = OkuFsConfig {
                relay_address: None,
            };
            let config_toml = toml::to_string(&config).into_diagnostic()?;
            std::fs::write(path, config_toml).into_diagnostic()?;
            Ok(config)
        }
    }
}
