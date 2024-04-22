use crate::discovery::DISCOVERY_PORT;
use crate::discovery::{announce_replica, INITIAL_PUBLISH_DELAY, REPUBLISH_DELAY};
use crate::error::OkuDiscoveryError;
use crate::{discovery::ContentRequest, error::OkuFsError};
use bytes::Bytes;
use futures::{pin_mut, StreamExt};
use iroh::base::ticket::Ticket;
use iroh::client::Entry;
use iroh::net::magic_endpoint::accept_conn;
use iroh::ticket::DocTicket;
use iroh::{
    bytes::Hash,
    net::{
        discovery::{ConcurrentDiscovery, Discovery},
        MagicEndpoint,
    },
    node::FsNode,
    rpc_protocol::ShareMode,
    sync::{Author, AuthorId, NamespaceId},
};
use iroh_mainline_content_discovery::protocol::{Query, QueryFlags};
use iroh_mainline_content_discovery::to_infohash;
use iroh_mainline_content_discovery::UdpDiscovery;
use iroh_pkarr_node_discovery::PkarrNodeDiscovery;
use path_clean::PathClean;
use rand_core::OsRng;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{error::Error, path::PathBuf};

/// The path on disk where the file system is stored.
pub const FS_PATH: &str = ".oku";

/// The protocol identifier for exchanging document tickets.
pub const ALPN_DOCUMENT_TICKET_FETCH: &[u8] = b"oku/document-ticket/fetch/v0";

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
    // let mut hasher = Sha3_256::new();
    // hasher.update(path.clone().into_os_string().into_encoded_bytes());
    // let path_hash = hasher.finalize();
    // format!("{}\u{F0000}{}", path.display(), hex::encode(path_hash))
    let mut path_bytes = path.into_os_string().into_encoded_bytes();
    path_bytes.push(b'\0');
    path_bytes.into()
}

/// An instance of an Oku file system.
///
/// The `OkuFs` struct is the primary interface for interacting with an Oku file system.
pub struct OkuFs {
    /// An Iroh node responsible for storing replicas on the local machine, as well as joining swarms to fetch replicas from other nodes.
    node: FsNode,
    /// The public key of the author of the file system.
    author_id: AuthorId,
}

impl OkuFs {
    /// Starts an instance of an Oku file system.
    /// In the background, an Iroh node is started, and the node's address is periodically announced to the mainline DHT.
    /// If no author credentials are found on disk, new credentials are generated.
    ///
    /// # Returns
    ///
    /// A running instance of an Oku file system.
    pub async fn start() -> Result<OkuFs, Box<dyn Error>> {
        let node_path = PathBuf::from(FS_PATH).join("node");
        let node = FsNode::persistent(node_path).await?.spawn().await?;
        let authors = node.authors.list().await?;
        futures::pin_mut!(authors);
        let authors_count = authors.as_mut().count().await.to_owned();
        let author_id = if authors_count == 0 {
            node.authors.create().await?
        } else {
            let authors = node.authors.list().await?;
            futures::pin_mut!(authors);
            let authors_list: Vec<AuthorId> = authors.map(|author| author.unwrap()).collect().await;
            authors_list[0]
        };
        let oku_fs = OkuFs { node, author_id };
        let node_addr = oku_fs.node.my_addr().await?;
        let addr_info = node_addr.info;
        let magic_endpoint = oku_fs.node.magic_endpoint();
        let secret_key = magic_endpoint.secret_key();
        let mut discovery_service = ConcurrentDiscovery::new();
        let pkarr = PkarrNodeDiscovery::builder().secret_key(secret_key).build();
        discovery_service.add(pkarr);
        discovery_service.publish(&addr_info);
        let docs_client = &oku_fs.node.docs;
        let docs_client = docs_client.clone();
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
        Ok(oku_fs)
    }

    /// Create a mechanism for discovering other nodes on the network given their IDs.
    ///
    /// # Returns
    ///
    /// A discovery service for finding other node's addresses given their IDs.
    pub async fn create_discovery_service(&self) -> Result<ConcurrentDiscovery, Box<dyn Error>> {
        let node_addr = self.node.my_addr().await?;
        let addr_info = node_addr.info;
        let magic_endpoint = self.node.magic_endpoint();
        let secret_key = magic_endpoint.secret_key();
        let mut discovery_service = ConcurrentDiscovery::new();
        let pkarr = PkarrNodeDiscovery::builder().secret_key(secret_key).build();
        discovery_service.add(pkarr);
        discovery_service.publish(&addr_info);
        Ok(discovery_service)
    }

    /// Shuts down the Oku file system.
    pub fn shutdown(self) {
        self.node.shutdown();
    }

    /// Creates a new replica in the file system.
    ///
    /// # Returns
    ///
    /// The ID of the new replica, being its public key.
    pub async fn create_replica(&self) -> Result<NamespaceId, Box<dyn Error>> {
        let docs_client = &self.node.docs;
        let new_document = docs_client.create().await?;
        let document_id = new_document.id();
        new_document.close().await?;
        Ok(document_id)
    }

    /// Deletes a replica from the file system.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to delete.
    pub async fn delete_replica(&self, namespace_id: NamespaceId) -> Result<(), Box<dyn Error>> {
        let docs_client = &self.node.docs;
        Ok(docs_client.drop_doc(namespace_id).await?)
    }

    /// Lists all replicas in the file system.
    ///
    /// # Returns
    ///
    /// A list of all replicas in the file system.
    pub async fn list_replicas(&self) -> Result<Vec<NamespaceId>, Box<dyn Error>> {
        let docs_client = &self.node.docs;
        let replicas = docs_client.list().await?;
        pin_mut!(replicas);
        let replica_ids: Vec<NamespaceId> =
            replicas.map(|replica| replica.unwrap().0).collect().await;
        Ok(replica_ids)
    }

    /// Lists all files in a replica.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to list files in.
    ///
    /// # Returns
    ///
    /// A list of all files in the replica.
    pub async fn list_files(
        &self,
        namespace_id: NamespaceId,
    ) -> Result<Vec<Entry>, Box<dyn Error>> {
        let docs_client = &self.node.docs;
        let document = docs_client
            .open(namespace_id)
            .await?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let query = iroh::sync::store::Query::single_latest_per_key().build();
        let entries = document.get_many(query).await?;
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
    ) -> Result<Hash, Box<dyn Error>> {
        let file_key = path_to_entry_key(path);
        let data_bytes = data.into();
        let docs_client = &self.node.docs;
        let document = docs_client
            .open(namespace_id)
            .await?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entry_hash = document
            .set_bytes(self.author_id, file_key, data_bytes)
            .await?;

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
    ) -> Result<usize, Box<dyn Error>> {
        let file_key = path_to_entry_key(path);
        let docs_client = &self.node.docs;
        let document = docs_client
            .open(namespace_id)
            .await?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entries_deleted = document.del(self.author_id, file_key).await?;
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
    ) -> Result<Bytes, Box<dyn Error>> {
        let file_key = path_to_entry_key(path);
        let docs_client = &self.node.docs;
        let document = docs_client
            .open(namespace_id)
            .await?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entry = document
            .get_exact(self.author_id, file_key, false)
            .await?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        Ok(entry.content_bytes(self.node.client()).await?)
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
    ) -> Result<(Hash, usize), Box<dyn Error>> {
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
    ) -> Result<usize, Box<dyn Error>> {
        let path = normalise_path(path).join(""); // Ensure path ends with a slash
        let docs_client = &self.node.docs;
        let document = docs_client
            .open(namespace_id)
            .await?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entries_deleted = document
            .del(self.author_id, format!("{}", path.display()))
            .await?;
        Ok(entries_deleted)
    }

    /// Handles incoming requests for document tickets.
    /// This function listens for incoming connections from peers and responds to requests for document tickets.
    pub async fn listen_for_document_ticket_fetch_requests(&self) -> Result<(), Box<dyn Error>> {
        let mut alpns: Vec<Vec<u8>> = Vec::new();
        alpns.push(ALPN_DOCUMENT_TICKET_FETCH.to_vec());
        let secret_key = self.node.magic_endpoint().secret_key();
        let endpoint = MagicEndpoint::builder()
            .alpns(alpns)
            .secret_key(secret_key.clone())
            .discovery(Box::new(self.create_discovery_service().await?))
            .bind(0)
            .await?;
        while let Some(conn) = endpoint.clone().accept().await {
            let (peer_id, alpn, conn) = accept_conn(conn).await?;
            println!(
                "new connection from {peer_id} with ALPN {alpn} (coming from {})",
                conn.remote_address()
            );
            match alpn.as_bytes() {
                ALPN_DOCUMENT_TICKET_FETCH => {
                    let (mut send, mut recv) = conn.accept_bi().await?;
                    let namespace_id_bytes: &[u8] = &recv.read_to_end(32).await?;
                    let namespace_id_bytes: &[u8; 32] = namespace_id_bytes.try_into()?;
                    let namespace = NamespaceId::from(namespace_id_bytes);
                    let docs_client = &self.node.docs;
                    let document = docs_client.open(namespace).await?;
                    if let Some(document) = document {
                        let ticket = document.share(ShareMode::Read).await?;
                        send.write_all(&postcard::to_stdvec(&ticket.to_bytes())?)
                            .await?;
                    }
                }
                _ => Err(OkuDiscoveryError::UnsupportedALPN(alpn.to_string()))?,
            }
        }
        Ok(())
    }

    /// Joins a swarm to fetch the latest version of a replica and save it to the local machine.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to fetch.
    ///
    /// * `partial` - Whether to discover peers who claim to only have a partial copy of the replica.
    ///
    /// * `verified` - Whether to discover peers who have been verified to have the replica.
    ///
    /// * `udp_port` - Optionally-specified UDP port to use for querying the mainline DHT.
    pub async fn get_external_replica(
        &self,
        namespace_id: NamespaceId,
        partial: bool,
        verified: bool,
    ) -> Result<(), Box<dyn Error>> {
        // let discovery_items_stream = self
        //     .discovery_service
        //     .resolve(self.node.magic_endpoint().clone(), node_id);
        // return match discovery_items_stream {
        //     None => None,
        //     Some(discovery_items) => {
        //         pin_mut!(discovery_items);
        //         let node_addrs: Vec<NodeAddr> = discovery_items
        //             .map(|item| NodeAddr {
        //                 node_id,
        //                 info: item.unwrap().addr_info,
        //             })
        //             .collect()
        //             .await;
        //         Some(node_addrs)
        //     }
        // };
        let content = ContentRequest::Hash(Hash::new(namespace_id));
        let secret_key = self.node.magic_endpoint().secret_key();
        let endpoint = MagicEndpoint::builder()
            .alpns(vec![])
            .secret_key(secret_key.clone())
            .discovery(Box::new(self.create_discovery_service().await?))
            .bind(0)
            .await?;
        let bind_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, DISCOVERY_PORT));
        let discovery = UdpDiscovery::new(bind_addr).await?;
        let dht = mainline::Dht::default();
        let q = Query {
            content: content.hash_and_format(),
            flags: QueryFlags {
                complete: !partial,
                verified,
            },
        };
        println!("content corresponds to infohash {}", to_infohash(q.content));

        let stream = discovery.query_dht(dht, q).await?;
        let connections = stream
            .map(move |announce| {
                println!("got announce {:?}", announce);
                let endpoint = endpoint.clone();
                async move {
                    endpoint
                        .connect_by_node_id(&announce.host, ALPN_DOCUMENT_TICKET_FETCH)
                        .await
                }
            })
            .buffer_unordered(4)
            .filter_map(|x| async {
                match x {
                    Ok(x) => Some(x),
                    Err(e) => {
                        eprintln!("error connecting to node: {:?}", e);
                        None
                    }
                }
            });
        tokio::pin!(connections);
        let connection = connections
            .next()
            .await
            .ok_or(OkuDiscoveryError::NoNodesFound)?;
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(&postcard::to_stdvec(namespace_id.as_bytes())?)
            .await?;
        let ticket = DocTicket::from_bytes(&recv.read_to_end(256).await?)?;
        let docs_client = &self.node.docs;
        docs_client.import(ticket).await?;
        Ok(())
    }

    // pub async fn get_external_replica(&self, namespace_id: NamespaceId) -> Result<(), Box<dyn Error>> {
    //     // let providers: Vec<NodeId> =
    //     //     discovery::query_dht(ContentRequest::Hash(Hash::new(namespace)), true, true, None)
    //     //         .await?;
    //     // for provider in providers {
    //     //     let node_addrs = self.discover_node(provider).await;
    //     //     if let Some(node_addrs) = node_addrs {
    //     //         for node_addr in node_addrs {
    //     //             self.node.inner.sync
    //     //         }
    //     //     }
    //     // }
    //     Ok(())
    // }
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
pub fn load_or_create_author() -> Result<Author, Box<dyn Error>> {
    let path = PathBuf::from(FS_PATH).join("author");
    let author_file = std::fs::read(path.clone());
    match author_file {
        Ok(bytes) => Ok(Author::from_bytes(&bytes[..32].try_into()?)),
        Err(_) => {
            let mut rng = OsRng;
            let author = Author::new(&mut rng);
            let author_bytes = author.to_bytes();
            std::fs::write(path, author_bytes)?;
            Ok(author)
        }
    }
}
