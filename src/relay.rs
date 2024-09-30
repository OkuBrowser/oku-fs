use ahash::AHashMap;
use clap::Parser;
use env_logger::Builder;
use futures::{SinkExt, TryStreamExt};
use iroh::docs::{CapabilityKind, DocTicket, NamespaceId};
use lazy_static::lazy_static;
use log::{error, info, LevelFilter};
use miette::IntoDiagnostic;
use oku_fs::{
    discovery::{
        announce_replica, PeerContentRequest, PeerContentResponse, RelayFetchRequest,
        ResponseToRelay, ALPN_DOCUMENT_TICKET_FETCH, ALPN_RELAY_LIST, DISCOVERY_PORT,
        INITIAL_PUBLISH_DELAY, REPUBLISH_DELAY,
    },
    error::OkuRelayError,
    fs::merge_tickets,
};
use std::{
    error::Error,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::Arc,
};
use tokio::{net::TcpListener, sync::RwLock};
use tokio::{
    net::TcpStream,
    sync::{Mutex, Notify},
    task::JoinSet,
};
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

static INITIAL_CONNECTION_NOTIFY: Notify = Notify::const_new();

lazy_static! {
    static ref NODES_BY_REPLICA: RwLock<AHashMap<NamespaceId, Vec<SocketAddr>>> =
        RwLock::new(AHashMap::new());
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// The level of log output; warnings, information, debugging messages, and trace logs.
    #[arg(short, long, action = clap::ArgAction::Count, default_value_t = 2)]
    verbosity: u8,
    /// The port to receive new node connections on.
    #[arg(short, long, default_value_t = 4939)]
    port: u16,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> miette::Result<()> {
    miette::set_panic_hook();
    let cli = Cli::parse();

    let verbosity_level = match cli.verbosity {
        0 => LevelFilter::Error,
        1 => LevelFilter::Warn,
        2 => LevelFilter::Info,
        3 => LevelFilter::Debug,
        4 => LevelFilter::Trace,
        _ => LevelFilter::Trace,
    };
    let mut builder = Builder::new();
    builder.filter(Some("oku_fs_relay"), verbosity_level);
    builder.format_module_path(false);
    if cli.verbosity >= 3 {
        builder.format_module_path(true);
    }
    builder.init();

    let mut spawn_handles = JoinSet::new();

    // Listen for node connections.
    // 1. A node connects to the relay, creating a new thread.
    // 2. The relay receives a list of replicas held by the node.
    // 3. The connection stays open, and the relay periodically updates the list of replicas held by the node.
    spawn_handles.spawn(async move {
        handle_node_connections(cli.port).await?;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });

    // Announce replicas held by connected nodes.
    spawn_handles.spawn(async move {
        INITIAL_CONNECTION_NOTIFY.notified().await;
        loop {
            tokio::time::sleep(INITIAL_PUBLISH_DELAY).await;
            for (replica, nodes) in NODES_BY_REPLICA.read().await.iter() {
                info!(
                    "Announcing replica {} held by nodes {:?} … ",
                    replica.to_string(),
                    nodes
                );
                announce_replica(*replica).await?;
            }
            tokio::time::sleep(REPUBLISH_DELAY - INITIAL_PUBLISH_DELAY).await;
        }
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });

    // Listen for content requests from external nodes.
    // 1. Get request from external node.
    // 2. Find IP of node that can satisfy this request.
    // 3. Pass the request to the node behind NAT and get its response.
    // 4. Pass the response to the external node.
    spawn_handles.spawn(async move {
        let socket = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, DISCOVERY_PORT);
        let listener = TcpListener::bind(socket).await?;
        info!("Listening for content requests on {} … ", socket);
        loop {
            let (stream, peer) = listener.accept().await?;
            tokio::spawn(async move {
                let length_delimited = Framed::new(stream, LengthDelimitedCodec::new());
                let mut deserialized = tokio_serde::SymmetricallyFramed::new(
                    length_delimited,
                    SymmetricalJson::<PeerContentRequest>::default(),
                );
                if let Some(peer_content_request) = deserialized.try_next().await? {
                    if peer_content_request.alpn == ALPN_DOCUMENT_TICKET_FETCH {
                        let peer_content_response =
                            respond_to_content_request(peer_content_request).await?;
                        info!(
                            "Responding to peer {}: {:#?} … ",
                            peer, peer_content_response
                        );
                        let mut serialized = tokio_serde::SymmetricallyFramed::new(
                            deserialized.into_inner(),
                            SymmetricalJson::<PeerContentResponse>::default(),
                        );
                        serialized.send(peer_content_response).await?;
                    }
                }
                Ok::<(), Box<dyn Error + Send + Sync>>(())
            });
        }
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });

    tokio::signal::ctrl_c().await.into_diagnostic()?;
    spawn_handles.shutdown().await;

    Ok(())
}

async fn respond_to_content_request(
    peer_content_request: PeerContentRequest,
) -> miette::Result<PeerContentResponse> {
    let nodes_by_replica_reader = NODES_BY_REPLICA.read().await;
    let nodes = nodes_by_replica_reader
        .get(&peer_content_request.namespace_id)
        .ok_or(OkuRelayError::CannotSatisfyRequest(
            peer_content_request.namespace_id.to_string(),
        ))?;
    info!(
        "Available nodes for {}: {:?}",
        peer_content_request.namespace_id.to_string(),
        nodes
    );
    let tickets: Arc<Mutex<Vec<DocTicket>>> = Arc::new(Mutex::new(Vec::new()));
    let content_sizes = Arc::new(Mutex::new(Vec::new()));
    let mut ticket_request_handles = JoinSet::new();
    for node in nodes.to_vec() {
        let tickets = tickets.clone();
        let content_sizes = content_sizes.clone();
        let peer_content_request = peer_content_request.clone();
        ticket_request_handles.spawn(async move {
            info!(
                "Requesting ticket for {} from {} … ",
                peer_content_request.namespace_id.to_string(),
                node
            );
            let stream = TcpStream::connect(node).await.into_diagnostic()?;
            let length_delimited = Framed::new(stream, LengthDelimitedCodec::new());
            let mut serialized = tokio_serde::SymmetricallyFramed::new(
                length_delimited,
                SymmetricalJson::<PeerContentRequest>::default(),
            );
            serialized.send(peer_content_request).await?;
            let mut deserialized = tokio_serde::SymmetricallyFramed::new(
                serialized.into_inner(),
                SymmetricalJson::<PeerContentResponse>::default(),
            );
            let response = deserialized.try_next().await?.ok_or(miette::miette!(
                "Did not receive peer content response from {} … ",
                node
            ))?;
            info!("Ticket received from {}: {:#?}", node, response);
            match response.ticket_response {
                oku_fs::discovery::PeerTicketResponse::Document(ticket) => {
                    match tickets.try_lock() {
                        Ok(mut tickets) => tickets.push(ticket),
                        Err(e) => error!("{}", e),
                    }
                    match content_sizes.try_lock() {
                        Ok(mut content_sizes) => content_sizes.push(response.content_size),
                        Err(e) => error!("{}", e),
                    }
                }
            }
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        });
    }
    ticket_request_handles.join_all().await;
    let tickets = tickets.lock().await.to_vec();
    let content_sizes = content_sizes.lock().await.clone();
    Ok(PeerContentResponse {
        alpn: ALPN_DOCUMENT_TICKET_FETCH.to_string(),
        ticket_response: oku_fs::discovery::PeerTicketResponse::Document(
            merge_tickets(tickets).ok_or(miette::miette!(
                "No tickets to merge for {}",
                peer_content_request.namespace_id.to_string()
            ))?,
        ),
        content_size: *content_sizes.iter().max().unwrap_or(&u64::MAX),
    })
}

async fn handle_node_connections(port: u16) -> miette::Result<()> {
    let socket = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    let listener = TcpListener::bind(socket).await.into_diagnostic()?;
    info!("Listening for node connections on {} … ", socket);
    loop {
        let (stream, node_ip) = listener.accept().await.into_diagnostic()?;
        let length_delimited = Framed::new(stream, LengthDelimitedCodec::new());
        info!("Node {} connecting … ", node_ip);
        tokio::spawn(async move {
            let mut deserialized = tokio_serde::SymmetricallyFramed::new(
                length_delimited,
                SymmetricalJson::<ResponseToRelay>::default(),
            );
            if let Some(response_to_relay) = deserialized.try_next().await? {
                if response_to_relay.alpn == ALPN_RELAY_LIST {
                    let replica_list: Vec<(NamespaceId, CapabilityKind)> =
                        response_to_relay.replicas;
                    info!(
                        "Node {} has connected, has replicas: {:?}",
                        node_ip, replica_list
                    );
                    // Add this node to replica-node mappings upon initial connection
                    for (replica, _capability_kind) in &replica_list {
                        let mut nodes_by_replica_writer = NODES_BY_REPLICA.write().await;
                        let nodes_list = nodes_by_replica_writer.get_mut(replica);
                        match nodes_list {
                            Some(nodes_list) => {
                                nodes_list.push(node_ip);
                            }
                            None => {
                                nodes_by_replica_writer.insert(*replica, vec![node_ip]);
                            }
                        }
                    }
                    INITIAL_CONNECTION_NOTIFY.notify_one();
                    tokio::time::sleep(INITIAL_PUBLISH_DELAY).await;
                    // Periodically update node in mappings while connection remains alive
                    let mut serialized = tokio_serde::SymmetricallyFramed::new(
                        deserialized.into_inner(),
                        SymmetricalJson::<RelayFetchRequest>::default(),
                    );
                    loop {
                        tokio::time::sleep(INITIAL_PUBLISH_DELAY).await;
                        // Remove this node from mappings
                        NODES_BY_REPLICA
                            .write()
                            .await
                            .values_mut()
                            .for_each(|nodes| nodes.retain(|node| *node != node_ip));
                        // Get updated list from node
                        serialized.send(RelayFetchRequest::new()).await?;
                        info!("Requesting new replica list from node {} … ", node_ip);
                        let mut deserialized = tokio_serde::SymmetricallyFramed::new(
                            serialized.into_inner(),
                            SymmetricalJson::<ResponseToRelay>::default(),
                        );
                        let response = deserialized
                            .try_next()
                            .await?
                            .ok_or(miette::miette!("No response to relay from {} … ", node_ip))?;
                        serialized = tokio_serde::SymmetricallyFramed::new(
                            deserialized.into_inner(),
                            SymmetricalJson::<RelayFetchRequest>::default(),
                        );
                        let replica_list = response.replicas;
                        info!(
                            "Refreshing replica list of node {}, has replicas: {:?}",
                            node_ip, replica_list
                        );
                        // Re-insert node in mappings
                        for (replica, _capability_kind) in &replica_list {
                            let mut nodes_by_replica_writer = NODES_BY_REPLICA.write().await;
                            let nodes_list = nodes_by_replica_writer.get_mut(replica);
                            match nodes_list {
                                Some(nodes_list) => {
                                    nodes_list.push(node_ip);
                                }
                                None => {
                                    nodes_by_replica_writer.insert(*replica, vec![node_ip]);
                                }
                            }
                        }
                        tokio::time::sleep(REPUBLISH_DELAY - INITIAL_PUBLISH_DELAY).await;
                    }
                }
            }
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        });
    }
}
