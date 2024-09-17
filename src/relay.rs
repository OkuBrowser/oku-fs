use ahash::AHashMap;
use iroh::docs::NamespaceId;
use lazy_static::lazy_static;
use miette::IntoDiagnostic;
use oku_fs::{
    discovery::{
        announce_replica, PeerContentRequest, PeerContentResponse, DISCOVERY_PORT,
        INITIAL_PUBLISH_DELAY, REPUBLISH_DELAY,
    },
    error::OkuRelayError,
    fs::{ALPN_DOCUMENT_TICKET_FETCH, ALPN_INITIAL_RELAY_CONNECTION, ALPN_RELAY_FETCH},
};
use std::{
    error::Error,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
};
use tokio::{io::AsyncReadExt, net::TcpStream};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::RwLock,
};

lazy_static! {
    static ref REPLICAS_BY_NODE: RwLock<AHashMap<NamespaceId, SocketAddr>> =
        RwLock::new(AHashMap::new());
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> miette::Result<()> {
    miette::set_panic_hook();
    // Listen for node connections.
    // 1. A node connects to the relay, creating a new thread.
    // 2. The relay receives a list of replicas held by the node.
    // 3. The connection stays open, and the relay periodically updates the list of replicas held by the node.
    tokio::spawn(async move {
        handle_node_connections().await?;
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });

    // Announce replicas held by connected nodes.
    tokio::spawn(async move {
        loop {
            // let responses = responses.clone();
            tokio::time::sleep(INITIAL_PUBLISH_DELAY).await;
            for replica_by_node in REPLICAS_BY_NODE.read().await.iter() {
                announce_replica(*replica_by_node.0).await?;
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
    tokio::spawn(async move {
        let socket = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, DISCOVERY_PORT);
        let listener = TcpListener::bind(socket).await?;
        loop {
            let (mut stream, _) = listener.accept().await?;
            tokio::spawn(async move {
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
                            let peer_content_request =
                                serde_json::from_str(&peer_content_request_str)?;
                            let peer_content_response =
                                respond_to_content_request(peer_content_request).await?;
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
            });
        }
        #[allow(unreachable_code)]
        Ok::<(), Box<dyn Error + Send + Sync>>(())
    });
    tokio::signal::ctrl_c().await.into_diagnostic()?;
    Ok(())
}

async fn respond_to_content_request(
    peer_content_request: PeerContentRequest,
) -> miette::Result<PeerContentResponse> {
    let replicas_by_node_reader = REPLICAS_BY_NODE.read().await;
    let replica_ip = replicas_by_node_reader
        .get(&peer_content_request.namespace_id)
        .ok_or(OkuRelayError::CannotSatisfyRequest(
            peer_content_request.namespace_id.to_string(),
        ))?;
    let peer_content_request_string =
        serde_json::to_string(&peer_content_request).into_diagnostic()?;
    let mut stream = TcpStream::connect(*replica_ip).await.into_diagnostic()?;
    let mut request = Vec::new();
    request
        .write_all(ALPN_DOCUMENT_TICKET_FETCH)
        .await
        .into_diagnostic()?;
    request.write_all(b"\n").await.into_diagnostic()?;
    request
        .write_all(peer_content_request_string.as_bytes())
        .await
        .into_diagnostic()?;
    request.flush().await.into_diagnostic()?;
    stream.write_all(&request).await.into_diagnostic()?;
    stream.flush().await.into_diagnostic()?;
    let mut response_bytes = Vec::new();
    stream
        .read_to_end(&mut response_bytes)
        .await
        .into_diagnostic()?;
    let response: PeerContentResponse =
        serde_json::from_str(String::from_utf8_lossy(&response_bytes).as_ref())
            .into_diagnostic()?;
    Ok(response)
}

async fn handle_node_connections() -> miette::Result<()> {
    let socket = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, DISCOVERY_PORT);
    let listener = TcpListener::bind(socket).await.into_diagnostic()?;
    loop {
        let (mut stream, _) = listener.accept().await.into_diagnostic()?;
        let node_ip = stream.peer_addr().into_diagnostic()?;
        tokio::spawn(async move {
            let mut buf_reader = BufReader::new(&mut stream);
            let received: Vec<u8> = buf_reader.fill_buf().await?.to_vec();
            buf_reader.consume(received.len());
            let mut incoming_lines = received.split(|x| *x == 10);
            if let Some(first_line) = incoming_lines.next() {
                if first_line == ALPN_INITIAL_RELAY_CONNECTION {
                    let remaining_lines: Vec<Vec<u8>> =
                        incoming_lines.map(|x| x.to_owned()).collect();
                    let replica_list_bytes = remaining_lines.concat();
                    let replica_list_str = String::from_utf8_lossy(&replica_list_bytes).to_string();
                    let replica_list: Vec<NamespaceId> = serde_json::from_str(&replica_list_str)?;
                    for replica in &replica_list {
                        REPLICAS_BY_NODE.write().await.insert(*replica, node_ip);
                    }
                    // Periodically update list while connection remains alive
                    loop {
                        tokio::time::sleep(INITIAL_PUBLISH_DELAY).await;
                        // Clear out list for this node
                        REPLICAS_BY_NODE.write().await.retain(|_k, v| *v != node_ip);
                        // Get updated list from node
                        stream.write_all(ALPN_RELAY_FETCH).await?;
                        stream.flush().await?;
                        let mut response_bytes = Vec::new();
                        stream.read_to_end(&mut response_bytes).await?;
                        let response: Vec<NamespaceId> = serde_json::from_str(
                            String::from_utf8_lossy(&response_bytes).as_ref(),
                        )?;
                        for replica in &response {
                            REPLICAS_BY_NODE.write().await.insert(*replica, node_ip);
                        }
                        tokio::time::sleep(REPUBLISH_DELAY - INITIAL_PUBLISH_DELAY).await;
                    }
                }
            }
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        });
    }
}
