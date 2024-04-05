use crate::error::OkuDiscoveryError;

use iroh::{
    bytes::{Hash, HashAndFormat},
    net::NodeId,
    ticket::BlobTicket,
};
use iroh_mainline_content_discovery::protocol::{Query, QueryFlags};
use iroh_mainline_content_discovery::to_infohash;
use iroh_mainline_content_discovery::UdpDiscovery;

use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::{error::Error, str::FromStr};

/*
The `ContentRequest` enum is derived from the `ContentArg` enum in the `iroh-examples` repository (https://github.com/n0-computer/iroh-examples/blob/6f184933efa72eec1d8cf2e8d07905650c0fdb46/content-discovery/iroh-mainline-content-discovery-cli/src/args.rs#L23).
*/
#[derive(Debug, Clone, derive_more::From)]
/// A request for content, which can be a raw hash, a hash and format pair, or a blob ticket.
pub enum ContentRequest {
    /// A raw hash.
    Hash(Hash),
    /// A hash and format pair.
    HashAndFormat(HashAndFormat),
    /// A blob ticket.
    Ticket(BlobTicket),
}

impl ContentRequest {
    /// Get the hash and format pair for this content request.
    pub fn hash_and_format(&self) -> HashAndFormat {
        match self {
            ContentRequest::Hash(hash) => HashAndFormat::raw(*hash),
            ContentRequest::HashAndFormat(haf) => *haf,
            ContentRequest::Ticket(ticket) => HashAndFormat {
                hash: ticket.hash(),
                format: ticket.format(),
            },
        }
    }
    pub fn hash(&self) -> Hash {
        match self {
            ContentRequest::Hash(hash) => *hash,
            ContentRequest::HashAndFormat(haf) => haf.hash,
            ContentRequest::Ticket(ticket) => ticket.hash(),
        }
    }
}

impl FromStr for ContentRequest {
    type Err = Box<dyn Error>;
    fn from_str(s: &str) -> Result<Self, Box<dyn Error>> {
        if let Ok(hash) = Hash::from_str(s) {
            Ok(hash.into())
        } else if let Ok(haf) = HashAndFormat::from_str(s) {
            Ok(haf.into())
        } else if let Ok(ticket) = BlobTicket::from_str(s) {
            Ok(ticket.into())
        } else {
            Err(OkuDiscoveryError::InvalidHashAndFormat.into())
        }
    }
}

pub async fn query_dht(
    content: ContentRequest,
    partial: bool,
    verified: bool,
    udp_port: Option<u16>,
) -> Result<(), Box<dyn Error>> {
    let _providers: Vec<NodeId> = Vec::new();
    let bind_addr = SocketAddr::V4(SocketAddrV4::new(
        Ipv4Addr::UNSPECIFIED,
        udp_port.unwrap_or_default(),
    ));
    let discovery = UdpDiscovery::new(bind_addr).await?;
    let dht = mainline::Dht::default();
    let q = Query {
        content: content.hash_and_format(),
        flags: QueryFlags {
            complete: !partial,
            verified: verified,
        },
    };
    println!("content corresponds to infohash {}", to_infohash(q.content));

    let _stream = discovery.query_dht(dht, q).await?;
    // while let Some(announce) = stream.next().await {
    //     if announce.verify().is_ok() {
    //         println!("found verified provider {}", announce.host);
    //         providers.push(announce.host);
    //     } else {
    //         println!("got wrong signed announce!");
    //     }
    // }
    // Ok(providers)

    Ok(())
}
