use crate::error::OkuDiscoveryError;
use futures::StreamExt;
use iroh::{
    base::hash::{Hash, HashAndFormat},
    base::ticket::BlobTicket,
    docs::{DocTicket, NamespaceId},
};
use iroh_mainline_content_discovery::announce_dht;
use log::error;
use miette::IntoDiagnostic;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, error::Error, str::FromStr, time::Duration};

/// The delay between republishing content to the mainline DHT.
pub const REPUBLISH_DELAY: Duration = Duration::from_secs(60 * 60);

/// The initial delay before publishing content to the mainline DHT.
pub const INITIAL_PUBLISH_DELAY: Duration = Duration::from_millis(500);

/// The port used for communication between other Oku filesystem nodes.
pub const DISCOVERY_PORT: u16 = 4938;

/// The number of parallel announcements to make to the mainline DHT.
pub const ANNOUNCE_PARALLELISM: usize = 10;

/// Announces a local replica to the mainline DHT.
///
/// # Arguments
///
/// * `namespace_id` - The ID of the replica to announce.
pub async fn announce_replica(namespace_id: NamespaceId) -> miette::Result<()> {
    let mut content = BTreeSet::new();
    content.insert(HashAndFormat::raw(Hash::new(namespace_id)));
    let dht = mainline::Dht::server().into_diagnostic()?;
    let announce_stream = announce_dht(dht, content, DISCOVERY_PORT, ANNOUNCE_PARALLELISM);
    tokio::pin!(announce_stream);
    while let Some((content, res)) = announce_stream.next().await {
        match res {
            Ok(_) => {}
            Err(e) => error!(
                "{}",
                OkuDiscoveryError::ProblemAnnouncingContent(content.to_string(), e.to_string())
            ),
        }
    }
    Ok(())
}

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
    /// Get the hash for this content request.
    pub fn hash(&self) -> Hash {
        match self {
            ContentRequest::Hash(hash) => *hash,
            ContentRequest::HashAndFormat(haf) => haf.hash,
            ContentRequest::Ticket(ticket) => ticket.hash(),
        }
    }
}

impl FromStr for ContentRequest {
    type Err = Box<dyn Error + Send + Sync>;
    fn from_str(s: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
/// A content ticket sent in response to a peer requesting content.
pub enum PeerTicketResponse {
    /// A ticket pointing to a replica.
    Document(DocTicket),
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Hash)]
/// A request for content from a peer.
pub struct PeerContentRequest {
    /// The ID of a requested replica.
    pub namespace_id: NamespaceId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
/// A response to a peer requesting content.
pub struct PeerContentResponse {
    /// A ticket satisfying the content request.
    pub ticket_response: PeerTicketResponse,
    /// The size, in bytes, of the requested content.
    pub content_size: u64,
}
