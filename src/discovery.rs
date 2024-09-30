use crate::{error::OkuDiscoveryError, fs::OkuFs};
use iroh::base::ticket::Ticket;
use iroh::{client::docs::ShareMode, docs::NamespaceId};
use log::{error, info};
use miette::IntoDiagnostic;
use std::{path::PathBuf, time::Duration};

/// The delay between republishing content to the mainline DHT.
pub const REPUBLISH_DELAY: Duration = Duration::from_secs(60 * 60);

/// The initial delay before publishing content to the mainline DHT.
pub const INITIAL_PUBLISH_DELAY: Duration = Duration::from_millis(500);

impl OkuFs {
    /// Announces a local replica to the mainline DHT.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to announce.
    pub async fn announce_replica(&self, namespace_id: NamespaceId) -> miette::Result<()> {
        let dht = mainline::Dht::server().into_diagnostic()?.as_async();
        let ticket = mainline::Bytes::from(
            self.create_document_ticket(namespace_id.clone(), ShareMode::Read)
                .await?
                .to_bytes(),
        );
        let newest_timestamp = self
            .get_newest_timestamp_in_folder(namespace_id, PathBuf::from("/"))
            .await? as i64;
        let replica_private_key = mainline::SigningKey::from_bytes(
            &self
                .create_document_ticket(namespace_id.clone(), ShareMode::Read)
                .await?
                .capability
                .secret_key()
                .into_diagnostic()?
                .to_bytes(),
        );
        let mutable_item =
            mainline::MutableItem::new(replica_private_key, ticket, newest_timestamp, None);
        match dht.put_mutable(mutable_item).await {
            Ok(_) => info!("Announced replica {} … ", namespace_id.to_string()),
            Err(e) => error!(
                "{}",
                OkuDiscoveryError::ProblemAnnouncingContent(
                    namespace_id.to_string(),
                    e.to_string()
                )
            ),
        }
        Ok(())
    }
}
