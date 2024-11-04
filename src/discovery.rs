use crate::{error::OkuDiscoveryError, fs::OkuFs};
use iroh::base::ticket::Ticket;
use iroh::docs::CapabilityKind;
use iroh::{client::docs::ShareMode, docs::NamespaceId};
use log::{error, info};
use miette::IntoDiagnostic;
use std::{path::PathBuf, time::Duration};

/// The delay between republishing content to the Mainline DHT.
pub const REPUBLISH_DELAY: Duration = Duration::from_secs(60 * 60);

/// The initial delay before publishing content to the Mainline DHT.
pub const INITIAL_PUBLISH_DELAY: Duration = Duration::from_millis(500);

impl OkuFs {
    /// Announces a writable replica to the Mainline DHT.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to announce.
    pub async fn announce_replica(&self, namespace_id: NamespaceId) -> miette::Result<()> {
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
                .create_document_ticket(namespace_id.clone(), ShareMode::Write)
                .await?
                .capability
                .secret_key()
                .into_diagnostic()?
                .to_bytes(),
        );
        let mutable_item =
            mainline::MutableItem::new(replica_private_key, ticket, newest_timestamp, None);
        match self.dht.put_mutable(mutable_item).await {
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

    /// Announce the home replica
    pub async fn announce_home_replica(&self) -> miette::Result<NamespaceId> {
        let home_replica = self
            .home_replica()
            .await
            .ok_or(miette::miette!("No home replica set … "))?;
        let ticket = mainline::Bytes::from(
            self.create_document_ticket(home_replica.clone(), ShareMode::Read)
                .await?
                .to_bytes(),
        );
        let newest_timestamp = self
            .get_newest_timestamp_in_folder(home_replica, PathBuf::from("/"))
            .await? as i64;
        let author_private_key = mainline::SigningKey::from_bytes(
            &self
                .get_author()
                .await
                .map_err(|e| miette::miette!("{}", e))?
                .to_bytes(),
        );
        let mutable_item =
            mainline::MutableItem::new(author_private_key, ticket, newest_timestamp, None);
        match self.dht.put_mutable(mutable_item).await {
            Ok(_) => info!("Announced home replica {} … ", home_replica.to_string()),
            Err(e) => error!(
                "{}",
                OkuDiscoveryError::ProblemAnnouncingContent(
                    home_replica.to_string(),
                    e.to_string()
                )
            ),
        }
        Ok(home_replica)
    }

    /// Announces all writable replicas to the Mainline DHT.
    pub async fn announce_replicas(&self) -> miette::Result<()> {
        let mut replicas = self.list_replicas().await?;
        replicas
            .retain(|(_replica, capability_kind)| matches!(capability_kind, CapabilityKind::Write));

        if let Ok(home_replica) = self.announce_home_replica().await {
            replicas.retain(|(replica, _capability_kind)| *replica != home_replica);
        }

        for (replica, _capability_kind) in replicas {
            self.announce_replica(replica).await?;
        }
        Ok(())
    }
}
