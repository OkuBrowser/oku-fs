use crate::database::core::DATABASE;
use crate::{error::OkuDiscoveryError, fs::OkuFs};
use iroh_base::ticket::Ticket;
use iroh_blobs::HashAndFormat;
use iroh_docs::rpc::client::docs::ShareMode;
use iroh_docs::sync::CapabilityKind;
use iroh_docs::NamespaceId;
use log::{error, info};
use miette::IntoDiagnostic;
use std::{path::PathBuf, time::Duration};
use tokio::task::JoinSet;

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
    pub async fn announce_mutable_replica(
        &self,
        namespace_id: &NamespaceId,
    ) -> miette::Result<NamespaceId> {
        let ticket = self
            .create_document_ticket(namespace_id, &ShareMode::Read)
            .await?
            .to_bytes();
        let newest_timestamp = self
            .get_newest_timestamp_in_folder(namespace_id, &PathBuf::from("/"))
            .await? as i64;
        let replica_private_key = mainline::SigningKey::from_bytes(
            &self
                .create_document_ticket(namespace_id, &ShareMode::Write)
                .await?
                .capability
                .secret_key()
                .into_diagnostic()?
                .to_bytes(),
        );
        let mutable_item =
            mainline::MutableItem::new(replica_private_key, &ticket, newest_timestamp, None);
        match self.dht.put_mutable(mutable_item, None).await {
            Ok(_) => info!(
                "Announced mutable replica {} … ",
                crate::fs::util::fmt(namespace_id)
            ),
            Err(e) => error!(
                "{}",
                OkuDiscoveryError::ProblemAnnouncingContent(
                    crate::fs::util::fmt(namespace_id),
                    e.to_string()
                )
            ),
        }
        Ok(*namespace_id)
    }

    /// Announces a read-only replica to the Mainline DHT.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to announce.
    pub async fn announce_immutable_replica(
        &self,
        namespace_id: &NamespaceId,
    ) -> miette::Result<NamespaceId> {
        let public_key_bytes = namespace_id
            .into_public_key()
            .map_err(|e| miette::miette!("{}", e))?
            .as_bytes()
            .to_vec();
        let announcement = DATABASE
            .get_announcement(&public_key_bytes)
            .ok()
            .flatten()
            .ok_or(miette::miette!(
                "Prior announcement not found in database for replica {} … ",
                crate::fs::util::fmt(namespace_id)
            ))?;

        let ticket = self
            .create_document_ticket(namespace_id, &ShareMode::Read)
            .await?
            .to_bytes();
        let newest_timestamp = self
            .get_newest_timestamp_in_folder(namespace_id, &PathBuf::from("/"))
            .await? as i64;
        let mutable_item = mainline::MutableItem::new_signed_unchecked(
            announcement.key.try_into().map_err(|_e| {
                miette::miette!("Replica announcement key does not fit into 32 bytes … ")
            })?,
            announcement.signature.try_into().map_err(|_e| {
                miette::miette!("Replica announcement signature does not fit into 64 bytes … ")
            })?,
            &ticket,
            newest_timestamp,
            None,
        );
        match self.dht.put_mutable(mutable_item, None).await {
            Ok(_) => info!(
                "Announced immutable replica {} … ",
                crate::fs::util::fmt(namespace_id)
            ),
            Err(e) => error!(
                "{}",
                OkuDiscoveryError::ProblemAnnouncingContent(
                    crate::fs::util::fmt(namespace_id),
                    e.to_string()
                )
            ),
        }
        Ok(*namespace_id)
    }

    /// Announces a replica to the Mainline DHT.
    ///
    /// # Arguments
    ///
    /// * `namespace_id` - The ID of the replica to announce.
    ///
    /// * `capability_kind` - Whether the replica is writable by the current node or read-only.
    pub async fn announce_replica(
        &self,
        namespace_id: &NamespaceId,
        capability_kind: &CapabilityKind,
    ) -> miette::Result<NamespaceId> {
        match capability_kind {
            CapabilityKind::Read => self.announce_immutable_replica(namespace_id).await,
            CapabilityKind::Write => self.announce_mutable_replica(namespace_id).await,
        }
    }

    /// Announce the home replica
    pub async fn announce_home_replica(&self) -> miette::Result<NamespaceId> {
        let home_replica = self
            .home_replica()
            .await
            .ok_or(miette::miette!("No home replica set … "))?;
        let ticket = self
            .create_document_ticket(&home_replica, &ShareMode::Read)
            .await?
            .to_bytes();
        let newest_timestamp = self
            .get_newest_timestamp_in_folder(&home_replica, &PathBuf::from("/"))
            .await? as i64;
        let author_private_key = mainline::SigningKey::from_bytes(
            &self
                .get_author()
                .await
                .map_err(|e| miette::miette!("{}", e))?
                .to_bytes(),
        );
        let mutable_item =
            mainline::MutableItem::new(author_private_key, &ticket, newest_timestamp, None);
        match self.dht.put_mutable(mutable_item, None).await {
            Ok(_) => info!(
                "Announced home replica {} … ",
                crate::fs::util::fmt(home_replica)
            ),
            Err(e) => error!(
                "{}",
                OkuDiscoveryError::ProblemAnnouncingContent(
                    crate::fs::util::fmt(home_replica),
                    e.to_string()
                )
            ),
        }
        Ok(home_replica)
    }

    /// Announces all writable replicas to the Mainline DHT.
    pub async fn announce_replicas(&self) -> miette::Result<()> {
        let mut future_set = JoinSet::new();

        // Prepare to announce home replica
        let self_clone = self.clone();
        future_set.spawn(async move { self_clone.announce_home_replica().await });

        // Prepare to announce all replicas
        let replicas = self.list_replicas().await?;
        for (replica, capability_kind) in replicas {
            let self_clone = self.clone();
            future_set.spawn(async move {
                self_clone
                    .announce_replica(&replica, &capability_kind)
                    .await
            });
        }
        info!("Pending announcements: {} … ", future_set.len());
        // Execute announcements in parallel
        while let Some(res) = future_set.join_next().await {
            match res {
                Ok(result) => match result {
                    Ok(_) => (),
                    Err(e) => error!("{}", e),
                },
                Err(e) => error!("{}", e),
            }
        }

        Ok(())
    }
}

/// From: <https://github.com/n0-computer/iroh-experiments/blob/4e052c6b34720e26683083270706926a84e49411/content-discovery/iroh-mainline-content-discovery/src/client.rs#L53>
///
/// The mapping from an iroh [HashAndFormat] to a bittorrent infohash, aka [mainline::Id].
///
/// Since an infohash is just 20 bytes, this can not be a bidirectional mapping.
pub fn to_infohash(haf: &HashAndFormat) -> mainline::Id {
    let mut data = [0u8; 20];
    data.copy_from_slice(&haf.hash.as_bytes()[..20]);
    mainline::Id::from_bytes(data).unwrap()
}
