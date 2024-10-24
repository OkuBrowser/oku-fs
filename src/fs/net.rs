use std::{path::PathBuf, time::SystemTime};

use crate::{
    config::OkuFsConfig,
    database::{OkuIdentity, OkuNote, OkuPost, OkuUser, DATABASE},
    discovery::REPUBLISH_DELAY,
    fs::{merge_tickets, OkuFs},
};
use anyhow::anyhow;
use futures::StreamExt;
use iroh::{
    base::ticket::Ticket,
    blobs::Hash,
    docs::{AuthorId, CapabilityKind, DocTicket, NamespaceId},
};
use miette::IntoDiagnostic;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use url::Url;

impl OkuFs {
    /// Retrieve the content authorship ID used by the node.
    ///
    /// # Returns
    ///
    /// The content authorship ID used by the node.
    pub async fn default_author(&self) -> anyhow::Result<AuthorId> {
        self.node.authors().default().await
    }

    /// Retrieve the home replica of the Oku user.
    ///
    /// # Returns
    ///
    /// The home replica of the Oku user.
    pub async fn home_replica(&self) -> Option<NamespaceId> {
        let config = OkuFsConfig::load_or_create_config().ok()?;
        let home_replica = config.home_replica().ok().flatten()?;
        let home_replica_capability = self.get_replica_capability(home_replica).await.ok()?;
        match home_replica_capability {
            CapabilityKind::Write => Some(home_replica),
            CapabilityKind::Read => None,
        }
    }

    /// Set the home replica of the Oku user.
    ///
    /// # Arguments
    ///
    /// * `home_replica` - The ID of the intended new home replica.
    pub fn set_home_replica(&self, home_replica: Option<NamespaceId>) -> miette::Result<()> {
        let config = OkuFsConfig::load_or_create_config()?;
        config.set_home_replica(home_replica)?;
        config.save()?;
        self.replica_sender.send_replace(());
        Ok(())
    }

    /// Create or modify an OkuNet post in the user's home replica.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to create, or modify, the post at; a suggested path is generated if none is provided.
    ///
    /// * `url` - The URL the post is regarding.
    ///
    /// * `title` - The title of the post.
    ///
    /// * `body` - The body of the post.
    ///
    /// * `tags` - A list of tags associated with the post.
    ///
    /// # Returns
    ///
    /// A hash of the post's content.
    pub async fn create_or_modify_post(
        &self,
        path: Option<PathBuf>,
        url: Url,
        title: String,
        body: String,
        tags: Vec<String>,
    ) -> miette::Result<Hash> {
        let home_replica_id = self
            .home_replica()
            .await
            .ok_or(miette::miette!("No home replica set … "))?;
        let new_note = OkuNote {
            url: url,
            title: title,
            body: body,
            tags: tags,
        };
        let post_path = match path {
            Some(given_path) => given_path,
            None => new_note.suggested_post_path().into(),
        };
        self.create_or_modify_file(
            home_replica_id,
            post_path,
            toml::to_string_pretty(&new_note).into_diagnostic()?,
        )
        .await
    }

    /// Delete an OkuNet post in the user's home replica.
    ///
    /// # Arguments
    ///
    /// * `path` - A path to a post in the user's home replica.
    ///
    /// # Returns
    ///
    /// The number of entries deleted in the replica, which should be 1 if the file was successfully deleted.
    pub async fn delete_post(&self, path: PathBuf) -> miette::Result<usize> {
        let home_replica_id = self
            .home_replica()
            .await
            .ok_or(miette::miette!("No home replica set … "))?;
        self.delete_file(home_replica_id, path).await
    }

    /// Updates the local database of users; the users one is following, and the users they're following, are recorded locally.
    /// Blocked users are ignored.
    pub async fn update_users(&self) -> miette::Result<()> {
        let this_user = self
            .get_or_fetch_user(
                self.default_author()
                    .await
                    .map_err(|e| miette::miette!("{}", e))?,
            )
            .await?;
        if let Some(identity) = this_user.identity {
            for followed_user_id in identity
                .following
                .iter()
                .filter(|x| !identity.blocked.contains(x))
            {
                let followed_user = self.get_or_fetch_user(*followed_user_id).await?;
                if let Some(followed_user_identity) = followed_user.identity {
                    for followed_followed_user_id in followed_user_identity
                        .following
                        .iter()
                        .filter(|x| !identity.blocked.contains(x))
                    {
                        self.get_or_fetch_user(*followed_followed_user_id).await?;
                    }
                }
            }
            let blocked_users: Vec<OkuUser> = identity
                .blocked
                .par_iter()
                .filter_map(|x| DATABASE.get_user(*x).ok().flatten())
                .collect();
            DATABASE.delete_users_with_posts(blocked_users)?;
        }
        Ok(())
    }

    /// Use the mainline DHT to obtain a ticket for the home replica of the user with the given content authorship ID.
    ///
    /// # Arguments
    ///
    /// * `author_id` - A content authorship ID.
    ///
    /// # Returns
    ///
    /// A ticket for the home replica of the user with the given content authorship ID.
    pub async fn resolve_author_id(&self, author_id: AuthorId) -> anyhow::Result<DocTicket> {
        let get_stream = self.dht.get_mutable(author_id.as_bytes(), None, None)?;
        tokio::pin!(get_stream);
        let mut tickets = Vec::new();
        while let Some(mutable_item) = get_stream.next().await {
            tickets.push(DocTicket::from_bytes(mutable_item.value())?)
        }
        merge_tickets(tickets).ok_or(anyhow!(
            "Could not find tickets for {} … ",
            author_id.to_string()
        ))
    }

    /// Join a swarm to fetch the latest version of a home replica and obtain the OkuNet identity within it.
    ///
    /// # Arguments
    ///
    /// * `author_id` - A content authorship ID.
    ///
    /// # Returns
    ///
    /// The OkuNet identity within the home replica of the user with the given content authorship ID.
    pub async fn fetch_profile(&self, author_id: AuthorId) -> miette::Result<OkuIdentity> {
        let ticket = self
            .resolve_author_id(author_id)
            .await
            .map_err(|e| miette::miette!("{}", e))?;
        match self
            .fetch_file_with_ticket(ticket, "/profile.toml".into())
            .await
        {
            Ok(profile_bytes) => Ok(toml::from_str(
                &String::from_utf8_lossy(&profile_bytes).to_string(),
            )
            .into_diagnostic()?),
            Err(e) => Err(miette::miette!("{}", e)),
        }
    }

    /// Join a swarm to fetch the latest version of a home replica and obtain the OkuNet posts within it.
    ///
    /// # Arguments
    ///
    /// * `author_id` - A content authorship ID.
    ///
    /// # Returns
    ///
    /// The OkuNet posts within the home replica of the user with the given content authorship ID.
    pub async fn fetch_posts(&self, author_id: AuthorId) -> miette::Result<Vec<OkuPost>> {
        let ticket = self
            .resolve_author_id(author_id)
            .await
            .map_err(|e| miette::miette!("{}", e))?;
        match self
            .fetch_directory_with_ticket(ticket, "/posts".into())
            .await
        {
            Ok(post_files) => Ok(post_files
                .par_iter()
                .filter_map(|(entry, bytes)| {
                    toml::from_str::<OkuNote>(&String::from_utf8_lossy(bytes).to_string())
                        .ok()
                        .map(|x| OkuPost {
                            entry: entry.clone(),
                            note: x,
                        })
                })
                .collect()),
            Err(e) => Err(miette::miette!("{}", e)),
        }
    }

    /// Obtain an OkuNet user's content, identified by their content authorship ID.
    ///
    /// If last retrieved longer than [`REPUBLISH_DELAY`](crate::discovery::REPUBLISH_DELAY) ago according to the system time, a known user's content will be re-fetched.
    ///
    /// # Arguments
    ///
    /// * `author_id` - A content authorship ID.
    ///
    /// # Returns
    ///
    /// An OkuNet user's content.
    pub async fn get_or_fetch_user(&self, author_id: AuthorId) -> miette::Result<OkuUser> {
        match DATABASE.get_user(author_id)? {
            Some(user) => {
                match SystemTime::now()
                    .duration_since(user.last_fetched)
                    .into_diagnostic()?
                    > REPUBLISH_DELAY
                {
                    true => self.fetch_user(author_id).await,
                    false => Ok(user),
                }
            }
            None => self.fetch_user(author_id).await,
        }
    }

    /// Fetch the latest version of an OkuNet user's content, identified by their content authorship ID.
    ///
    /// # Arguments
    ///
    /// * `author_id` - A content authorship ID.
    ///
    /// # Returns
    ///
    /// The latest version of an OkuNet user's content.
    pub async fn fetch_user(&self, author_id: AuthorId) -> miette::Result<OkuUser> {
        let profile = self.fetch_profile(author_id).await.ok();
        let posts = self.fetch_posts(author_id).await.ok();
        if let Some(posts) = posts.clone() {
            DATABASE.upsert_posts(posts)?;
        }
        DATABASE.upsert_user(OkuUser {
            author_id: author_id,
            last_fetched: SystemTime::now(),
            posts: posts.map(|x| x.into_par_iter().map(|y| y.entry).collect()),
            identity: profile,
        })?;
        Ok(DATABASE
            .get_user(author_id)?
            .ok_or(miette::miette!("User {} not found … ", author_id))?)
    }
}
