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
    client::docs::Entry,
    docs::{Author, AuthorId, CapabilityKind, DocTicket, NamespaceId},
};
use miette::IntoDiagnostic;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Serialize, Deserialize, Debug, Clone)]
/// An Oku user's credentials, which are sensitive, exported from a node, able to be imported into another.
pub struct ExportedUser {
    author: Author,
    home_replica: Option<NamespaceId>,
    home_replica_ticket: Option<DocTicket>,
}

impl OkuFs {
    /// Retrieve the content authorship ID used by the node.
    ///
    /// # Returns
    ///
    /// The content authorship ID used by the node.
    pub async fn default_author(&self) -> anyhow::Result<AuthorId> {
        self.node.authors().default().await
    }

    /// Exports the local Oku user's credentials.
    ///
    /// # Returns
    ///
    /// The local Oku user's credentials, containing sensitive information.
    pub async fn export_user(&self) -> Option<ExportedUser> {
        let default_author = self.get_author().await.ok();
        let home_replica = self.home_replica().await;
        let home_replica_ticket = match home_replica {
            Some(home_replica_id) => self
                .create_document_ticket(home_replica_id, iroh::client::docs::ShareMode::Write)
                .await
                .ok(),
            None => None,
        };
        default_author.map(|author| ExportedUser {
            author,
            home_replica,
            home_replica_ticket,
        })
    }

    /// Imports Oku user credentials that were exported from another node.
    ///
    /// # Arguments
    ///
    /// * `exported_user` - Oku user credentials, which contain sensitive information.
    pub async fn import_user(&self, exported_user: ExportedUser) -> miette::Result<()> {
        self.node
            .authors()
            .import(exported_user.author.clone())
            .await
            .map_err(|e| miette::miette!("{}", e))?;
        self.node
            .authors()
            .set_default(exported_user.author.id())
            .await
            .map_err(|e| miette::miette!("{}", e))?;
        match (
            exported_user.home_replica,
            exported_user.home_replica_ticket,
        ) {
            (Some(home_replica), Some(home_replica_ticket)) => match self
                .fetch_replica_by_ticket(home_replica_ticket, None)
                .await
            {
                Ok(_) => (),
                Err(_e) => self
                    .fetch_replica_by_id(home_replica, None)
                    .await
                    .map_err(|e| miette::miette!("{}", e))?,
            },
            (Some(home_replica), None) => self
                .fetch_replica_by_id(home_replica, None)
                .await
                .map_err(|e| miette::miette!("{}", e))?,
            _ => (),
        }
        self.set_home_replica(exported_user.home_replica)
    }

    /// Exports the local Oku user's credentials in TOML format.
    ///
    /// # Returns
    ///
    /// The local Oku user's credentials, containing sensitive information.
    pub async fn export_user_toml(&self) -> miette::Result<String> {
        toml::to_string(
            &self
                .export_user()
                .await
                .ok_or(miette::miette!("No authorship credentials to export … "))?,
        )
        .into_diagnostic()
    }

    /// Imports Oku user credentials that were exported from another node.
    ///
    /// # Arguments
    ///
    /// * `exported_user` - Oku user credentials, encoded in TOML format. They contain sensitive information.
    pub async fn import_user_toml(&self, exported_user_toml: String) -> miette::Result<()> {
        let exported_user: ExportedUser = toml::from_str(&exported_user_toml).into_diagnostic()?;
        self.import_user(exported_user).await
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

    /// Retrieves the OkuNet posts by the local user, if any.
    ///
    /// # Returns
    ///
    /// A list of the OkuNet posts by the local user.
    pub async fn posts(&self) -> Option<Vec<OkuPost>> {
        let post_files = self
            .read_directory(self.home_replica().await?, "/posts".into())
            .await
            .ok()?;
        Some(
            post_files
                .par_iter()
                .filter_map(|(entry, bytes)| {
                    toml::from_str::<OkuNote>(&String::from_utf8_lossy(bytes).to_string())
                        .ok()
                        .map(|x| OkuPost {
                            entry: entry.clone(),
                            note: x,
                        })
                })
                .collect(),
        )
    }

    /// Retrieves an OkuNet post authored by the local user using its path.
    ///
    /// # Arguments
    ///
    /// * `path` - A path to a post in the user's home replica.
    ///
    /// # Returns
    ///
    /// The OkuNet post at the given path.
    pub async fn post(&self, path: PathBuf) -> miette::Result<OkuPost> {
        let namespace_id = self
            .home_replica()
            .await
            .ok_or(miette::miette!("Home replica not set … "))?;
        match self.read_file(namespace_id, path.clone().into()).await {
            Ok(bytes) => {
                let note = toml::from_str::<OkuNote>(&String::from_utf8_lossy(&bytes).to_string())
                    .into_diagnostic()?;
                Ok(OkuPost {
                    entry: self.get_entry(namespace_id, path).await?,
                    note: note,
                })
            }
            Err(e) => Err(miette::miette!("{}", e)),
        }
    }

    /// Retrieves the OkuNet identity of the local user.
    ///
    /// # Returns
    ///
    /// The local user's OkuNet identity, if they have one.
    pub async fn identity(&self) -> Option<OkuIdentity> {
        let profile_bytes = self
            .read_file(self.home_replica().await?, "/profile.toml".into())
            .await
            .ok()?;
        Some(toml::from_str(&String::from_utf8_lossy(&profile_bytes).to_string()).ok()?)
    }

    /// Replaces the current OkuNet identity of the local user.
    ///
    /// # Arguments
    ///
    /// * `identity` - The new OkuNet identity.
    ///
    /// # Returns
    ///
    /// The hash of the new identity file in the local user's home replica.
    pub async fn set_identity(&self, identity: OkuIdentity) -> miette::Result<Hash> {
        self.create_or_modify_file(
            self.home_replica()
                .await
                .ok_or(miette::miette!("No home replica set … "))?,
            "/profile.toml".into(),
            toml::to_string_pretty(&identity).into_diagnostic()?,
        )
        .await
    }

    /// Replaces the current display name of the local user.
    ///
    /// # Arguments
    ///
    /// * `display_name` - The new display name.
    ///
    /// # Returns
    ///
    /// # The hash of the new identity file in the local user's home replica.
    pub async fn set_display_name(&self, display_name: String) -> miette::Result<Hash> {
        let mut identity = self.identity().await.unwrap_or_default();
        identity.name = display_name;
        self.set_identity(identity).await
    }

    /// Retrieves an [`OkuUser`] representing the local user.
    ///
    /// # Returns
    ///
    /// An [`OkuUser`] representing the current user, as if it were retrieved from another Oku user's database.
    pub async fn user(&self) -> miette::Result<OkuUser> {
        Ok(OkuUser {
            author_id: self
                .default_author()
                .await
                .map_err(|e| miette::miette!("{}", e))?,
            last_fetched: SystemTime::now(),
            posts: self
                .posts()
                .await
                .map(|x| x.into_iter().map(|y| y.entry).collect()),
            identity: self.identity().await,
        })
    }

    /// Attempts to retrieve an OkuNet post from a file entry.
    ///
    /// # Arguments
    ///
    /// * `entry` - The file entry to parse.
    ///
    /// # Returns
    ///
    /// An OkuNet post, if the entry represents one.
    pub async fn post_from_entry(&self, entry: &Entry) -> miette::Result<OkuPost> {
        let bytes = entry
            .content_bytes(&self.node)
            .await
            .map_err(|e| miette::miette!("{}", e))?;
        let note = toml::from_str::<OkuNote>(&String::from_utf8_lossy(&bytes).to_string())
            .into_diagnostic()?;
        Ok(OkuPost {
            entry: entry.clone(),
            note: note,
        })
    }

    /// Retrieves OkuNet posts from the file entries in an [`OkuUser`].
    ///
    /// # Arguments
    ///
    /// * `user` - The OkuNet user record containing the file entries.
    ///
    /// # Returns
    ///
    /// A list of OkuNet posts contained within the user record.
    pub async fn posts_from_user(&self, user: &OkuUser) -> miette::Result<Vec<OkuPost>> {
        let mut posts: Vec<_> = Vec::new();
        for post in user.posts.clone().unwrap_or_default() {
            posts.push(self.post_from_entry(&post).await?);
        }
        Ok(posts)
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

    /// Refreshes any user data last retrieved longer than [`REPUBLISH_DELAY`](crate::discovery::REPUBLISH_DELAY) ago according to the system time; the users one is following, and the users they're following, are recorded locally.
    /// Blocked users are not recorded.
    pub async fn refresh_users(&self) -> miette::Result<()> {
        if let Some(identity) = self.identity().await {
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

    /// Retrieves user data regardless of when last retrieved; the users one is following, and the users they're following, are recorded locally.
    /// Blocked users are not recorded.
    pub async fn fetch_users(&self) -> miette::Result<()> {
        if let Some(identity) = self.identity().await {
            for followed_user_id in identity
                .following
                .iter()
                .filter(|x| !identity.blocked.contains(x))
            {
                let followed_user = self.fetch_user(*followed_user_id).await?;
                if let Some(followed_user_identity) = followed_user.identity {
                    for followed_followed_user_id in followed_user_identity
                        .following
                        .iter()
                        .filter(|x| !identity.blocked.contains(x))
                    {
                        self.fetch_user(*followed_followed_user_id).await?;
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

    /// Join a swarm to fetch the latest version of an OkuNet post.
    ///
    /// # Arguments
    ///
    /// * `author_id` - The authorship ID of the post's author.
    ///
    /// * `path` - The path to the post in the author's home replica.
    ///
    /// # Returns
    ///
    /// The requested OkuNet post.
    pub async fn fetch_post(&self, author_id: AuthorId, path: PathBuf) -> miette::Result<OkuPost> {
        let ticket = self
            .resolve_author_id(author_id)
            .await
            .map_err(|e| miette::miette!("{}", e))?;
        let namespace_id = ticket.capability.id();
        match self
            .fetch_file_with_ticket(ticket, path.clone().into())
            .await
        {
            Ok(bytes) => {
                let note = toml::from_str::<OkuNote>(&String::from_utf8_lossy(&bytes).to_string())
                    .into_diagnostic()?;
                Ok(OkuPost {
                    entry: self.get_entry(namespace_id, path).await?,
                    note: note,
                })
            }
            Err(e) => Err(miette::miette!("{}", e)),
        }
    }

    /// Retrieves an OkuNet post from the database, or from the mainline DHT if not found locally.
    ///
    /// # Arguments
    ///
    /// * `author_id` - The authorship ID of the post's author.
    ///
    /// * `path` - The path to the post in the author's home replica.
    ///
    /// # Returns
    ///
    /// The requested OkuNet post.
    pub async fn get_or_fetch_post(
        &self,
        author_id: AuthorId,
        path: PathBuf,
    ) -> miette::Result<OkuPost> {
        match DATABASE.get_post(author_id, path.clone()).ok().flatten() {
            Some(post) => Ok(post),
            None => self.fetch_post(author_id, path).await,
        }
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
        match DATABASE.get_user(author_id).ok().flatten() {
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
