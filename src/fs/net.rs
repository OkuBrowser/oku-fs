use std::{collections::HashSet, path::PathBuf, time::SystemTime};

use crate::{
    config::OkuFsConfig,
    database::{
        core::DATABASE,
        dht::ReplicaAnnouncement,
        posts::{OkuNote, OkuPost},
        users::{OkuIdentity, OkuUser},
    },
    discovery::REPUBLISH_DELAY,
    fs::{merge_tickets, OkuFs},
};
use anyhow::anyhow;
use futures::StreamExt;
use iroh_base::hash::Hash;
use iroh_base::ticket::Ticket;
use iroh_docs::rpc::client::docs::Entry;
use iroh_docs::rpc::client::docs::ShareMode;
use iroh_docs::store::FilterKind;
use iroh_docs::sync::CapabilityKind;
use iroh_docs::Author;
use iroh_docs::AuthorId;
use iroh_docs::DocTicket;
use iroh_docs::NamespaceId;
use miette::IntoDiagnostic;
use rayon::iter::{
    FromParallelIterator, IntoParallelIterator, IntoParallelRefIterator, ParallelIterator,
};
use serde::{Deserialize, Serialize};
use url::Url;

use super::{path_to_entry_key, path_to_entry_prefix};

#[derive(Serialize, Deserialize, Debug, Clone)]
/// An Oku user's credentials, which are sensitive, exported from a node, able to be imported into another.
pub struct ExportedUser {
    author: Author,
    home_replica: Option<NamespaceId>,
    home_replica_ticket: Option<DocTicket>,
}

/// Filters to prevent downloading the entirety of a home replica.
/// Only the `/profile.toml` file and the `/posts/` directory are downloaded.
///
/// # Returns
///
/// The download filters specifying the only content allowed to be downloaded from a home replica.
pub fn home_replica_filters() -> Vec<FilterKind> {
    let profile_filter = FilterKind::Exact(path_to_entry_key("/profile.toml".into()));
    let posts_filter = FilterKind::Prefix(path_to_entry_prefix("/posts/".into()));
    vec![profile_filter, posts_filter]
}

impl OkuFs {
    /// Retrieve the content authorship ID used by the node.
    ///
    /// # Returns
    ///
    /// The content authorship ID used by the node.
    pub fn default_author(&self) -> AuthorId {
        self.docs_engine.default_author.get()
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
                .create_document_ticket(home_replica_id, ShareMode::Write)
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
        self.docs_engine
            .client()
            .authors()
            .import(exported_user.author.clone())
            .await
            .map_err(|e| miette::miette!("{}", e))?;
        self.docs_engine
            .client()
            .authors()
            .set_default(exported_user.author.id())
            .await
            .map_err(|e| miette::miette!("{}", e))?;
        match (
            exported_user.home_replica,
            exported_user.home_replica_ticket,
        ) {
            (Some(home_replica), Some(home_replica_ticket)) => match self
                .fetch_replica_by_ticket(&home_replica_ticket, None, None)
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
            .read_directory(self.home_replica().await?, "/posts/".into())
            .await
            .ok()
            .unwrap_or_default();
        Some(
            post_files
                .par_iter()
                .filter_map(|(entry, bytes)| {
                    toml::from_str::<OkuNote>(String::from_utf8_lossy(bytes).as_ref())
                        .ok()
                        .map(|x| OkuPost {
                            entry: entry.clone(),
                            note: x,
                        })
                })
                .collect(),
        )
    }

    /// Retrieves all posts containing a given tag, whether they are authored by the local user or an OkuNet user.
    ///
    /// # Arguments
    ///
    /// * `tag` - A tag.
    ///
    /// # Returns
    ///
    /// A list of OkuNet posts with the given tag.
    pub async fn all_posts_with_tag(&self, tag: String) -> Vec<OkuPost> {
        let mut posts = HashSet::<_>::from_par_iter(self.posts().await.unwrap_or_default());
        posts.extend(DATABASE.get_posts().unwrap_or_default());
        posts
            .into_par_iter()
            .filter(|x| x.note.tags.contains(&tag))
            .collect()
    }

    /// Retrieves the set of all tags that appear in posts authored by the local user or an OkuNet user.
    ///
    /// # Returns
    ///
    /// All tags that appear across all posts on the local machine.
    pub async fn all_tags(&self) -> HashSet<String> {
        let mut tags = HashSet::<_>::from_par_iter(
            self.posts()
                .await
                .unwrap_or_default()
                .into_par_iter()
                .flat_map(|x| x.note.tags),
        );
        tags.extend(
            DATABASE
                .get_posts()
                .unwrap_or_default()
                .into_iter()
                .flat_map(|x| x.note.tags),
        );
        tags
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
        match self.read_file(namespace_id, path.clone()).await {
            Ok(bytes) => {
                let note = toml::from_str::<OkuNote>(String::from_utf8_lossy(&bytes).as_ref())
                    .into_diagnostic()?;
                Ok(OkuPost {
                    entry: self.get_entry(namespace_id, path).await?,
                    note,
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
        toml::from_str(String::from_utf8_lossy(&profile_bytes).as_ref()).ok()
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
        // It is not valid to follow or unfollow yourself.
        let mut validated_identity = identity.clone();
        validated_identity.following.retain(|y| !self.is_me(y));
        validated_identity.blocked.retain(|y| !self.is_me(y));
        // It is not valid to follow blocked people.
        validated_identity.following = validated_identity
            .following
            .difference(&validated_identity.blocked)
            .copied()
            .collect();

        self.create_or_modify_file(
            self.home_replica()
                .await
                .ok_or(miette::miette!("No home replica set … "))?,
            "/profile.toml".into(),
            toml::to_string_pretty(&validated_identity).into_diagnostic()?,
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

    /// Follow or unfollow a user.
    ///
    /// # Arguments
    ///
    /// * `author_id` - The user to follow or unfollow's content authorship ID.
    ///
    /// # Returns
    ///
    /// The hash of the new identity file in the local user's home replica.
    pub async fn toggle_follow(&self, author_id: AuthorId) -> miette::Result<Hash> {
        let mut identity = self.identity().await.unwrap_or_default();
        match identity.following.contains(&author_id) {
            true => identity.following.remove(&author_id),
            false => identity.following.insert(author_id),
        };
        self.set_identity(identity).await
    }

    /// Block or unblock a user.
    ///
    /// # Arguments
    ///
    /// * `author_id` - The user to block or unblock's content authorship ID.
    ///
    /// # Returns
    ///
    /// The hash of the new identity file in the local user's home replica.
    pub async fn toggle_block(&self, author_id: AuthorId) -> miette::Result<Hash> {
        let mut identity = self.identity().await.unwrap_or_default();
        match identity.blocked.contains(&author_id) {
            true => identity.blocked.remove(&author_id),
            false => identity.blocked.insert(author_id),
        };
        self.set_identity(identity).await
    }

    /// Check if a user is followed.
    ///
    /// # Arguments
    ///
    /// * `author_id` - The user's content authorship ID.
    ///
    /// # Returns
    ///
    /// Whether or not the user is followed.
    pub async fn is_followed(&self, author_id: &AuthorId) -> bool {
        self.identity()
            .await
            .map(|x| x.following.contains(author_id))
            .unwrap_or(false)
    }

    /// Check if a user is blocked.
    ///
    /// # Arguments
    ///
    /// * `author_id` - The user's content authorship ID.
    ///
    /// # Returns
    ///
    /// Whether or not the user is blocked.
    pub async fn is_blocked(&self, author_id: &AuthorId) -> bool {
        self.identity()
            .await
            .map(|x| x.blocked.contains(author_id))
            .unwrap_or(false)
    }

    /// Check whether or not an author ID is the local user's.
    ///
    /// # Arguments
    ///
    /// * `author_id` - A user's content authorship ID.
    ///
    /// # Returns
    ///
    /// Whether or not the user's authorship ID is the local user's.
    pub fn is_me(&self, author_id: &AuthorId) -> bool {
        &self.default_author() == author_id
    }

    /// Retrieves an [`OkuUser`] representing the local user.
    ///
    /// # Returns
    ///
    /// An [`OkuUser`] representing the current user, as if it were retrieved from another Oku user's database.
    pub async fn user(&self) -> miette::Result<OkuUser> {
        Ok(OkuUser {
            author_id: self.default_author(),
            last_fetched: SystemTime::now(),
            posts: self
                .posts()
                .await
                .map(|x| x.into_par_iter().map(|y| y.entry).collect())
                .unwrap_or_default(),
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
        let bytes = self
            .content_bytes(entry)
            .await
            .map_err(|e| miette::miette!("{}", e))?;
        let note = toml::from_str::<OkuNote>(String::from_utf8_lossy(&bytes).as_ref())
            .into_diagnostic()?;
        Ok(OkuPost {
            entry: entry.clone(),
            note,
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
        for post in user.posts.clone() {
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
        tags: HashSet<String>,
    ) -> miette::Result<Hash> {
        let home_replica_id = self
            .home_replica()
            .await
            .ok_or(miette::miette!("No home replica set … "))?;
        let new_note = OkuNote {
            url,
            title,
            body,
            tags,
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

    /// Refreshes any user data last retrieved longer than [`REPUBLISH_DELAY`] ago according to the system time; the users one is following, and the users they're following, are recorded locally.
    /// Blocked users are not recorded.
    pub async fn refresh_users(&self) -> miette::Result<()> {
        // Wanted users: followed users
        // Unwanted users: blocked users, unfollowed users
        let (followed_users, blocked_users) = match self.identity().await {
            Some(identity) => (identity.following, identity.blocked),
            None => (HashSet::new(), HashSet::new()),
        };
        // In case a user is somehow followed and blocked (additional checks should already prevent this)
        let users_to_add: HashSet<_> = followed_users
            .difference(&blocked_users)
            .map(|x| x.to_owned())
            .collect();
        let local_users: HashSet<_> = DATABASE.all_local_users().into_par_iter().collect();
        let users_to_delete: HashSet<_> = local_users
            .difference(&users_to_add)
            .map(|x| x.to_owned())
            .collect();

        for user_id in users_to_add {
            let user = self.get_or_fetch_user(user_id).await?;
            let (user_followed_users, user_blocked_users) = match user.identity {
                Some(identity) => (identity.following, identity.blocked),
                None => (HashSet::new(), HashSet::new()),
            };
            for user_user in user_followed_users.difference(&user_blocked_users) {
                self.get_or_fetch_user(*user_user).await?;
            }
        }
        DATABASE.delete_by_author_ids(Vec::from_par_iter(users_to_delete))?;
        Ok(())
    }

    /// Retrieves user data regardless of when last retrieved; the users one is following, and the users they're following, are recorded locally.
    /// Blocked users are not recorded.
    pub async fn fetch_users(&self) -> miette::Result<()> {
        // Wanted users: followed users
        // Unwanted users: blocked users, unfollowed users
        let (followed_users, blocked_users) = match self.identity().await {
            Some(identity) => (identity.following, identity.blocked),
            None => (HashSet::new(), HashSet::new()),
        };
        // In case a user is somehow followed and blocked (additional checks should already prevent this)
        let users_to_add: HashSet<_> = followed_users
            .difference(&blocked_users)
            .map(|x| x.to_owned())
            .collect();
        let local_users: HashSet<_> = DATABASE.all_local_users().into_par_iter().collect();
        let users_to_delete: HashSet<_> = local_users
            .difference(&users_to_add)
            .map(|x| x.to_owned())
            .collect();

        for user_id in users_to_add {
            let user = self.fetch_user(user_id).await?;
            let (user_followed_users, user_blocked_users) = match user.identity {
                Some(identity) => (identity.following, identity.blocked),
                None => (HashSet::new(), HashSet::new()),
            };
            for user_user in user_followed_users.difference(&user_blocked_users) {
                self.fetch_user(*user_user).await?;
            }
        }
        DATABASE.delete_by_author_ids(Vec::from_par_iter(users_to_delete))?;
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
        self.okunet_fetch_sender.send_replace(true);
        let get_stream = self.dht.get_mutable(author_id.as_bytes(), None, None)?;
        tokio::pin!(get_stream);
        let mut tickets = Vec::new();
        while let Some(mutable_item) = get_stream.next().await {
            let _ = DATABASE.upsert_announcement(ReplicaAnnouncement {
                key: mutable_item.key().to_vec(),
                signature: mutable_item.signature().to_vec(),
            });
            tickets.push(DocTicket::from_bytes(mutable_item.value())?)
        }
        self.okunet_fetch_sender.send_replace(false);
        merge_tickets(tickets).ok_or(anyhow!(
            "Could not find tickets for {} … ",
            iroh_base::base32::fmt(author_id)
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
            .fetch_file_with_ticket(&ticket, path.clone(), Some(home_replica_filters()))
            .await
        {
            Ok(bytes) => {
                let note = toml::from_str::<OkuNote>(String::from_utf8_lossy(&bytes).as_ref())
                    .into_diagnostic()?;
                Ok(OkuPost {
                    entry: self.get_entry(namespace_id, path).await?,
                    note,
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
    pub async fn fetch_profile(&self, ticket: &DocTicket) -> miette::Result<OkuIdentity> {
        match self
            .fetch_file_with_ticket(ticket, "/profile.toml".into(), Some(home_replica_filters()))
            .await
        {
            Ok(profile_bytes) => Ok(toml::from_str(
                String::from_utf8_lossy(&profile_bytes).as_ref(),
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
    pub async fn fetch_posts(&self, ticket: &DocTicket) -> miette::Result<Vec<OkuPost>> {
        match self
            .fetch_directory_with_ticket(ticket, "/posts/".into(), Some(home_replica_filters()))
            .await
        {
            Ok(post_files) => Ok(post_files
                .par_iter()
                .filter_map(|(entry, bytes)| {
                    toml::from_str::<OkuNote>(String::from_utf8_lossy(bytes).as_ref())
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
    /// If last retrieved longer than [`REPUBLISH_DELAY`] ago according to the system time, a known user's content will be re-fetched.
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
        self.okunet_fetch_sender.send_replace(true);
        let ticket = self
            .resolve_author_id(author_id)
            .await
            .map_err(|e| miette::miette!("{}", e))?;

        let profile = self.fetch_profile(&ticket).await.ok();
        let posts = self.fetch_posts(&ticket).await.ok();
        if let Some(posts) = posts.clone() {
            DATABASE.upsert_posts(posts)?;
        }
        DATABASE.upsert_user(OkuUser {
            author_id,
            last_fetched: SystemTime::now(),
            posts: posts
                .map(|x| x.into_par_iter().map(|y| y.entry).collect())
                .unwrap_or_default(),
            identity: profile,
        })?;
        self.okunet_fetch_sender.send_replace(false);
        DATABASE
            .get_user(author_id)?
            .ok_or(miette::miette!("User {} not found … ", author_id))
    }
}
