use crate::fs::{path_to_entry_key, FS_PATH};
use iroh::{client::docs::Entry, docs::AuthorId};
use miette::IntoDiagnostic;
use native_db::*;
use native_model::{native_model, Model};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, LazyLock},
    time::SystemTime,
};
use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    query::QueryParser,
    schema::{Field, Schema, Value, FAST, STORED, TEXT},
    u64_to_i64, Directory, Index, IndexReader, IndexWriter, TantivyDocument, Term,
};
use tokio::sync::Mutex;
use url::Url;

pub(crate) static DATABASE_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| PathBuf::from(FS_PATH).join("database"));
/// An Oku node's database.
pub static DATABASE: LazyLock<OkuDatabase> = LazyLock::new(|| OkuDatabase::new().unwrap());
pub(crate) static POST_INDEX_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| PathBuf::from(FS_PATH).join("POST_INDEX"));
pub(crate) static MODELS: LazyLock<Models> = LazyLock::new(|| {
    let mut models = Models::new();
    models.define::<OkuUser>().unwrap();
    models.define::<OkuPost>().unwrap();
    models
});
pub(crate) static POST_SCHEMA: LazyLock<(Schema, HashMap<&str, Field>)> = LazyLock::new(|| {
    let mut schema_builder = Schema::builder();
    let fields = HashMap::from([
        ("id", schema_builder.add_bytes_field("id", STORED)),
        (
            "author_id",
            schema_builder.add_text_field("author_id", TEXT | STORED),
        ),
        ("path", schema_builder.add_text_field("path", TEXT | STORED)),
        ("url", schema_builder.add_text_field("url", TEXT | STORED)),
        (
            "title",
            schema_builder.add_text_field("title", TEXT | STORED),
        ),
        ("body", schema_builder.add_text_field("body", TEXT | STORED)),
        ("tag", schema_builder.add_text_field("tag", TEXT | STORED)),
        (
            "timestamp",
            schema_builder.add_date_field("timestamp", FAST),
        ),
    ]);
    let schema = schema_builder.build();
    (schema, fields)
});
pub(crate) static POST_INDEX: LazyLock<Index> = LazyLock::new(|| {
    let _ = std::fs::create_dir_all(&*POST_INDEX_PATH);
    let mmap_directory: Box<dyn Directory> =
        Box::new(MmapDirectory::open(&*POST_INDEX_PATH).unwrap());
    Index::open_or_create(mmap_directory, POST_SCHEMA.0.clone()).unwrap()
});
pub(crate) static POST_INDEX_READER: LazyLock<IndexReader> =
    LazyLock::new(|| POST_INDEX.reader().unwrap());
pub(crate) static POST_INDEX_WRITER: LazyLock<Arc<Mutex<IndexWriter>>> =
    LazyLock::new(|| Arc::new(Mutex::new(POST_INDEX.writer(50_000_000).unwrap())));

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[native_model(id = 1, version = 1)]
#[native_db(
    primary_key(author_id -> Vec<u8>)
)]
/// An Oku user.
pub struct OkuUser {
    /// The content authorship identifier associated with the Oku user.
    pub author_id: AuthorId,
    /// The system time of when this user's content was last retrieved from OkuNet.
    pub last_fetched: SystemTime,
    /// The posts made by this user on OkuNet.
    pub posts: Option<Vec<Entry>>,
    /// The OkuNet identity of the user.
    pub identity: Option<OkuIdentity>,
}

impl OkuUser {
    fn author_id(&self) -> Vec<u8> {
        self.author_id.as_bytes().to_vec()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
/// An OkuNet identity for an Oku user.
pub struct OkuIdentity {
    /// The display name of the Oku user.
    pub name: String,
    /// The content authors followed by the Oku user.
    /// OkuNet content is retrieved from followed users and the users those users follow.
    pub following: Vec<AuthorId>,
    /// The content authors blocked by the Oku user.
    /// Blocked authors are ignored when fetching new OkuNet posts.
    pub blocked: Vec<AuthorId>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
#[native_model(id = 2, version = 1)]
#[native_db(
    primary_key(primary_key -> (Vec<u8>, Vec<u8>))
)]
/// An OkuNet post.
pub struct OkuPost {
    /// A record of a version of the post file.
    pub entry: Entry,
    /// The content of the post on OkuNet.
    pub note: OkuNote,
}

impl From<OkuPost> for TantivyDocument {
    fn from(value: OkuPost) -> Self {
        let post_key: [Vec<u8>; 2] = value.primary_key().into();
        let post_key_bytes = post_key.concat();

        let mut doc = TantivyDocument::default();
        doc.add_bytes(POST_SCHEMA.1["id"], post_key_bytes);
        doc.add_text(POST_SCHEMA.1["author_id"], value.entry.author().to_string());
        doc.add_text(
            POST_SCHEMA.1["path"],
            String::from_utf8_lossy(value.entry.key()),
        );
        doc.add_text(POST_SCHEMA.1["url"], value.note.url.to_string());
        doc.add_text(POST_SCHEMA.1["title"], value.note.title);
        doc.add_text(POST_SCHEMA.1["body"], value.note.body);
        for tag in value.note.tags {
            doc.add_text(POST_SCHEMA.1["tag"], tag);
        }
        doc.add_date(
            POST_SCHEMA.1["timestamp"],
            tantivy::DateTime::from_timestamp_micros(u64_to_i64(value.entry.timestamp())),
        );
        doc
    }
}

impl TryFrom<TantivyDocument> for OkuPost {
    type Error = anyhow::Error;

    fn try_from(value: TantivyDocument) -> Result<Self, Self::Error> {
        let author_id = AuthorId::from_str(
            value
                .get_first(POST_SCHEMA.1["author_id"])
                .ok_or(anyhow::anyhow!("No author ID for document in index … "))?
                .as_str()
                .ok_or(anyhow::anyhow!("No author ID for document in index … "))?,
        )?;
        let path = value
            .get_first(POST_SCHEMA.1["path"])
            .ok_or(anyhow::anyhow!("No path for document in index … "))?
            .as_str()
            .ok_or(anyhow::anyhow!("No path for document in index … "))?
            .to_string();
        DATABASE
            .get_post(author_id, path.clone().into())
            .ok()
            .flatten()
            .ok_or(anyhow::anyhow!(
                "No post with author {} and path {} found … ",
                author_id,
                path
            ))
    }
}

impl OkuPost {
    fn primary_key(&self) -> (Vec<u8>, Vec<u8>) {
        (
            self.entry.author().as_bytes().to_vec(),
            self.entry.key().to_vec(),
        )
    }

    fn index_term(&self) -> Term {
        let post_key: [Vec<u8>; 2] = self.primary_key().into();
        let post_key_bytes = post_key.concat();
        Term::from_field_bytes(POST_SCHEMA.1["id"], &post_key_bytes)
    }

    /// Obtain the author of this post from the OkuNet database.
    pub fn user(&self) -> OkuUser {
        match DATABASE.get_user(self.entry.author()).ok().flatten() {
            Some(user) => user,
            None => OkuUser {
                author_id: self.entry.author(),
                last_fetched: SystemTime::now(),
                posts: Some(vec![self.entry.clone()]),
                identity: None,
            },
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
/// A note left by an Oku user regarding some URL-addressed content.
pub struct OkuNote {
    /// The URL the note is regarding.
    pub url: Url,
    /// The title of the note.
    pub title: String,
    /// The body of the note.
    pub body: String,
    /// A list of tags associated with the note.
    pub tags: Vec<String>,
}

impl OkuNote {
    /// Generate a suggested post path for the note.
    pub fn suggested_post_path(&self) -> String {
        Self::suggested_post_path_from_url(self.url.to_string())
    }

    /// Generate a suggested post path using a URL.
    pub fn suggested_post_path_from_url(url: String) -> String {
        format!("/posts/{}.toml", bs58::encode(url.as_bytes()).into_string())
    }
}

/// The database used by Oku's protocol.
pub struct OkuDatabase {
    database: Database<'static>,
}

impl OkuDatabase {
    /// Open an existing Oku database, or create one if it does not exist.
    ///
    /// # Returns
    ///
    /// An Oku database.
    pub fn new() -> miette::Result<Self> {
        Ok(Self {
            database: native_db::Builder::new()
                .create(&MODELS, &*DATABASE_PATH)
                .into_diagnostic()?,
        })
    }

    /// Search OkuNet posts with a query string.
    ///
    /// # Arguments
    ///
    /// * `query_string` - The string used to query for posts.
    ///
    /// * `result_limit` - The maximum number of results to get (defaults to 10).
    ///
    /// # Returns
    ///
    /// A list of OkuNet posts.
    pub fn search_posts(
        query_string: String,
        result_limit: Option<usize>,
    ) -> miette::Result<Vec<OkuPost>> {
        let searcher = POST_INDEX_READER.searcher();
        let query_parser = QueryParser::for_index(
            &*POST_INDEX,
            vec![
                POST_SCHEMA.1["author_id"],
                POST_SCHEMA.1["path"],
                POST_SCHEMA.1["title"],
                POST_SCHEMA.1["body"],
                POST_SCHEMA.1["tag"],
            ],
        );
        let query = query_parser.parse_query(&query_string).into_diagnostic()?;
        let limit = result_limit.unwrap_or(10);
        let top_docs = searcher
            .search(&query, &TopDocs::with_limit(limit))
            .into_diagnostic()?;
        Ok(top_docs
            .par_iter()
            .filter_map(|x| searcher.doc(x.1).ok())
            .collect::<Vec<TantivyDocument>>()
            .into_par_iter()
            .filter_map(|x| TryInto::try_into(x).ok())
            .collect())
    }

    /// Insert or update an OkuNet post.
    ///
    /// # Arguments
    ///
    /// * `post` - An OkuNet post to upsert.
    ///
    /// # Returns
    ///
    /// The previous version of the post, if one existed.
    pub fn upsert_post(&self, post: OkuPost) -> miette::Result<Option<OkuPost>> {
        let rw: transaction::RwTransaction<'_> =
            self.database.rw_transaction().into_diagnostic()?;
        let old_value: Option<OkuPost> = rw.upsert(post.clone()).into_diagnostic()?;
        rw.commit().into_diagnostic()?;

        let mut index_writer = POST_INDEX_WRITER
            .clone()
            .try_lock_owned()
            .into_diagnostic()?;
        if let Some(old_post) = old_value.clone() {
            index_writer.delete_term(old_post.index_term());
        }
        index_writer.add_document(post.into()).into_diagnostic()?;
        index_writer.commit().into_diagnostic()?;

        Ok(old_value)
    }

    /// Insert or update multiple OkuNet posts.
    ///
    /// # Arguments
    ///
    /// * `posts` - A list of OkuNet posts to upsert.
    ///
    /// # Returns
    ///
    /// A list containing the previous version of each post, if one existed.
    pub fn upsert_posts(&self, posts: Vec<OkuPost>) -> miette::Result<Vec<Option<OkuPost>>> {
        let rw = self.database.rw_transaction().into_diagnostic()?;
        let old_posts: Vec<_> = posts
            .clone()
            .into_iter()
            .filter_map(|post| rw.upsert(post).ok())
            .collect();
        rw.commit().into_diagnostic()?;

        let mut index_writer = POST_INDEX_WRITER
            .clone()
            .try_lock_owned()
            .into_diagnostic()?;
        old_posts.par_iter().for_each(|old_post| {
            if let Some(old_post) = old_post {
                index_writer.delete_term(old_post.index_term());
            }
        });
        posts.par_iter().for_each(|post| {
            let _ = index_writer.add_document(post.clone().into());
        });
        index_writer.commit().into_diagnostic()?;

        Ok(old_posts)
    }

    /// Delete an OkuNet post.
    ///
    /// # Arguments
    ///
    /// * `post` - An OkuNet post to delete.
    ///
    /// # Returns
    ///
    /// The deleted post.
    pub fn delete_post(&self, post: OkuPost) -> miette::Result<OkuPost> {
        let rw = self.database.rw_transaction().into_diagnostic()?;
        let removed_post = rw.remove(post).into_diagnostic()?;
        rw.commit().into_diagnostic()?;

        let mut index_writer = POST_INDEX_WRITER
            .clone()
            .try_lock_owned()
            .into_diagnostic()?;
        index_writer.delete_term(removed_post.index_term());
        index_writer.commit().into_diagnostic()?;

        Ok(removed_post)
    }

    /// Delete multiple OkuNet posts.
    ///
    /// # Arguments
    ///
    /// * `posts` - A list of OkuNet posts to delete.
    ///
    /// # Returns
    ///
    /// A list containing the deleted posts.
    pub fn delete_posts(&self, posts: Vec<OkuPost>) -> miette::Result<Vec<OkuPost>> {
        let rw = self.database.rw_transaction().into_diagnostic()?;
        let removed_posts: Vec<_> = posts
            .into_iter()
            .filter_map(|post| rw.remove(post).ok())
            .collect();
        rw.commit().into_diagnostic()?;

        let mut index_writer = POST_INDEX_WRITER
            .clone()
            .try_lock_owned()
            .into_diagnostic()?;
        removed_posts.par_iter().for_each(|removed_post| {
            index_writer.delete_term(removed_post.index_term());
        });
        index_writer.commit().into_diagnostic()?;

        Ok(removed_posts)
    }

    /// Retrieves all known OkuNet posts.
    ///
    /// # Returns
    ///
    /// A list of all known OkuNet posts.
    pub fn get_posts(&self) -> miette::Result<Vec<OkuPost>> {
        let r = self.database.r_transaction().into_diagnostic()?;
        Ok(r.scan()
            .primary()
            .into_diagnostic()?
            .all()
            .into_diagnostic()?
            .collect::<Result<Vec<_>, _>>()
            .into_diagnostic()?)
    }

    /// Retrieves all known OkuNet posts by a given author.
    ///
    /// # Arguments
    ///
    /// * `author_id` - A content authorship ID.
    ///
    /// # Returns
    ///
    /// A list of all known OkuNet posts by the given author.
    pub fn get_posts_by_author(&self, author_id: AuthorId) -> miette::Result<Vec<OkuPost>> {
        let r = self.database.r_transaction().into_diagnostic()?;
        Ok(r.scan()
            .primary()
            .into_diagnostic()?
            .start_with(author_id.as_bytes().to_vec())
            .into_diagnostic()?
            .collect::<Result<Vec<_>, _>>()
            .into_diagnostic()?)
    }

    /// Retrieves all known OkuNet posts by a given tag.
    ///
    /// # Arguments
    ///
    /// * `tag` - A tag.
    ///
    /// # Returns
    ///
    /// A list of all known OkuNet posts with the given tag.
    pub fn get_posts_by_tag(&self, tag: String) -> miette::Result<Vec<OkuPost>> {
        Ok(self
            .get_posts()?
            .into_iter()
            .filter(|x| x.note.tags.contains(&tag))
            .collect())
    }

    /// Retrieves all distinct tags used in OkuNet posts.
    ///
    /// # Returns
    ///
    /// A list of all tags that appear in an OkuNet post.
    pub fn get_tags(&self) -> miette::Result<Vec<String>> {
        let mut tags: Vec<_> = self
            .get_posts()?
            .into_iter()
            .flat_map(|x| x.note.tags)
            .collect();
        tags.sort_unstable();
        tags.dedup();
        Ok(tags)
    }

    /// Retrieves an OkuNet post.
    ///
    /// # Arguments
    ///
    /// * `author_id` - A content authorship ID.
    ///
    /// * `path` - A path to a post in the author's home replica.
    ///
    /// # Returns
    ///
    /// The OkuNet post by the given author at the given path, if one exists.
    pub fn get_post(&self, author_id: AuthorId, path: PathBuf) -> miette::Result<Option<OkuPost>> {
        let r = self.database.r_transaction().into_diagnostic()?;
        let entry_key = (
            author_id.as_bytes().to_vec(),
            path_to_entry_key(path).to_vec(),
        );
        Ok(r.get().primary(entry_key).into_diagnostic()?)
    }

    /// Insert or update an OkuNet user.
    ///
    /// # Arguments
    ///
    /// * `user` - An OkuNet user to upsert.
    ///
    /// # Returns
    ///
    /// The previous version of the user, if one existed.
    pub fn upsert_user(&self, user: OkuUser) -> miette::Result<Option<OkuUser>> {
        let rw = self.database.rw_transaction().into_diagnostic()?;
        let old_value: Option<OkuUser> = rw.upsert(user).into_diagnostic()?;
        rw.commit().into_diagnostic()?;
        Ok(old_value)
    }

    /// Delete an OkuNet user.
    ///
    /// # Arguments
    ///
    /// * `user` - An OkuNet user to delete.
    ///
    /// # Returns
    ///
    /// The deleted user.
    pub fn delete_user(&self, user: OkuUser) -> miette::Result<OkuUser> {
        let rw = self.database.rw_transaction().into_diagnostic()?;
        let removed_user = rw.remove(user).into_diagnostic()?;
        rw.commit().into_diagnostic()?;
        Ok(removed_user)
    }

    /// Delete multiple OkuNet users.
    ///
    /// # Arguments
    ///
    /// * `users` - A list of OkuNet users to delete.
    ///
    /// # Returns
    ///
    /// A list containing the deleted users.
    pub fn delete_users(&self, users: Vec<OkuUser>) -> miette::Result<Vec<OkuUser>> {
        let rw = self.database.rw_transaction().into_diagnostic()?;
        let removed_users = users
            .into_iter()
            .filter_map(|user| rw.remove(user).ok())
            .collect();
        rw.commit().into_diagnostic()?;
        Ok(removed_users)
    }

    /// Delete multiple OkuNet users and their posts.
    ///
    /// # Arguments
    ///
    /// * `users` - A list of OkuNet users to delete.
    ///
    /// # Returns
    ///
    /// A list containing the deleted posts.
    pub fn delete_users_with_posts(&self, users: Vec<OkuUser>) -> miette::Result<Vec<OkuPost>> {
        Ok(self
            .delete_users(users)?
            .par_iter()
            .filter_map(|x| self.get_posts_by_author(x.author_id).ok())
            .collect::<Vec<_>>()
            .into_par_iter()
            .flat_map(|x| self.delete_posts(x).ok())
            .collect::<Vec<_>>()
            .concat())
    }

    /// Gets the OkuNet content of all known users.
    ///
    /// # Returns
    ///
    /// The OkuNet content of all users known to this node.
    pub fn get_users(&self) -> miette::Result<Vec<OkuUser>> {
        let r = self.database.r_transaction().into_diagnostic()?;
        Ok(r.scan()
            .primary()
            .into_diagnostic()?
            .all()
            .into_diagnostic()?
            .collect::<Result<Vec<_>, _>>()
            .into_diagnostic()?)
    }

    /// Gets an OkuNet user's content by their content authorship ID.
    ///
    /// # Arguments
    ///
    /// * `author_id` - A content authorship ID.
    ///
    /// # Returns
    ///
    /// An OkuNet user's content.
    pub fn get_user(&self, author_id: AuthorId) -> miette::Result<Option<OkuUser>> {
        let r = self.database.r_transaction().into_diagnostic()?;
        Ok(r.get()
            .primary(author_id.as_bytes().to_vec())
            .into_diagnostic()?)
    }
}
