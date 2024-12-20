use super::core::*;
use super::users::*;
use crate::fs::{path_to_entry_key, FS_PATH};
use iroh_docs::rpc::client::docs::Entry;
use iroh_docs::AuthorId;
use miette::IntoDiagnostic;
use native_db::*;
use native_model::{native_model, Model};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};
use std::{
    collections::{HashMap, HashSet},
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
    Directory, Index, IndexReader, IndexWriter, TantivyDocument, Term,
};
use tokio::sync::Mutex;
use url::Url;

pub(crate) static POST_INDEX_PATH: LazyLock<PathBuf> =
    LazyLock::new(|| PathBuf::from(FS_PATH).join("POST_INDEX"));
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

#[derive(Serialize, Deserialize, Debug, Clone)]
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

impl PartialEq for OkuPost {
    fn eq(&self, other: &Self) -> bool {
        self.primary_key() == other.primary_key()
    }
}
impl Eq for OkuPost {}
impl Hash for OkuPost {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.primary_key().hash(state);
    }
}

impl From<OkuPost> for TantivyDocument {
    fn from(value: OkuPost) -> Self {
        let post_key: [Vec<u8>; 2] = value.primary_key().into();
        let post_key_bytes = post_key.concat();

        let mut doc = TantivyDocument::default();
        doc.add_bytes(POST_SCHEMA.1["id"], post_key_bytes);
        doc.add_text(
            POST_SCHEMA.1["author_id"],
            iroh_base::base32::fmt(value.entry.author()),
        );
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
            tantivy::DateTime::from_timestamp_micros(value.entry.timestamp() as i64),
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
                posts: vec![self.entry.clone()],
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
    pub tags: HashSet<String>,
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

impl OkuDatabase {
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
            &POST_INDEX,
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
        r.scan()
            .primary()
            .into_diagnostic()?
            .all()
            .into_diagnostic()?
            .collect::<Result<Vec<_>, _>>()
            .into_diagnostic()
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
        Ok(self
            .get_posts()?
            .into_par_iter()
            .filter(|x| x.entry.author() == author_id)
            .collect())
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
            .into_par_iter()
            .filter(|x| x.note.tags.contains(&tag))
            .collect())
    }

    /// Retrieves all distinct tags used in OkuNet posts.
    ///
    /// # Returns
    ///
    /// A list of all tags that appear in an OkuNet post.
    pub fn get_tags(&self) -> miette::Result<HashSet<String>> {
        Ok(self
            .get_posts()?
            .into_iter()
            .flat_map(|x| x.note.tags)
            .collect())
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
        r.get().primary(entry_key).into_diagnostic()
    }
}
