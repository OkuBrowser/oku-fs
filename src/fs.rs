use bytes::Bytes;
use futures::StreamExt;
use iroh::{
    bytes::Hash, node::FsNode, rpc_protocol::ShareMode, sync::{
        store::{fs::StoreInstance, Store},
        Author, AuthorId, NamespaceId, NamespaceSecret, Replica,
    }
};
use path_clean::PathClean;
use rand_core::OsRng;
use sha3::{Digest, Sha3_256};
use std::{error::Error, path::PathBuf};

use crate::error::OkuFsError;

const FS_PATH: &str = ".oku";

fn normalise_path(path: PathBuf) -> PathBuf {
    PathBuf::from("/").join(path).clean()
}

pub fn path_to_entry_key(path: PathBuf) -> String {
    let path = normalise_path(path.clone());
    let mut hasher = Sha3_256::new();
    hasher.update(path.clone().into_os_string().into_encoded_bytes());
    let path_hash = hasher.finalize();
    format!("{}\u{F0000}{}", path.display(), hex::encode(path_hash))
}

pub struct OkuFs {
    node: FsNode,
    author_id: AuthorId,
}

impl OkuFs {
    pub async fn start() -> Result<OkuFs, Box<dyn Error>> {
        let node_path = PathBuf::from(FS_PATH).join("node");
        let node = FsNode::persistent(node_path).await?.spawn().await?;
        let authors = node.authors.list().await?;
        futures::pin_mut!(authors);
        let authors_count = (&authors.as_mut().count().await).to_owned();
        let author_id = if authors_count == 0 {
            node.authors.create().await?
        } else {
            let authors_list: Vec<AuthorId> = authors.map(|author| author.unwrap()).collect().await;
            authors_list[0]
        };
        Ok(OkuFs { node, author_id })
    }

    pub fn shutdown(self) {
        self.node.shutdown();
    }

    pub async fn create_replica(&self) -> Result<NamespaceId, Box<dyn Error>> {
        let docs_client = &self.node.docs;
        let new_document = docs_client.create().await?;
        let document_id = new_document.id();
        new_document.close().await?;
        Ok(document_id)
    }

    pub async fn delete_replica(&self, namespace: NamespaceId) -> Result<(), Box<dyn Error>> {
        let docs_client = &self.node.docs;
        Ok(docs_client.drop_doc(namespace).await?)
    }

    pub async fn create_or_modify_file(
        &self,
        namespace: NamespaceId,
        path: PathBuf,
        data: impl Into<Bytes>,
    ) -> Result<Hash, Box<dyn Error>> {
        let file_key = path_to_entry_key(path);
        let data_bytes = data.into();
        let docs_client = &self.node.docs;
        let document = docs_client
            .open(namespace)
            .await?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entry_hash = document
            .set_bytes(self.author_id, file_key, data_bytes)
            .await?;

        Ok(entry_hash)
    }

    pub async fn delete_file(&self, namespace: NamespaceId, path: PathBuf) -> Result<usize, Box<dyn Error>> {
        let file_key = path_to_entry_key(path);
        let docs_client = &self.node.docs;
        let document = docs_client
            .open(namespace)
            .await?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entries_deleted = document.del(self.author_id, file_key).await?;
        Ok(entries_deleted)
    }

    pub async fn read_file(&self, namespace: NamespaceId, path: PathBuf) -> Result<Bytes, Box<dyn Error>> {
        let file_key = path_to_entry_key(path);
        let docs_client = &self.node.docs;
        let document = docs_client
            .open(namespace)
            .await?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entry = document.get_exact(self.author_id, file_key, false).await?.ok_or(OkuFsError::FsEntryNotFound)?;
        Ok(entry.content_bytes(self.node.client()).await?)
    }

    pub async fn move_file(&self, namespace: NamespaceId, from: PathBuf, to: PathBuf) -> Result<(Hash, usize), Box<dyn Error>> {
        let data = self.read_file(namespace, from.clone()).await?;
        let hash = self.create_or_modify_file(namespace, to.clone(), data).await?;
        let entries_deleted = self.delete_file(namespace, from).await?;
        Ok((hash, entries_deleted))
    }

    pub async fn delete_directory(&self, namespace: NamespaceId, path: PathBuf) -> Result<usize, Box<dyn Error>> {
        let path = normalise_path(path).join(""); // Ensure path ends with a slash
        let docs_client = &self.node.docs;
        let document = docs_client
            .open(namespace)
            .await?
            .ok_or(OkuFsError::FsEntryNotFound)?;
        let entries_deleted = document.del(self.author_id, format!("{}", path.display())).await?;
        Ok(entries_deleted)
    }
}

/// Imports the author credentials of the file system from disk, or creates new credentials if none exist.
///
/// # Arguments
///
/// * `path` - The path on disk of the file holding the author's credentials.
///
/// # Returns
///
/// The author credentials.
pub fn load_or_create_author() -> Result<Author, Box<dyn Error>> {
    let path = PathBuf::from(FS_PATH).join("author");
    let author_file = std::fs::read(path.clone());
    match author_file {
        Ok(bytes) => Ok(Author::from_bytes(&bytes[..32].try_into()?)),
        Err(_) => {
            let mut rng = OsRng;
            let author = Author::new(&mut rng);
            let author_bytes = author.to_bytes();
            std::fs::write(path, &author_bytes)?;
            Ok(author)
        }
    }
}
