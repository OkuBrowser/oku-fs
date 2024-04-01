use crate::error::OkuFsError;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use iroh::{
    bytes::Hash,
    node::FsNode,
    sync::{store::Query, Author, NamespaceId},
};
use path_clean::PathClean;
use rand_core::OsRng;
use serde::Deserialize;
use serde::Serialize;
use std::{
    error::Error,
    path::PathBuf,
    sync::{Arc, Mutex},
};

fn normalise_path(path: PathBuf) -> PathBuf {
    PathBuf::from("/").join(path).clean()
}

/// An instance of an Oku file system.
pub struct OkuFs {
    /// The Iroh node responsible for managing the file system.
    node: FsNode,
    /// The default author for the file system.
    author: Author,
    /// The root directory of the file system.
    root: Arc<Mutex<Directory>>,
}

/// A file in an Oku file system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct File {
    /// The ID of the Iroh document holding the file's data.
    id: NamespaceId,
    /// The path of the file.
    name: String,
}

/// A directory in an Oku file system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Directory {
    /// The children of the directory.
    children: Vec<FsEntry>,
    /// The name of the directory. `None` if the directory is the root directory.
    name: Option<String>,
}

/// An entry in an Oku file system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FsEntry {
    File(Arc<Mutex<File>>),
    Directory(Arc<Mutex<Directory>>),
}

impl FsEntry {
    /// Returns whether the entry is a directory.
    pub fn is_dir(&self) -> bool {
        matches!(self, Self::Directory(_))
    }

    /// Returns whether the entry is a file.
    pub fn is_file(&self) -> bool {
        matches!(self, Self::File(_))
    }

    /// Returns the entry as a directory.
    pub fn as_dir(&self) -> Result<&Arc<Mutex<Directory>>, Box<dyn Error>> {
        match self {
            Self::Directory(dir) => Ok(dir),
            _ => Err(Box::new(OkuFsError::CannotGetLock)),
        }
    }

    /// Returns the entry as a file.
    pub fn as_file(&self) -> Result<&Arc<Mutex<File>>, Box<dyn Error>> {
        match self {
            Self::File(file) => Ok(file),
            _ => Err(Box::new(OkuFsError::CannotGetLock)),
        }
    }
}

impl OkuFs {
    /// Imports the author credentials of the file system from disk, or creates new credentials if none exist.
    ///
    /// # Arguments
    ///
    /// * `path` - The path on disk of the file holding the author's credentials.
    ///
    /// # Returns
    ///
    /// The author credentials.
    pub async fn load_or_create_author(path: PathBuf) -> Result<Author, Box<dyn Error>> {
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

    /// Imports the root directory of the file system from disk, or creates a new root directory if none exists.
    ///
    /// # Arguments
    ///
    /// * `path` - The path on disk of the file holding the root directory.
    ///
    /// # Returns
    ///
    /// The root directory.
    pub fn load_or_create_root(path: PathBuf) -> Result<Directory, Box<dyn Error>> {
        let root_file = std::fs::read(path.clone());
        match root_file {
            Ok(bytes) => {
                let root: Directory = bincode::deserialize(&bytes[..])?;
                Ok(root)
            }
            Err(_) => {
                let root = Directory {
                    children: Vec::new(),
                    name: None,
                };
                let root_bytes = bincode::serialize(&root)?;
                std::fs::write(path, &root_bytes)?;
                Ok(root)
            }
        }
    }

    /// Opens an existing Oku file system, or creates a new file system if none exists.
    ///
    /// # Arguments
    ///
    /// * `node_path` - The path of the file on disk holding the Iroh node's data.
    ///
    /// * `author_path` - The path of the file on disk holding the author's credentials.
    ///
    /// * `root_path` - The path of the file on disk holding the root directory.
    ///
    /// # Returns
    ///
    /// A running instance of an Oku file system.
    pub async fn open_or_create(
        node_path: PathBuf,
        author_path: PathBuf,
        root_path: PathBuf,
    ) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            node: FsNode::persistent(node_path).await?.spawn().await?,
            author: Self::load_or_create_author(author_path).await?,
            root: Arc::new(Mutex::new(Self::load_or_create_root(root_path)?)),
        })
    }

    /// Shuts down the file system.
    pub async fn shutdown(self) {
        self.node.shutdown();
    }

    /// Obtains a file system entry given its path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the entry to obtain.
    ///
    /// # Returns
    ///
    /// The file system entry.
    pub fn get_entry(&self, path: PathBuf) -> Result<FsEntry, Box<dyn Error>> {
        let path = normalise_path(path);
        let mut current: FsEntry = FsEntry::Directory(self.root.clone());
        let mut traversed_path = PathBuf::new();
        for component in path.components() {
            if current.is_dir() {
                let current_clone = current.clone();
                let current_dir = current_clone.as_dir()?;
                current = current_dir
                    .lock()
                    .unwrap()
                    .children
                    .iter()
                    .find(|entry| match entry {
                        FsEntry::Directory(dir) => {
                            dir.lock().unwrap().name
                                == Some(component.as_os_str().to_string_lossy().to_string())
                        }
                        _ => false,
                    })
                    .ok_or(OkuFsError::FsEntryNotFound)?
                    .clone();
                traversed_path.push(component);
            } else {
                return Err(Box::new(OkuFsError::FsEntryNotFound));
            }
        }
        Ok(current)
    }

    /// Obtains the parent directory of a file system entry.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the entry to obtain the parent directory of.
    ///
    /// # Returns
    ///
    /// The parent directory of the entry.
    pub fn get_parent_directory(
        &self,
        path: PathBuf,
    ) -> Result<Arc<Mutex<Directory>>, Box<dyn Error>> {
        let path = normalise_path(path);
        let entry = self.get_entry(
            path.parent()
                .ok_or(OkuFsError::FsEntryNotFound)?
                .to_path_buf(),
        )?;
        let entry_result = entry.as_dir();
        Ok(entry_result?.clone())
    }

    /// Creates a new directory in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the directory to create.
    pub fn create_directory(&self, path: PathBuf) -> Result<Arc<Mutex<Directory>>, Box<dyn Error>> {
        let path = normalise_path(path);
        let parent = self.get_parent_directory(path.clone())?;
        let name = path
            .components()
            .last()
            .unwrap()
            .as_os_str()
            .to_string_lossy()
            .to_string();
        let already_exists = self.get_entry(path.clone()).is_ok();
        if already_exists {
            return Err(Box::new(OkuFsError::DirectoryAlreadyExists));
        }
        let new_directory = Directory {
            children: Vec::new(),
            name: Some(name),
        };
        let new_directory = Arc::new(Mutex::new(new_directory));
        parent
            .lock()
            .unwrap()
            .children
            .push(FsEntry::Directory(new_directory.clone()));
        Ok(new_directory)
    }

    /// Renames a directory in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the directory to rename.
    ///
    /// * `new_name` - The new name of the directory.
    pub fn rename_directory(&self, path: PathBuf, new_name: String) -> Result<(), Box<dyn Error>> {
        let path = normalise_path(path);
        let entry = self.get_entry(path.clone())?;
        let directory = entry.as_dir()?;
        directory.lock().unwrap().name = Some(new_name);
        Ok(())
    }

    /// Renames a file in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the file to rename.
    ///
    /// * `new_name` - The new name of the file.
    pub fn rename_file(&self, path: PathBuf, new_name: String) -> Result<(), Box<dyn Error>> {
        let path = normalise_path(path);
        let entry = self.get_entry(path.clone())?;
        let file = entry.as_file()?;
        file.lock().unwrap().name = new_name;
        Ok(())
    }

    /// Moves a file from one location to another in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the file to move.
    ///
    /// * `new_path` - The new path of the file.
    pub fn move_file(&self, path: PathBuf, new_path: PathBuf) -> Result<(), Box<dyn Error>> {
        let path = normalise_path(path);
        let new_path = normalise_path(new_path);
        let entry = self.get_entry(path.clone())?;
        let old_parent = self.get_parent_directory(path.clone())?;
        let new_parent = self.get_parent_directory(new_path.clone())?;
        let new_name = new_path
            .clone()
            .file_name()
            .ok_or(OkuFsError::FileNoName)?
            .to_string_lossy()
            .to_string();
        let file = entry.as_file()?;
        old_parent
            .lock()
            .unwrap()
            .children
            .retain(|entry| match entry {
                FsEntry::File(f) => f.lock().unwrap().name != file.lock().unwrap().name,
                _ => true,
            });
        file.lock().unwrap().name = new_name;
        new_parent
            .lock()
            .unwrap()
            .children
            .push(FsEntry::File(file.clone()));
        Ok(())
    }

    /// Moves a directory from one location to another in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the directory to move.
    ///
    /// * `new_path` - The new path of the directory.
    pub fn move_directory(&self, path: PathBuf, new_path: PathBuf) -> Result<(), Box<dyn Error>> {
        let path = normalise_path(path);
        let new_path = normalise_path(new_path);
        let entry = self.get_entry(path.clone())?;
        let old_parent = self.get_parent_directory(path.clone())?;
        let new_parent = self.get_parent_directory(new_path.clone())?;
        let new_name = new_path
            .components()
            .last()
            .unwrap()
            .as_os_str()
            .to_string_lossy()
            .to_string();
        let directory = entry.as_dir()?;
        old_parent
            .lock()
            .unwrap()
            .children
            .retain(|entry| match entry {
                FsEntry::Directory(d) => d.lock().unwrap().name != directory.lock().unwrap().name,
                _ => true,
            });
        directory.lock().unwrap().name = Some(new_name);
        new_parent
            .lock()
            .unwrap()
            .children
            .push(FsEntry::Directory(directory.clone()));
        Ok(())
    }

    /// Creates a new file in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the file to create.
    ///
    /// * `data` - The data to write to the file.
    ///
    /// # Returns
    ///
    /// The ID and hash of the new document.
    pub async fn create_file(
        &self,
        path: PathBuf,
        data: impl Into<Bytes>,
    ) -> Result<(NamespaceId, Hash), Box<dyn Error>> {
        // Creates a new Iroh document, making note of the document ID and its path in the file system.
        let path = normalise_path(path);
        let parent = self.get_parent_directory(path.clone())?;
        let name = path
            .clone()
            .file_name()
            .ok_or(OkuFsError::FileNoName)?
            .to_string_lossy()
            .to_string();
        let already_exists = self.get_entry(path.clone()).is_ok();
        if already_exists {
            return Err(Box::new(OkuFsError::FileAlreadyExists));
        }
        let docs_client = &self.node.docs;
        let new_document = docs_client.create().await?;
        // The document's initial entry has the current time as its key.
        let current_time: DateTime<Utc> = Utc::now();
        let entry_key_string = current_time.to_rfc3339();
        let hash = new_document
            .set_bytes(self.author.id(), entry_key_string, data.into())
            .await?;
        let id = new_document.id();
        new_document.close().await?;
        let new_file = File { id, name: name };
        parent
            .lock()
            .unwrap()
            .children
            .push(FsEntry::File(Arc::new(Mutex::new(new_file))));
        Ok((id, hash))
    }

    /// Modifies an existing file in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the file to modify.
    ///
    /// * `data` - The data to write to the file.
    ///
    /// # Returns
    ///
    /// The hash of the document's new state.
    pub async fn modify_file(
        &self,
        path: PathBuf,
        data: impl Into<Bytes>,
    ) -> Result<Hash, Box<dyn Error>> {
        let path = normalise_path(path);
        let entry = &self.get_entry(path.clone())?;
        let id = match entry.clone() {
            FsEntry::File(file) => {
                let file = file.lock().unwrap();
                file.id
            }
            _ => return Err(Box::new(OkuFsError::FsEntryNotFound)),
        };
        let docs_client = &self.node.docs;
        let document = docs_client.open(id).await?;
        match document {
            None => return Err(Box::new(OkuFsError::FsEntryNotFound)),
            Some(document) => {
                // The current state of the document is given the current time as its key.
                let current_time: DateTime<Utc> = Utc::now();
                let entry_key_string = current_time.to_rfc3339();
                let hash = document
                    .set_bytes(self.author.id(), entry_key_string, data.into())
                    .await?;
                document.close().await?;
                Ok(hash)
            }
        }
    }

    /// Deletes a folder in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the folder to delete.
    pub fn delete_folder(&self, path: PathBuf) -> Result<(), Box<dyn Error>> {
        let path = normalise_path(path);
        let entry = self.get_entry(path.clone())?;
        let parent = self.get_parent_directory(path.clone())?;
        let entry = entry.as_dir()?;
        let directory = entry.lock().unwrap();
        let directory_name = directory.name.clone();
        if directory.children.len() > 0 {
            return Err(Box::new(OkuFsError::DirectoryNotEmpty));
        }
        parent.lock().unwrap().children.retain(|child| match child {
            FsEntry::Directory(d) => d.lock().unwrap().name != directory_name,
            _ => true,
        });
        Ok(())
    }

    /// Deletes a file in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the file to delete.
    ///
    /// # Returns
    ///
    /// The number of document entries deleted.
    pub async fn delete_file(&self, path: PathBuf) -> Result<usize, Box<dyn Error>> {
        let path = normalise_path(path);
        let entry = self.get_entry(path.clone())?;
        let parent = self.get_parent_directory(path.clone())?;
        let entry = entry.as_file()?;
        let file = entry.lock().unwrap();
        let file_name = file.name.clone();
        parent.lock().unwrap().children.retain(|child| match child {
            FsEntry::File(f) => f.lock().unwrap().name != file_name,
            _ => true,
        });
        let file_id = file.id;
        let docs_client = &self.node.docs;
        let document = docs_client.open(file_id).await?;
        match document {
            None => return Err(Box::new(OkuFsError::FsEntryNotFound)),
            Some(document) => {
                let entries_deleted = document.del(self.author.id(), "").await?;
                document.close().await?;
                Ok(entries_deleted)
            }
        }
    }

    /// Lists the contents of a directory in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the directory to list.
    ///
    /// # Returns
    ///
    /// A vector of this directory's children.
    pub fn list_directory(&self, path: PathBuf) -> Result<Vec<FsEntry>, Box<dyn Error>> {
        let path = normalise_path(path);
        let entry = self.get_entry(path)?;
        let directory = entry.as_dir()?;
        let directory = directory.lock().unwrap();
        Ok(directory.children.clone())
    }

    /// Reads the contents of a file in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the file to read.
    ///
    /// # Returns
    ///
    /// The data stored in the file.
    pub async fn read_file(&self, path: PathBuf) -> Result<Bytes, Box<dyn Error>> {
        let path = normalise_path(path);
        let entry = self.get_entry(path)?;
        let file = entry.as_file()?;
        let file = file.lock().unwrap();
        let file_id = file.id;
        let docs_client = &self.node.docs;
        let document = docs_client.open(file_id).await?;
        match document {
            None => return Err(Box::new(OkuFsError::FsEntryNotFound)),
            Some(document) => {
                let query = Query::all()
                    .sort_by(
                        iroh::sync::store::SortBy::KeyAuthor,
                        iroh::sync::store::SortDirection::Desc,
                    )
                    .build();
                let document_entry = document.get_one(query).await?;
                match document_entry {
                    None => return Err(Box::new(OkuFsError::FsEntryNotFound)),
                    Some(entry) => {
                        let data = entry.content_bytes(&document).await?;
                        Ok(data)
                    }
                }
            }
        }
    }

    /// Copies a file in the file system.
    ///
    /// # Arguments
    ///
    /// * `path` - The path of the file to copy.
    ///
    /// * `new_path` - The destination path of the copy.
    ///
    /// # Returns
    ///
    /// The ID and hash of the copy.
    pub async fn copy_file(
        &self,
        path: PathBuf,
        new_path: PathBuf,
    ) -> Result<(NamespaceId, Hash), Box<dyn Error>> {
        let path = normalise_path(path);
        let new_path = normalise_path(new_path);
        let file_contents = self.read_file(path.clone()).await?;
        self.create_file(new_path.clone(), file_contents).await
    }
}
