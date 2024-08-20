use bytes::Bytes;
use clap::{Parser, Subcommand};
use iroh::docs::NamespaceId;
use oku_fs::fs::OkuFs;
use std::{path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}
#[derive(Subcommand)]
enum Commands {
    /// Create a new replica.
    CreateReplica,
    /// Create a new file in a replica.
    CreateFile {
        #[arg(short, long, value_name = "REPLICA_ID")]
        /// The ID of the replica to create the file in.
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH")]
        /// The path of the file to create.
        path: PathBuf,
        #[arg(short, long, value_name = "DATA")]
        /// The data to write to the file.
        data: Bytes,
    },
    /// List files in a replica.
    ListFiles {
        #[arg(short, long, value_name = "REPLICA_ID")]
        /// The ID of the replica to list files from.
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH", default_missing_value = None)]
        /// The optional path of the directory to list files from.
        path: Option<PathBuf>,
    },
    /// List local replicas.
    ListReplicas,
    /// Get the contents of a file in a replica.
    GetFile {
        #[arg(short, long, value_name = "REPLICA_ID")]
        /// The ID of the replica to get the file from.
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH")]
        /// The path of the file to get.
        path: PathBuf,
    },
    /// Remove a file from a replica.
    RemoveFile {
        #[arg(short, long, value_name = "REPLICA_ID")]
        /// The ID of the replica to remove the file from.
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH")]
        /// The path of the file to remove.
        path: PathBuf,
    },
    /// Remove a directory from a replica.
    RemoveDirectory {
        #[arg(short, long, value_name = "REPLICA_ID")]
        /// The ID of the replica to remove the directory from.
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH")]
        /// The path to the directory to remove.
        path: PathBuf,
    },
    /// Remove a replica from the node.
    RemoveReplica {
        #[arg(value_name = "REPLICA_ID")]
        /// The ID of the replica to remove.
        replica_id: NamespaceId,
    },
    /// Move a file from one path to another in a replica.
    MoveFile {
        #[arg(short, long, value_name = "REPLICA_ID")]
        /// The ID of the replica to move the file in.
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "OLD_PATH")]
        /// The path of the file to move.
        old_path: PathBuf,
        #[arg(short, long, value_name = "NEW_PATH")]
        /// The new path of the file.
        new_path: PathBuf,
    },
    /// Get a replica from another node.
    GetReplica {
        #[arg(short, long, value_name = "REPLICA_ID")]
        /// The ID of the replica to get.
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH", default_missing_value = None)]
        /// The optional path of the directory to get within the replica.
        path: Option<PathBuf>,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> miette::Result<()> {
    miette::set_panic_hook();
    let cli = Cli::parse();
    let node = OkuFs::start().await?;
    match cli.command {
        Some(Commands::CreateReplica) => {
            let replica_id = node.create_replica().await?;
            println!("Created replica with ID: {}", replica_id);
        }
        Some(Commands::CreateFile {
            replica_id,
            path,
            data,
        }) => {
            node.create_or_modify_file(replica_id, path.clone(), data)
                .await?;
            println!("Created file at {:?}", path);
        }
        Some(Commands::ListFiles { replica_id, path }) => {
            let files = node.list_files(replica_id, path).await?;
            for file in files {
                println!("{:#?}", file);
            }
        }
        Some(Commands::ListReplicas) => {
            let replicas = node.list_replicas().await?;
            for replica in replicas {
                println!("{}", replica);
            }
        }
        Some(Commands::GetFile { replica_id, path }) => {
            let data = node.read_file(replica_id, path).await?;
            println!("{}", String::from_utf8_lossy(&data));
        }
        Some(Commands::RemoveFile { replica_id, path }) => {
            node.delete_file(replica_id, path.clone()).await?;
            println!("Removed file at {:?}", path);
        }
        Some(Commands::RemoveDirectory { replica_id, path }) => {
            node.delete_directory(replica_id, path.clone()).await?;
            println!("Removed directory at {:?}", path);
        }
        Some(Commands::RemoveReplica { replica_id }) => {
            node.delete_replica(replica_id).await?;
            println!("Removed replica with ID: {}", replica_id);
        }
        Some(Commands::MoveFile {
            replica_id,
            old_path,
            new_path,
        }) => {
            node.move_file(replica_id, old_path.clone(), new_path.clone())
                .await?;
            println!("Moved file from {:?} to {:?}", old_path, new_path);
        }
        Some(Commands::GetReplica { replica_id, path }) => {
            node.get_external_replica(replica_id, path.clone(), true, true)
                .await?;
            let files = node.list_files(replica_id, path).await?;
            for file in files {
                println!("{:#?}", file);
            }
        }
        None => {
            println!("Node will listen for incoming connections.");
            loop {
                std::thread::sleep(Duration::from_secs(86400));
            }
        }
    }
    Ok(())
}
