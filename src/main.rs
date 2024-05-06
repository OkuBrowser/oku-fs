use bytes::Bytes;
use clap::{Parser, Subcommand};
use iroh::sync::NamespaceId;
use oku_fs::fs::OkuFs;
use std::{error::Error, path::PathBuf};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}
#[derive(Subcommand)]
enum Commands {
    CreateReplica,
    CreateFile {
        #[arg(short, long, value_name = "REPLICA_ID")]
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH")]
        path: PathBuf,
        #[arg(short, long, value_name = "DATA")]
        data: Bytes,
    },
    ListFiles {
        #[arg(value_name = "REPLICA_ID")]
        replica_id: NamespaceId,
    },
    ListReplicas,
    GetFile {
        #[arg(short, long, value_name = "REPLICA_ID")]
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH")]
        path: PathBuf,
    },
    RemoveFile {
        #[arg(short, long, value_name = "REPLICA_ID")]
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH")]
        path: PathBuf,
    },
    RemoveDirectory {
        #[arg(short, long, value_name = "REPLICA_ID")]
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH")]
        path: PathBuf,
    },
    RemoveReplica {
        #[arg(value_name = "REPLICA_ID")]
        replica_id: NamespaceId,
    },
    MoveFile {
        #[arg(short, long, value_name = "REPLICA_ID")]
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "OLD_PATH")]
        old_path: PathBuf,
        #[arg(short, long, value_name = "NEW_PATH")]
        new_path: PathBuf,
    },
    GetReplica {
        #[arg(short, long, value_name = "REPLICA_ID")]
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH", default_missing_value = None)]
        path: Option<PathBuf>,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
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
        Some(Commands::ListFiles { replica_id }) => {
            let files = node.list_files(replica_id).await?;
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
            node.get_external_replica(replica_id, path, true, true)
                .await?;
            let files = node.list_files(replica_id).await?;
            for file in files {
                println!("{:#?}", file);
            }
        }
        None => {
            println!("Node will listen for incoming connections.");
            loop {}
        }
    }
    Ok(())
}
