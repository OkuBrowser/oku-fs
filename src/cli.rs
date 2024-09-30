use bytes::Bytes;
use clap::{Parser, Subcommand};
use env_logger::Builder;
use iroh::base::ticket::Ticket;
use iroh::docs::DocTicket;
use iroh::{client::docs::ShareMode, docs::NamespaceId};
use log::{info, LevelFilter};
use miette::{miette, IntoDiagnostic};
use oku_fs::fs::OkuFs;
use std::path::PathBuf;
#[cfg(feature = "fuse")]
use tokio::runtime::Handle;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
    /// The level of log output; warnings, information, debugging messages, and trace logs.
    #[arg(short, long, action = clap::ArgAction::Count, default_value_t = 2, global = true)]
    verbosity: u8,
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
    /// Create a ticket with which a replica can be retrieved.
    Share {
        #[arg(short, long, value_name = "REPLICA_ID")]
        /// The ID of the replica to share.
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "SHARE_MODE", default_value_t = ShareMode::Read)]
        /// Whether the replica should be shared as read-only, or if read & write permissions are to be shared.
        share_mode: ShareMode,
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
        #[arg(short, long, value_name = "OLD_REPLICA_ID")]
        /// The ID of the replica containing the file to move.
        old_replica_id: NamespaceId,
        #[arg(short, long, value_name = "OLD_PATH")]
        /// The path of the file to move.
        old_path: PathBuf,
        #[arg(short, long, value_name = "NEW_REPLICA_ID")]
        /// The ID of the replica to move the file to.
        new_replica_id: NamespaceId,
        #[arg(short, long, value_name = "NEW_PATH")]
        /// The new path of the file.
        new_path: PathBuf,
    },
    /// Move a directory from one path to another in a replica.
    MoveDirectory {
        #[arg(short, long, value_name = "OLD_REPLICA_ID")]
        /// The ID of the replica containing the directory to move.
        old_replica_id: NamespaceId,
        #[arg(short, long, value_name = "OLD_PATH")]
        /// The path of the directory to move.
        old_path: PathBuf,
        #[arg(short, long, value_name = "NEW_REPLICA_ID")]
        /// The ID of the replica to move the directory to.
        new_replica_id: NamespaceId,
        #[arg(short, long, value_name = "NEW_PATH")]
        /// The new path of the directory.
        new_path: PathBuf,
    },
    /// Get a replica from other nodes by its ID.
    GetReplicaById {
        #[arg(short, long, value_name = "REPLICA_ID")]
        /// The ID of the replica to get.
        replica_id: NamespaceId,
        #[arg(short, long, value_name = "PATH", default_missing_value = None)]
        /// The optional path to get within the replica.
        path: Option<PathBuf>,
    },
    /// Get a replica from other nodes using a ticket.
    GetReplicaByTicket {
        #[arg(short, long, value_name = "REPLICA_TICKET")]
        /// A ticket for the replica to get.
        replica_ticket: DocTicket,
        #[arg(short, long, value_name = "PATH", default_missing_value = None)]
        /// The optional path to get within the replica.
        path: Option<PathBuf>,
    },
    /// Mount the filesystem.
    #[cfg(feature = "fuse")]
    Mount {
        #[arg(value_name = "PATH")]
        /// The path of the directory to mount the filesystem in.
        path: PathBuf,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> miette::Result<()> {
    miette::set_panic_hook();
    let cli = Cli::parse();
    cfg_if::cfg_if! {
        if #[cfg(any(feature = "fuse"))] {
            let handle = Handle::current();
            let node = OkuFs::start(&handle).await?;
        } else {
            let node = OkuFs::start().await?;
        }
    };

    let verbosity_level = match cli.verbosity {
        0 => LevelFilter::Error,
        1 => LevelFilter::Warn,
        2 => LevelFilter::Info,
        3 => LevelFilter::Debug,
        4 => LevelFilter::Trace,
        _ => LevelFilter::Trace,
    };
    let mut builder = Builder::new();
    builder.filter(Some("oku_fs"), verbosity_level);
    builder.format_module_path(false);
    if cli.verbosity >= 3 {
        builder.format_module_path(true);
    }
    builder.init();

    match cli.command {
        Some(Commands::CreateReplica) => {
            let replica_id = node.create_replica().await?;
            info!("Created replica with ID: {}", replica_id);
        }
        Some(Commands::CreateFile {
            replica_id,
            path,
            data,
        }) => {
            node.create_or_modify_file(replica_id, path.clone(), data)
                .await?;
            info!("Created file at {:?}", path);
        }
        Some(Commands::ListFiles { replica_id, path }) => {
            let files = node.list_files(replica_id, path).await?;
            println!("Files: {:#?}", files);
        }
        Some(Commands::Share {
            replica_id,
            share_mode,
        }) => {
            let ticket = node.create_document_ticket(replica_id, share_mode).await?;
            println!("{}", ticket.serialize());
        }
        Some(Commands::ListReplicas) => {
            let replicas = node.list_replicas().await?;
            println!(
                "Replicas: {:#?}",
                replicas
                    .iter()
                    .map(|replica| replica.0.to_string())
                    .collect::<Vec<String>>()
            );
        }
        Some(Commands::GetFile { replica_id, path }) => {
            let data = node.read_file(replica_id, path).await?;
            println!("{}", String::from_utf8_lossy(&data));
        }
        Some(Commands::RemoveFile { replica_id, path }) => {
            node.delete_file(replica_id, path.clone()).await?;
            info!("Removed file at {:?}", path);
        }
        Some(Commands::RemoveDirectory { replica_id, path }) => {
            node.delete_directory(replica_id, path.clone()).await?;
            info!("Removed directory at {:?}", path);
        }
        Some(Commands::RemoveReplica { replica_id }) => {
            node.delete_replica(replica_id).await?;
            info!("Removed replica with ID: {}", replica_id);
        }
        Some(Commands::MoveFile {
            old_replica_id,
            old_path,
            new_replica_id,
            new_path,
        }) => {
            node.move_file(
                old_replica_id.clone(),
                old_path.clone(),
                new_replica_id.clone(),
                new_path.clone(),
            )
            .await?;
            info!(
                "Moved file from {:?} in {} to {:?} in {}",
                old_path, old_replica_id, new_path, new_replica_id
            );
        }
        Some(Commands::MoveDirectory {
            old_replica_id,
            old_path,
            new_replica_id,
            new_path,
        }) => {
            node.move_directory(
                old_replica_id.clone(),
                old_path.clone(),
                new_replica_id.clone(),
                new_path.clone(),
            )
            .await?;
            info!(
                "Moved directory from {:?} in {} to {:?} in {}",
                old_path, old_replica_id, new_path, new_replica_id
            );
        }
        Some(Commands::GetReplicaById { replica_id, path }) => {
            node.fetch_replica_by_id(replica_id, path.clone(), true, true)
                .await
                .map_err(|e| miette!("{}", e))?;
            let files = node.list_files(replica_id, path).await?;
            println!("Files: {:#?}", files);
        }
        Some(Commands::GetReplicaByTicket {
            replica_ticket,
            path,
        }) => {
            node.fetch_replica_by_ticket(replica_ticket.clone(), path.clone())
                .await
                .map_err(|e| miette!("{}", e))?;
            let files = node
                .list_files(replica_ticket.capability.id(), path)
                .await?;
            println!("Files: {:#?}", files);
        }
        #[cfg(feature = "fuse")]
        Some(Commands::Mount { path }) => {
            info!("Node will listen for incoming connections.");
            let mount_handle = node.mount(path)?;
            tokio::signal::ctrl_c().await.into_diagnostic()?;
            mount_handle.join();
        }
        None => {
            info!("Node will listen for incoming connections.");
            tokio::signal::ctrl_c().await.into_diagnostic()?;
            node.shutdown().await?;
        }
    }
    Ok(())
}
