use bytes::Bytes;
use clap::{Parser, Subcommand};
use env_logger::Builder;
use iroh_base::ticket::Ticket;
use iroh_docs::rpc::client::docs::ShareMode;
use iroh_docs::AuthorId;
use iroh_docs::DocTicket;
use iroh_docs::NamespaceId;
use log::{info, LevelFilter};
use miette::{miette, IntoDiagnostic};
use oku_fs::database::core::OkuDatabase;
use oku_fs::fs::OkuFs;
use rayon::iter::IntoParallelIterator;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use rayon::slice::ParallelSliceMut;
use std::cmp::Reverse;
use std::collections::HashSet;
use std::path::PathBuf;
#[cfg(feature = "fuse")]
use tokio::runtime::Handle;
use url::Url;

mod util;

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
    #[clap(about = "File system commands.")]
    Fs(Fs),
    #[clap(about = "OkuNet commands.")]
    Net(Net),
}

#[derive(Parser)]
struct Fs {
    #[command(subcommand)]
    fs_commands: FsCommands,
}

#[derive(Parser)]
struct Net {
    #[command(subcommand)]
    net_commands: NetCommands,
}

#[derive(Subcommand)]
enum NetCommands {
    /// Follow a user.
    Follow {
        #[arg(value_name = "AUTHOR_ID")]
        /// The ID of the author to follow.
        author_id: AuthorId,
    },
    /// Unfollow a user.
    Unfollow {
        #[arg(value_name = "AUTHOR_ID")]
        /// The ID of the author to unfollow.
        author_id: AuthorId,
    },
    /// Show a user's list of followers.
    Following {
        #[arg(value_name = "AUTHOR_ID")]
        /// The ID of the author whose followers should be shown. If none is specified, the current user's followers will be shown.
        author_id: Option<AuthorId>,
    },
    /// Block a user.
    Block {
        #[arg(value_name = "AUTHOR_ID")]
        /// The ID of the author to block.
        author_id: AuthorId,
    },
    /// Unblock a user.
    Unblock {
        #[arg(value_name = "AUTHOR_ID")]
        /// The ID of the author to unblock.
        author_id: AuthorId,
    },
    /// See a user's blocked list.
    Blocked {
        #[arg(value_name = "AUTHOR_ID")]
        /// The ID of the author whose blocked list should be shown. If none is specified, the current user's blocked list will be shown.
        author_id: Option<AuthorId>,
    },
    /// View posts in chronological order.
    Timeline {
        #[arg(value_name = "AUTHOR_ID")]
        /// The ID of the author whose timeline should be shown. If none is specified, the main timeline will be shown.
        author_id: Option<AuthorId>,
        #[arg(short, long, value_name = "TAG")]
        /// The optional tags to filter posts by.
        tags: Option<Vec<String>>,
    },
    /// Create a post.
    Post {
        #[arg(short, long, value_name = "URL")]
        /// The URL of the post.
        url: Url,
        #[arg(short, long, value_name = "TITLE")]
        /// The title of the post.
        title: Option<String>,
        #[arg(short, long, value_name = "TAGS")]
        /// The tags of the post.
        tags: Option<Vec<String>>,
        #[arg(value_name = "BODY")]
        /// The body of the post.
        body: String,
    },
    /// View a post.
    View {
        #[arg(short, long, value_name = "AUTHOR_ID")]
        /// The ID of the post author. If none is specified, the author is assumed to be the current user.
        author_id: Option<AuthorId>,
        #[arg(short, long, value_name = "POST_PATH")]
        /// The path to the post in the author's home replica.
        post_path: PathBuf,
    },
    /// Search posts.
    Search {
        #[arg(value_name = "QUERY")]
        /// The search query.
        query: String,
        #[arg(default_value_t = 10)]
        /// The maximum number of results to show.
        result_limit: usize,
    },
    /// View all tags used in at least one post.
    Tags {
        #[arg(default_value_t = false)]
        /// Whether to count the number of posts per tag.
        count: bool,
    },
}

#[derive(Subcommand)]
enum FsCommands {
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
pub async fn main() -> miette::Result<()> {
    miette::set_panic_hook();
    let cli = Cli::parse();
    cfg_if::cfg_if! {
        if #[cfg(any(feature = "fuse"))] {
            let handle = Handle::current();
            let node = OkuFs::start(&handle).await.map_err(|e| miette::miette!("{}", e))?;
        } else {
            let node = OkuFs::start().await.map_err(|e| miette::miette!("{}", e))?;
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
        Some(Commands::Fs(Fs {
            fs_commands: command,
        })) => match command {
            FsCommands::CreateReplica => {
                let replica_id = node.create_replica().await?;
                info!("Created replica with ID: {}", replica_id);
            }
            FsCommands::CreateFile {
                replica_id,
                path,
                data,
            } => {
                node.create_or_modify_file(&replica_id, &path, data).await?;
                info!("Created file at {:?}", path);
            }
            FsCommands::ListFiles { replica_id, path } => {
                let files = node.list_files(&replica_id, &path).await?;
                println!("Files: {:#?}", files);
            }
            FsCommands::Share {
                replica_id,
                share_mode,
            } => {
                let ticket = node
                    .create_document_ticket(&replica_id, &share_mode)
                    .await?;
                println!("{}", ticket.serialize());
            }
            FsCommands::ListReplicas => {
                let replicas = node.list_replicas().await?;
                println!(
                    "Replicas: {:#?}",
                    replicas
                        .par_iter()
                        .map(|replica| (oku_fs::fs::util::fmt(replica.0), replica.1))
                        .collect::<Vec<_>>()
                );
            }
            FsCommands::GetFile { replica_id, path } => {
                let data = node.read_file(&replica_id, &path).await?;
                println!("{}", String::from_utf8_lossy(&data));
            }
            FsCommands::RemoveFile { replica_id, path } => {
                node.delete_file(&replica_id, &path).await?;
                info!("Removed file at {:?}", path);
            }
            FsCommands::RemoveDirectory { replica_id, path } => {
                node.delete_directory(&replica_id, &path).await?;
                info!("Removed directory at {:?}", path);
            }
            FsCommands::RemoveReplica { replica_id } => {
                node.delete_replica(&replica_id).await?;
                info!("Removed replica with ID: {}", replica_id);
            }
            FsCommands::MoveFile {
                old_replica_id,
                old_path,
                new_replica_id,
                new_path,
            } => {
                node.move_file(&old_replica_id, &old_path, &new_replica_id, &new_path)
                    .await?;
                info!(
                    "Moved file from {:?} in {} to {:?} in {}",
                    old_path, old_replica_id, new_path, new_replica_id
                );
            }
            FsCommands::MoveDirectory {
                old_replica_id,
                old_path,
                new_replica_id,
                new_path,
            } => {
                node.move_directory(&old_replica_id, &old_path, &new_replica_id, &new_path)
                    .await?;
                info!(
                    "Moved directory from {:?} in {} to {:?} in {}",
                    old_path, old_replica_id, new_path, new_replica_id
                );
            }
            FsCommands::GetReplicaById { replica_id, path } => {
                node.fetch_replica_by_id(&replica_id, &path)
                    .await
                    .map_err(|e| miette!("{}", e))?;
                let files = node.list_files(&replica_id, &path).await?;
                println!("Files: {:#?}", files);
            }
            FsCommands::GetReplicaByTicket {
                replica_ticket,
                path,
            } => {
                node.fetch_replica_by_ticket(&replica_ticket, &path, &None)
                    .await
                    .map_err(|e| miette!("{}", e))?;
                let files = node
                    .list_files(&replica_ticket.capability.id(), &path)
                    .await?;
                println!("Files: {:#?}", files);
            }
            #[cfg(feature = "fuse")]
            FsCommands::Mount { path } => {
                info!("Node will listen for incoming connections.");
                let mount_handle = node.mount(path)?;
                tokio::signal::ctrl_c().await.into_diagnostic()?;
                mount_handle.join();
            }
        },
        Some(Commands::Net(Net {
            net_commands: command,
        })) => match command {
            NetCommands::Follow { author_id } => {
                node.follow(&author_id).await?;
                println!("Now following {} … ", util::name(&node, &author_id).await);
            }
            NetCommands::Unfollow { author_id } => {
                node.unfollow(&author_id).await?;
                println!(
                    "No longer following {} … ",
                    util::name(&node, &author_id).await
                );
            }
            NetCommands::Post {
                url,
                body,
                title,
                tags,
            } => {
                let tags = tags.unwrap_or_default().into_par_iter().collect();
                let hash = node
                    .create_or_modify_post(&None, &url, &title.unwrap_or_default(), &body, &tags)
                    .await?;
                println!(
                    "{:#?}",
                    node.content_bytes_by_hash(&hash)
                        .await
                        .ok()
                        .map(|x| String::from_utf8_lossy(&x).to_string())
                );
            }
            NetCommands::Following { author_id } => {
                let list = match author_id {
                    None => node
                        .identity()
                        .await
                        .map(|x| x.following)
                        .unwrap_or_default(),
                    Some(id) => node
                        .get_or_fetch_user(&id)
                        .await
                        .ok()
                        .and_then(|x| x.identity.map(|y| y.following))
                        .unwrap_or_default(),
                };
                for user in list {
                    println!("{}", util::name(&node, &user).await);
                }
            }
            NetCommands::Timeline { author_id, tags } => {
                let mut posts = match author_id {
                    None => node.posts().await.unwrap_or_default(),
                    Some(id) => node
                        .posts_from_user(&node.get_or_fetch_user(&id).await?)
                        .await
                        .unwrap_or_default(),
                };
                match tags {
                    None => (),
                    Some(tags) => {
                        let tag_set: HashSet<_> = tags.into_par_iter().collect();
                        posts = node.posts_with_tags(&posts, &tag_set).await;
                    }
                }
                posts.par_sort_unstable_by_key(|x| Reverse(x.entry.timestamp()));
                for post in posts {
                    println!("⮞ {}", util::post(&post).await);
                }
            }
            NetCommands::Block { author_id } => {
                node.block(&author_id).await?;
                println!("{} is now blocked … ", util::name(&node, &author_id).await);
            }
            NetCommands::Unblock { author_id } => {
                node.unblock(&author_id).await?;
                println!(
                    "{} is no longer blocked … ",
                    util::name(&node, &author_id).await
                );
            }
            NetCommands::Blocked { author_id } => {
                let list = match author_id {
                    None => node.identity().await.map(|x| x.blocked).unwrap_or_default(),
                    Some(id) => node
                        .get_or_fetch_user(&id)
                        .await
                        .ok()
                        .and_then(|x| x.identity.map(|y| y.blocked))
                        .unwrap_or_default(),
                };
                for user in list {
                    println!("{}", util::name(&node, &user).await);
                }
            }
            NetCommands::View {
                author_id,
                post_path,
            } => {
                let post = match author_id {
                    None => node.post(&post_path).await?,
                    Some(id) => node.get_or_fetch_post(&id, &post_path).await?,
                };
                println!("{}", util::post(&post).await)
            }
            NetCommands::Search {
                query,
                result_limit,
            } => {
                let posts =
                    OkuDatabase::search_posts(&query, &Some(result_limit)).unwrap_or_default();
                for post in posts {
                    println!("⮞ {}", util::post(&post).await);
                }
            }
            NetCommands::Tags { count } => match count {
                false => println!("{:?}", node.all_tags(&node.all_posts().await).await),
                true => println!("{:#?}", node.count_tags(&node.all_posts().await).await),
            },
        },
        None => {
            info!("Node will listen for incoming connections.");
            tokio::signal::ctrl_c().await.into_diagnostic()?;
            node.shutdown().await;
        }
    }
    Ok(())
}
