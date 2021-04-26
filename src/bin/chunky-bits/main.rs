use std::{
    collections::{
        BTreeSet,
        HashSet,
    },
    error::Error,
    net::SocketAddr,
    path::PathBuf,
};

use chunky_bits::{
    cluster::sized_int::{
        ChunkSize,
        DataChunkCount,
        ParityChunkCount,
    },
    http::cluster_filter,
};
use futures::stream::StreamExt;
use structopt::StructOpt;
use tokio::io;
pub mod any_destination;
pub mod cluster_location;
pub mod config;
pub mod error_message;
use crate::{
    cluster_location::ClusterLocation,
    config::Config,
    error_message::ErrorMessage,
};

/// An interface for Chunky Bits files and clusters.
///
/// This provides coreutils-like commands for accessing
/// files. Many of these commands accept cluster locations,
/// which are formatted `cluster-name#path/to/file`.
///
/// Instead of a cluster-name, you may also provide a location
/// for the cluster definition to be read from. Both local
/// file paths and http URL are supported.
/// Example `./cluster.yml#path/to/file`.
#[derive(StructOpt)]
struct Opt {
    /// Location for the config file
    #[structopt(long)]
    config: Option<PathBuf>,
    /// Set the default chunk size for non-cluster destinations
    #[structopt(long)]
    chunk_size: Option<ChunkSize>,
    /// Set the default data chunks for non-cluster destinations
    #[structopt(long)]
    data_chunks: Option<DataChunkCount>,
    /// Set the default parity chunks for non-cluster destinations
    #[structopt(long)]
    parity_chunks: Option<ParityChunkCount>,
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
enum Command {
    /// Concatenate files together
    Cat {
        #[structopt(min_values(1))]
        targets: Vec<ClusterLocation>,
    },
    /// Show the parsed configuration definition
    ConfigInfo,
    /// Show the parsed cluster definition
    ClusterInfo {
        /// The name/location of the cluster to show
        cluster: String,
    },
    /// Copy file from source to destination
    Cp {
        source: ClusterLocation,
        destination: ClusterLocation,
    },
    /// Get all the known hashes for a location
    GetHashes {
        /// Deduplicate all hashes
        #[structopt(short, long = "dedup")]
        deduplicate: bool,
        /// Sort all hashes (Implies --dedup)
        #[structopt(short, long)]
        sort: bool,
        target: ClusterLocation,
    },
    /// Provide a HTTP Gateway for a cluster
    HttpGateway {
        /// The name/location of the cluster to show
        cluster: String,
        /// Listen Address to bind to
        #[structopt(short, long, default_value = "127.0.0.1:8000")]
        listen_addr: SocketAddr,
    },
    /// List the files in a cluster directory
    Ls {
        #[structopt(short, long)]
        recursive: bool,
        target: ClusterLocation,
    },
    /// Resilver a cluster file
    Resilver { target: ClusterLocation },
    /// Verify a cluster file
    Verify { target: ClusterLocation },
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(_) => {},
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        },
    }
}

async fn run() -> Result<(), Box<dyn Error>> {
    let Opt {
        command,
        config,
        chunk_size,
        data_chunks,
        parity_chunks,
    } = Opt::from_args();
    let config = Config::builder()
        .default_data_chunks(data_chunks)
        .default_parity_chunks(parity_chunks)
        .default_chunk_size(chunk_size)
        .path(config);
    match command {
        Command::Cat { targets } => {
            if targets.is_empty() {
                return Err(ErrorMessage::from("At least 1 cat target must be specified").into());
            }
            let config = config.load_or_default().await?;
            let mut stdout = io::stdout();
            for target in targets {
                let mut reader = target.get_reader(&config).await?;
                io::copy(&mut reader, &mut stdout).await?;
            }
        },
        Command::ConfigInfo => {
            let config = config.load_or_default().await?;
            serde_yaml::to_writer(std::io::stdout(), &config)?;
        },
        Command::ClusterInfo { cluster } => {
            let config = config.load_or_default().await?;
            let cluster = config.get_cluster(&cluster).await?;
            serde_yaml::to_writer(std::io::stdout(), &cluster)?;
        },
        Command::Cp {
            source,
            destination,
        } => {
            let config = config.load_or_default().await?;
            let mut reader = source.get_reader(&config).await?;
            destination.write_from_reader(&config, &mut reader).await?;
        },
        Command::GetHashes {
            deduplicate,
            sort,
            target,
        } => {
            let config = config.load_or_default().await?;
            let hash_stream = target.get_hashes_rec(config).await?;
            let hash_stream = hash_stream.filter_map(|res| async move {
                match res {
                    Ok(hash) => Some(hash),
                    Err(err) => {
                        eprintln!("{}", err);
                        None
                    },
                }
            });
            tokio::pin!(hash_stream);

            match (deduplicate, sort) {
                (_, true) => {
                    let hashes = hash_stream.collect::<BTreeSet<_>>().await;
                    for hash in hashes {
                        println!("{}", hash);
                    }
                },
                (true, false) => {
                    let hashes = hash_stream.collect::<HashSet<_>>().await;
                    for hash in hashes {
                        println!("{}", hash);
                    }
                },
                (false, false) => {
                    while let Some(hash) = hash_stream.next().await {
                        println!("{}", hash);
                    }
                },
            }
        },
        Command::HttpGateway {
            cluster,
            listen_addr,
        } => {
            let config = config.load_or_default().await?;
            let cluster = config.get_cluster(&cluster).await?;
            let (_socket, server) = warp::serve(cluster_filter(cluster))
                .try_bind_with_graceful_shutdown(listen_addr, async {
                    let _ = tokio::signal::ctrl_c().await;
                })?;
            server.await;
        },
        Command::Ls { target, recursive } => {
            let config = config.load_or_default().await?;
            let mut files = if recursive {
                target.list_files_recursive(&config).await?
            } else {
                target.list_files(&config).await?
            };
            while let Some(file_res) = files.next().await {
                match file_res {
                    Ok(file) => println!("{}", file.as_ref().display()),
                    Err(err) => eprintln!("Error: {}", err),
                }
            }
        },
        Command::Resilver { target } => {
            let config = config.load_or_default().await?;
            let report = target.resilver(&config).await?;
            println!("{}", report.display_full_report());
        },
        Command::Verify { target } => {
            let config = config.load_or_default().await?;
            let report = target.verify(&config).await?;
            println!("{}", report.display_full_report());
        },
    }
    Ok(())
}
