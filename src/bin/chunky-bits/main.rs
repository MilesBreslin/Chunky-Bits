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
use futures::stream::{
    FuturesOrdered,
    FuturesUnordered,
    StreamExt,
};
use reed_solomon_erasure::{
    galois_8,
    ReedSolomon,
};
use structopt::StructOpt;
use tokio::io::{
    self,
    AsyncReadExt,
    AsyncWriteExt,
};
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
    DecodeShards {
        targets: Vec<ClusterLocation>,
    },
    EncodeShards {
        source: ClusterLocation,
        targets: Vec<ClusterLocation>,
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
    Resilver {
        target: ClusterLocation,
    },
    /// Verify a cluster file
    Verify {
        target: ClusterLocation,
    },
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
        Command::DecodeShards { targets } => {
            let config = config.load_or_default().await?;
            let (data_chunks, _parity_chunks, encoder) =
                get_shard_encoder(data_chunks, parity_chunks, &targets)?;

            let config = &config;
            let mut shard_data: Vec<Option<Vec<u8>>> = targets
                .iter()
                .map(|target| async move {
                    let mut bytes = Vec::new();
                    let mut reader = match target.get_reader(&config).await {
                        Ok(reader) => reader,
                        Err(err) => {
                            eprintln!("Error {}: {}", target, err);
                            return None;
                        },
                    };
                    match reader.read_to_end(&mut bytes).await {
                        Ok(_) => Some(bytes),
                        Err(err) => {
                            eprintln!("Error {}: {}", target, err);
                            None
                        },
                    }
                })
                .collect::<FuturesOrdered<_>>()
                .collect()
                .await;
            encoder.reconstruct_data(&mut shard_data)?;
            let mut writer = io::stdout();
            for data in shard_data
                .iter()
                .take(data_chunks)
                .filter_map(Option::as_ref)
            {
                writer.write_all(&data).await?;
            }
        },
        Command::EncodeShards { source, targets } => {
            let config = config.load_or_default().await?;
            let (data_chunks, parity_chunks, encoder) =
                get_shard_encoder(data_chunks, parity_chunks, &targets)?;
            let mut data_buf = Vec::new();
            let mut reader = source.get_reader(&config).await?;
            reader.read_to_end(&mut data_buf).await?;
            let buf_length = (data_buf.len() + data_chunks - 1) / data_chunks;
            data_buf.extend((0..((buf_length * data_chunks) - data_buf.len())).map(|_| 0));
            let data_chunks: Vec<&[u8]> = (0..data_chunks)
                .map(|index| -> &[u8] {
                    &data_buf[(buf_length * index)..(buf_length * (index + 1))]
                })
                .collect();
            let mut parity_chunks: Vec<Vec<u8>> = vec![vec![0; buf_length]; parity_chunks];

            encoder.encode_sep::<&[u8], Vec<u8>>(&data_chunks, &mut parity_chunks)?;

            let config = &config;
            targets
                .iter()
                .zip(
                    data_chunks
                        .into_iter()
                        .chain(parity_chunks.iter().map(AsRef::as_ref)),
                )
                .map(|(target, data)| async move {
                    let mut data: &[u8] = &data;
                    let reader: &mut &[u8] = &mut data;
                    match target.write_from_reader(&config, reader).await {
                        Ok(_) => {},
                        Err(err) => {
                            eprintln!("Error {}: {}", target, err);
                        },
                    }
                })
                .collect::<FuturesUnordered<_>>()
                .collect::<Vec<()>>()
                .await;
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

fn get_shard_encoder(
    data_chunks: Option<DataChunkCount>,
    parity_chunks: Option<ParityChunkCount>,
    targets: &[ClusterLocation],
) -> Result<(usize, usize, ReedSolomon<galois_8::Field>), Box<dyn Error>> {
    let parity_chunks: usize = parity_chunks
        .ok_or_else(|| ErrorMessage::from("Parity Chunk Count must be known to decode shards"))?
        .into();
    let data_chunks = match data_chunks {
        Some(data_chunks) => {
            let data_chunks: usize = data_chunks.into();
            let expected = data_chunks + parity_chunks;
            if targets.len() == expected {
                data_chunks
            } else {
                let msg = format!(
                    "Invalid targets: Expected {} targets but got {}",
                    expected,
                    targets.len(),
                );
                return Err(ErrorMessage::from(msg).into());
            }
        },
        None if targets.len() <= parity_chunks => {
            let msg = format!(
                "Invalid targets: Expected more than {} targets but got {}",
                parity_chunks,
                targets.len(),
            );
            return Err(ErrorMessage::from(msg).into());
        },
        None => targets.len() - parity_chunks,
    };

    Ok((
        data_chunks,
        parity_chunks,
        ReedSolomon::new(data_chunks, parity_chunks)?,
    ))
}
