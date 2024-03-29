use std::{
    collections::{
        BTreeSet,
        HashMap,
        HashSet,
    },
    ffi::OsStr,
    net::SocketAddr,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use anyhow::{
    bail,
    Result,
};
use chunky_bits::{
    cluster::{
        sized_int::{
            ChunkSize,
            DataChunkCount,
            ParityChunkCount,
        },
        FileOrDirectory,
    },
    file::{
        hash::AnyHash,
        Location,
    },
    http::cluster_filter,
};
use futures::stream::{
    self,
    FuturesOrdered,
    FuturesUnordered,
    StreamExt,
};
use reed_solomon_erasure::{
    galois_8,
    ReedSolomon,
};
use tokio::{
    fs,
    io::{
        self,
        AsyncReadExt,
        AsyncWriteExt,
    },
};
pub mod any_destination;
pub mod cluster_location;
pub mod config;
pub mod error_message;
pub mod util;
use clap::{
    Parser,
    Subcommand,
};

use crate::{
    cluster_location::ClusterLocation,
    config::Config,
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
#[derive(Parser)]
#[command(version, about)]
struct Opt {
    /// Location for the config file
    #[arg(long)]
    config: Option<PathBuf>,
    /// Set the default chunk size for non-cluster destinations
    #[arg(long)]
    chunk_size: Option<ChunkSize>,
    /// Set the default data chunks for non-cluster destinations
    #[arg(long)]
    data_chunks: Option<DataChunkCount>,
    /// Set the default parity chunks for non-cluster destinations
    #[arg(long)]
    parity_chunks: Option<ParityChunkCount>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Concatenate files together
    Cat {
        #[arg(required(true))]
        targets: Vec<ClusterLocation>,
    },
    /// Show the parsed configuration definition
    ConfigInfo {
        #[arg(long)]
        json: bool,
    },
    /// Show the parsed cluster definition
    ClusterInfo {
        #[arg(long)]
        json: bool,
        /// The name/location of the cluster to show
        cluster: String,
    },
    /// Copy file from source to destination
    Cp {
        source: ClusterLocation,
        destination: ClusterLocation,
    },
    DecodeShards {
        #[arg(required(true))]
        targets: Vec<ClusterLocation>,
    },
    EncodeShards {
        source: ClusterLocation,
        #[arg(required(true))]
        targets: Vec<ClusterLocation>,
    },
    FileInfo {
        #[arg(long)]
        json: bool,
        source: ClusterLocation,
    },
    /// Find all hashes that are not referenced
    FindUnusedHashes {
        #[arg(long, default_value = "100000")]
        batch_size: usize,
        #[arg(short, long)]
        remove: bool,
        #[arg(required(true))]
        source: Vec<ClusterLocation>,
        #[arg(last(true), required(true))]
        hashes: Vec<ClusterLocation>,
    },
    /// Get all the known hashes for a location
    GetHashes {
        /// Deduplicate all hashes
        #[arg(short, long = "dedup")]
        deduplicate: bool,
        /// Sort all hashes (Implies --dedup)
        #[arg(short, long)]
        sort: bool,
        target: ClusterLocation,
    },
    /// Provide a HTTP Gateway for a cluster
    HttpGateway {
        /// The name/location of the cluster to show
        cluster: String,
        /// Listen Address to bind to
        #[arg(short, long, default_value = "127.0.0.1:8000")]
        listen_addr: SocketAddr,
    },
    /// List the files in a cluster directory
    Ls {
        #[arg(short, long)]
        recursive: bool,
        target: ClusterLocation,
    },
    /// Reference the file in its existing location and add parity
    Migrate {
        source: ClusterLocation,
        destination: ClusterLocation,
    },
    /// Resilver a cluster file
    Resilver { target: ClusterLocation },
    /// Verify a cluster file
    Verify { target: ClusterLocation },
}

#[tokio::main]
async fn main() {
    match run(Opt::parse()).await {
        Ok(_) => {},
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        },
    }
}

async fn run(opts: Opt) -> Result<()> {
    let Opt {
        command,
        config,
        chunk_size,
        data_chunks,
        parity_chunks,
    } = opts;
    let config = Config::builder()
        .default_data_chunks(data_chunks)
        .default_parity_chunks(parity_chunks)
        .default_chunk_size(chunk_size)
        .path(config);
    match command {
        Command::Cat { targets } => {
            let config = config.load_or_default().await?;
            let destination = ClusterLocation::Stdio;
            for target in targets {
                let mut reader = target.get_reader(&config).await?;
                destination.write_from_reader(&config, &mut reader).await?;
            }
        },
        Command::ConfigInfo { json } => {
            let config = config.load_or_default().await?;
            match json {
                true => serde_json::to_writer_pretty(std::io::stdout(), &config)?,
                false => serde_yaml::to_writer(std::io::stdout(), &config)?,
            }
        },
        Command::ClusterInfo { cluster, json } => {
            let config = config.load_or_default().await?;
            let cluster = config.get_cluster(&cluster).await?;
            match json {
                true => serde_json::to_writer_pretty(std::io::stdout(), &cluster)?,
                false => serde_yaml::to_writer(std::io::stdout(), &cluster)?,
            }
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
        Command::FileInfo { source, json } => {
            let config = config.load_or_default().await?;
            let file_ref = source
                .get_file_reference(
                    &config,
                    config.get_default_data_chunks().await?,
                    config.get_default_parity_chunks().await?,
                    config.get_default_chunk_size().await?,
                )
                .await?;

            match json {
                true => serde_json::to_writer_pretty(std::io::stdout(), &file_ref)?,
                false => serde_yaml::to_writer(std::io::stdout(), &file_ref)?,
            }
        },
        Command::FindUnusedHashes {
            batch_size,
            remove,
            source,
            hashes,
        } => {
            let config = config.load_or_default().await?;
            let config = Arc::new(config);
            let source = source
                .into_iter()
                .map(|source| match source {
                    ClusterLocation::ClusterFile { .. } | ClusterLocation::FileRef(..) => {
                        Ok(source)
                    },
                    _ => bail!("Unsupported source location: {}", source),
                })
                .collect::<Result<Vec<ClusterLocation>>>()?;
            let hashes = hashes
                .into_iter()
                .map(|hashes| match &hashes {
                    ClusterLocation::Other(Location::Local { .. }) => Ok(hashes),
                    _ => bail!("Unsupported hashes location: {}", hashes),
                })
                .collect::<Result<Vec<ClusterLocation>>>()?;
            let list_files = stream::iter(
                hashes
                    .iter()
                    .map(|location| (location, location.list_files_recursive(&config))),
            )
            .then(|(location, future)| async move {
                match future.await {
                    Ok(stream) => stream
                        .map(move |result| match result {
                            Ok(file) => Ok(file),
                            Err(err) => bail!("{}: {}", location, err),
                        })
                        .boxed(),
                    Err(err) => stream::once(async move { bail!("{}: {}", location, err) }).boxed(),
                }
            })
            .flatten()
            .peekable();
            tokio::pin!(list_files);
            while let Some(_) = list_files.as_mut().peek().await {
                let mut existing_hashes = HashMap::new();
                while let Some(result) = list_files.next().await {
                    match result {
                        Ok(FileOrDirectory::File(path)) => {
                            if let Some(file_name) = path.file_name().map(OsStr::to_str).flatten() {
                                match AnyHash::from_str(file_name) {
                                    Ok(hash) => {
                                        existing_hashes
                                            .entry(hash)
                                            .or_insert(Vec::new())
                                            .push(path);
                                    },
                                    Err(_) => {
                                        eprintln!("Unknown hash: {}", file_name)
                                    },
                                }
                            }
                        },
                        Ok(_) => {},
                        Err(err) => {
                            eprintln!("{}", err);
                        },
                    }
                    if existing_hashes.len() >= batch_size {
                        break;
                    }
                }

                let source_hashes = stream::iter(
                    source
                        .iter()
                        .map(|source| source.get_hashes_rec(config.clone())),
                )
                .then(|future| async move {
                    match future.await {
                        Ok(stream) => stream.boxed(),
                        Err(err) => stream::once(async move { Err(err) }).boxed(),
                    }
                })
                .flatten();
                tokio::pin!(source_hashes);
                while let Some(result) = source_hashes.next().await {
                    match result {
                        Ok(hash) => {
                            existing_hashes.remove(&hash);
                        },
                        Err(err) => {
                            eprintln!("{}", err);
                        },
                    }
                }

                for (hash, paths) in existing_hashes.into_iter() {
                    println!("{}", hash);
                    if remove {
                        for path in paths {
                            eprintln!("Removing {}", path.display());
                            fs::remove_file(path).await?;
                        }
                    }
                }
            }
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
                    Ok(file) => println!("{}", file),
                    Err(err) => eprintln!("Error: {}", err),
                }
            }
        },
        Command::Migrate {
            source,
            destination,
        } => {
            let config = config.load_or_default().await?;
            source.migrate(&config, &destination).await?;
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
) -> Result<(usize, usize, ReedSolomon<galois_8::Field>)> {
    let parity_chunks: usize = match parity_chunks {
        Some(parity_chunks) => parity_chunks.into(),
        None => bail!("Parity Chunk Count must be known to decode shards"),
    };
    let data_chunks = match data_chunks {
        Some(data_chunks) => {
            let data_chunks: usize = data_chunks.into();
            let expected = data_chunks + parity_chunks;
            if targets.len() == expected {
                data_chunks
            } else {
                bail!(
                    "Invalid targets: Expected {} targets but got {}",
                    expected,
                    targets.len(),
                );
            }
        },
        None if targets.len() <= parity_chunks => {
            bail!(
                "Invalid targets: Expected more than {} targets but got {}",
                parity_chunks,
                targets.len(),
            );
        },
        None => targets.len() - parity_chunks,
    };

    Ok((
        data_chunks,
        parity_chunks,
        ReedSolomon::new(data_chunks, parity_chunks)?,
    ))
}
