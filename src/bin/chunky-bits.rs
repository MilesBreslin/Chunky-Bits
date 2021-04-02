use std::{
    fmt::{
        self,
        Display,
        Formatter,
    },
    net::SocketAddr,
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
};

use chunky_bits::{
    cluster::{
        ChunkSize,
        Cluster,
        DataChunkCount,
        ParityChunkCount,
    },
    file::{
        self,
        FileReference,
        Location,
        WeightedLocation,
    },
    http::{
        cluster_filter,
        cluster_filter_get,
    },
};
use futures::stream::{
    FuturesOrdered,
    StreamExt,
};
use reed_solomon_erasure::{
    galois_8,
    ReedSolomon,
};
use structopt::StructOpt;
use tokio::{
    fs::{
        self,
        File,
    },
    io::{
        self,
        AsyncWrite,
        AsyncWriteExt,
    },
};

#[derive(StructOpt)]
pub struct Opt {
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
pub enum Command {
    /// Provide a HTTP Gateway
    HttpGateway {
        /// Cluster configuration to create the gateway for
        #[structopt(short, long)]
        cluster: Location,
        /// Address to listen on
        #[structopt(short, long, default_value = "127.0.0.1:8000")]
        listen_addr: SocketAddr,
        /// Read only setting to disable put requests
        #[structopt(long)]
        read_only: bool,
    },
    /// Put a file in the cluster
    Put {
        /// A reference to a cluster config file
        #[structopt(short, long)]
        cluster: Location,
        /// Local file to upload to the cluster
        file: PathBuf,
        /// Rename the file during the upload
        #[structopt(long)]
        filename: Option<PathBuf>,
        /// Profile to use during the upload
        #[structopt(long)]
        profile: Option<String>,
    },
    /// Show information about a given cluster
    ClusterInfo {
        /// A reference to a cluster config file
        cluster: Location,
    },
    /// Create a file reference from a file
    EncodeFile {
        /// Target file to encode
        #[structopt(short, long)]
        file: PathBuf,
        /// List of weighted destinations (example: "5:http://localhost/repo")
        #[structopt(short = "D", long)]
        destination: Vec<WeightedLocation>,
        /// Number of data chunks
        #[structopt(short, long)]
        data: DataChunkCount,
        /// Number of parity chunks
        #[structopt(short, long)]
        parity: ParityChunkCount,
        /// Chunk size in powers of 2 (20 == 1MiB)
        #[structopt(long, default_value)]
        chunk_size: ChunkSize,
        /// The number of parts to write concurrently
        #[structopt(long, default_value = "20")]
        concurrency: NonZeroUsize,
    },
    /// Given only a list of shards, reconstruct the source
    DecodeShards {
        /// Number of data chunks
        #[structopt(short, long)]
        data: usize,
        /// Number of parity chunks
        #[structopt(short, long)]
        parity: usize,
        /// Where to rebuild the file
        #[structopt(long)]
        destination: PathBuf,
        /// A ordered list of paths to each shard. At least `data` paths must be
        /// valid
        shards: Vec<PathBuf>,
    },
    /// Given a file reference, reconstruct the source
    DecodeFile {
        /// File reference metadata file
        #[structopt(short, long = "file")]
        file_reference: PathBuf,
        /// Where to store the destination file
        #[structopt(long)]
        destination: PathBuf,
    },
    /// Given a file reference, identify the integrity of it
    VerifyFile {
        /// File reference metadata file
        file: PathBuf,
    },
    /// Given a file reference, show all chunk hashes
    GetHashes {
        /// File reference metadata file
        file: PathBuf,
    },
}

#[derive(Debug)]
struct ErrorMessage(String);
impl ErrorMessage {
    fn with_prefix<T>(prefix: &'static str) -> impl Fn(T) -> ErrorMessage
    where
        T: std::fmt::Display,
    {
        move |msg| ErrorMessage(format!("{}: {}", prefix, msg))
    }
}
impl<T: AsRef<str>> From<T> for ErrorMessage {
    fn from(msg: T) -> Self {
        ErrorMessage(format!("{}", msg.as_ref()))
    }
}
impl Display for ErrorMessage {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl std::error::Error for ErrorMessage {}

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

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let Opt { command } = Opt::from_args();
    match command {
        Command::HttpGateway {
            cluster,
            listen_addr,
            read_only,
        } => {
            let cluster = Cluster::from_location(cluster)
                .await
                .map_err(ErrorMessage::with_prefix("Cluster Definition"))?;
            if read_only {
                warp::serve(cluster_filter_get(cluster))
                    .bind(listen_addr)
                    .await;
            } else {
                warp::serve(cluster_filter(cluster)).bind(listen_addr).await;
            }
        },
        Command::Put {
            file,
            cluster,
            filename,
            profile,
        } => {
            let cluster = Cluster::from_location(cluster)
                .await
                .map_err(ErrorMessage::with_prefix("Cluster Definition"))?;
            let mut f = File::open(&file)
                .await
                .map_err(ErrorMessage::with_prefix("Target File"))?;
            let cluster_profile = cluster
                .get_profile(profile.as_ref().map(|s| s.as_str()))
                .ok_or(ErrorMessage::from("Profile not found"))?;
            let output_name = match filename {
                Some(filename) => filename,
                None => file,
            };
            cluster
                .write_file(
                    &format!("{}", output_name.display()),
                    &mut f,
                    cluster_profile,
                    None,
                )
                .await?;
        },
        Command::ClusterInfo { cluster } => {
            let cluster = Cluster::from_location(cluster)
                .await
                .map_err(ErrorMessage::with_prefix("Cluster Definition"))?;
            println!("{}", serde_yaml::to_string(&cluster)?);
        },
        Command::EncodeFile {
            file,
            destination,
            chunk_size,
            data,
            parity,
            concurrency,
        } => {
            let data: usize = data.into();
            let parity: usize = parity.into();
            let chunk_size: usize = chunk_size.into();
            if destination.len() < (data + parity) {
                eprintln!("Warning: Not enough destinations to distribute the data evenly");
            }
            let mut f = File::open(&file).await?;
            let writer = Arc::new(destination);
            let file_ref = file::FileReference::from_reader(
                &mut f,
                writer,
                1 << chunk_size,
                data,
                parity,
                concurrency,
            )
            .await?;
            println!("{}", serde_yaml::to_string(&file_ref)?);
        },
        Command::DecodeShards {
            data,
            parity,
            shards,
            destination,
        } => {
            let data: usize = data.into();
            let parity: usize = parity.into();
            let mut shard_bufs = shards
                .iter()
                .map(|filepath| async move { fs::read(&filepath).await.ok() })
                .collect::<FuturesOrdered<_>>()
                .collect::<Vec<Option<Vec<u8>>>>()
                .await;
            let r: ReedSolomon<galois_8::Field> = ReedSolomon::new(data, parity)?;
            r.reconstruct(&mut shard_bufs)?;
            let mut f: Box<dyn AsyncWrite + Unpin> = match destination.to_str() {
                Some("-") => Box::new(io::stdout()),
                _ => Box::new(File::create(destination).await?),
            };
            for buf in shard_bufs.drain(..).take(data).filter_map(|op| op) {
                f.write_all(&buf).await?;
            }
        },
        Command::DecodeFile {
            file_reference,
            destination,
        } => {
            let file_reference: FileReference =
                serde_yaml::from_reader(&std::fs::File::open(file_reference)?)?;
            let mut f_dest = File::create(destination).await?;
            file_reference.to_writer(&mut f_dest).await?;
        },
        Command::VerifyFile { file } => {
            let file_reference: FileReference =
                serde_yaml::from_reader(&std::fs::File::open(file)?)?;
            println!("{}", serde_yaml::to_string(&file_reference.verify().await)?,);
        },
        Command::GetHashes { file } => {
            let file_reference: FileReference =
                serde_yaml::from_reader(&std::fs::File::open(file)?)?;
            for part in &file_reference.parts {
                for location_with_hash in part.data.iter().chain(part.parity.iter()) {
                    println!("{}", location_with_hash.sha256);
                }
            }
        },
    }
    Ok(())
}
