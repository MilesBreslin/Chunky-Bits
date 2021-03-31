use std::{
    convert::Infallible,
    num::NonZeroUsize,
    path::PathBuf,
    sync::Arc,
};

use futures::stream::{
    FuturesOrdered,
    Stream,
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
    task,
};
use tokio_util::codec::{
    BytesCodec,
    FramedRead,
};
use warp::{
    http::{
        header::HeaderValue,
        response::Response,
        StatusCode,
    },
    hyper::body::Body,
};

use chunky_bits::{
    cluster::{
        ChunkSize,
        Cluster,
        DataChunkCount,
        ParityChunkCount,
    },
    file::WeightedLocation,
    file::FileReference,
    file,
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
        cluster: PathBuf,
        /// Address to listen on
        #[structopt(short, long, default_value = "127.0.0.1:8000")]
        listen_addr: std::net::SocketAddr,
        /// Read only setting to disable put requests
        #[structopt(long)]
        read_only: bool,
    },
    /// Put a file in the cluster
    Put {
        /// A reference to a cluster config file
        #[structopt(short, long)]
        cluster: PathBuf,
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
        cluster: PathBuf,
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
pub enum Error {
    Io(io::Error),
    JoinError(task::JoinError),
    Erasure(reed_solomon_erasure::Error),
    Reqwest(reqwest::Error),
    HttpStatus(reqwest::StatusCode),
    ExpiredWriter,
    NotEnoughWriters,
    UnknownError,
    Unimplemented,
    NotHttp,
    UrlParseError(url::ParseError),
    Json(serde_json::Error),
    Yaml(serde_yaml::Error),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
impl From<task::JoinError> for Error {
    fn from(e: task::JoinError) -> Self {
        Error::JoinError(e)
    }
}
impl From<reed_solomon_erasure::Error> for Error {
    fn from(e: reed_solomon_erasure::Error) -> Self {
        Error::Erasure(e)
    }
}
impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Reqwest(e)
    }
}
impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Error::UrlParseError(e)
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}
impl From<serde_yaml::Error> for Error {
    fn from(e: serde_yaml::Error) -> Self {
        Error::Yaml(e)
    }
}
impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        panic!("Infallible")
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}

macro_rules! warp_headers {
    ($resp:expr => { $header_name:literal => $header_value:expr }) => {
        Box::new(warp::reply::with_header(
            $resp,
            $header_name,
            $header_value,
        ))
    };
    ($resp:expr => { $header_name:literal => $header_value:expr, }) => {
        warp_headers!($resp => { $header_name => $header_value })
    };
    ($resp:expr => {
        $header_name:literal => $header_value:expr,
        $($header_names:literal => $header_values:expr),*
    }) => {
        warp_headers!(
            warp_headers!($resp => {
                $header_name => $header_value
            }) => {
            $($header_names => $header_values),*
        })
    };
    ($resp:expr => {
        $($header_names:literal => $header_values:expr),*,
    }) => {
        warp_headers!(
            $resp => {
                $($header_names => $header_values),*
            }
        )
    };
}

async fn index_get(
    cluster: Arc<Cluster>,
    path: warp::path::FullPath,
) -> Result<Box<dyn warp::Reply>, std::convert::Infallible> {
    if let Ok(file_ref) = cluster.get_file_ref(path.as_str()).await {
        let length = file_ref.length.clone();
        let content_type = file_ref.content_type.clone();
        let (reader, mut writer) = tokio::io::duplex(1 << 24);
        tokio::spawn(async move { file_ref.to_writer(&mut writer).await });
        let stream = FramedRead::new(reader, BytesCodec::new());
        let base_resp =
            warp::reply::with_status(Response::new(Body::wrap_stream(stream)), StatusCode::OK);
        return Ok(match (length, content_type) {
            (Some(length), Some(content_type)) => {
                warp_headers!(base_resp => {
                    "Content-Length" => length,
                    "Content-Type" => content_type,
                })
            },
            (Some(length), None) => {
                warp_headers!(base_resp => {
                    "Content-Length" => length,
                })
            },
            (None, Some(content_type)) => {
                warp_headers!(base_resp => {
                    "Content-Type" => content_type,
                })
            },
            (None, None) => Box::new(base_resp),
        });
    }
    return Ok(Box::new(warp::reply::with_status(
        Vec::<u8>::new(),
        StatusCode::NOT_FOUND,
    )));
}

async fn index_put(
    cluster: Arc<Cluster>,
    path: warp::path::FullPath,
    content_type: HeaderValue,
    body: impl Stream<Item = Result<impl bytes::buf::Buf, warp::Error>> + Unpin,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let profile = cluster.get_profile(None).unwrap();
    let write = cluster
        .write_file(
            path.as_str(),
            &mut tokio_util::io::StreamReader::new(
                body.map(|res| -> io::Result<_> { Ok(res.unwrap()) }),
            ),
            profile,
            content_type.to_str().ok().map(str::to_string),
        )
        .await;
    if let Ok(_) = write {
        Ok(warp::http::StatusCode::OK)
    } else {
        Ok(warp::http::StatusCode::INTERNAL_SERVER_ERROR)
    }
}

#[tokio::main]
async fn main() {
    let Opt { command } = Opt::from_args();
    match command {
        Command::HttpGateway {
            cluster,
            listen_addr,
            read_only,
        } => {
            use warp::Filter;
            let cluster: Arc<Cluster> =
                Arc::new(serde_yaml::from_reader(std::fs::File::open(&cluster).unwrap()).unwrap());
            let cluster_get = cluster.clone();
            let route_get = warp::get()
                .or(warp::head())
                .map(move |_| cluster_get.clone())
                .and(warp::path::full())
                .and_then(index_get);
            let route_put = warp::put()
                .map(move || cluster.clone())
                .and(warp::path::full())
                .and(warp::header::value("content-type"))
                .and(warp::body::stream())
                .and_then(index_put);
            if read_only {
                warp::serve(route_get).run(listen_addr).await;
            } else {
                warp::serve(route_get.or(route_put)).run(listen_addr).await;
            }
        },
        Command::Put {
            file,
            cluster,
            filename,
            profile,
        } => {
            let cluster: Cluster =
                serde_yaml::from_reader(std::fs::File::open(&cluster).unwrap()).unwrap();
            let mut f = File::open(&file).await.unwrap();
            let cluster_profile = cluster
                .get_profile(profile.as_ref().map(|s| s.as_str()))
                .unwrap();
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
                .await
                .unwrap();
        },
        Command::ClusterInfo { cluster } => {
            let cluster: Cluster =
                serde_yaml::from_reader(std::fs::File::open(&cluster).unwrap()).unwrap();
            println!("{}", serde_yaml::to_string(&cluster).unwrap());
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
            let mut f = File::open(&file).await.unwrap();
            let writer = Arc::new(destination);
            let file_ref = file::FileReference::from_reader(
                &mut f,
                writer,
                1 << chunk_size,
                data,
                parity,
                concurrency,
            )
            .await
            .unwrap();
            println!("{}", serde_yaml::to_string(&file_ref).unwrap());
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
            let r: ReedSolomon<galois_8::Field> = ReedSolomon::new(data, parity).unwrap();
            r.reconstruct(&mut shard_bufs).unwrap();
            let mut f: Box<dyn AsyncWrite + Unpin> = match destination.to_str() {
                Some("-") => Box::new(io::stdout()),
                _ => Box::new(File::create(destination).await.unwrap()),
            };
            for buf in shard_bufs.drain(..).take(data).filter_map(|op| op) {
                f.write_all(&buf).await.unwrap()
            }
        },
        Command::DecodeFile {
            file_reference,
            destination,
        } => {
            let file_reference: FileReference =
                serde_yaml::from_reader(&std::fs::File::open(file_reference).unwrap()).unwrap();
            let mut f_dest = File::create(destination).await.unwrap();
            file_reference.to_writer(&mut f_dest).await.unwrap();
        },
        Command::VerifyFile { file } => {
            let file_reference: FileReference =
                serde_yaml::from_reader(&std::fs::File::open(file).unwrap()).unwrap();
            println!(
                "{}",
                serde_yaml::to_string(&file_reference.verify().await).unwrap(),
            );
        },
        Command::GetHashes { file } => {
            let file_reference: FileReference =
                serde_yaml::from_reader(&std::fs::File::open(file).unwrap()).unwrap();
            for part in &file_reference.parts {
                for location_with_hash in part.data.iter().chain(part.parity.iter()) {
                    println!("{}", location_with_hash.sha256);
                }
            }
        },
    }
}