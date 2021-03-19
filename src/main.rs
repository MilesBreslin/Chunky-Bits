use reed_solomon_erasure::{
    galois_8,
    shards,
    ReedSolomon,
};
use structopt::StructOpt;
use std::path::PathBuf;
use std::sync::Arc;
use futures::{
    stream::{
        StreamExt,
        FuturesOrdered,
    },
};
use tokio::{
    task::{
        self,
        JoinError,
    },
    fs::{
        self,
        File,
    },
    io::{
        self,
        AsyncRead,
        AsyncReadExt,
        AsyncWrite,
        AsyncWriteExt,
    },
};

pub mod file;
pub mod localfiles;
pub mod http;
pub mod cluster;
use file::{
    FileReference,
};
use crate::{
    cluster::{
        Cluster,
        ChunkSize,
        DataChunkCount,
        ParityChunkCount,
        SizedInt,
    },
    file::{
        Location,
        LocationDestination,
        WeightedLocation,
        HttpUrl,
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
    HttpGateway{
        /// Cluster to gateway for
        #[structopt(short,long)]
        cluster: PathBuf,
        /// Address to listen on
        #[structopt(short,long)]
        listen_addr: HttpUrl,
    },
    /// Put a file in the cluster
    Put{
        /// A reference to a cluster config file
        #[structopt(short,long)]
        cluster: PathBuf,
        /// Local file to upload to the cluster
        file: PathBuf,
        /// Rename the file during the upload
        #[structopt(long)]
        filename: Option<PathBuf>,
    },
    /// Show information about a given cluster
    ClusterInfo{
        /// A reference to a cluster config file
        cluster: PathBuf,
    },
    /// Create a file reference from a file
    EncodeFile{
        /// Target file to encode
        #[structopt(short,long)]
        file: PathBuf,
        /// List of weighted destinations (example: "5:http://localhost/repo")
        #[structopt(short = "D", long)]
        destination: Vec<WeightedLocation>,
        /// Number of data chunks
        #[structopt(short,long)]
        data: DataChunkCount,
        /// Number of parity chunks
        #[structopt(short,long)]
        parity: ParityChunkCount,
        /// Chunk size in powers of 2 (20 == 1MiB)
        #[structopt(long,default_value)]
        chunk_size: ChunkSize,
    },
    /// Given only a list of shards, reconstruct the source
    DecodeShards{
        /// Number of data chunks
        #[structopt(short,long)]
        data: usize,
        /// Number of parity chunks
        #[structopt(short,long)]
        parity: usize,
        /// Where to rebuild the file
        #[structopt(long)]
        destination: PathBuf,
        /// A ordered list of paths to each shard. At least `data` paths must be valid
        shards: Vec<PathBuf>,
    },
    /// Given a file reference, reconstruct the source
    DecodeFile{
        /// File reference metadata file
        #[structopt(short,long = "file")]
        file_reference: PathBuf,
        /// Where to store the destination file
        #[structopt(long)]
        destination: PathBuf,
    }
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
    NotHttp,
    UrlParseError(url::ParseError),
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
impl From<serde_yaml::Error> for Error {
    fn from(e: serde_yaml::Error) -> Self {
        Error::Yaml(e)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[rocket::get("/<file..>")]
async fn index_get(state: rocket::State<'_, Cluster>, file: PathBuf) -> rocket::response::Stream<impl AsyncRead + Unpin> {
    let cluster = state.inner();
    let s = cluster.read_file(&format!("{}", file.display())).await.unwrap();
    rocket::response::Stream::chunked(s, 1 << 20)
}

#[rocket::put("/<file..>", data = "<data>")]
async fn index_put(state: rocket::State<'_, Cluster>, data: rocket::data::Data, file: PathBuf) -> () {
    use rocket::data::ToByteUnit;
    let cluster = state.inner();
    let mut writer = data.open(10.gibibytes());
    let profile = cluster.get_profile(None).unwrap();
    let s = cluster.write_file(&format!("{}", file.display()), &mut writer, &profile).await.unwrap();
}

#[tokio::main]
async fn main() {
    let Opt{command} = Opt::from_args();
    match command {
        Command::HttpGateway{cluster,listen_addr} => {
            let cluster: Cluster = serde_yaml::from_reader(std::fs::File::open(&cluster).unwrap()).unwrap();
            rocket::ignite()
                .mount("/", rocket::routes![index_get,index_put])
                .manage(cluster)
                .launch().await;
        },
        Command::Put{file,cluster,filename} => {
            let cluster: Cluster = serde_yaml::from_reader(std::fs::File::open(&cluster).unwrap()).unwrap();
            let mut f = File::open(&file).await.unwrap();
            let cluster_profile = cluster.get_profile(None).unwrap();
            let output_name = match filename {
                Some(filename) => filename,
                None => file,
            };
            let destination = cluster.write_file(
                &format!("{}", output_name.display()),
                &mut f,
                cluster_profile,
            ).await.unwrap();
        },
        Command::ClusterInfo{cluster} => {
            let cluster: Cluster = serde_yaml::from_reader(std::fs::File::open(&cluster).unwrap()).unwrap();
            println!("{}", serde_yaml::to_string(&cluster).unwrap());
        },
        Command::EncodeFile{file, destination, chunk_size, data, parity} => {
            let data: usize = data.into();
            let parity: usize = parity.into();
            let chunk_size: usize = chunk_size.into();
            if destination.len() < (data + parity) {
                eprintln!("Warning: Not enough destinations to distribute the data evenly");
            }
            let mut f = File::open(&file).await.unwrap();
            let writer = Arc::new(destination);
            let fileRef = file::FileReference::from_reader(&mut f, writer, 1 << chunk_size, data, parity).await.unwrap();
            println!("{}", serde_yaml::to_string(&fileRef).unwrap());
        },
        Command::DecodeShards{data, parity, shards, destination} => {
            let data: usize = data.into();
            let parity: usize = parity.into();
            let mut shard_bufs = shards.iter()
                .map(|filepath| async move {
                    fs::read(&filepath).await.ok()
                })
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
        Command::DecodeFile{file_reference, destination} => {
            let file_reference: FileReference = serde_yaml::from_reader(
                &std::fs::File::open(file_reference).unwrap()
            ).unwrap();
            let mut f_dest = File::create(destination).await.unwrap();
            file_reference.to_writer(&mut f_dest).await.unwrap();
        },
    }
}
