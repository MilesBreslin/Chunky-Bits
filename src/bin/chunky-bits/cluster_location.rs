use std::{
    convert::TryFrom,
    error::Error,
    fmt,
    path::{
        Path,
        PathBuf,
    },
    pin::Pin,
    str::FromStr,
    string::ToString,
    sync::Arc,
};

use chunky_bits::{
    cluster::FileOrDirectory,
    file::{
        hash::AnyHash,
        Chunk,
        FilePart,
        FileReference,
        Location,
        ResilverFileReportOwned,
        VerifyFileReportOwned,
    },
};
use futures::{
    future,
    future::{
        BoxFuture,
        FutureExt,
    },
    stream::{
        self,
        BoxStream,
        StreamExt,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    io,
    io::AsyncRead,
};

use crate::{
    config::Config,
    error_message::ErrorMessage,
};

type FilesStreamer<'a> = BoxStream<'a, io::Result<FileOrDirectory>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub enum ClusterLocation {
    ClusterFile { cluster: String, path: PathBuf },
    FileRef(Location),
    Other(Location),
}

impl ClusterLocation {
    pub async fn get_reader(
        &self,
        config: &Config,
    ) -> Result<impl AsyncRead + Unpin + '_, Box<dyn Error>> {
        use ClusterLocation::*;
        let result: Result<Pin<Box<dyn AsyncRead + Unpin>>, Box<dyn Error>>;
        match self {
            ClusterFile { cluster, path } => {
                let cluster = config.get_cluster(&cluster).await?;
                let file_ref = cluster.get_file_ref(&path).await?;
                let reader = file_ref.read_builder_owned().reader_owned();
                result = Ok(Box::pin(reader));
            },
            FileRef(loc) => {
                let bytes = loc.read().await?;
                let file_ref: FileReference = serde_yaml::from_slice(&bytes)?;
                let reader = file_ref.read_builder_owned().reader_owned();
                result = Ok(Box::pin(reader));
            },
            Other(loc) => {
                let reader = loc.reader_with_context(&Default::default()).await?;
                result = Ok(Box::pin(reader));
            },
        };
        result
    }

    pub async fn write_from_reader(
        &self,
        config: &Config,
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<u64, Box<dyn Error>> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                let profile_name = config.get_profile(&cluster_name).await;
                let profile = cluster
                    .get_profile(profile_name.as_deref())
                    .ok_or_else(|| {
                        ErrorMessage::from(format!("Profile not found: {}", profile_name.unwrap()))
                    })?;
                let file_ref = cluster.get_file_writer(&profile).write(reader).await?;
                cluster.write_file_ref(path, &file_ref).await?;
                Ok(file_ref.length.unwrap())
            },
            FileRef(loc) => {
                let destination = config.get_default_destination().await?;
                let file_ref = FileReference::write_builder()
                    .destination(destination)
                    .write(reader)
                    .await?;
                let file_str = serde_json::to_string_pretty(&file_ref)?;
                loc.write(file_str.as_bytes()).await?;
                Ok(file_ref.length.unwrap())
            },
            Other(loc) => Ok(loc
                .write_from_reader_with_context(&Default::default(), reader)
                .await?),
        }
    }

    pub async fn list_files(
        &self,
        config: &Config,
    ) -> Result<FilesStreamer<'static>, Box<dyn Error>> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                return Ok(cluster.list_files(path).await?.boxed());
            },
            _ => {
                todo!();
            },
        }
    }

    pub async fn list_files_recursive<'a>(
        &self,
        config: &'a Config,
    ) -> Result<FilesStreamer<'a>, Box<dyn Error>> {
        self.clone().list_files_recursive_inner(config).await
    }

    fn list_files_recursive_inner<'a>(
        self,
        config: &'a Config,
    ) -> BoxFuture<'a, Result<FilesStreamer<'a>, Box<dyn Error>>> {
        async move {
            let stream = self.list_files(config).await?;
            let self_arc = Arc::new(self);
            let stream = stream
                .map(move |res| (res, self_arc.clone()))
                .then(move |(result, self_arc)| async move {
                    match &result {
                        Ok(FileOrDirectory::Directory(path)) => {
                            let sub_self = self_arc.make_sub_location(path.to_owned());
                            let current_result = stream::once(future::ready(result));
                            let sub_results =
                                match sub_self.list_files_recursive_inner(config).await {
                                    Ok(s) => s,
                                    Err(err) => {
                                        let err = ErrorMessage::from(&err.to_string());
                                        let result = io::Error::new(io::ErrorKind::Other, err);
                                        stream::once(future::ready(Err(result))).boxed()
                                    },
                                };
                            current_result.chain(sub_results).boxed()
                        },
                        _ => stream::once(future::ready(result)).boxed(),
                    }
                })
                .flatten();
            Ok(stream.boxed())
        }
        .boxed()
    }

    fn make_sub_location(&self, new_path: PathBuf) -> Self {
        use ClusterLocation::*;
        match self {
            ClusterFile { cluster, path } => {
                let new_path_parent = new_path.parent().unwrap();
                assert_eq!(new_path_parent, path);
                ClusterFile {
                    cluster: cluster.clone(),
                    path: new_path,
                }
            },
            _ => {
                todo!();
            },
        }
    }

    pub async fn resilver(
        &self,
        config: &Config,
    ) -> Result<ResilverFileReportOwned, Box<dyn Error>> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                let profile_name = config.get_profile(&cluster_name).await;
                let profile = cluster
                    .get_profile(profile_name.as_deref())
                    .ok_or_else(|| {
                        ErrorMessage::from(format!("Profile not found: {}", profile_name.unwrap()))
                    })?;
                let destination = cluster.get_destination(&profile);
                let file_ref = cluster.get_file_ref(&path).await?;
                let destination = Arc::new(destination);
                let report = file_ref.resilver_owned(destination).await;
                Ok(report)
            },
            FileRef(loc) => {
                let bytes = loc.read().await?;
                let file_ref: FileReference = serde_yaml::from_slice(&bytes)?;
                let destination = config.get_default_destination().await?;
                let report = file_ref.resilver_owned(destination).await;
                Ok(report)
            },
            _ => Err(ErrorMessage::from("Resilver is only supported on cluster files").into()),
        }
    }

    pub async fn verify(&self, config: &Config) -> Result<VerifyFileReportOwned, Box<dyn Error>> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                let file_ref = cluster.get_file_ref(&path).await?;
                let report = file_ref.verify_owned().await;
                Ok(report)
            },
            FileRef(loc) => {
                let bytes = loc.read().await?;
                let file_ref: FileReference = serde_yaml::from_slice(&bytes)?;
                let report = file_ref.verify_owned().await;
                Ok(report)
            },
            _ => Err(ErrorMessage::from("Resilver is only supported on files").into()),
        }
    }

    pub async fn get_hashes(
        &self,
        config: &Config,
    ) -> Result<BoxStream<'_, Result<AnyHash, Box<dyn Error>>>, Box<dyn Error>> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                let file_ref = cluster.get_file_ref(&path).await?;
                let iter =
                    file_ref
                        .parts
                        .into_iter()
                        .flat_map(|FilePart { data, parity, .. }| {
                            data.into_iter()
                                .chain(parity.into_iter())
                                .map(|Chunk { hash, .. }| Ok(hash))
                        });
                Ok(stream::iter(iter).boxed())
            },
            FileRef(loc) => {
                let bytes = loc.read().await?;
                let file_ref: FileReference = serde_yaml::from_slice(&bytes)?;
                let iter =
                    file_ref
                        .parts
                        .into_iter()
                        .flat_map(|FilePart { data, parity, .. }| {
                            data.into_iter()
                                .chain(parity.into_iter())
                                .map(|Chunk { hash, .. }| Ok(hash))
                        });
                Ok(stream::iter(iter).boxed())
            },
            _ => Err(ErrorMessage::from("Get hashes is only supported on files").into()),
        }
    }
}

impl FromStr for ClusterLocation {
    type Err = Box<dyn Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split('#');
        match (split.next(), split.next(), split.next()) {
            (Some("@"), Some(path), None) => {
                Ok(ClusterLocation::FileRef(Location::from_str(path)?))
            },
            (Some(cluster), Some(path), None)
                if !(cluster
                    .chars()
                    .next()
                    .as_ref()
                    .map_or(false, char::is_ascii_alphanumeric)
                    && cluster.len() == 1) =>
            {
                Ok(ClusterLocation::ClusterFile {
                    cluster: cluster.to_string(),
                    path: path.into(),
                })
            },
            (Some(cluster), Some(_), None) => {
                Err(ErrorMessage::from(format!("Invalid cluster name: {}", cluster)).into())
            },
            (Some(_), None, None) => Ok(Location::from_str(s).map(Into::into)?),
            _ => Err(ErrorMessage::from(format!("Invalid cluster location format: {}", s)).into()),
        }
    }
}

impl fmt::Display for ClusterLocation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ClusterLocation::*;
        match self {
            ClusterFile { cluster, path } => write!(f, "{}#{}", cluster, path.display()),
            FileRef(location) => write!(f, "@#{}", location),
            Other(location) => write!(f, "{}", location),
        }
    }
}

impl From<ClusterLocation> for String {
    fn from(c: ClusterLocation) -> Self {
        c.to_string()
    }
}

impl From<Location> for ClusterLocation {
    fn from(loc: Location) -> Self {
        ClusterLocation::Other(loc)
    }
}

macro_rules! impl_try_from_string {
    ($type:ty) => {
        impl TryFrom<$type> for ClusterLocation {
            type Error = Box<dyn Error>;

            fn try_from(s: $type) -> Result<Self, Self::Error> {
                FromStr::from_str(AsRef::<str>::as_ref(&s))
            }
        }
    };
}
impl_try_from_string!(&str);
impl_try_from_string!(String);

macro_rules! impl_from_path {
    ($type:ty) => {
        impl From<$type> for ClusterLocation {
            fn from(p: $type) -> Self {
                ClusterLocation::Other(p.into())
            }
        }
    };
}
impl_from_path!(&Path);
impl_from_path!(PathBuf);
