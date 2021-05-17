use std::{
    borrow::Borrow,
    convert::{
        TryFrom,
        TryInto,
    },
    error::Error,
    ffi::OsStr,
    fmt,
    iter,
    path::{
        Component,
        Path,
        PathBuf,
    },
    pin::Pin,
    str::FromStr,
    string::ToString,
    sync::{
        Arc,
        Once,
    },
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
    future::{
        self,
        BoxFuture,
        FutureExt,
    },
    stream::{
        self,
        BoxStream,
        Stream,
        StreamExt,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    io::{
        self,
        AsyncRead,
    },
    sync::mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use url::Url;

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
    Stdio,
}

impl ClusterLocation {
    pub async fn get_reader(
        &self,
        config: &Config,
    ) -> Result<impl AsyncRead + Send + Unpin + '_, ErrorMessage> {
        use ClusterLocation::*;
        let result: Result<Pin<Box<dyn AsyncRead + Send + Unpin>>, ErrorMessage>;
        match self {
            ClusterFile { cluster, path } => {
                let cluster = config
                    .get_cluster(&cluster)
                    .await
                    .map_err(ErrorMessage::new)?;
                let file_ref = cluster
                    .get_file_ref(&path)
                    .await
                    .map_err(ErrorMessage::new)?;
                let reader = file_ref.read_builder_owned().reader_owned();
                result = Ok(Box::pin(reader));
            },
            FileRef(loc) => {
                let bytes = loc.read().await.map_err(ErrorMessage::new)?;
                let file_ref: FileReference =
                    serde_yaml::from_slice(&bytes).map_err(ErrorMessage::new)?;
                let reader = file_ref.read_builder_owned().reader_owned();
                result = Ok(Box::pin(reader));
            },
            Other(loc) => {
                let reader = loc
                    .reader_with_context(&Default::default())
                    .await
                    .map_err(ErrorMessage::new)?;
                result = Ok(Box::pin(reader));
            },
            Stdio => {
                result = Ok(Box::pin(io::stdin()));
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
                let data_chunks = config.get_default_data_chunks().await?;
                let parity_chunks = config.get_default_parity_chunks().await?;
                let chunk_size = config.get_default_chunk_size().await?;
                static WARNING: Once = Once::new();
                WARNING.call_once(|| {
                    eprintln!(
                        "Warning: Writing using default destination data = {}, parity = {}, chunk_size = 2^{}",
                        data_chunks,
                        parity_chunks,
                        chunk_size,
                    );
                });
                let file_ref = FileReference::write_builder()
                    .destination(destination)
                    .data_chunks(data_chunks)
                    .parity_chunks(parity_chunks)
                    .chunk_size(1 << chunk_size)
                    .write(reader)
                    .await?;
                let file_str = serde_json::to_string_pretty(&file_ref)?;
                loc.write(file_str.as_bytes()).await?;
                Ok(file_ref.length.unwrap())
            },
            Other(loc) => Ok(loc
                .write_from_reader_with_context(&Default::default(), reader)
                .await?),
            Stdio => {
                let mut writer = io::stdout();
                Ok(io::copy(reader, &mut writer).await?)
            },
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
            Stdio => {
                let stream: FilesStreamer<'static> =
                    stream::once(future::ready(Ok(FileOrDirectory::File("-".into())))).boxed();
                return Ok(stream);
            },
            FileRef(Location::Local { path, .. }) | Other(Location::Local { path, .. }) => {
                Ok(FileOrDirectory::list(&path).await?.boxed())
            },
            FileRef(Location::Http { url, .. }) | Other(Location::Http { url, .. }) => {
                let url: &Url = url.as_ref();
                let components = url
                    .path_segments()
                    .unwrap()
                    .map(|s| Component::Normal(s.as_ref()));
                let path = iter::once(Component::RootDir).chain(components).collect();
                let stream: FilesStreamer<'static> =
                    stream::once(future::ready(Ok(FileOrDirectory::File(path)))).boxed();
                return Ok(stream);
            },
        }
    }

    pub async fn list_files_recursive<'a>(
        &'a self,
        config: &'a Config,
    ) -> Result<FilesStreamer<'a>, Box<dyn Error>> {
        Self::list_files_recursive_inner(self, config).await
    }

    fn list_files_recursive_inner<'a>(
        self_outer: (impl Borrow<Self> + Clone + Send + Sync + 'a),
        config: &'a Config,
    ) -> BoxFuture<'a, Result<FilesStreamer<'a>, Box<dyn Error>>> {
        async move {
            let mut stream = self_outer.borrow().list_files(config).await?;
            let top_level = stream.next().await;
            let stream = stream
                .map(move |res| (res, self_outer.clone()))
                .then(move |(result, self_inner)| async move {
                    match &result {
                        Ok(FileOrDirectory::Directory(path)) => {
                            let sub_self = self_inner.borrow().make_sub_location(path.to_owned());
                            let sub_results =
                                match Self::list_files_recursive_inner(Arc::new(sub_self), config)
                                    .await
                                {
                                    Ok(s) => s,
                                    Err(err) => {
                                        let err = ErrorMessage::from(&err.to_string());
                                        let result = io::Error::new(io::ErrorKind::Other, err);
                                        stream::once(future::ready(Err(result))).boxed()
                                    },
                                };
                            sub_results
                        },
                        _ => stream::once(future::ready(result)).boxed(),
                    }
                })
                .flatten();
            let top_level_stream = stream::once(future::ready(top_level)).filter_map(future::ready);
            Ok(top_level_stream.chain(stream).boxed())
        }
        .boxed()
    }

    fn make_sub_location(&self, new_path: PathBuf) -> Self {
        match self {
            ClusterLocation::ClusterFile { cluster, .. } => ClusterLocation::ClusterFile {
                cluster: cluster.clone(),
                path: new_path,
            },
            ClusterLocation::Other(loc) | ClusterLocation::FileRef(loc) => {
                fn trim_components<'a>(
                    parent_components: &mut dyn Iterator<Item = &str>,
                    sub_components: &mut iter::Peekable<impl Iterator<Item = &'a str>>,
                ) {
                    loop {
                        let parent_part = parent_components.next();
                        let sub_part = sub_components.peek();
                        match (parent_part, sub_part) {
                            (Some(parent_part), Some(sub_part)) if parent_part == *sub_part => {
                                sub_components.next();
                            },
                            _ => {
                                break;
                            },
                        }
                    }
                }
                let mut sub_components = new_path
                    .components()
                    .filter_map(|comp| {
                        use Component::*;
                        match comp {
                            Normal(x) => Some(x),
                            _ => None,
                        }
                    })
                    .filter_map(OsStr::to_str)
                    .peekable();
                let loc = match loc {
                    Location::Http { url, .. } => {
                        let mut url: Url = url.clone().into();
                        let mut parent_components = url.path_segments().unwrap();
                        trim_components(&mut parent_components, &mut sub_components);
                        let mut path_seg = url.path_segments_mut().unwrap();
                        for sub_part in sub_components {
                            path_seg.push(sub_part);
                        }
                        drop(path_seg);
                        Location::Http {
                            url: url.try_into().unwrap(),
                            range: Default::default(),
                        }
                    },
                    Location::Local { path, .. } => {
                        let mut parent_components = path
                            .components()
                            .filter_map(|comp| {
                                use Component::*;
                                match comp {
                                    Normal(x) => Some(x),
                                    _ => None,
                                }
                            })
                            .filter_map(OsStr::to_str);
                        trim_components(&mut parent_components, &mut sub_components);
                        Location::Local {
                            path: path
                                .components()
                                .chain(sub_components.map(|s| Component::Normal(s.as_ref())))
                                .collect(),
                            range: Default::default(),
                        }
                    },
                };
                match self {
                    ClusterLocation::Other(_) => ClusterLocation::Other(loc),
                    ClusterLocation::FileRef(_) => ClusterLocation::FileRef(loc),
                    _ => panic!("Only Other and FileRef should be matched"),
                }
            },
            ClusterLocation::Stdio => ClusterLocation::Stdio,
        }
    }

    pub async fn list_cluster_locations<'a>(
        &'a self,
        config: &'a Config,
    ) -> Result<impl Stream<Item = io::Result<Self>> + 'a, Box<dyn Error>> {
        let stream = self
            .list_files_recursive(config)
            .await?
            .filter_map(move |res| {
                let res = match res {
                    Ok(FileOrDirectory::File(path)) => Some(Ok(self.make_sub_location(path))),
                    Ok(FileOrDirectory::Directory(_)) => None,
                    Err(err) => Some(Err(err)),
                };
                future::ready(res)
            });
        Ok(stream)
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
    ) -> Result<BoxStream<'_, Result<AnyHash, Box<dyn Error + Send + Sync>>>, ErrorMessage> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config
                    .get_cluster(&cluster_name)
                    .await
                    .map_err(ErrorMessage::with_prefix(self))?;
                let file_ref = cluster
                    .get_file_ref(&path)
                    .await
                    .map_err(ErrorMessage::with_prefix(self))?;
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
                let bytes = loc.read().await.map_err(ErrorMessage::with_prefix(self))?;
                let file_ref: FileReference =
                    serde_yaml::from_slice(&bytes).map_err(ErrorMessage::with_prefix(self))?;
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
            Other(_) | Stdio => {
                let data_chunks = config
                    .get_default_data_chunks()
                    .await
                    .map_err(ErrorMessage::new)?;
                let parity_chunks = config
                    .get_default_parity_chunks()
                    .await
                    .map_err(ErrorMessage::new)?;
                let chunk_size = config
                    .get_default_chunk_size()
                    .await
                    .map_err(ErrorMessage::new)?;
                static WARNING: Once = Once::new();
                WARNING.call_once(|| {
                    eprintln!(
                        "Warning: Hashes generated from binary data using data = {}, parity = {}, chunk_size = 2^{}",
                        data_chunks,
                        parity_chunks,
                        chunk_size,
                    );
                });
                let mut reader = self
                    .get_reader(config)
                    .await
                    .map_err(ErrorMessage::with_prefix(self))?;
                let file_ref = FileReference::write_builder()
                    .destination(())
                    .data_chunks(data_chunks)
                    .parity_chunks(parity_chunks)
                    .chunk_size(1 << chunk_size)
                    .write(&mut reader)
                    .await
                    .map_err(ErrorMessage::with_prefix(self))?;
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
        }
    }

    pub async fn get_hashes_rec(
        &self,
        config: impl Into<Arc<Config>>,
    ) -> Result<impl Stream<Item = Result<AnyHash, ErrorMessage>>, ErrorMessage> {
        let config = config.into();
        let target = self.clone();
        let (hashes_tx, hashes_rx) = mpsc::channel(50);
        let mut locations = target
            .list_cluster_locations(&config)
            .await
            .map_err(ErrorMessage::with_prefix(self))?;
        while let Some(result) = locations.next().await {
            let hashes_tx = hashes_tx.clone();
            let config = config.clone();
            let loc = result.map_err(ErrorMessage::new)?;
            tokio::spawn(async move {
                let mut hashes = match loc.get_hashes(&config).await {
                    Ok(h) => h,
                    Err(err) => {
                        let _ = hashes_tx.send(Err(ErrorMessage::new(err))).await;
                        return;
                    },
                };
                while let Some(result) = hashes.next().await {
                    match result {
                        Ok(hash) => {
                            let _ = hashes_tx.send(Ok(hash)).await;
                        },
                        Err(err) => {
                            let _ = hashes_tx.send(Err(ErrorMessage::new(err))).await;
                        },
                    }
                }
            });
        }

        Ok(ReceiverStream::new(hashes_rx))
    }
}

impl FromStr for ClusterLocation {
    type Err = Box<dyn Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if "-" == s {
            return Ok(ClusterLocation::Stdio);
        }
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
            Stdio => write!(f, "-"),
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
