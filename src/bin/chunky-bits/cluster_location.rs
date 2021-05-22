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

use anyhow::{
    anyhow,
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
    error_message::PrefixError,
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
    pub async fn get_reader(&self, config: &Config) -> Result<impl AsyncRead + Send + Unpin + '_> {
        use ClusterLocation::*;
        let result: Result<Pin<Box<dyn AsyncRead + Send + Unpin>>>;
        match self {
            ClusterFile { cluster, path } => {
                let cluster = config.get_cluster(&cluster).await.prefix_err(self)?;
                let file_ref = cluster.get_file_ref(&path).await.prefix_err(self)?;
                let reader = file_ref.read_builder_owned().reader_owned();
                result = Ok(Box::pin(reader));
            },
            FileRef(loc) => {
                let bytes = loc.read().await.prefix_err(self)?;
                let file_ref: FileReference = serde_yaml::from_slice(&bytes).prefix_err(self)?;
                let reader = file_ref.read_builder_owned().reader_owned();
                result = Ok(Box::pin(reader));
            },
            Other(loc) => {
                let reader = loc
                    .reader_with_context(&Default::default())
                    .await
                    .prefix_err(self)?;
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
    ) -> Result<u64> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                let profile_name = config.get_profile(&cluster_name).await;
                let profile = match cluster.get_profile(profile_name.as_deref()) {
                    Some(profile) => profile,
                    None => bail!("Profile not found: {}", profile_name.unwrap()),
                };
                let file_ref = cluster.get_file_writer(&profile).write(reader).await?;
                cluster.write_file_ref(path, &file_ref).await?;
                Ok(file_ref.len_bytes())
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
                    .data_chunks(data_chunks.into())
                    .parity_chunks(parity_chunks.into())
                    .chunk_size(1 << usize::from(chunk_size))
                    .write(reader)
                    .await?;
                let file_str = serde_json::to_string_pretty(&file_ref)?;
                loc.write(file_str.as_bytes()).await?;
                Ok(file_ref.len_bytes())
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

    pub async fn list_files(&self, config: &Config) -> Result<FilesStreamer<'static>> {
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
    ) -> Result<FilesStreamer<'a>> {
        Self::list_files_recursive_inner(self, config).await
    }

    fn list_files_recursive_inner<'a>(
        self_outer: (impl Borrow<Self> + Clone + Send + Sync + 'a),
        config: &'a Config,
    ) -> BoxFuture<'a, Result<FilesStreamer<'a>>> {
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
    ) -> Result<impl Stream<Item = io::Result<Self>> + 'a> {
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

    pub async fn resilver(&self, config: &Config) -> Result<ResilverFileReportOwned> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                let profile_name = config.get_profile(&cluster_name).await;
                let profile = match cluster.get_profile(profile_name.as_deref()) {
                    Some(profile) => profile,
                    None => bail!("Profile not found: {}", profile_name.unwrap()),
                };
                let destination = cluster.get_destination(&profile);
                let file_ref = cluster.get_file_ref(&path).await?;
                let destination = Arc::new(destination);
                let report = file_ref.resilver_owned(destination).await;
                cluster.write_file_ref(&path, report.as_ref()).await?;
                Ok(report)
            },
            FileRef(loc) => {
                let bytes = loc.read().await?;
                let file_ref: FileReference = serde_yaml::from_slice(&bytes)?;
                let destination = config.get_default_destination().await?;
                let report = file_ref.resilver_owned(destination).await;
                let file_ref: &FileReference = report.as_ref();
                let file_str = serde_json::to_string_pretty(file_ref)?;
                loc.write(file_str.as_bytes()).await?;
                Ok(report)
            },
            _ => bail!("Resilver is only supported on cluster files"),
        }
    }

    pub async fn verify(&self, config: &Config) -> Result<VerifyFileReportOwned> {
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
            _ => bail!("Resilver is only supported on files"),
        }
    }

    pub async fn get_hashes(
        &self,
        config: &Config,
    ) -> Result<BoxStream<'_, Result<AnyHash, Box<dyn Error + Send + Sync>>>> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await.prefix_err(self)?;
                let file_ref = cluster.get_file_ref(&path).await.prefix_err(self)?;
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
                let bytes = loc.read().await.prefix_err(self)?;
                let file_ref: FileReference = serde_yaml::from_slice(&bytes).prefix_err(self)?;
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
                let data_chunks = config.get_default_data_chunks().await?;
                let parity_chunks = config.get_default_parity_chunks().await?;
                let chunk_size = config.get_default_chunk_size().await?;
                static WARNING: Once = Once::new();
                WARNING.call_once(|| {
                    eprintln!(
                        "Warning: Hashes generated from binary data using data = {}, parity = {}, chunk_size = 2^{}",
                        data_chunks,
                        parity_chunks,
                        chunk_size,
                    );
                });
                let mut reader = self.get_reader(config).await.prefix_err(self)?;
                let file_ref = FileReference::write_builder()
                    .destination(())
                    .data_chunks(data_chunks.into())
                    .parity_chunks(parity_chunks.into())
                    .chunk_size(1 << usize::from(chunk_size))
                    .write(&mut reader)
                    .await
                    .prefix_err(self)?;
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
    ) -> Result<impl Stream<Item = Result<AnyHash>>> {
        let config = config.into();
        let target = self.clone();
        let (hashes_tx, hashes_rx) = mpsc::channel(50);
        let mut locations = target
            .list_cluster_locations(&config)
            .await
            .prefix_err(self)?;
        while let Some(result) = locations.next().await {
            let hashes_tx = hashes_tx.clone();
            let config = config.clone();
            let loc = result?;
            tokio::spawn(async move {
                let mut hashes = match loc.get_hashes(&config).await {
                    Ok(h) => h,
                    Err(err) => {
                        let _ = hashes_tx.send(Err(anyhow!("{}", err))).await;
                        return;
                    },
                };
                while let Some(result) = hashes.next().await {
                    match result {
                        Ok(hash) => {
                            let _ = hashes_tx.send(Ok(hash)).await;
                        },
                        Err(err) => {
                            let _ = hashes_tx.send(Err(anyhow!("{}", err))).await;
                        },
                    }
                }
            });
        }

        Ok(ReceiverStream::new(hashes_rx))
    }

    pub async fn migrate(&self, config: &Config, destination: &Self) -> Result<()> {
        match destination {
            ClusterLocation::ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config
                    .get_cluster(&cluster_name)
                    .await
                    .prefix_err(destination)?;
                let profile_name = config.get_profile(&cluster_name).await;
                let profile = match cluster.get_profile(profile_name.as_deref()) {
                    Some(profile) => profile,
                    None => bail!("Profile not found: {}", profile_name.unwrap()),
                };
                let file_ref = self
                    .get_file_reference(
                        config,
                        profile.data_chunks,
                        profile.parity_chunks,
                        profile.chunk_size,
                    )
                    .await?;
                cluster
                    .write_file_ref(path, &file_ref)
                    .await
                    .prefix_err(destination)?;
            },
            ClusterLocation::FileRef(location) => {
                let file_ref = self
                    .get_file_reference(
                        config,
                        config.get_default_data_chunks().await.unwrap(),
                        config.get_default_parity_chunks().await.unwrap(),
                        config.get_default_chunk_size().await.unwrap(),
                    )
                    .await?;
                let file_str = serde_json::to_string_pretty(&file_ref).prefix_err(destination)?;
                location
                    .write(file_str.as_bytes())
                    .await
                    .prefix_err(destination)?;
            },
            _ => bail!("Cannot migrate to {}", destination),
        }
        Ok(())
    }

    pub async fn get_file_reference(
        &self,
        config: &Config,
        data_chunks: DataChunkCount,
        parity_chunks: ParityChunkCount,
        chunk_size: ChunkSize,
    ) -> Result<FileReference> {
        match self {
            ClusterLocation::Other(_)
            | ClusterLocation::FileRef(_)
            | ClusterLocation::ClusterFile { .. } => {},
            _ => bail!("Cannot get a file reference for {}", self),
        }
        Ok(match &self {
            ClusterLocation::Other(location) => {
                let location = location.clone();
                let mut reader = self.get_reader(&config).await?;
                let mut file_ref = FileReference::write_builder()
                    .data_chunks(data_chunks.into())
                    .parity_chunks(parity_chunks.into())
                    .chunk_size(1 << usize::from(chunk_size))
                    .write(&mut reader)
                    .await
                    .prefix_err(&location)?;
                let mut bytes_seen: u64 = 0;
                for FilePart {
                    ref chunksize,
                    ref mut data,
                    ..
                } in file_ref.parts.iter_mut()
                {
                    let chunksize = *chunksize as u64;
                    for Chunk {
                        ref mut locations, ..
                    } in data.iter_mut()
                    {
                        let mut location = location.clone();
                        let range = location.range_mut();
                        range.start = bytes_seen;
                        range.length = Some(chunksize);
                        locations.push(location);
                        bytes_seen += chunksize;
                    }
                }
                let last_location = file_ref
                    .parts
                    .last_mut()
                    .map(|part| part.data.last_mut())
                    .flatten()
                    .map(|chunk| chunk.locations.last_mut())
                    .flatten();
                if let Some(location) = last_location {
                    location.range_mut().extend_zeros = true;
                }
                file_ref
            },
            ClusterLocation::FileRef(location) => {
                let bytes = location.read().await?;
                serde_yaml::from_slice(&bytes)?
            },
            ClusterLocation::ClusterFile { cluster, path } => {
                let cluster = config.get_cluster(&cluster).await?;
                cluster.get_file_ref(&path).await?
            },
            _ => todo!(),
        })
    }
}

impl FromStr for ClusterLocation {
    type Err = anyhow::Error;

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
            (Some(cluster), Some(_), None) => bail!("Invalid cluster name: {}", cluster),
            (Some(_), None, None) => Ok(Location::from_str(s).map(Into::into)?),
            _ => bail!("Invalid cluster location format: {}", s),
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
            type Error = anyhow::Error;

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
