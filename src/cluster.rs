use std::{
    cmp::Ordering,
    collections::{
        BTreeMap,
        BTreeSet,
        HashMap,
        HashSet,
    },
    convert::{
        Infallible,
        TryFrom,
    },
    fmt,
    marker::PhantomData,
    path::PathBuf,
    sync::Arc,
};

use async_trait::async_trait;
use rand::{
    self,
    Rng,
};
use serde::{
    de::DeserializeOwned,
    Deserialize,
    Serialize,
};
use tokio::{
    fs,
    io::AsyncRead,
    process::Command,
    sync::{
        Mutex,
        RwLock,
    },
};

use crate::{
    file::{
        self,
        CollectionDestination,
        FileReference,
        Location,
        ShardWriter,
        WeightedLocation,
    },
    Error,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Cluster {
    #[serde(alias = "destination")]
    #[serde(alias = "nodes")]
    #[serde(alias = "node")]
    destinations: ClusterNodes,
    #[serde(alias = "metadata")]
    metadata: MetadataTypes,
    profiles: ClusterProfiles,
}

impl Cluster {
    pub async fn write_file<R>(
        &self,
        filename: &str,
        reader: &mut R,
        profile: &ClusterProfile,
    ) -> Result<(), Error>
    where
        R: AsyncRead + Unpin,
    {
        let destination = self.get_destination(profile).await;
        let file_ref = file::FileReference::from_reader(
            reader,
            Arc::new(destination),
            1 << profile.get_chunk_size(),
            profile.get_data_chunks(),
            profile.get_parity_chunks(),
        )
        .await?;
        self.metadata.write(&filename, &file_ref).await?;
        Ok(())
    }

    pub async fn get_file_ref(&self, filename: &str) -> Result<FileReference, Error> {
        self.metadata.read(&filename).await
    }

    pub async fn read_file(&self, filename: &str) -> Result<impl AsyncRead + Unpin, Error> {
        let file_ref = self.get_file_ref(filename).await?;
        let (reader, mut writer) = tokio::io::duplex(64);
        tokio::spawn(async move { file_ref.to_writer(&mut writer).await });
        Ok(reader)
    }

    pub async fn get_destination(&self, profile: &ClusterProfile) -> impl CollectionDestination {
        self.destinations.clone().with_profile(profile.clone())
    }

    pub fn get_profile<'a, T>(&self, profile: T) -> Option<&'_ ClusterProfile>
    where
        T: Into<Option<&'a str>>,
    {
        self.profiles.get(profile)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
enum MetadataTypes {
    Path(MetadataPath),
}

impl MetadataTypes {
    pub async fn write<T, U>(&self, filename: &T, payload: &U) -> Result<(), Error>
    where
        T: std::borrow::Borrow<str>,
        U: Serialize,
    {
        match self {
            MetadataTypes::Path(meta_path) => meta_path.write(filename, payload).await,
        }
    }

    pub async fn read<T, U>(&self, filename: &T) -> Result<U, Error>
    where
        T: std::borrow::Borrow<str>,
        U: DeserializeOwned,
    {
        match self {
            MetadataTypes::Path(meta_path) => meta_path.read(filename).await,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct MetadataPath {
    #[serde(default)]
    format: MetadataFormat,
    path: PathBuf,
    put_script: Option<String>,
    #[serde(default)]
    fail_on_script_error: bool,
}

impl MetadataPath {
    pub async fn write<T, U>(&self, filename: &T, payload: &U) -> Result<(), Error>
    where
        T: std::borrow::Borrow<str>,
        U: Serialize,
    {
        let payload = self.format.to_string(payload)?;
        fs::write(
            &format!("{}/{}", self.path.display(), filename.borrow()),
            payload,
        )
        .await?;
        if let Some(put_script) = &self.put_script {
            let res = Command::new("/bin/sh")
                .arg("-c")
                .arg(put_script)
                .current_dir(&self.path)
                .spawn()
                .unwrap()
                .wait()
                .await;
            if self.fail_on_script_error {
                res?;
            }
        }
        Ok(())
    }

    pub async fn read<T, U>(&self, filename: &T) -> Result<U, Error>
    where
        T: std::borrow::Borrow<str>,
        U: DeserializeOwned,
    {
        let bytes = fs::read(&format!("{}/{}", self.path.display(), filename.borrow())).await?;
        self.format.from_bytes(&bytes)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum MetadataFormat {
    Json,
    JsonPretty,
    Yaml,
}

impl Default for MetadataFormat {
    fn default() -> Self {
        MetadataFormat::Yaml
    }
}

impl MetadataFormat {
    fn to_string<T>(&self, payload: &T) -> Result<String, Error>
    where
        T: Serialize,
    {
        use MetadataFormat::*;
        Ok(match self {
            Json => serde_json::to_string(payload)?,
            JsonPretty => serde_json::to_string_pretty(payload)?,
            Yaml => serde_yaml::to_string(payload)?,
        })
    }

    fn from_bytes<T, U>(&self, v: &T) -> Result<U, Error>
    where
        T: AsRef<[u8]>,
        U: DeserializeOwned,
    {
        use MetadataFormat::*;
        Ok(match self {
            Json | JsonPretty | Yaml => serde_yaml::from_slice(v.as_ref())?,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ClusterProfiles {
    default: ClusterProfile,
    #[serde(flatten)]
    custom: BTreeMap<String, ClusterProfile>,
}

impl ClusterProfiles {
    fn get<'a, T>(&self, profile: T) -> Option<&'_ ClusterProfile>
    where
        T: Into<Option<&'a str>>,
    {
        let profile = profile.into();
        match profile {
            Some("default") | None => Some(&self.default),
            Some(profile) => self.custom.get(profile),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClusterProfile {
    #[serde(default)]
    chunk_size: ChunkSize,
    #[serde(alias = "data")]
    data_chunks: DataChunkCount,
    #[serde(alias = "parity")]
    parity_chunks: ParityChunkCount,
    #[serde(default)]
    #[serde(alias = "zone")]
    #[serde(alias = "zones")]
    #[serde(alias = "rules")]
    zone_rules: ZoneRules,
}

impl ClusterProfile {
    fn get_chunk_size(&self) -> usize {
        self.chunk_size.clone().into()
    }

    fn get_data_chunks(&self) -> usize {
        self.data_chunks.clone().into()
    }

    fn get_parity_chunks(&self) -> usize {
        self.parity_chunks.clone().into()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ZoneRules(BTreeMap<String, ZoneRule>);
impl Default for ZoneRules {
    fn default() -> Self {
        ZoneRules(BTreeMap::new())
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct ZoneRule {
    #[serde(default = "ChunkCount::none")]
    minimum: ChunkCount,
    ideal: ChunkCount,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(from = "ClusterNodesDeserializer")]
#[serde(into = "BTreeSet<ClusterNode>")]
pub struct ClusterNodes(Vec<ClusterNode>);

impl ClusterNodes {
    fn with_profile(self, profile: ClusterProfile) -> ClusterNodesWithProfile {
        ClusterNodesWithProfile(Arc::new((self, profile)))
    }
}

impl Into<BTreeSet<ClusterNode>> for ClusterNodes {
    fn into(mut self) -> BTreeSet<ClusterNode> {
        self.0.drain(..).collect()
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ClusterNodesDeserializer {
    Single(ClusterNode),
    Set(Vec<ClusterNodesDeserializer>),
    Map(HashMap<String, ClusterNodesDeserializer>),
}

impl From<ClusterNodesDeserializer> for ClusterNodes {
    fn from(des: ClusterNodesDeserializer) -> ClusterNodes {
        use ClusterNodesDeserializer::*;
        ClusterNodes(match des {
            Single(node) => {
                vec![node]
            },
            Set(mut nodes) => {
                let mut nodes_out = Vec::<ClusterNode>::new();
                for sub_nodes in nodes.drain(..) {
                    let mut nodes: ClusterNodes = sub_nodes.into();
                    nodes_out.append(&mut nodes.0);
                }
                nodes_out
            },
            Map(mut nodes) => {
                let mut nodes_out = Vec::<ClusterNode>::new();
                for (name, sub_nodes) in nodes.drain() {
                    let nodes: ClusterNodes = sub_nodes.into();
                    for sub_node in nodes.0 {
                        let mut sub_node = sub_node.clone();
                        sub_node.zones.insert(name.clone());
                        nodes_out.push(sub_node);
                    }
                }
                nodes_out
            },
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClusterNode {
    #[serde(flatten)]
    location: WeightedLocation,
    #[serde(default)]
    zones: BTreeSet<String>,
    #[serde(default)]
    repeat: usize,
}

impl Ord for ClusterNode {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.zones == other.zones {
            self.location.cmp(&other.location)
        } else {
            self.zones.cmp(&other.zones)
        }
    }
}

impl PartialOrd for ClusterNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
struct ClusterNodesWithProfile(Arc<(ClusterNodes, ClusterProfile)>);

impl AsRef<ClusterNodes> for ClusterNodesWithProfile {
    fn as_ref(&self) -> &ClusterNodes {
        &self.0.as_ref().0
    }
}
impl AsRef<ClusterProfile> for ClusterNodesWithProfile {
    fn as_ref(&self) -> &ClusterProfile {
        &self.0.as_ref().1
    }
}

impl CollectionDestination for ClusterNodesWithProfile {
    type Error = Infallible;
    type Writer = ClusterWriter;

    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, Self::Error> {
        let writer = ClusterWriter {
            state: Arc::new(ClusterWriterState {
                parent: self.clone(),
                inner_state: Mutex::new(ClusterWriterInnerState {
                    available_indexes: <Self as AsRef<ClusterNodes>>::as_ref(self)
                        .0
                        .iter()
                        .enumerate()
                        .map(|(i, node)| (i, node.repeat + 1))
                        .collect(),
                    failed_indexes: HashSet::new(),
                }),
            }),
        };
        Ok((0..count).map(|_| writer.clone()).collect())
    }
}

struct ClusterWriterState {
    parent: ClusterNodesWithProfile,
    inner_state: Mutex<ClusterWriterInnerState>,
}
struct ClusterWriterInnerState {
    available_indexes: HashMap<usize, usize>,
    failed_indexes: HashSet<usize>,
}

impl ClusterWriterState {
    async fn next_writer(&self) -> Option<(usize, &ClusterNode)> {
        let (nodes, profile) = self.parent.0.as_ref();
        let mut state = self.inner_state.lock().await;
        let ClusterWriterInnerState {
            ref mut available_indexes,
            ref failed_indexes,
        } = *state;
        if available_indexes.len() == 0 {
            return None;
        }
        let mut available_locations = nodes
            .0
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                if let Some(availability) = available_indexes.get(&i) {
                    if *availability >= 1 {
                        return !failed_indexes.contains(&i);
                    }
                }
                false
            })
            .collect::<Vec<_>>();
        let total_weight: usize = available_locations
            .iter()
            .map(|(_, node)| node.location.weight)
            .sum();
        if total_weight == 0 {
            return None;
        }
        let sample = rand::thread_rng().gen_range(1..(total_weight + 1));
        let mut current_weight: usize = 0;
        for (index, node) in available_locations.drain(..) {
            current_weight += node.location.weight;
            if current_weight > sample {
                let mut availability = available_indexes.get_mut(&index).unwrap();
                *availability -= 1;
                return Some((index, node));
            }
        }
        None
    }

    async fn invalidate_index(&self, index: usize) -> () {
        let mut state = self.inner_state.lock().await;
        state.failed_indexes.insert(index);
    }
}

#[derive(Clone)]
struct ClusterWriter {
    state: Arc<ClusterWriterState>,
}

#[async_trait]
impl ShardWriter for ClusterWriter {
    async fn write_shard(&mut self, hash: &str, bytes: &[u8]) -> Result<Vec<Location>, Error> {
        loop {
            match self.state.as_ref().next_writer().await {
                Some((index, node)) => {
                    let writer = &node.location.location;
                    if let Ok(loc) = writer.write_subfile(hash, bytes).await {
                        return Ok(vec![loc]);
                    } else {
                        self.state.invalidate_index(index).await;
                    }
                },
                None => {
                    return Err(Error::NotEnoughWriters);
                },
            }
        }
    }
}

pub trait SizedInt {
    const MAX: usize;
    const MIN: usize;
    const NAME: &'static str;
}

#[derive(Clone, PartialEq, Eq)]
pub struct SizeError<T: SizedInt>(PhantomData<T>);
impl<T: SizedInt> SizeError<T> {
    fn new() -> Self {
        Self(PhantomData)
    }
}
impl<T> std::fmt::Display for SizeError<T>
where
    T: SizedInt,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} must be greater than {} and less than {}",
            T::NAME,
            T::MIN,
            T::MAX,
        )
    }
}
impl<T> std::fmt::Debug for SizeError<T>
where
    T: SizedInt,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SizeError({})", T::NAME)
    }
}
impl<T> std::error::Error for SizeError<T> where T: SizedInt {}

macro_rules! sized_uint {
    ($type:ident, $closest_int:ident, $max:expr, $min:expr) => {
        #[derive(Clone,Debug,PartialEq,Eq,Hash,PartialOrd,Ord,Serialize,Deserialize)]
        #[serde(try_from = "usize")]
        #[serde(into = "usize")]
        pub struct $type($closest_int);

        impl SizedInt for $type {
            const MAX: usize = $max;
            const MIN: usize = $min;
            const NAME: &'static str = stringify!{$type};
        }
        impl PartialEq<$closest_int> for $type {
            fn eq(&self, other: &$closest_int) -> bool {
                PartialEq::eq(&self.0, other)
            }
        }
        impl std::str::FromStr for $type {
            type Err = <Self as TryFrom<$closest_int>>::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match <$closest_int as std::str::FromStr>::from_str(s) {
                    Ok(i) => Ok(std::convert::TryInto::try_into(i)?),
                    Err(_) => Err(SizeError::new()),
                }
            }
        }
        impl fmt::Display for $type {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
        sized_uint!(
            @Into<
                u8,
                u16,
                u32,
                u64,
                usize,
                i8,
                i16,
                i32,
                i64,
                isize
            >
            for $type
        );
        sized_uint!(
            @TryFrom<
                u8,
                u16,
                u32,
                usize,
                i8,
                i16,
                i32,
                isize
            >
            for $type as $closest_int
        );
    };
    (@Into<$($int:path),*> for $type:ident) => {
        $(
            impl Into<$int> for $type {
                fn into(self) -> $int {
                    self.0 as $int
                }
            }
        )*
    };
    (@TryFrom<$($int:path),*> for $type:ident as $closest_int:ident) => {
        $(
            impl std::convert::TryFrom<$int> for $type {
                type Error = SizeError<$type>;
                fn try_from(i: $int) -> Result<Self, Self::Error> {
                    match i as usize {
                        i if i > $type::MAX  => Err(SizeError::new()),
                        i if i < $type::MIN => Err(SizeError::new()),
                        _ => Ok(Self(i as $closest_int)),
                    }
                }
            }
        )*
    };
}

sized_uint!(ChunkSize, u8, 32, 10);
impl Default for ChunkSize {
    fn default() -> Self {
        ChunkSize(20)
    }
}
sized_uint!(DataChunkCount, u8, 256, 1);
sized_uint!(ParityChunkCount, u8, 256, 1);
sized_uint!(ChunkCount, u8, 256, 0);
impl ChunkCount {
    pub fn none() -> ChunkCount {
        ChunkCount(0)
    }
}
