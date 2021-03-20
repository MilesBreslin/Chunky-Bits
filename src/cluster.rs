use std::{
    collections::BTreeMap,
    convert::TryFrom,
    fmt,
    marker::PhantomData,
    path::PathBuf,
    sync::Arc,
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
};

use crate::{
    file::{
        self,
        CollectionDestination,
        FileReference,
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

    pub async fn get_destination(&self, _profile: &ClusterProfile) -> impl CollectionDestination {
        Into::<Vec<Vec<WeightedLocation>>>::into(self.destinations.clone())
            .iter_mut()
            .map(|d| d.drain(..))
            .flatten()
            .collect::<Vec<WeightedLocation>>()
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
            let _ = Command::new("/bin/sh")
                .arg("-c")
                .arg(put_script)
                .current_dir(&self.path)
                .spawn()
                .unwrap()
                .wait()
                .await;
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
#[serde(rename_all = "lowercase")]
enum MetadataFormat {
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
        Ok(serde_yaml::to_string(payload)?)
    }

    fn from_bytes<T, U>(&self, v: &T) -> Result<U, Error>
    where
        T: AsRef<[u8]>,
        U: DeserializeOwned,
    {
        Ok(serde_yaml::from_slice(v.as_ref())?)
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
    #[serde(default = "ChunkSize::default")]
    chunk_size: ChunkSize,
    #[serde(alias = "data")]
    data_chunks: DataChunkCount,
    #[serde(alias = "parity")]
    parity_chunks: ParityChunkCount,
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
#[serde(untagged)]
#[serde(into = "Vec<Vec<WeightedLocation>>")]
enum ClusterNodes {
    Single(WeightedLocation),
    List(Vec<WeightedLocation>),
    MDList(Vec<Vec<WeightedLocation>>),
}

impl Into<Vec<Vec<WeightedLocation>>> for ClusterNodes {
    fn into(self) -> Vec<Vec<WeightedLocation>> {
        use ClusterNodes::*;
        match self {
            Single(node) => vec![vec![node]],
            List(nodes) => vec![nodes],
            MDList(md_nodes) => md_nodes,
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
            T::MAX
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
