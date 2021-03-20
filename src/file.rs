use std::{
    fmt,
    hash::Hash,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use futures::stream::{
    FuturesOrdered,
    StreamExt,
};
use rand::Rng;
use reed_solomon_erasure::{
    galois_8,
    ReedSolomon,
};
use serde::{
    Deserialize,
    Serialize,
};
use sha2::{
    Digest,
    Sha256,
};
use tokio::{
    fs::{self,},
    io::{
        AsyncRead,
        AsyncReadExt,
        AsyncWrite,
        AsyncWriteExt,
    },
    task::JoinHandle,
};
use url::Url;

use crate::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct FileReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    compression: Option<Compression>,
    length: Option<u64>,
    parts: Vec<FilePart>,
}

impl FileReference {
    pub async fn from_reader<R, D>(
        reader: &mut R,
        destination: Arc<D>,
        chunksize: usize,
        data: usize,
        parity: usize,
    ) -> Result<Self, Error>
    where
        R: AsyncRead + Unpin,
        D: CollectionDestination + Send + Sync + 'static,
    {
        let mut parts_fut = FuturesOrdered::<JoinHandle<Result<FilePart, Error>>>::new();
        let r: Arc<ReedSolomon<galois_8::Field>> =
            Arc::new(ReedSolomon::new(data, parity).unwrap());
        let mut done = false;
        let mut total_bytes: u64 = 0;
        while !done {
            let mut bufs: Vec<Vec<u8>> = (0..(data + parity))
                .map(|_| (0..chunksize).map(|_| 0).collect())
                .collect();
            let mut wrote_data = false;
            'buffers: for buf in bufs.iter_mut().take(data) {
                let mut buf = &mut buf[..];
                while buf.len() != 0 {
                    match reader.read(&mut buf).await {
                        Ok(bytes_written) if bytes_written > 0 => {
                            wrote_data = true;
                            total_bytes += bytes_written as u64;
                            buf = &mut buf[bytes_written..];
                        },
                        Err(_) | Ok(_) => {
                            done = true;
                            break 'buffers;
                        },
                    }
                }
            }

            if wrote_data {
                let r = r.clone();
                let destination = destination.clone();
                parts_fut.push(tokio::spawn(async move {
                    r.encode(&mut bufs)?;
                    let buf_futures: FuturesOrdered<_> = bufs
                        .drain(..)
                        .map(|data| {
                            tokio::spawn(async move {
                                let hash = Sha256Hash::from_buf(&data);
                                (data, hash)
                            })
                        })
                        .collect();
                    let (mut bufs, mut hashes): (Vec<Vec<u8>>, Vec<Sha256Hash>) =
                        buf_futures.map(|res| res.unwrap()).unzip().await;
                    let mut writers = destination.get_writers(&hashes).await?;
                    let mut write_results = bufs
                        .drain(..)
                        .zip(writers.drain(..))
                        .zip(hashes.drain(..))
                        .map(|((data, (mut writer, location)), hash)| async move {
                            writer.write_shard(&data).await.map(|_| HashWithLocation {
                                sha256: hash,
                                locations: location,
                            })
                        })
                        .collect::<FuturesOrdered<_>>()
                        .collect::<Vec<Result<HashWithLocation<Sha256Hash>, Error>>>()
                        .await;
                    if write_results.iter().any(Result::is_err) {
                        Err(write_results
                            .drain(..)
                            .filter_map(Result::err)
                            .next()
                            .unwrap()
                            .into())
                    } else {
                        let mut hashes_with_location: Vec<HashWithLocation<Sha256Hash>> =
                            write_results.drain(..).filter_map(Result::ok).collect();
                        Ok(FilePart {
                            encryption: None,
                            chunksize: Some(chunksize),
                            data: hashes_with_location.drain(..data).collect(),
                            parity: hashes_with_location,
                        })
                    }
                }))
            }
        }
        let mut parts_res: Vec<Result<FilePart, Error>> = parts_fut
            .map(|res| match res {
                Ok(Ok(value)) => Ok(value),
                Ok(Err(e)) => Err(e.into()),
                Err(e) => Err(e.into()),
            })
            .collect()
            .await;
        if parts_res.iter().any(Result::is_err) {
            Err(parts_res.drain(..).filter_map(Result::err).next().unwrap())
        } else {
            let parts = parts_res.drain(..).filter_map(Result::ok).collect();
            Ok(FileReference {
                compression: None,
                length: Some(total_bytes),
                parts: parts,
            })
        }
    }

    pub async fn to_writer<W>(&self, writer: &mut W) -> Result<(), Error>
    where
        W: AsyncWrite + Unpin,
    {
        let mut bytes_written: u64 = 0;
        for FilePart { data, parity, .. } in &self.parts {
            if self.length.map(|len| bytes_written >= len).unwrap_or(false) {
                break;
            }
            let r: ReedSolomon<galois_8::Field> =
                ReedSolomon::new(data.len(), parity.len()).unwrap();
            let mut shard_bufs = data
                .iter()
                .chain(parity.iter())
                .map(|filepart| async move {
                    for location in &filepart.locations {
                        let read = match location {
                            Location::Local(path) => fs::read(&path).await.ok(),
                            Location::Http(url) => {
                                match reqwest::get(Into::<Url>::into(url.clone())).await {
                                    Ok(resp) => {
                                        resp.bytes().await.ok().map(|b| b.into_iter().collect())
                                    },
                                    Err(_) => None,
                                }
                            },
                        };
                        if let Some(buf) = read {
                            return Some(buf);
                        }
                    }
                    None
                })
                .collect::<FuturesOrdered<_>>()
                .collect::<Vec<Option<Vec<u8>>>>()
                .await;
            r.reconstruct(&mut shard_bufs)?;
            for buf in shard_bufs.drain(..).take(data.len()).filter_map(|op| op) {
                if let Some(file_length) = self.length {
                    let bytes_remaining: u64 = file_length - bytes_written;
                    let mut w_buf: &[u8] = &buf;
                    if bytes_remaining < w_buf.len() as u64 {
                        w_buf = &buf[..bytes_remaining as usize];
                    }
                    bytes_written += w_buf.len() as u64;
                    writer.write_all(&w_buf).await?;
                } else {
                    writer.write_all(&buf).await?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilePart {
    #[serde(skip_serializing_if = "Option::is_none")]
    encryption: Option<Encryption>,
    #[serde(skip_serializing_if = "Option::is_none")]
    chunksize: Option<usize>,
    data: Vec<HashWithLocation<Sha256Hash>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    parity: Vec<HashWithLocation<Sha256Hash>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Location {
    Http(HttpUrl),
    Local(PathBuf),
}

impl FromStr for Location {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("http://") || s.starts_with("https://") {
            return Ok(Location::Http(HttpUrl::from_str(s)?));
        }
        Ok(Location::Local(FromStr::from_str(s).unwrap()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HashWithLocation<T: Serialize + Clone + PartialEq + Eq + Hash + PartialOrd + Ord> {
    pub sha256: T,
    pub locations: Vec<Location>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Sha256Hash(#[serde(with = "hex")] [u8; 32]);

impl Sha256Hash {
    pub fn from_buf<T>(buf: &T) -> Self
    where
        T: AsRef<[u8]>,
    {
        let mut ret = Sha256Hash(Default::default());
        ret.0.copy_from_slice(&Sha256::digest(buf.as_ref())[..]);
        ret
    }

    pub fn from_reader<R>(reader: &mut R) -> std::io::Result<Self>
    where
        R: std::io::Read,
    {
        let mut hasher: Sha256 = Default::default();
        std::io::copy(reader, &mut hasher)?;
        let mut ret = Sha256Hash(Default::default());
        ret.0.copy_from_slice(&hasher.finalize()[..]);
        Ok(ret)
    }

    pub fn to_string(&self) -> String {
        hex::encode(&self.0)
    }
}

impl FromStr for Sha256Hash {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut new = Sha256Hash(Default::default());
        hex::decode_to_slice(s, &mut new.0)?;
        Ok(new)
    }
}

impl fmt::Display for Sha256Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", &self.to_string())
    }
}

#[async_trait]
pub trait CollectionDestination {
    async fn get_writers<T: fmt::Display + Send + Sync + 'static>(
        &self,
        addrs: &[T],
    ) -> Result<
        Vec<(
            Box<dyn ShardWriter + Unpin + Send + Sync + 'static>,
            Vec<Location>,
        )>,
        Error,
    >;
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LocationDestination {
    Http(crate::http::HttpFiles),
    Local(crate::localfiles::LocalFiles),
}

impl FromStr for LocationDestination {
    type Err = <Location as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Location::from_str(s)?.into())
    }
}
impl From<Location> for LocationDestination {
    fn from(loc: Location) -> Self {
        use crate::{
            http::HttpFiles,
            localfiles::LocalFiles,
        };
        match loc {
            Location::Local(path) => LocationDestination::Local(LocalFiles::new(&path)),
            Location::Http(url) => LocationDestination::Http(HttpFiles::new(url)),
        }
    }
}

#[async_trait]
impl CollectionDestination for LocationDestination {
    async fn get_writers<T: fmt::Display + Send + Sync + 'static>(
        &self,
        addrs: &[T],
    ) -> Result<
        Vec<(
            Box<dyn ShardWriter + Unpin + Send + Sync + 'static>,
            Vec<Location>,
        )>,
        Error,
    > {
        use LocationDestination::*;
        match self {
            Local(loc) => loc.get_writers(addrs).await,
            Http(loc) => loc.get_writers(addrs).await,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WeightedLocation {
    #[serde(default = "WeightedLocation::default_weight")]
    pub weight: usize,
    pub location: LocationDestination,
}
impl WeightedLocation {
    fn default_weight() -> usize {
        1000
    }
}
impl FromStr for WeightedLocation {
    type Err = <LocationDestination as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split_string = s.split(":");
        if let (Some(prefix), Some(postfix)) = (split_string.next(), split_string.next()) {
            if let Ok(weight) = prefix.parse::<usize>() {
                return Ok(WeightedLocation {
                    weight: weight,
                    location: LocationDestination::from_str(postfix)?,
                });
            }
        }
        Ok(WeightedLocation {
            weight: Self::default_weight(),
            location: LocationDestination::from_str(s)?,
        })
    }
}

#[async_trait]
impl CollectionDestination for Vec<WeightedLocation> {
    async fn get_writers<T: fmt::Display + Send + Sync + 'static>(
        &self,
        addrs: &[T],
    ) -> Result<
        Vec<(
            Box<dyn ShardWriter + Unpin + Send + Sync + 'static>,
            Vec<Location>,
        )>,
        Error,
    > {
        let mut working_locations: Vec<&WeightedLocation> = self.iter().collect();
        let mut res = vec![];
        for addr in addrs {
            let mut total_weight: usize = working_locations
                .iter()
                .map(|WeightedLocation { weight, .. }| *weight)
                .sum();
            if total_weight == 0 {
                working_locations = self.iter().collect();
                total_weight = working_locations
                    .iter()
                    .map(|WeightedLocation { weight, .. }| *weight)
                    .sum();
            }
            let sample = rand::thread_rng().gen_range(1..(total_weight + 1));
            let mut counted_weight = 0;
            let mut found_index = None;
            for (index, WeightedLocation { weight, .. }) in working_locations.iter().enumerate() {
                counted_weight += weight;
                if sample <= counted_weight {
                    found_index = Some(index);
                    break;
                }
            }
            if let Some(index) = found_index {
                res.push(
                    working_locations
                        .remove(index)
                        .location
                        .get_writers(&[format!("{}", addr)])
                        .await?
                        .drain(..)
                        .next()
                        .unwrap(),
                )
            } else {
                return Err(Error::NotEnoughWriters);
            }
        }
        Ok(res)
    }
}

#[async_trait]
pub trait ShardWriter {
    async fn write_shard(&mut self, bytes: &[u8]) -> Result<(), Error>;
}

#[async_trait]
impl ShardWriter for fs::File {
    async fn write_shard(&mut self, bytes: &[u8]) -> Result<(), Error> {
        Ok(self.write_all(&bytes).await?)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Compression {}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Encryption {}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "Url")]
#[serde(into = "Url")]
pub struct HttpUrl(Url);

impl FromStr for HttpUrl {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use std::convert::TryFrom;
        Ok(Self::try_from(Url::from_str(s)?)?)
    }
}

impl Into<Url> for HttpUrl {
    fn into(self) -> Url {
        self.0
    }
}

impl std::convert::TryFrom<Url> for HttpUrl {
    type Error = Error;

    fn try_from(u: Url) -> Result<Self, Self::Error> {
        match u.scheme() {
            "http" | "https" => Ok(Self(u)),
            _ => Err(Error::NotHttp),
        }
    }
}
