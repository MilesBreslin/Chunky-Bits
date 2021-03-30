use std::{
    collections::HashMap,
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
use rand::{
    self,
    Rng,
};
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
    fs::{
        self,
        File,
    },
    io::{
        AsyncRead,
        AsyncReadExt,
        AsyncWrite,
        AsyncWriteExt,
    },
    sync::Mutex,
    task::JoinHandle,
};
use url::Url;

use crate::Error;

#[derive(Debug, Serialize, Deserialize)]
pub struct FileReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub length: Option<u64>,
    pub parts: Vec<FilePart>,
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
            // data * chunksize all initialized to 0
            let mut data_buf: Vec<u8> = (0..data)
                .flat_map(|_| (0..chunksize).map(|_| 0))
                .collect();
            let mut bytes_read: usize = 0;
            {
                let mut buf = &mut data_buf[..];
                while buf.len() != 0 {
                    match reader.read(&mut buf).await {
                        Ok(bytes) if bytes > 0 => {
                            bytes_read += bytes;
                            buf = &mut buf[bytes..];
                        },
                        Ok(_) => {
                            done = true;
                            break
                        },
                        Err(err) => {
                            return Err(err.into());
                        },
                    }
                }
            }
            total_bytes += bytes_read as u64;
            if bytes_read == 0 {
                break;
            } else {
                let r = r.clone();
                let destination = destination.clone();
                parts_fut.push(tokio::spawn(async move {
                    // Move data_buf into scope and make it immutable
                    let data_buf = data_buf.as_slice();
                    // Divide the bytes into bytes_read/data rounded up chunks
                    let buf_length = (bytes_read + data - 1) / data;
                    let data_chunks: Vec<&[u8]> = (0..data)
                        .map(|index| -> &[u8] {
                            &data_buf[(buf_length*index)..(buf_length*(index+1))]
                        })
                        .collect();

                    // Zero out parity chunks
                    let mut parity_chunks: Vec<Vec<u8>> = 
                        vec![vec![0; buf_length]; parity];

                    // Calculate parity
                    r.encode_sep::<&[u8],Vec<u8>>(&data_chunks, &mut parity_chunks)?;

                    // Get some writers
                    let mut writers = destination.get_writers(data + parity).unwrap();

                    // Hash and write all chunks
                    let mut write_results = data_chunks
                        .iter()
                        .map(|slice| -> &[u8] {
                            *slice
                        })
                        .chain(parity_chunks.iter().map(|vec| vec.as_slice()))
                        .zip(writers.drain(..))
                        .map(|(data, mut writer)| async move {
                            let hash = Sha256Hash::from_buf(&data);
                            writer.write_shard(&format!("{}", hash), &data).await.map(
                                |locations| HashWithLocation {
                                    sha256: hash,
                                    locations: locations,
                                },
                            )
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
                            chunksize: Some(buf_length),
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
                content_type: None,
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
        for file_part in &self.parts {
            if self.length.map(|len| bytes_written >= len).unwrap_or(false) {
                break;
            }
            let buf = file_part.read().await?;
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
        Ok(())
    }

    pub async fn verify(&self) -> Vec<HashMap<&'_ Location, Integrity>> {
        self.parts
            .iter()
            .map(FilePart::verify)
            .collect::<FuturesOrdered<_>>()
            .collect()
            .await
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilePart {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption: Option<Encryption>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunksize: Option<usize>,
    pub data: Vec<HashWithLocation<Sha256Hash>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub parity: Vec<HashWithLocation<Sha256Hash>>,
}

impl FilePart {
    pub(crate) async fn read(&self) -> Result<Vec<u8>, Error> {
        let r: ReedSolomon<galois_8::Field> = ReedSolomon::new(self.data.len(), self.parity.len())?;
        let all_chunks_owned = self
            .data
            .iter()
            .chain(self.parity.iter())
            .cloned()
            .enumerate()
            .collect::<Vec<_>>();
        let all_chunks = Arc::new(Mutex::new(all_chunks_owned));
        let mut indexed_chunks = self
            .data
            .iter()
            .map(|_| {
                let all_chunks = all_chunks.clone();
                async move {
                    loop {
                        let mut all_chunks = all_chunks.as_ref().lock().await;
                        if all_chunks.is_empty() {
                            return None;
                        }
                        let sample = rand::thread_rng().gen_range(0..all_chunks.len());
                        let (index, mut chunk) = all_chunks.remove(sample);
                        drop(all_chunks);
                        for location in chunk.locations.drain(..) {
                            if let Some(data) = location.read().await {
                                let (data, hash) = Sha256Hash::from_vec_async(data).await;
                                if hash == chunk.sha256 {
                                    return Some((index, data));
                                }
                            }
                        }
                    }
                }
            })
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<Option<(usize, Vec<u8>)>>>()
            .await;
        let mut all_read_chunks: Vec<Option<Vec<u8>>> = self
            .data
            .iter()
            .chain(self.parity.iter())
            .map(|_| None)
            .collect();
        for indexed_chunk in indexed_chunks.drain(..) {
            if let Some((index, chunk)) = indexed_chunk {
                *all_read_chunks.get_mut(index).unwrap() = Some(chunk);
            }
        }
        if !all_read_chunks
            .iter()
            .take(self.data.len())
            .all(Option::is_some)
        {
            r.reconstruct(&mut all_read_chunks)?;
        }
        let mut output = Vec::<u8>::new();
        for buf in all_read_chunks.drain(..).take(self.data.len()) {
            output.append(&mut buf.unwrap())
        }
        Ok(output)
    }

    pub(crate) async fn verify(&self) -> HashMap<&'_ Location, Integrity> {
        let mut out = HashMap::new();
        for hash_with_location in self.data.iter().chain(self.parity.iter()) {
            let ref hash = hash_with_location.sha256;
            for location in hash_with_location.locations.iter() {
                let integrity: Integrity = {
                    if let Some(data) = location.read().await {
                        if hash.verify(data).await.1 {
                            Integrity::Valid
                        } else {
                            Integrity::Invalid
                        }
                    } else {
                        Integrity::Unavailable
                    }
                };
                out.insert(location, integrity);
            }
        }
        out
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Integrity {
    Valid,
    Invalid,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Location {
    Http(HttpUrl),
    Local(PathBuf),
}

impl Location {
    pub(crate) async fn read(&self) -> Option<Vec<u8>> {
        match self {
            Location::Local(path) => fs::read(&path).await.ok(),
            Location::Http(url) => match reqwest::get(Into::<Url>::into(url.clone())).await {
                Ok(resp) => resp.bytes().await.ok().map(|b| b.into_iter().collect()),
                Err(_) => None,
            },
        }
    }

    pub(crate) async fn write_subfile(&self, name: &str, bytes: &[u8]) -> Result<Location, Error> {
        use Location::*;
        match self {
            Http(url) => {
                let mut target_url: Url = url.clone().into();
                target_url.path_segments_mut().unwrap().push(name);
                reqwest::Client::new()
                    .put(target_url.clone())
                    .body(bytes.to_owned())
                    .send()
                    .await?;
                Ok(Http(HttpUrl(target_url)))
            },
            Local(path) => {
                let mut target_path = path.clone();
                target_path.push(name);
                File::create(&target_path).await?.write_all(bytes).await?;
                Ok(Local(target_path))
            },
        }
    }
}

#[async_trait]
impl ShardWriter for Location {
    async fn write_shard(&mut self, hash: &str, bytes: &[u8]) -> Result<Vec<Location>, Error> {
        self.write_subfile(hash, bytes)
            .await
            .map(|location| vec![location])
    }
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
    pub async fn verify(&self, buf: Vec<u8>) -> (Vec<u8>, bool) {
        let (buf, other) = Self::from_vec_async(buf).await;
        (buf, self.eq(&other))
    }

    pub async fn from_vec_async(buf: Vec<u8>) -> (Vec<u8>, Self) {
        tokio::task::spawn_blocking(move || {
            let hash = Sha256Hash::from_buf(&buf);
            (buf, hash)
        })
        .await
        .unwrap()
    }

    pub fn from_buf<T>(buf: &T) -> Self
    where
        T: AsRef<[u8]>,
    {
        let mut ret = Sha256Hash(Default::default());
        ret.0.copy_from_slice(&Sha256::digest(buf.as_ref())[..]);
        ret
    }

    pub async fn from_buf_async(buf: &[u8]) -> Self {
        struct SendMySlice {
            ptr: *const u8,
            len: usize,
        }
        unsafe impl Send for SendMySlice { }

        let buf_ptr = SendMySlice {
            len: buf.len(),
            ptr: buf.as_ptr(),
        };

        // Safe because lifetime is valid until the function completes
        unsafe {
            tokio::task::spawn_blocking(move || {
                let buf = std::slice::from_raw_parts(buf_ptr.ptr, buf_ptr.len);
                Self::from_buf(&buf)
            })
        }.await.unwrap()
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

pub trait CollectionDestination {
    type Writer: ShardWriter + Send + Sync;
    type Error: Into<Error> + std::error::Error + 'static;
    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, Self::Error>;
}

impl CollectionDestination for Vec<WeightedLocation> {
    type Error = Error;
    type Writer = Location;

    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, Self::Error> {
        use rand::seq::SliceRandom;
        if self.len() < count {
            return Err(Error::NotEnoughWriters);
        }
        let mut rng = rand::thread_rng();
        Ok(self
            .choose_multiple_weighted(&mut rng, count, |WeightedLocation { weight, .. }| {
                *weight as f64
            })
            .unwrap()
            .map(|WeightedLocation { location, .. }| location.clone())
            .collect())
    }
}

#[derive(Clone, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeightedLocation {
    #[serde(default = "WeightedLocation::default_weight")]
    pub weight: usize,
    pub location: Location,
}
impl WeightedLocation {
    fn default_weight() -> usize {
        1000
    }
}
impl FromStr for WeightedLocation {
    type Err = <Location as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split_string = s.split(":");
        if let (Some(prefix), Some(postfix)) = (split_string.next(), split_string.next()) {
            if let Ok(weight) = prefix.parse::<usize>() {
                return Ok(WeightedLocation {
                    weight: weight,
                    location: Location::from_str(postfix)?,
                });
            }
        }
        Ok(WeightedLocation {
            weight: Self::default_weight(),
            location: Location::from_str(s)?,
        })
    }
}

#[async_trait]
pub trait ShardWriter {
    async fn write_shard(&mut self, hash: &str, bytes: &[u8]) -> Result<Vec<Location>, Error>;
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
