use std::{
    convert::TryFrom,
    fmt,
    hash::Hash,
    io::Cursor,
    path::{
        Path,
        PathBuf,
    },
    pin::Pin,
    str::FromStr,
    string::ToString,
};

use async_trait::async_trait;
use futures::{
    stream,
    stream::StreamExt,
};
use lazy_static::lazy_static;
use reqwest::Body;
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    fs::{
        self,
        File,
    },
    io::{
        self,
        AsyncRead,
        AsyncReadExt,
        AsyncWriteExt,
    },
    sync::mpsc,
    time::Instant,
};
use tokio_util::io::StreamReader;
use url::Url;

use crate::{
    error::{
        LocationError,
        LocationParseError,
        ShardError,
    },
    file::{
        hash::AnyHash,
        profiler::Profiler,
        ShardWriter,
    },
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Location {
    Http(HttpUrl),
    Local(PathBuf),
}

impl Location {
    pub async fn read(&self) -> Result<Vec<u8>, LocationError> {
        self.read_with_context(&Self::default_context()).await
    }

    pub async fn write<T>(&self, bytes: T) -> Result<(), LocationError>
    where
        T: AsRef<[u8]> + Into<Vec<u8>>,
    {
        self.write_with_context(&Self::default_context(), bytes)
            .await
    }

    pub async fn write_subfile<T>(&self, name: &str, bytes: T) -> Result<Location, ShardError>
    where
        T: AsRef<[u8]> + Into<Vec<u8>>,
    {
        self.write_subfile_with_context(&Self::default_context(), name, bytes)
            .await
    }

    pub async fn delete(&self) -> Result<(), LocationError> {
        self.delete_with_context(&Self::default_context()).await
    }

    pub async fn read_with_context(&self, cx: &LocationContext) -> Result<Vec<u8>, LocationError> {
        let LocationContext {
            http_client,
            profiler,
            ..
        } = cx;
        let profiler = profiler.as_ref();
        let op_start = profiler.map(|_| Instant::now());

        use Location::*;
        let result: Result<Vec<u8>, LocationError> = match self {
            Local(path) => match fs::read(&path).await {
                Ok(bytes) => Ok(bytes),
                Err(err) => Err(err.into()),
            },
            Http(url) => match http_client.get(Url::from(url.clone())).send().await {
                Ok(resp) => match resp.bytes().await {
                    Ok(bytes) => Ok(bytes.into_iter().collect()),
                    Err(err) => Err(err.into()),
                },
                Err(err) => Err(err.into()),
            },
        };

        if let Some(op_start) = op_start {
            let profiler = profiler.unwrap();
            profiler.log_read(&result, self.clone(), op_start);
        }
        result
    }

    pub async fn reader_with_context(
        &self,
        cx: &LocationContext,
    ) -> Result<(impl AsyncRead + Unpin), LocationError> {
        // TODO: Profiler
        use Location::*;
        let result: Result<Pin<Box<dyn AsyncRead + Unpin>>, _> = match self {
            Local(path) => Ok(Box::pin(File::open(&path).await?)),
            Http(url) => {
                let url: Url = url.clone().into();
                let s = cx.http_client.get(url).send().await?.bytes_stream();
                let reader = StreamReader::new(s.map(|res| match res {
                    Ok(bytes) => Ok(Cursor::new(bytes)),
                    Err(err) => Err(io::Error::new(io::ErrorKind::Other, err)),
                }));
                Ok(Box::pin(reader))
            },
        };
        result
    }

    pub async fn write_with_context<T>(
        &self,
        cx: &LocationContext,
        bytes: T,
    ) -> Result<(), LocationError>
    where
        T: AsRef<[u8]> + Into<Vec<u8>>,
    {
        let LocationContext {
            http_client,
            profiler,
            ..
        } = cx;
        let profiler = profiler.as_ref();
        let op_start = profiler.map(|_| Instant::now());
        let length = bytes.as_ref().len();

        use Location::*;
        let result: Result<(), LocationError> = async move {
            match self {
                Local(path) => {
                    let mut file = File::create(&path).await?;
                    file.write_all(bytes.as_ref()).await?;
                    file.flush().await?;
                    Ok(())
                },
                Http(url) => {
                    let response = http_client
                        .put(Url::from(url.clone()))
                        .body(bytes.into())
                        .send()
                        .await;
                    response?;
                    Ok(())
                },
            }
        }
        .await;

        if let Some(op_start) = op_start {
            let profiler = profiler.unwrap();
            profiler.log_write(&result, self.clone(), length, op_start);
        }
        result
    }

    pub async fn write_from_reader_with_context(
        &self,
        cx: &LocationContext,
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<u64, LocationError> {
        // TODO: Profiler
        use Location::*;
        match self {
            Local(path) => {
                let mut file = File::create(&path).await?;
                let bytes = io::copy(reader, &mut file).await?;
                file.flush().await?;
                Ok(bytes)
            },
            Http(url) => {
                let url: Url = url.clone().into();
                let (tx, rx) = mpsc::channel::<io::Result<Vec<u8>>>(5);
                let cx = cx.clone();
                let response = tokio::spawn(async move {
                    let LocationContext { http_client, .. } = cx;
                    let s = stream::unfold(rx, |mut rx| async move {
                        rx.recv().await.map(|result| (result, rx))
                    });
                    http_client.put(url).body(Body::wrap_stream(s)).send().await
                });
                let mut total_bytes = 0;
                let mut bytes = vec![0; 1 << 20];
                loop {
                    match reader.read(&mut bytes).await {
                        Ok(0) => {
                            break;
                        },
                        Ok(length) => {
                            total_bytes += length as u64;
                            let buf: &[u8] = &bytes[0..length];
                            if tx.send(Ok(buf.to_owned())).await.is_err() {
                                break;
                            }
                        },
                        Err(err) => {
                            let _ = tx.send(Err(err.kind().into()));
                            return Err(err.into());
                        },
                    }
                }
                drop(tx);
                response.await.unwrap()?;
                eprintln!("Total: {}", total_bytes);
                Ok(total_bytes)
            },
        }
    }

    pub async fn write_subfile_with_context<T>(
        &self,
        cx: &LocationContext,
        name: &str,
        bytes: T,
    ) -> Result<Location, ShardError>
    where
        T: AsRef<[u8]> + Into<Vec<u8>>,
    {
        use Location::*;
        let target_location: Location = match self {
            Http(url) => {
                let mut target_url: Url = url.clone().into();
                target_url.path_segments_mut().unwrap().push(name);
                Http(HttpUrl(target_url))
            },
            Local(path) => {
                let mut target_path = path.clone();
                target_path.push(name);
                target_path.into()
            },
        };
        match target_location.write_with_context(cx, bytes).await {
            Ok(_) => Ok(target_location),
            Err(err) => Err(ShardError::LocationError {
                location: target_location,
                error: err,
            }),
        }
    }

    pub async fn delete_with_context(&self, cx: &LocationContext) -> Result<(), LocationError> {
        let LocationContext { http_client, .. } = cx;
        use Location::*;
        match self {
            Http(url) => {
                let url: Url = url.clone().into();
                http_client.delete(url).send().await?;
                Ok(())
            },
            Local(path) => {
                fs::remove_file(path).await?;
                Ok(())
            },
        }
    }

    pub fn default_context() -> LocationContext {
        Default::default()
    }

    pub fn is_child_of(&self, other: &Location) -> bool {
        let (left, right) = (self, other);
        use Location::*;
        match (left, right) {
            (Http(left), Http(right)) => {
                let (mut left, right): (Url, Url) = (left.clone().into(), right.clone().into());
                if let Ok(mut left) = left.path_segments_mut() {
                    left.pop();
                } else {
                    return false;
                }
                left == right
            },
            (Local(left), Local(right)) => {
                if let Some(left) = left.parent() {
                    left == right
                } else {
                    false
                }
            },
            _ => false,
        }
    }

    pub fn is_parent_of(&self, other: &Location) -> bool {
        other.is_child_of(self)
    }
}

#[derive(Clone)]
pub struct LocationContext {
    http_client: reqwest::Client,
    profiler: Option<Profiler>,
}

impl Default for LocationContext {
    fn default() -> Self {
        lazy_static! {
            static ref CX: LocationContext = LocationContext::builder().build();
        }
        <LocationContext as Clone>::clone(&CX)
    }
}

impl LocationContext {
    pub fn builder() -> LocationContextBuilder {
        Default::default()
    }
}

#[derive(Default)]
pub struct LocationContextBuilder {
    http_client: Option<reqwest::Client>,
    profiler: Option<Profiler>,
}

impl LocationContextBuilder {
    pub fn http_client(self, http_client: reqwest::Client) -> Self {
        LocationContextBuilder {
            http_client: Some(http_client),
            profiler: self.profiler,
        }
    }

    pub fn profiler(self, profiler: Profiler) -> Self {
        LocationContextBuilder {
            http_client: self.http_client,
            profiler: Some(profiler),
        }
    }

    pub fn build(self) -> LocationContext {
        LocationContext {
            http_client: self.http_client.unwrap_or_else(reqwest::Client::new),
            profiler: self.profiler,
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.serialize(f)
    }
}

#[async_trait]
impl ShardWriter for Location {
    async fn write_shard(
        &mut self,
        hash: &AnyHash,
        bytes: &[u8],
    ) -> Result<Vec<Location>, ShardError> {
        self.write_subfile(&hash.to_string(), bytes)
            .await
            .map(|location| vec![location])
    }
}

impl FromStr for Location {
    type Err = LocationParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with("http://") || s.starts_with("https://") {
            return Ok(Location::Http(HttpUrl::from_str(s)?));
        }
        if s.starts_with("file://") {
            return Ok(Location::Local(
                Url::parse(s)?
                    .to_file_path()
                    .map_err(|_| LocationParseError::FilePathNotAbsolute)?,
            ));
        }
        Ok(Location::Local(FromStr::from_str(s).unwrap()))
    }
}

impl TryFrom<Url> for Location {
    type Error = LocationParseError;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        match url.scheme() {
            "file" => Ok(Location::Local(
                url.to_file_path()
                    .map_err(|_| LocationParseError::FilePathNotAbsolute)?,
            )),
            "http" => Ok(Location::Http(HttpUrl::try_from(url)?)),
            _ => Err(LocationParseError::InvalidScheme),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "Url")]
#[serde(into = "Url")]
pub struct HttpUrl(Url);

impl FromStr for HttpUrl {
    type Err = LocationParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(Url::from_str(s)?)
    }
}

impl From<HttpUrl> for Url {
    fn from(h: HttpUrl) -> Self {
        h.0
    }
}

impl TryFrom<Url> for HttpUrl {
    type Error = LocationParseError;

    fn try_from(u: Url) -> Result<Self, Self::Error> {
        match u.scheme() {
            "http" | "https" => Ok(Self(u)),
            _ => Err(LocationParseError::NotHttp),
        }
    }
}

macro_rules! impl_try_from_string {
    ($type:ty) => {
        impl TryFrom<$type> for HttpUrl {
            type Error = LocationParseError;

            fn try_from(s: $type) -> Result<Self, Self::Error> {
                FromStr::from_str(AsRef::<str>::as_ref(&s))
            }
        }
        impl TryFrom<$type> for Location {
            type Error = LocationParseError;

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
        impl From<$type> for Location {
            fn from(p: $type) -> Self {
                Location::Local(p.to_owned())
            }
        }
    };
}
impl_from_path!(&Path);
impl_from_path!(PathBuf);

impl From<HttpUrl> for Location {
    fn from(url: HttpUrl) -> Self {
        Location::Http(url)
    }
}
