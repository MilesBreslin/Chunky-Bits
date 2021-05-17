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
use reqwest::{
    header,
    Body,
};
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
        AsyncSeekExt,
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
#[serde(try_from = "String")]
#[serde(into = "String")]
pub enum Location {
    Http { url: HttpUrl, range: Range },
    Local { path: PathBuf, range: Range },
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
        let LocationContext { profiler, .. } = cx;
        let profiler_info = profiler.as_ref().map(|profiler| (profiler, Instant::now()));

        let result: Result<Vec<u8>, LocationError> = async move {
            let mut out = Vec::new();
            self.reader_with_context(cx)
                .await?
                .read_to_end(&mut out)
                .await?;
            Ok(out)
        }
        .await;

        if let Some((profiler, op_start)) = profiler_info {
            profiler.log_read(&result, self.clone(), op_start);
        }
        result
    }

    pub async fn reader_with_context(
        &self,
        cx: &LocationContext,
    ) -> Result<(impl AsyncRead + Send + Unpin), LocationError> {
        // TODO: Profiler
        use Location::*;
        let result: Result<Pin<Box<dyn AsyncRead + Send + Unpin>>, _> = match self {
            Local { path, range } => {
                let mut file = File::open(&path).await?;
                if range.is_specified() {
                    file.seek(io::SeekFrom::Start(range.start)).await?;
                    if let Some(length) = range.length {
                        if range.extend_zeros {
                            Ok(Box::pin(file.chain(io::repeat(0)).take(length)))
                        } else {
                            Ok(Box::pin(file.take(length)))
                        }
                    } else {
                        Ok(Box::pin(file))
                    }
                } else {
                    Ok(Box::pin(file))
                }
            },
            Http { url, range } => {
                let url: Url = url.clone().into();
                let mut header_map = header::HeaderMap::new();
                if range.is_specified() {
                    let range_s = if let Some(length) = range.length {
                        format!("{}-{}", range.start, length)
                    } else {
                        format!("{}-", range.start)
                    };
                    let header_value = header::HeaderValue::from_str(&range_s).unwrap();
                    header_map.insert(header::RANGE, header_value);
                }
                let s = cx
                    .http_client
                    .get(url)
                    .headers(header_map)
                    .send()
                    .await?
                    .bytes_stream();
                let reader = StreamReader::new(s.map(|res| match res {
                    Ok(bytes) => Ok(Cursor::new(bytes)),
                    Err(err) => Err(io::Error::new(io::ErrorKind::Other, err)),
                }));
                if let Some(length) = range.length {
                    if range.extend_zeros {
                        Ok(Box::pin(reader.chain(io::repeat(0)).take(length)))
                    } else {
                        Ok(Box::pin(reader.take(length)))
                    }
                } else {
                    Ok(Box::pin(reader))
                }
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
        if self.range().is_specified() {
            return Err(LocationError::WriteToRange);
        }

        let LocationContext {
            http_client,
            profiler,
            ..
        } = cx;
        let profiler_info = profiler.as_ref().map(|profiler| (profiler, Instant::now()));
        let length = bytes.as_ref().len();

        match &cx.on_conflict {
            OnConflict::Overwrite => {},
            OnConflict::Ignore => {
                if self.file_exists(cx).await? {
                    let result = Ok(());
                    if let Some((profiler, op_start)) = profiler_info {
                        profiler.log_write(&result, self.clone(), length, op_start);
                    }
                    return result;
                }
            },
        };

        use Location::*;
        let result: Result<(), LocationError> = async move {
            match self {
                Local { path, .. } => {
                    let mut file = File::create(&path).await?;
                    file.write_all(bytes.as_ref()).await?;
                    file.flush().await?;
                    Ok(())
                },
                Http { url, .. } => {
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

        if let Some((profiler, op_start)) = profiler_info {
            profiler.log_write(&result, self.clone(), length, op_start);
        }
        result
    }

    pub async fn write_from_reader_with_context(
        &self,
        cx: &LocationContext,
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<u64, LocationError> {
        if self.range().is_specified() {
            return Err(LocationError::WriteToRange);
        }

        // TODO: Profiler
        match &cx.on_conflict {
            OnConflict::Overwrite => {},
            OnConflict::Ignore => {
                if self.file_exists(cx).await? {
                    return Ok(0);
                }
            },
        };

        use Location::*;
        match self {
            Local { path, .. } => {
                let mut file = File::create(&path).await?;
                let bytes = io::copy(reader, &mut file).await?;
                file.flush().await?;
                Ok(bytes)
            },
            Http { url, .. } => {
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
            Http { url, .. } => {
                let mut target_url: Url = url.clone().into();
                target_url.path_segments_mut().unwrap().push(name);
                Http {
                    url: HttpUrl(target_url),
                    range: Default::default(),
                }
            },
            Local { path, .. } => {
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
            Http { url, .. } => {
                let url: Url = url.clone().into();
                http_client.delete(url).send().await?;
                Ok(())
            },
            Local { path, .. } => {
                fs::remove_file(path).await?;
                Ok(())
            },
        }
    }

    pub async fn file_exists(&self, cx: &LocationContext) -> Result<bool, LocationError> {
        let LocationContext { http_client, .. } = cx;
        use Location::*;
        match self {
            Http { url, .. } => {
                let url: Url = url.clone().into();
                let resp = http_client.head(url).send().await?;
                Ok(resp.status().is_success())
            },
            Local { path, .. } => match fs::metadata(path).await {
                Ok(_) => Ok(true),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(err) => Err(err.into()),
            },
        }
    }

    pub async fn file_len(&self, cx: &LocationContext) -> Result<u64, LocationError> {
        let LocationContext { http_client, .. } = cx;
        use Location::*;
        match self {
            Http { url, .. } => {
                let url: Url = url.clone().into();
                let resp = http_client.head(url).send().await?;
                if resp.status().is_success() {
                    if let Some(length) = resp.headers().get(header::RANGE) {
                        if let Ok(length) = length.to_str() {
                            if let Ok(length) = u64::from_str(length) {
                                return Ok(length);
                            }
                        }
                    }
                }
                todo!();
            },
            Local { path, .. } => match fs::metadata(path).await {
                Ok(meta) => Ok(meta.len()),
                Err(err) => Err(err.into()),
            },
        }
    }

    pub fn default_context() -> LocationContext {
        Default::default()
    }

    pub fn is_child_of(&self, other: &Location) -> bool {
        let (left, right) = (self, other);
        if left.range().is_specified() {
            return false;
        }
        use Location::*;
        match (left, right) {
            (Http { url: left, .. }, Http { url: right, .. }) => {
                let (mut left, right): (Url, Url) = (left.clone().into(), right.clone().into());
                if let Ok(mut left) = left.path_segments_mut() {
                    left.pop();
                } else {
                    return false;
                }
                left == right
            },
            (Local { path: left, .. }, Local { path: right, .. }) => {
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

    pub fn range(&self) -> &Range {
        self.as_ref()
    }

    pub fn range_mut(&mut self) -> &mut Range {
        self.as_mut()
    }
}

#[derive(Clone)]
pub struct LocationContext {
    on_conflict: OnConflict,
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
    on_conflict: Option<OnConflict>,
    http_client: Option<reqwest::Client>,
    profiler: Option<Profiler>,
}

#[derive(Clone)]
enum OnConflict {
    Overwrite,
    Ignore,
}

impl LocationContextBuilder {
    pub fn http_client(mut self, http_client: reqwest::Client) -> Self {
        self.http_client = Some(http_client);
        self
    }

    pub fn profiler(mut self, profiler: Profiler) -> Self {
        self.profiler = Some(profiler);
        self
    }

    pub fn conflict_ignore(mut self) -> Self {
        self.on_conflict = Some(OnConflict::Ignore);
        self
    }

    pub fn conflict_overwrite(mut self) -> Self {
        self.on_conflict = Some(OnConflict::Overwrite);
        self
    }

    pub fn build(self) -> LocationContext {
        LocationContext {
            on_conflict: self.on_conflict.unwrap_or(OnConflict::Overwrite),
            http_client: self.http_client.unwrap_or_else(reqwest::Client::new),
            profiler: self.profiler,
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Location::*;
        match self {
            Http { url, range } if range.is_specified() => write!(f, "{}{}", range, url),
            Http { url, .. } => write!(f, "{}", url),
            Local { path, range } if range.is_specified() => {
                write!(f, "{}{}", range, path.display())
            },
            Local { path, .. } => write!(f, "{}", path.display()),
        }
    }
}

impl From<Location> for String {
    fn from(loc: Location) -> String {
        loc.to_string()
    }
}

impl AsRef<Range> for Location {
    fn as_ref(&self) -> &Range {
        use Location::*;
        match self {
            Http { range, .. } | Local { range, .. } => range,
        }
    }
}

impl AsMut<Range> for Location {
    fn as_mut(&mut self) -> &mut Range {
        use Location::*;
        match self {
            Http { range, .. } | Local { range, .. } => range,
        }
    }
}

#[derive(Default, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Range {
    #[serde(default)]
    pub start: u64,
    pub length: Option<u64>,
    pub extend_zeros: bool,
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Range{start, length: Some(length), extend_zeros: false}
                => write!(f, "%{}%{}%", start, length),
            Range{start, length: Some(length), extend_zeros: true}
                => write!(f, "%{}%0{}%", start, length),
            Range{start, ..} => write!(f, "%{}%%", start),
        }
    }
}

impl Range {
    pub fn is_specified(&self) -> bool {
        self.start != 0 || self.length.is_some()
    }

    fn from_str_prefix<'a>(orig: &'a str) -> (Self, &'a str) {
        let mut split = orig.splitn(4, '%');
        if let Some("") = split.next() {
        } else {
            return (Default::default(), orig);
        }
        match (split.next(), split.next(), split.next()) {
            (Some(start), Some(len), Some(suffix)) => {
                let extend_zeros = len.starts_with('0');
                let start = match start {
                    "" => Ok(0),
                    start => u64::from_str(start),
                };
                let len = match len {
                    "" => Ok(None),
                    len => Some(u64::from_str(len)).transpose(),
                };
                if let (Ok(start), Ok(length)) = (start, len) {
                    let range = Range { start, length, extend_zeros };
                    return (range, suffix);
                }
                return (Default::default(), orig);
            },
            _ => {
                return (Default::default(), orig);
            },
        }
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
        let (range, s) = Range::from_str_prefix(s);
        if s.starts_with("http://") || s.starts_with("https://") {
            return Ok(Location::Http {
                url: HttpUrl::from_str(s)?,
                range,
            });
        }
        if s.starts_with("file://") {
            return Ok(Location::Local {
                path: Url::parse(s)?
                    .to_file_path()
                    .map_err(|_| LocationParseError::FilePathNotAbsolute)?,
                range,
            });
        }
        Ok(Location::Local {
            path: FromStr::from_str(s)?,
            range,
        })
    }
}

impl TryFrom<Url> for Location {
    type Error = LocationParseError;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        match url.scheme() {
            "file" => Ok(Location::Local {
                path: url
                    .to_file_path()
                    .map_err(|_| LocationParseError::FilePathNotAbsolute)?,
                range: Default::default(),
            }),
            "http" => Ok(Location::Http {
                url: HttpUrl::try_from(url)?,
                range: Default::default(),
            }),
            _ => Err(LocationParseError::InvalidScheme),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "Url")]
#[serde(into = "Url")]
pub struct HttpUrl(Url);

impl fmt::Display for HttpUrl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        <Url as fmt::Display>::fmt(&self.0, f)
    }
}

impl AsRef<Url> for HttpUrl {
    fn as_ref(&self) -> &Url {
        &self.0
    }
}

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
                Location::Local {
                    path: p.to_owned(),
                    range: Default::default(),
                }
            }
        }
    };
}
impl_from_path!(&Path);
impl_from_path!(PathBuf);

impl From<HttpUrl> for Location {
    fn from(url: HttpUrl) -> Self {
        Location::Http {
            url,
            range: Default::default(),
        }
    }
}
