use std::{
    convert::TryFrom,
    fmt,
    hash::Hash,
    path::{
        Path,
        PathBuf,
    },
    str::FromStr,
};

use async_trait::async_trait;
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    fs::{
        self,
        File,
    },
    io::AsyncWriteExt,
};
use url::Url;

use crate::file::{
    error::{
        LocationError,
        LocationParseError,
        ShardError,
    },
    ShardWriter,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Location {
    Http(HttpUrl),
    Local(PathBuf),
}

impl Location {
    pub async fn read(&self) -> Result<Vec<u8>, LocationError> {
        use Location::*;
        match self {
            Local(path) => match fs::read(&path).await {
                Ok(bytes) => Ok(bytes),
                Err(err) => Err(err.into()),
            },
            Http(url) => match reqwest::get(Into::<Url>::into(url.clone())).await {
                Ok(resp) => Ok(resp.bytes().await?.into_iter().collect()),
                Err(err) => Err(err.into()),
            },
        }
    }

    pub async fn write<T>(&self, bytes: T) -> Result<(), LocationError>
    where
        T: AsRef<[u8]> + Into<Vec<u8>>,
    {
        use Location::*;
        match self {
            Local(path) => {
                File::create(&path).await?.write_all(bytes.as_ref()).await?;
                Ok(())
            },
            Http(url) => {
                let response = reqwest::Client::new()
                    .put(Into::<Url>::into(url.clone()))
                    .body(bytes.into())
                    .send()
                    .await;
                response?;
                Ok(())
            },
        }
    }

    pub async fn write_subfile<T>(&self, name: &str, bytes: T) -> Result<Location, ShardError>
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
        match target_location.write(bytes).await {
            Ok(_) => Ok(target_location),
            Err(err) => Err(ShardError {
                location: target_location,
                error: err.into(),
            }),
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
    async fn write_shard(&mut self, hash: &str, bytes: &[u8]) -> Result<Vec<Location>, ShardError> {
        self.write_subfile(hash, bytes)
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HashWithLocation<T: Serialize + Clone + PartialEq + Eq + Hash + PartialOrd + Ord> {
    pub sha256: T,
    pub locations: Vec<Location>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "Url")]
#[serde(into = "Url")]
pub struct HttpUrl(Url);

impl FromStr for HttpUrl {
    type Err = LocationParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::try_from(Url::from_str(s)?)?)
    }
}

impl Into<Url> for HttpUrl {
    fn into(self) -> Url {
        self.0
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
