use std::{
    fmt,
    hash::Hash,
    path::PathBuf,
    str::FromStr,
};

use async_trait::async_trait;
use futures::{
    future::FutureExt,
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
    io::AsyncWriteExt,
};
use url::Url;

use crate::file::{
    error::*,
    *,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Location {
    Http(HttpUrl),
    Local(PathBuf),
}

impl Location {
    pub(crate) async fn read(&self) -> Result<Vec<u8>, LocationError> {
        match self {
            Location::Local(path) => match fs::read(&path).await {
                Ok(bytes) => Ok(bytes),
                Err(err) => Err(err.into()),
            },
            Location::Http(url) => match reqwest::get(Into::<Url>::into(url.clone())).await {
                Ok(resp) => Ok(resp.bytes().await?.into_iter().collect()),
                Err(err) => Err(err.into()),
            },
        }
    }

    pub(crate) async fn write_subfile(
        &self,
        name: &str,
        bytes: &[u8],
    ) -> Result<Location, ShardError> {
        use Location::*;
        match self {
            Http(url) => {
                let mut target_url: Url = url.clone().into();
                target_url.path_segments_mut().unwrap().push(name);
                let result = reqwest::Client::new()
                    .put(target_url.clone())
                    .body(bytes.to_owned())
                    .send()
                    .await;
                let location = Http(HttpUrl(target_url));
                match result {
                    Ok(_) => Ok(location),
                    Err(err) => Err(ShardError {
                        location: location,
                        error: err.into(),
                    }),
                }
            },
            Local(path) => {
                let mut target_path = path.clone();
                target_path.push(name);
                let result = File::create(&target_path)
                    .then(|res| async move { res?.write_all(bytes).await })
                    .await;
                let location = Local(target_path);
                match result {
                    Ok(_) => Ok(location),
                    Err(err) => Err(ShardError {
                        location: location,
                        error: err.into(),
                    }),
                }
            },
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
    type Err = HttpUrlError;

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
#[serde(try_from = "Url")]
#[serde(into = "Url")]
pub struct HttpUrl(Url);

impl FromStr for HttpUrl {
    type Err = HttpUrlError;

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
    type Error = HttpUrlError;

    fn try_from(u: Url) -> Result<Self, Self::Error> {
        match u.scheme() {
            "http" | "https" => Ok(Self(u)),
            _ => Err(HttpUrlError::NotHttp),
        }
    }
}
