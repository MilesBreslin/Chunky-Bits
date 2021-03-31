use std::{
    collections::HashMap,
    fmt,
    hash::Hash,
    num::NonZeroUsize,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use async_trait::async_trait;
use futures::{
    future::FutureExt,
    stream::StreamExt,
};
use serde::{
    Deserialize,
    Serialize,
};
use sha2::Digest;
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
    sync::{
        mpsc,
        Mutex,
    },
    task::JoinHandle,
};
use url::Url;

use crate::{
    file::*,
    Error,
};

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
