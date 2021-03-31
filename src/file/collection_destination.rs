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
    stream::{
        FuturesOrdered,
        FuturesUnordered,
        StreamExt,
    },
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
    sync::{
        mpsc,
        Mutex,
    },
    task::JoinHandle,
};
use url::Url;

use crate::{
    Error,
    *,
    file::*,
};

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

#[async_trait]
pub trait ShardWriter {
    async fn write_shard(&mut self, hash: &str, bytes: &[u8]) -> Result<Vec<Location>, Error>;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Compression {}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Encryption {}