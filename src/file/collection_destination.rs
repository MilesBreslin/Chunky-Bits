use async_trait::async_trait;
use rand::{self,};
use serde::{
    Deserialize,
    Serialize,
};

use crate::file::{
    error::{
        FileWriteError,
        ShardError,
    },
    Location,
    WeightedLocation,
};

pub trait CollectionDestination {
    type Writer: ShardWriter + Send + Sync;
    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, FileWriteError>;
}

impl CollectionDestination for Vec<WeightedLocation> {
    type Writer = Location;

    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, FileWriteError> {
        use rand::seq::SliceRandom;
        if self.len() < count {
            return Err(FileWriteError::NotEnoughWriters);
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
    async fn write_shard(&mut self, hash: &str, bytes: &[u8]) -> Result<Vec<Location>, ShardError>;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Compression {}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Encryption {}
