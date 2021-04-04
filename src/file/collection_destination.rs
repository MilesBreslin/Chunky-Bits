use async_trait::async_trait;
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    error::{
        FileWriteError,
        ShardError,
    },
    file::{
        Location,
        WeightedLocation,
    },
};

pub trait CollectionDestination {
    type Writer: ShardWriter + Send + Sync;
    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, FileWriteError>;
    fn get_used_writers(
        &self,
        locations: &[Option<&Location>],
    ) -> Result<Vec<Self::Writer>, FileWriteError> {
        let writers_needed = locations.iter().filter_map(|loc| *loc).count();
        self.get_writers(writers_needed)
    }
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

/// Does not write anything. Just send the data to the void
#[derive(Clone)]
pub struct VoidDestination;

impl CollectionDestination for VoidDestination {
    type Writer = VoidDestination;

    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, FileWriteError> {
        Ok(vec![VoidDestination; count])
    }
}

#[async_trait]
impl ShardWriter for VoidDestination {
    async fn write_shard(
        &mut self,
        _hash: &str,
        _bytes: &[u8],
    ) -> Result<Vec<Location>, ShardError> {
        Ok(vec![])
    }
}
