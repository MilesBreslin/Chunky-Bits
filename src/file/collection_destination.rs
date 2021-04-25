use std::sync::Arc;

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
        hash::AnyHash,
        Location,
        LocationContext,
        WeightedLocation,
    },
};

pub trait CollectionDestination {
    type Writer: ShardWriter + Send + Sync + 'static;
    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, FileWriteError>;
    fn get_used_writers(
        &self,
        locations: &[Option<&Location>],
    ) -> Result<Vec<Self::Writer>, FileWriteError> {
        let writers_needed = locations.iter().filter_map(|loc| *loc).count();
        self.get_writers(writers_needed)
    }
    fn get_context(&self) -> LocationContext {
        Default::default()
    }
}

impl<T: CollectionDestination> CollectionDestination for Arc<T> {
    type Writer = <T as CollectionDestination>::Writer;

    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, FileWriteError> {
        <T as CollectionDestination>::get_writers(&self, count)
    }

    fn get_used_writers(
        &self,
        locations: &[Option<&Location>],
    ) -> Result<Vec<Self::Writer>, FileWriteError> {
        <T as CollectionDestination>::get_used_writers(&self, locations)
    }

    fn get_context(&self) -> LocationContext {
        <T as CollectionDestination>::get_context(&self)
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
    async fn write_shard(
        &mut self,
        hash: &AnyHash,
        bytes: &[u8],
    ) -> Result<Vec<Location>, ShardError>;
}

#[async_trait]
impl ShardWriter for Box<dyn ShardWriter + Send + Sync> {
    async fn write_shard(
        &mut self,
        hash: &AnyHash,
        bytes: &[u8],
    ) -> Result<Vec<Location>, ShardError> {
        self.as_mut().write_shard(hash, bytes).await
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Compression {}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Encryption {}

/// Does not write anything. Just send the data to the void
pub type VoidDestination = ();

impl CollectionDestination for VoidDestination {
    type Writer = ();

    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, FileWriteError> {
        Ok(vec![(); count])
    }
}

#[async_trait]
impl ShardWriter for () {
    async fn write_shard(
        &mut self,
        _hash: &AnyHash,
        _bytes: &[u8],
    ) -> Result<Vec<Location>, ShardError> {
        Ok(vec![])
    }
}
