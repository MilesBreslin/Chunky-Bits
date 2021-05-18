use std::sync::Arc;

use anyhow::Result;
use chunky_bits::{
    cluster,
    cluster::sized_int::{
        ChunkSize,
        DataChunkCount,
        ParityChunkCount,
    },
    error::FileWriteError,
    file::{
        CollectionDestination,
        Location,
        LocationContext,
        ShardWriter,
        WeightedLocation,
    },
};
use serde::{
    Deserialize,
    Serialize,
};

use crate::{
    config::Config,
    error_message::ErrorMessage,
};

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "type")]
pub enum AnyDestinationRef {
    Cluster {
        cluster: String,
        profile: Option<String>,
    },
    Locations {
        #[serde(default)]
        data: DataChunkCount,
        #[serde(default)]
        parity: ParityChunkCount,
        #[serde(default)]
        chunk_size: ChunkSize,
        #[serde(default)]
        locations: Vec<WeightedLocation>,
    },
    Void {
        #[serde(default)]
        data: DataChunkCount,
        #[serde(default)]
        parity: ParityChunkCount,
        #[serde(default)]
        chunk_size: ChunkSize,
    },
}

impl AnyDestinationRef {
    pub fn is_void(&self) -> bool {
        matches!(self, AnyDestinationRef::Void { .. })
    }

    pub async fn get_destination(&self, config: &Config) -> Result<AnyDestination> {
        Ok(match self {
            AnyDestinationRef::Cluster {
                cluster: cluster_name,
                profile: profile_name,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                let profile_name = match profile_name {
                    Some(profile_name) => Some(profile_name.clone()),
                    None => config.get_profile(&cluster_name).await,
                };
                let profile = cluster
                    .get_profile(profile_name.as_deref())
                    .ok_or_else(|| {
                        ErrorMessage::from(format!("Profile not found: {}", profile_name.unwrap()))
                    })?;
                let destination = cluster.get_destination(&profile);
                AnyDestination::Cluster(destination)
            },
            AnyDestinationRef::Locations { locations, .. } => {
                AnyDestination::Locations(locations.clone().into())
            },
            AnyDestinationRef::Void { .. } => AnyDestination::Void,
        })
    }
}

impl Default for AnyDestinationRef {
    fn default() -> Self {
        AnyDestinationRef::Void {
            data: Default::default(),
            parity: Default::default(),
            chunk_size: Default::default(),
        }
    }
}

#[derive(Clone)]
pub enum AnyDestination {
    Cluster(cluster::Destination),
    Locations(Arc<Vec<WeightedLocation>>),
    Void,
}

impl AnyDestination {
    pub fn is_void(&self) -> bool {
        matches!(self, AnyDestination::Void)
    }

    fn dynamic_writer(
        writer: (impl ShardWriter + Send + Sync + 'static),
    ) -> <Self as CollectionDestination>::Writer {
        Box::new(writer)
    }

    fn dynamic_writers(
        writers: Vec<(impl ShardWriter + Send + Sync + 'static)>,
    ) -> Vec<<Self as CollectionDestination>::Writer> {
        writers.into_iter().map(Self::dynamic_writer).collect()
    }
}

impl CollectionDestination for AnyDestination {
    type Writer = Box<dyn ShardWriter + Send + Sync>;

    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, FileWriteError> {
        use AnyDestination::*;
        match self {
            Cluster(c) => c.get_writers(count).map(Self::dynamic_writers),
            Locations(l) => l.get_writers(count).map(Self::dynamic_writers),
            Void => ().get_writers(count).map(Self::dynamic_writers),
        }
    }

    fn get_used_writers(
        &self,
        locations: &[Option<&Location>],
    ) -> Result<Vec<Self::Writer>, FileWriteError> {
        use AnyDestination::*;
        match self {
            Cluster(c) => c.get_used_writers(locations).map(Self::dynamic_writers),
            Locations(l) => l.get_used_writers(locations).map(Self::dynamic_writers),
            Void => ().get_used_writers(locations).map(Self::dynamic_writers),
        }
    }

    fn get_context(&self) -> LocationContext {
        use AnyDestination::*;
        match self {
            Cluster(c) => c.get_context(),
            Locations(l) => l.get_context(),
            Void => ().get_context(),
        }
    }
}
