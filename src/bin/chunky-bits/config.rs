use std::{
    collections::BTreeMap,
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
};

use anyhow::Result;
use chunky_bits::{
    cluster::{
        sized_int::{
            ChunkSize,
            DataChunkCount,
            ParityChunkCount,
        },
        Cluster,
        ClusterProfile,
    },
    file::Location,
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    fs,
    io::AsyncReadExt,
    sync::RwLock,
};

use crate::{
    any_destination::{
        AnyDestination,
        AnyDestinationRef,
    },
    error_message::ErrorMessage,
};

#[derive(Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    clusters: BTreeMap<String, LocalCluster>,
    #[serde(default)]
    default_destination: AnyDestinationRef,
    #[serde(default)]
    default_profile: Option<String>,

    #[serde(skip)]
    cluster_cache: RwLock<BTreeMap<String, Arc<Cluster>>>,
}

#[derive(Serialize, Deserialize)]
struct LocalCluster {
    #[serde(flatten)]
    cluster: ClusterType,
    default_profile: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ClusterType {
    Inline(Arc<Cluster>),
    Location(Location),
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        Default::default()
    }

    pub async fn get_cluster(&self, target: &str) -> Result<Arc<Cluster>> {
        if let Some(cluster) = self.cluster_cache.read().await.get(target) {
            return Ok(cluster.clone());
        }

        let is_valid_localname = !target.contains(|c| match c {
            '_' | '-' => false,
            _ if c.is_ascii_alphanumeric() => false,
            _ => true,
        });

        let cluster: Arc<Cluster>;
        if is_valid_localname {
            let cluster_info = self.clusters.get(target).ok_or_else(|| {
                ErrorMessage::from(format!("Cluster not defined in configuration: {}", target))
            })?;
            cluster = match &cluster_info.cluster {
                ClusterType::Inline(cluster) => cluster.clone(),
                ClusterType::Location(loc) => {
                    let cluster = Cluster::from_location(loc.clone())
                        .await
                        .map_err(ErrorMessage::with_prefix(target))?;
                    Arc::new(cluster)
                },
            };
        } else {
            cluster = Cluster::from_location(target).await?.into()
        }
        self.cluster_cache
            .write()
            .await
            .insert(target.to_string(), cluster.clone());
        Ok(cluster)
    }

    pub async fn get_profile(&self, target: &str) -> Option<String> {
        if let Some(cluster_info) = self.clusters.get(target) {
            if let Some(profile) = &cluster_info.default_profile {
                return Some(profile.clone());
            }
        }
        self.default_profile.clone()
    }

    pub async fn get_default_destination(&self) -> Result<AnyDestination> {
        let destination = self.default_destination.get_destination(self).await?;
        if destination.is_void() {
            eprintln!("Warning: Using void destination");
        }
        Ok(destination)
    }

    pub async fn get_cluster_profile(
        &self,
        target: &str,
        profile_name: &Option<String>,
    ) -> Result<ClusterProfile> {
        let cluster = self.get_cluster(&target).await?;
        let profile_name: Option<String> = match profile_name {
            Some(profile_name) => Some(profile_name.clone()),
            None => self.get_profile(&target).await,
        };
        let profile = cluster.get_profile(profile_name.as_deref());
        Ok(profile
            .unwrap_or_else(|| cluster.get_profile(None).unwrap())
            .clone())
    }

    pub async fn get_default_chunk_size(&self) -> Result<ChunkSize> {
        match &self.default_destination {
            AnyDestinationRef::Cluster {
                cluster: cluster_name,
                profile: profile_name,
            } => {
                let profile = self.get_cluster_profile(&cluster_name, profile_name).await;
                Ok(profile?.chunk_size)
            },
            AnyDestinationRef::Locations { chunk_size, .. }
            | AnyDestinationRef::Void { chunk_size, .. } => Ok(*chunk_size),
        }
    }

    pub async fn get_default_data_chunks(&self) -> Result<DataChunkCount> {
        match &self.default_destination {
            AnyDestinationRef::Cluster {
                cluster: cluster_name,
                profile: profile_name,
            } => {
                let profile = self.get_cluster_profile(&cluster_name, profile_name).await;
                Ok(profile?.data_chunks)
            },
            AnyDestinationRef::Locations { data, .. } | AnyDestinationRef::Void { data, .. } => {
                Ok(*data)
            },
        }
    }

    pub async fn get_default_parity_chunks(&self) -> Result<ParityChunkCount> {
        match &self.default_destination {
            AnyDestinationRef::Cluster {
                cluster: cluster_name,
                profile: profile_name,
            } => {
                let profile = self.get_cluster_profile(&cluster_name, profile_name).await;
                Ok(profile?.parity_chunks)
            },
            AnyDestinationRef::Locations { parity, .. }
            | AnyDestinationRef::Void { parity, .. } => Ok(*parity),
        }
    }
}

#[derive(Default)]
pub struct ConfigBuilder {
    path: Option<PathBuf>,
    defaults: DefaultOverlay,
}

impl ConfigBuilder {
    pub fn path(self, path: impl Into<Option<PathBuf>>) -> Self {
        let mut new = self;
        new.path = path.into();
        new
    }

    pub fn default_data_chunks(self, data_chunks: Option<DataChunkCount>) -> Self {
        let mut new = self;
        new.defaults.data_chunks = data_chunks;
        new
    }

    pub fn default_parity_chunks(self, parity_chunks: Option<ParityChunkCount>) -> Self {
        let mut new = self;
        new.defaults.parity_chunks = parity_chunks;
        new
    }

    pub fn default_chunk_size(self, chunk_size: Option<ChunkSize>) -> Self {
        let mut new = self;
        new.defaults.chunk_size = chunk_size;
        new
    }

    async fn load(path: Option<impl AsRef<Path>>) -> Result<Config> {
        let mut reader = match path {
            Some(path) => fs::File::open(path).await?,
            None => fs::File::open("/etc/chunky-bits.yaml").await?,
        };
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await?;
        Ok(serde_yaml::from_slice(&bytes)?)
    }

    pub async fn load_or_default(self) -> Result<Config> {
        let ConfigBuilder { defaults, path } = self;
        let config = if path.is_some() {
            Self::load(path).await
        } else {
            match Self::load(path).await {
                Ok(config) => Ok(config),
                Err(_) => Ok(Default::default()),
            }
        };
        let mut config = match config {
            Ok(config) => config,
            Err(err) => {
                return Err(err);
            },
        };
        defaults.apply_to_config(&mut config);
        Ok(config)
    }
}

#[derive(Default)]
struct DefaultOverlay {
    chunk_size: Option<ChunkSize>,
    data_chunks: Option<DataChunkCount>,
    parity_chunks: Option<ParityChunkCount>,
}

impl DefaultOverlay {
    fn apply_to_config(self, config: &mut Config) {
        let Config {
            ref mut default_destination,
            ..
        } = config;
        match default_destination {
            AnyDestinationRef::Void {
                ref mut chunk_size,
                ref mut data,
                ref mut parity,
            }
            | AnyDestinationRef::Locations {
                ref mut chunk_size,
                ref mut data,
                ref mut parity,
                ..
            } => {
                if let Some(c) = &self.chunk_size {
                    *chunk_size = *c;
                }
                if let Some(d) = &self.data_chunks {
                    *data = *d;
                }
                if let Some(p) = &self.parity_chunks {
                    *parity = *p;
                }
            },
            _ => {},
        }
    }
}
