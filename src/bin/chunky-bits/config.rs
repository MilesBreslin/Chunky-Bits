use std::{
    collections::BTreeMap,
    error::Error,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use std::cell::RefCell;
use std::rc::Rc;
use std::ops::Deref;

use chunky_bits::{
    cluster::Cluster,
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
    cluster_location::ClusterLocation,
    error_message::ErrorMessage,
};

#[derive(Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    clusters: BTreeMap<String, LocalCluster>,
    #[serde(default)]
    default_profile: Option<String>,

    #[serde(skip)]
    cluster_cache: RwLock<BTreeMap<String, Arc<Cluster>>>,
}

#[derive(Serialize,Deserialize)]
struct LocalCluster {
    #[serde(flatten)]
    cluster: ClusterType,
    default_profile: Option<String>,
}

#[derive(Serialize,Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ClusterType {
    Inline(Arc<Cluster>),
    Location(Location),
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        Default::default()
    }

    pub async fn get_cluster(&self, target: &str) -> Result<Arc<Cluster>, Box<dyn Error>> {
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
                    let cluster = Arc::new(cluster);
                    cluster
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
}

#[derive(Default)]
pub struct ConfigBuilder {
    path: Option<PathBuf>,
}

impl ConfigBuilder {
    pub fn path(self, path: impl Into<Option<PathBuf>>) -> Self {
        let mut new = self;
        new.path = path.into();
        new
    }

    pub async fn load(self) -> Result<Config, Box<dyn Error>> {
        let mut reader = match self.path {
            Some(path) => fs::File::open(path).await?,
            None => fs::File::open("/etc/chunky-bits.yaml").await?,
        };
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await?;
        Ok(serde_yaml::from_slice(&bytes)?)
    }

    pub async fn load_or_default(self) -> Result<Config, Box<dyn Error>> {
        if let Some(_) = &self.path {
            self.load().await
        } else {
            match self.load().await {
                Ok(config) => Ok(config),
                Err(_) => Ok(Default::default()),
            }
        }
    }
}