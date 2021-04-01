use std::{
    collections::BTreeMap,
    mem::swap,
};

use serde::{
    Deserialize,
    Serialize,
};

use crate::cluster::{
    ChunkSize,
    DataChunkCount,
    ParityChunkCount,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct ClusterProfiles {
    default: ClusterProfile,
    #[serde(flatten)]
    custom: BTreeMap<String, ClusterProfile>,
}

impl ClusterProfiles {
    pub fn get<'a, T>(&self, profile: T) -> Option<&'_ ClusterProfile>
    where
        T: Into<Option<&'a str>>,
    {
        let profile = profile.into();
        match profile {
            Some("default") | None => Some(&self.default),
            Some(profile) => self.custom.get(profile),
        }
    }

    pub fn insert<T>(&mut self, name: T, profile: ClusterProfile) -> Option<ClusterProfile>
    where
        T: Into<Option<String>>,
    {
        let name = name.into();
        match name.as_ref().map(|s| s.as_str()) {
            Some("default") | None => {
                let mut profile = profile;
                swap(&mut self.default, &mut profile);
                Some(profile)
            },
            Some(_name) => {
                let name = name.unwrap();
                self.custom.insert(name, profile)
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ClusterProfile {
    #[serde(default)]
    pub chunk_size: ChunkSize,
    #[serde(alias = "data")]
    pub data_chunks: DataChunkCount,
    #[serde(alias = "parity")]
    pub parity_chunks: ParityChunkCount,
    #[serde(default)]
    #[serde(alias = "zone")]
    #[serde(alias = "zones")]
    #[serde(alias = "rules")]
    pub zone_rules: ZoneRules,
}

impl ClusterProfile {
    pub fn get_chunk_size(&self) -> usize {
        self.chunk_size.clone().into()
    }

    pub fn get_data_chunks(&self) -> usize {
        self.data_chunks.clone().into()
    }

    pub fn get_parity_chunks(&self) -> usize {
        self.parity_chunks.clone().into()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ZoneRules(pub BTreeMap<String, ZoneRule>);
impl AsRef<BTreeMap<String, ZoneRule>> for ZoneRules {
    fn as_ref(&self) -> &BTreeMap<String, ZoneRule> {
        &self.0
    }
}
impl AsMut<BTreeMap<String, ZoneRule>> for ZoneRules {
    fn as_mut(&mut self) -> &mut BTreeMap<String, ZoneRule> {
        &mut self.0
    }
}
impl Default for ZoneRules {
    fn default() -> Self {
        ZoneRules(BTreeMap::new())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ZoneRule {
    #[serde(default)]
    pub minimum: i8,
    pub maximum: Option<i8>,
    #[serde(default)]
    pub ideal: i8,
}
