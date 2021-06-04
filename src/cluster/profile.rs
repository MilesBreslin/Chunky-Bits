use std::{
    collections::{
        BTreeMap,
        HashMap,
    },
    mem::swap,
};

use serde::{
    Deserialize,
    Serialize,
};

use crate::cluster::sized_int::{
    ChunkSize,
    DataChunkCount,
    ParityChunkCount,
};

#[derive(Clone, Serialize)]
pub struct ClusterProfiles {
    default: ClusterProfile,
    #[serde(flatten)]
    custom: BTreeMap<String, ClusterProfile>,
}

impl<'de> Deserialize<'de> for ClusterProfiles {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_map(hollow::HollowClusterProfilesVisitor)
    }
}

impl ClusterProfiles {
    pub fn get_default(&self) -> &ClusterProfile {
        &self.default
    }

    pub fn get<'a>(&self, profile: impl Into<Option<&'a str>>) -> Option<&'_ ClusterProfile> {
        let profile = profile.into();
        match Self::filter_profile_name(profile) {
            None => Some(&self.default),
            Some(profile) => self.custom.get(profile),
        }
    }

    pub fn insert(
        &mut self,
        name: impl Into<Option<String>>,
        profile: ClusterProfile,
    ) -> Option<ClusterProfile> {
        let name = name.into();
        match Self::filter_profile_name(name) {
            None => {
                let mut profile = profile;
                swap(&mut self.default, &mut profile);
                Some(profile)
            },
            Some(name) => self.custom.insert(name, profile),
        }
    }

    fn filter_profile_name<T>(name: Option<T>) -> Option<T>
    where
        T: AsRef<str>,
    {
        match name {
            None => None,
            Some(name) if name.as_ref().eq_ignore_ascii_case("default") => None,
            Some(name) => Some(name),
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
        self.chunk_size.into()
    }

    pub fn get_data_chunks(&self) -> usize {
        self.data_chunks.into()
    }

    pub fn get_parity_chunks(&self) -> usize {
        self.parity_chunks.into()
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

mod hollow {
    use std::fmt::{
        self,
        Formatter,
    };

    use serde::de::{
        Error,
        MapAccess,
        Visitor,
    };

    use super::*;

    pub(super) struct HollowClusterProfilesVisitor;

    impl<'de> Visitor<'de> for HollowClusterProfilesVisitor {
        type Value = ClusterProfiles;

        fn expecting(&self, f: &mut Formatter) -> fmt::Result {
            write!(f, "Needs default")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut default: Option<ClusterProfile> = None;
            let mut custom: HashMap<String, HollowClusterProfile> = HashMap::new();
            while let Some(key) = map.next_key::<String>()? {
                if key.eq_ignore_ascii_case("default") {
                    if default.is_none() {
                        default = Some(map.next_value()?);
                    } else {
                        return Err(A::Error::duplicate_field("default"));
                    }
                } else {
                    use std::collections::hash_map::Entry;
                    match custom.entry(key) {
                        Entry::Vacant(entry) => {
                            entry.insert(map.next_value()?);
                        },
                        Entry::Occupied(entry) => {
                            return Err(A::Error::custom(format!(
                                "duplicate field `{}`",
                                entry.key(),
                            )));
                        },
                    }
                }
            }

            let default = match default {
                Some(default) => default,
                None => {
                    return Err(A::Error::missing_field("default"));
                },
            };

            let custom = custom
                .into_iter()
                .map(|(name, hollow_profile)| {
                    let default = default.clone();
                    let profile = hollow_profile.merge_with_default(default);
                    (name, profile)
                })
                .collect();

            Ok(ClusterProfiles { default, custom })
        }
    }

    #[derive(Deserialize)]
    #[serde(rename = "ClusterProfile")]
    struct HollowClusterProfile {
        pub chunk_size: Option<ChunkSize>,
        #[serde(alias = "data")]
        pub data_chunks: Option<DataChunkCount>,
        #[serde(alias = "parity")]
        pub parity_chunks: Option<ParityChunkCount>,
        #[serde(default)]
        #[serde(alias = "zone")]
        #[serde(alias = "zones")]
        #[serde(alias = "rules")]
        pub zone_rules: HashMap<String, Option<ZoneRule>>,
    }

    impl HollowClusterProfile {
        fn merge_with_default(self, mut def: ClusterProfile) -> ClusterProfile {
            let HollowClusterProfile {
                chunk_size,
                data_chunks,
                parity_chunks,
                zone_rules,
            } = self;
            if let Some(chunk_size) = chunk_size {
                def.chunk_size = chunk_size;
            }
            if let Some(data_chunks) = data_chunks {
                def.data_chunks = data_chunks;
            }
            if let Some(parity_chunks) = parity_chunks {
                def.parity_chunks = parity_chunks;
            }
            for (zone, rule) in zone_rules {
                match rule {
                    Some(rule) => {
                        def.zone_rules.0.insert(zone, rule);
                    },
                    None => {
                        def.zone_rules.0.remove(&zone);
                    },
                }
            }
            def
        }
    }
}
