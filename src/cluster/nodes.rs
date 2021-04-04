use std::{
    cmp::Ordering,
    collections::{
        BTreeSet,
        HashMap,
        HashSet,
    },
    sync::Arc,
};

use serde::{
    Deserialize,
    Serialize,
};
use tokio::sync::Mutex;

use crate::{
    cluster::{
        ClusterProfile,
        ClusterWriter,
        ClusterWriterInnerState,
        ClusterWriterState,
    },
    error::FileWriteError,
    file::{
        CollectionDestination,
        Location,
        WeightedLocation,
    },
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(from = "ClusterNodesDeserializer")]
pub struct ClusterNodes(pub Vec<ClusterNode>);

impl ClusterNodes {
    pub(super) fn with_profile(self, profile: ClusterProfile) -> ClusterNodesWithProfile {
        ClusterNodesWithProfile(Arc::new((self, profile)))
    }
}

impl Into<BTreeSet<ClusterNode>> for ClusterNodes {
    fn into(mut self) -> BTreeSet<ClusterNode> {
        self.0.drain(..).collect()
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ClusterNodesDeserializer {
    Single(ClusterNode),
    Set(Vec<ClusterNodesDeserializer>),
    Map(HashMap<String, ClusterNodesDeserializer>),
}

impl From<ClusterNodesDeserializer> for ClusterNodes {
    fn from(des: ClusterNodesDeserializer) -> ClusterNodes {
        use ClusterNodesDeserializer::*;
        ClusterNodes(match des {
            Single(node) => {
                vec![node]
            },
            Set(mut nodes) => {
                let mut nodes_out = Vec::<ClusterNode>::new();
                for sub_nodes in nodes.drain(..) {
                    let mut nodes: ClusterNodes = sub_nodes.into();
                    nodes_out.append(&mut nodes.0);
                }
                nodes_out
            },
            Map(mut nodes) => {
                let mut nodes_out = Vec::<ClusterNode>::new();
                for (name, sub_nodes) in nodes.drain() {
                    let nodes: ClusterNodes = sub_nodes.into();
                    for sub_node in nodes.0 {
                        let mut sub_node = sub_node.clone();
                        sub_node.zones.insert(name.clone());
                        nodes_out.push(sub_node);
                    }
                }
                nodes_out
            },
        })
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterNode {
    #[serde(flatten)]
    pub location: WeightedLocation,
    #[serde(default)]
    pub zones: BTreeSet<String>,
    #[serde(default)]
    pub repeat: usize,
}

impl Ord for ClusterNode {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.zones == other.zones {
            self.location.cmp(&other.location)
        } else {
            self.zones.cmp(&other.zones)
        }
    }
}

impl PartialOrd for ClusterNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone)]
pub(super) struct ClusterNodesWithProfile(pub Arc<(ClusterNodes, ClusterProfile)>);

impl AsRef<ClusterNodes> for ClusterNodesWithProfile {
    fn as_ref(&self) -> &ClusterNodes {
        &self.0.as_ref().0
    }
}
impl AsRef<ClusterProfile> for ClusterNodesWithProfile {
    fn as_ref(&self) -> &ClusterProfile {
        &self.0.as_ref().1
    }
}

impl CollectionDestination for ClusterNodesWithProfile {
    type Writer = ClusterWriter;

    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, FileWriteError> {
        self.get_used_writers(&vec![None; count])
    }

    fn get_used_writers(
        &self,
        locations: &[Option<&Location>],
    ) -> Result<Vec<Self::Writer>, FileWriteError> {
        let count = locations.iter().filter(|opt| opt.is_none()).count();
        let (nodes, profile) = self.0.as_ref();
        // Does not account for zone rules. ShardWriters will handle that
        let possible_nodes: usize = nodes.0.iter().map(|node| node.repeat + 1).sum();
        if possible_nodes < count {
            return Err(FileWriteError::NotEnoughWriters);
        }
        let mut inner_state = ClusterWriterInnerState {
            available_indexes: nodes
                .0
                .iter()
                .enumerate()
                .map(|(i, node)| (i, node.repeat + 1))
                .collect(),
            failed_indexes: HashSet::new(),
            zone_status: profile.zone_rules.clone(),
            errors: vec![],
        };
        for location in locations.iter() {
            if let Some(location) = location {
                let parent_nodes = nodes
                    .0
                    .iter()
                    .enumerate()
                    .filter(|(_, node)| node.location.location.is_parent_of(location));
                for (index, node) in parent_nodes {
                    inner_state.remove_availability(index, node);
                }
            }
        }
        let writer = ClusterWriter {
            state: Arc::new(ClusterWriterState {
                parent: self.clone(),
                inner_state: Mutex::new(inner_state),
            }),
        };
        Ok((0..count).map(|_| writer.clone()).collect())
    }
}
