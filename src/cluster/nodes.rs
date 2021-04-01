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
    cluster::*,
    file::{
        error::*,
        CollectionDestination,
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
        let writer = ClusterWriter {
            state: Arc::new(ClusterWriterState {
                parent: self.clone(),
                inner_state: Mutex::new(ClusterWriterInnerState {
                    available_indexes: <Self as AsRef<ClusterNodes>>::as_ref(self)
                        .0
                        .iter()
                        .enumerate()
                        .map(|(i, node)| (i, node.repeat + 1))
                        .collect(),
                    failed_indexes: HashSet::new(),
                    zone_status: <Self as AsRef<ClusterProfile>>::as_ref(self)
                        .zone_rules
                        .clone(),
                    errors: vec![],
                }),
            }),
        };
        Ok((0..count).map(|_| writer.clone()).collect())
    }
}
