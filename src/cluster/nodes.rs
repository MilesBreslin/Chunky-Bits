use std::{
    cmp::Ordering,
    collections::{
        BTreeSet,
        BTreeMap,
    },
};

use serde::{
    Deserialize,
    Serialize,
};

use crate::file::WeightedLocation;

#[derive(Clone, Serialize, Deserialize)]
#[serde(from = "ClusterNodesDeserializer")]
pub struct ClusterNodes(pub Vec<ClusterNode>);

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
    Map(BTreeMap<String, ClusterNodesDeserializer>),
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
            Map(nodes) => {
                let mut nodes_out = Vec::<ClusterNode>::new();
                for (name, sub_nodes) in nodes.into_iter() {
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
