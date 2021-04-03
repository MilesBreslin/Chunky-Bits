use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::Arc,
};

use async_trait::async_trait;
use rand::{
    self,
    Rng,
};
use tokio::sync::Mutex;

use crate::{
    cluster::{
        ClusterNode,
        ClusterNodesWithProfile,
        ZoneRule,
        ZoneRules,
    },
    error::ShardError,
    file::{
        Location,
        ShardWriter,
    },
};

pub(super) struct ClusterWriterState {
    pub parent: ClusterNodesWithProfile,
    pub inner_state: Mutex<ClusterWriterInnerState>,
}
pub(super) struct ClusterWriterInnerState {
    pub available_indexes: HashMap<usize, usize>,
    pub failed_indexes: HashSet<usize>,
    pub zone_status: ZoneRules,
    pub errors: Vec<ShardError>,
}

impl ClusterWriterState {
    async fn next_writer(&self) -> Result<(usize, &ClusterNode), ShardError> {
        let (nodes, _profile) = self.parent.0.as_ref();
        let mut state = self.inner_state.lock().await;
        let ClusterWriterInnerState {
            ref mut available_indexes,
            ref failed_indexes,
            zone_status: ZoneRules(ref mut zone_status),
            ref mut errors,
        } = *state;
        if available_indexes.len() == 0 {
            return Err(errors.pop().unwrap_or(ShardError::NotEnoughAvailability));
        }
        let required_zones = zone_status
            .iter()
            .filter_map(
                |(zone, ZoneRule { minimum, .. })| {
                    if *minimum > 0 {
                        Some(zone)
                    } else {
                        None
                    }
                },
            )
            .collect::<HashSet<&String>>();
        let banned_zones = zone_status
            .iter()
            .filter_map(|(zone, ZoneRule { maximum, .. })| {
                maximum
                    .map(|maximum| if maximum <= 0 { Some(zone) } else { None })
                    .flatten()
            })
            .collect::<HashSet<&String>>();
        let ideal_zones = zone_status
            .iter()
            .filter_map(
                |(zone, ZoneRule { ideal, .. })| {
                    if *ideal > 0 {
                        Some(zone)
                    } else {
                        None
                    }
                },
            )
            .collect::<HashSet<&String>>();
        let available_locations = nodes
            .0
            .iter()
            .enumerate()
            .filter(|(i, node)| {
                if required_zones.len() > 0 {
                    let is_required = required_zones.iter().any(|zone| node.zones.contains(*zone));
                    if !is_required {
                        return false;
                    }
                } else if banned_zones.len() > 0 {
                    let is_banned = banned_zones.iter().any(|zone| node.zones.contains(*zone));
                    if !is_banned {
                        return false;
                    }
                } else if ideal_zones.len() > 0 {
                    let is_ideal = ideal_zones.iter().any(|zone| node.zones.contains(*zone));
                    if !is_ideal {
                        return false;
                    }
                }
                if failed_indexes.contains(&i) {
                    return false;
                }
                if let Some(availability) = available_indexes.get(&i) {
                    if *availability >= 1 {
                        return true;
                    }
                }
                false
            })
            .collect::<Vec<_>>();
        let total_weight: usize = available_locations
            .iter()
            .map(|(_, node)| node.location.weight)
            .sum();
        if total_weight == 0 {
            return Err(errors.pop().unwrap_or(ShardError::NotEnoughAvailability));
        }
        let sample = rand::thread_rng().gen_range(0..total_weight);
        let mut current_weight: usize = 0;
        for (index, node) in available_locations.iter() {
            current_weight += node.location.weight;
            if current_weight > sample {
                let availability = available_indexes.get_mut(&index).unwrap();
                *availability -= 1;
                for zone in node.zones.iter() {
                    if let Some(ref mut zone_rule) = zone_status.get_mut(zone) {
                        zone_rule.ideal -= 1;
                        zone_rule.minimum -= 1;
                        if let Some(ref mut maximum) = zone_rule.maximum {
                            *maximum -= 1;
                        }
                    }
                }
                return Ok((*index, node));
            }
        }
        panic!("Invalid writer sample")
    }

    async fn invalidate_index(&self, index: usize, err: ShardError) -> () {
        let (nodes, _profile) = self.parent.0.as_ref();
        let mut state = self.inner_state.lock().await;
        state.failed_indexes.insert(index);
        state.errors.push(err);
        if let Some(node) = nodes.0.get(index) {
            for zone in node.zones.iter() {
                if let Some(ZoneRule {
                    ref mut minimum, ..
                }) = state.zone_status.0.get_mut(zone)
                {
                    *minimum += 1;
                }
                if let Some(ZoneRule {
                    maximum: Some(ref mut maximum),
                    ..
                }) = state.zone_status.0.get_mut(zone)
                {
                    *maximum += 1;
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct ClusterWriter {
    pub(super) state: Arc<ClusterWriterState>,
}

#[async_trait]
impl ShardWriter for ClusterWriter {
    async fn write_shard(&mut self, hash: &str, bytes: &[u8]) -> Result<Vec<Location>, ShardError> {
        loop {
            match self.state.as_ref().next_writer().await {
                Ok((index, node)) => {
                    let writer = &node.location.location;
                    match writer.write_subfile(hash, bytes).await {
                        Ok(loc) => {
                            return Ok(vec![loc]);
                        },
                        Err(err) => {
                            self.state.invalidate_index(index, err).await;
                        },
                    }
                },
                Err(err) => {
                    return Err(err);
                },
            }
        }
    }
}
