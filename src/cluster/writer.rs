use std::{
    collections::{
        HashMap,
        HashSet,
    },
    string::ToString,
    sync::Arc,
};

use async_trait::async_trait;
use rand::{
    self,
    rngs::SmallRng,
    Rng,
    SeedableRng,
};
use tokio::{
    pin,
    select,
    sync::{
        oneshot,
        Mutex,
    },
    time::{
        sleep,
        Duration,
    },
};

use crate::{
    cluster::{
        ClusterNode,
        ClusterNodes,
        DestinationContainer,
        DestinationInner,
        ZoneRule,
        ZoneRules,
    },
    error::ShardError,
    file::{
        hash::AnyHash,
        Location,
        ShardWriter,
    },
};

pub(super) struct ClusterWriterState {
    pub parent: DestinationContainer,
    pub inner_state: Mutex<ClusterWriterInnerState>,
}
pub(super) struct ClusterWriterInnerState {
    pub available_indexes: HashMap<usize, usize>,
    pub failed_indexes: HashSet<usize>,
    pub zone_status: ZoneRules,
    pub errors: Vec<ShardError>,
    pub rng: Option<SmallRng>,
}

impl ClusterWriterState {
    async fn next_writer(&self, hash: &AnyHash) -> Result<(usize, &ClusterNode), ShardError> {
        let DestinationInner { ref nodes, .. } = &*self.parent;
        let mut state = self.inner_state.lock().await;
        if state.available_indexes.is_empty() {
            return Err(state
                .errors
                .pop()
                .unwrap_or(ShardError::NotEnoughAvailability));
        }
        let available_locations = state.get_available_locations(nodes);
        let total_weight: usize = available_locations
            .iter()
            .map(|(_, node)| node.location.weight)
            .sum();
        if total_weight == 0 {
            return Err(state
                .errors
                .pop()
                .unwrap_or(ShardError::NotEnoughAvailability));
        }

        let rng = state.rng.get_or_insert_with(|| {
            let mut seed_bytes: [u8; 32] = Default::default();
            let hash_bytes: &[u8] = hash.as_ref();
            seed_bytes.copy_from_slice(&hash_bytes[..32]);
            SmallRng::from_seed(seed_bytes)
        });

        let sample = rng.gen_range(0..total_weight);
        let mut current_weight: usize = 0;
        for (index, node) in available_locations.iter() {
            current_weight += node.location.weight;
            if current_weight > sample {
                state.remove_availability(*index, node);
                return Ok((*index, node));
            }
        }
        panic!("Invalid writer sample")
    }

    async fn invalidate_index(&self, index: usize, err: ShardError) {
        let DestinationInner { nodes, .. } = &*self.parent;
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

impl ClusterWriterInnerState {
    pub(super) fn get_available_locations<'a>(
        &self,
        nodes: &'a ClusterNodes,
    ) -> Vec<(usize, &'a ClusterNode)> {
        let ClusterWriterInnerState {
            ref available_indexes,
            ref failed_indexes,
            zone_status: ZoneRules(ref zone_status),
            ..
        } = self;
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
        nodes
            .0
            .iter()
            .enumerate()
            .filter(|(i, node)| {
                if !required_zones.is_empty() {
                    let is_required = required_zones.iter().any(|zone| node.zones.contains(*zone));
                    if !is_required {
                        return false;
                    }
                } else if !banned_zones.is_empty() {
                    let is_banned = banned_zones.iter().any(|zone| node.zones.contains(*zone));
                    if !is_banned {
                        return false;
                    }
                } else if !ideal_zones.is_empty() {
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
            .collect::<Vec<_>>()
    }

    pub(super) fn remove_availability(&mut self, index: usize, node: &ClusterNode) {
        let ClusterWriterInnerState {
            ref mut available_indexes,
            zone_status: ZoneRules(ref mut zone_status),
            ..
        } = self;

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
    }
}

pub struct ClusterWriter {
    pub(super) state: Arc<ClusterWriterState>,
    pub(super) waiter: Option<oneshot::Receiver<()>>,
    pub(super) staller: Option<oneshot::Sender<()>>,
}

#[async_trait]
impl ShardWriter for ClusterWriter {
    async fn write_shard(
        &mut self,
        hash: &AnyHash,
        bytes: &[u8],
    ) -> Result<Vec<Location>, ShardError> {
        let ClusterWriter {
            ref state,
            ref mut waiter,
            ref mut staller,
        } = self;
        let DestinationInner {
            location_context: cx,
            ..
        } = &*state.parent;

        if let Some(waiter) = waiter.take() {
            let sleeper = sleep(Duration::from_millis(100));
            pin!(sleeper);
            select! {
                _ = waiter => {},
                _ = &mut sleeper => {},
            }
        }

        loop {
            let next_writer = state.next_writer(hash).await;
            staller.take().map(|tx| tx.send(()));
            match next_writer {
                Ok((index, node)) => {
                    let writer = &node.location.location;
                    match writer
                        .write_subfile_with_context(cx, &hash.to_string(), bytes)
                        .await
                    {
                        Ok(loc) => {
                            return Ok(vec![loc]);
                        },
                        Err(err) => {
                            state.invalidate_index(index, err).await;
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
