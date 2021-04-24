use std::{
    collections::HashSet,
    iter::{
        once,
        repeat_with,
    },
    ops::Deref,
    sync::Arc,
};

use tokio::sync::{
    oneshot,
    Mutex,
};

use crate::{
    cluster::{
        ClusterNodes,
        ClusterProfile,
        ClusterWriter,
        ClusterWriterInnerState,
        ClusterWriterState,
    },
    error::FileWriteError,
    file::{
        CollectionDestination,
        Location,
        LocationContext,
    },
};

#[derive(Clone)]
pub(super) struct DestinationContainer(pub Arc<DestinationInner>);

impl Deref for DestinationContainer {
    type Target = DestinationInner;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<T> AsRef<T> for DestinationContainer
where
    DestinationInner: AsRef<T>,
{
    fn as_ref(&self) -> &T {
        self.0.as_ref().as_ref()
    }
}

impl From<DestinationInner> for DestinationContainer {
    fn from(inner: DestinationInner) -> Self {
        DestinationContainer(Arc::new(inner))
    }
}

pub(super) struct DestinationInner {
    pub location_context: LocationContext,
    pub nodes: ClusterNodes,
    pub profile: ClusterProfile,
}

impl AsRef<ClusterNodes> for DestinationInner {
    fn as_ref(&self) -> &ClusterNodes {
        &self.nodes
    }
}
impl AsRef<ClusterProfile> for DestinationInner {
    fn as_ref(&self) -> &ClusterProfile {
        &self.profile
    }
}

impl CollectionDestination for DestinationContainer {
    type Writer = ClusterWriter;

    fn get_writers(&self, count: usize) -> Result<Vec<Self::Writer>, FileWriteError> {
        self.get_used_writers(&vec![None; count])
    }

    fn get_used_writers(
        &self,
        locations: &[Option<&Location>],
    ) -> Result<Vec<Self::Writer>, FileWriteError> {
        let count = locations.iter().filter(|opt| opt.is_none()).count();
        let DestinationInner {
            ref nodes,
            ref profile,
            ..
        } = self.0.as_ref();
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
            rng: None,
        };
        for location in locations.iter().flatten() {
            let parent_nodes = nodes
                .0
                .iter()
                .enumerate()
                .filter(|(_, node)| node.location.location.is_parent_of(location));
            for (index, node) in parent_nodes {
                inner_state.remove_availability(index, node);
            }
        }
        let state = Arc::new(ClusterWriterState {
            parent: self.clone(),
            inner_state: Mutex::new(inner_state),
        });

        let (tx_waiters, rx_waiters): (Vec<_>, Vec<_>) =
            repeat_with(oneshot::channel::<()>).take(count).unzip();

        let writers = once(None)
            .chain(rx_waiters.into_iter().map(Some))
            .zip(tx_waiters)
            .map(|(rx, tx)| ClusterWriter {
                state: state.clone(),
                waiter: rx,
                staller: Some(tx),
            })
            .collect();

        Ok(writers)
    }
}
