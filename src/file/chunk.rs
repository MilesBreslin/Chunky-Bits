use std::hash::Hash;

use serde::{
    Deserialize,
    Serialize,
};

use crate::file::{
    hash::AnyHash,
    Location,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Chunk {
    #[serde(flatten)]
    pub hash: AnyHash,
    pub locations: Vec<Location>,
}
