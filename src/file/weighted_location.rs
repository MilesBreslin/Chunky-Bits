use std::str::FromStr;

use serde::{
    Deserialize,
    Serialize,
};

use crate::file::Location;

#[derive(Clone, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeightedLocation {
    #[serde(default = "WeightedLocation::default_weight")]
    pub weight: usize,
    pub location: Location,
}
impl WeightedLocation {
    fn default_weight() -> usize {
        1000
    }
}
impl FromStr for WeightedLocation {
    type Err = <Location as FromStr>::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split_string = s.split(':');
        if let (Some(prefix), Some(postfix)) = (split_string.next(), split_string.next()) {
            if let Ok(weight) = prefix.parse::<usize>() {
                return Ok(WeightedLocation {
                    weight,
                    location: Location::from_str(postfix)?,
                });
            }
        }
        Ok(WeightedLocation {
            weight: Self::default_weight(),
            location: Location::from_str(s)?,
        })
    }
}

impl From<Location> for WeightedLocation {
    fn from(loc: Location) -> Self {
        WeightedLocation {
            weight: Self::default_weight(),
            location: loc,
        }
    }
}
