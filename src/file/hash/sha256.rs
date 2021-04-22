use std::{
    fmt,
    str::FromStr,
};

use serde::{
    Deserialize,
    Serialize,
};
use sha2::{
    Digest,
    Sha256,
};

use super::DataHasher;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Sha256Hash(#[serde(with = "hex")] [u8; 32]);

impl DataHasher for Sha256Hash {
    fn from_buf(buf: &[u8]) -> Self {
        let mut ret = Sha256Hash(Default::default());
        ret.0.copy_from_slice(&Sha256::digest(buf.as_ref())[..]);
        ret
    }
}

impl Sha256Hash {
    pub fn from_reader<R>(reader: &mut R) -> std::io::Result<Self>
    where
        R: std::io::Read,
    {
        let mut hasher: Sha256 = Default::default();
        std::io::copy(reader, &mut hasher)?;
        let mut ret = Sha256Hash(Default::default());
        ret.0.copy_from_slice(&hasher.finalize()[..]);
        Ok(ret)
    }
}

impl FromStr for Sha256Hash {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut new = Sha256Hash(Default::default());
        hex::decode_to_slice(s, &mut new.0)?;
        Ok(new)
    }
}

impl fmt::Display for Sha256Hash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", hex::encode(&self.0))
    }
}

impl AsRef<[u8]> for Sha256Hash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
