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

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Sha256Hash(#[serde(with = "hex")] [u8; 32]);

impl Sha256Hash {
    pub async fn verify(&self, buf: Vec<u8>) -> (Vec<u8>, bool) {
        let (buf, other) = Self::from_vec_async(buf).await;
        (buf, self.eq(&other))
    }

    pub async fn from_vec_async(buf: Vec<u8>) -> (Vec<u8>, Self) {
        tokio::task::spawn_blocking(move || {
            let hash = Sha256Hash::from_buf(&buf);
            (buf, hash)
        })
        .await
        .unwrap()
    }

    pub fn from_buf<T>(buf: &T) -> Self
    where
        T: AsRef<[u8]>,
    {
        let mut ret = Sha256Hash(Default::default());
        ret.0.copy_from_slice(&Sha256::digest(buf.as_ref())[..]);
        ret
    }

    pub async fn from_buf_async(buf: &[u8]) -> Self {
        struct SendMySlice {
            ptr: *const u8,
            len: usize,
        }
        unsafe impl Send for SendMySlice {}

        let buf_ptr = SendMySlice {
            len: buf.len(),
            ptr: buf.as_ptr(),
        };

        // Safe because lifetime is valid until the function completes
        unsafe {
            tokio::task::spawn_blocking(move || {
                let buf = std::slice::from_raw_parts(buf_ptr.ptr, buf_ptr.len);
                Self::from_buf(&buf)
            })
        }
        .await
        .unwrap()
    }

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

    pub fn to_string(&self) -> String {
        hex::encode(&self.0)
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
        write!(f, "{}", &self.to_string())
    }
}
