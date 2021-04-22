use std::fmt;

use serde::{
    Deserialize,
    Serialize,
};
use tokio::task::{
    self,
    JoinHandle,
};

use super::Sha256Hash;

pub trait DataHasher: Eq + Sized + Send + Sync + 'static {
    fn from_buf(data: &[u8]) -> Self;
    fn from_buf_async<T>(data: T) -> JoinHandle<(Self, T)>
    where
        T: AsRef<[u8]> + Send + Sync + Sized + 'static,
    {
        task::spawn_blocking(move || (<Self as DataHasher>::from_buf(data.as_ref()), data))
    }
}

pub trait DataVerifier: Sized {
    fn verify(&self, data: &[u8]) -> bool;
    fn verify_async<T>(&self, data: T) -> JoinHandle<(bool, T)>
    where
        T: AsRef<[u8]> + Send + Sync + 'static;
}

impl<H> DataVerifier for H
where
    H: DataHasher + Clone,
{
    fn verify(&self, data: &[u8]) -> bool {
        *self == <Self as DataHasher>::from_buf(data)
    }

    fn verify_async<T>(&self, data: T) -> JoinHandle<(bool, T)>
    where
        T: AsRef<[u8]> + Send + Sync + Sized + 'static,
    {
        let inner = self.clone();
        task::spawn_blocking(move || {
            let other = <Self as DataHasher>::from_buf(data.as_ref());
            (inner == other, data)
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnyHash {
    Sha256(Sha256Hash),
}

impl AnyHash {
    pub fn from_buf(&self, data: &[u8]) -> AnyHash {
        match self {
            AnyHash::Sha256(_) => <Sha256Hash as DataHasher>::from_buf(data).into(),
        }
    }

    pub fn from_buf_async<T>(&self, data: T) -> JoinHandle<(AnyHash, T)>
    where
        T: AsRef<[u8]> + Send + Sync + 'static,
    {
        task::spawn_blocking(match self {
            AnyHash::Sha256(_) => move || {
                (
                    <Sha256Hash as DataHasher>::from_buf(data.as_ref()).into(),
                    data,
                )
            },
        })
    }
}

impl DataVerifier for AnyHash {
    fn verify(&self, data: &[u8]) -> bool {
        match self {
            AnyHash::Sha256(h) => h.verify(data),
        }
    }

    fn verify_async<T>(&self, data: T) -> JoinHandle<(bool, T)>
    where
        T: AsRef<[u8]> + Send + Sync + 'static,
    {
        match self {
            AnyHash::Sha256(h) => h.verify_async(data),
        }
    }
}

impl fmt::Display for AnyHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use AnyHash::*;
        match self {
            Sha256(h) => write!(f, "sha256-{}", h),
        }
    }
}

impl From<Sha256Hash> for AnyHash {
    fn from(h: Sha256Hash) -> Self {
        AnyHash::Sha256(h)
    }
}
