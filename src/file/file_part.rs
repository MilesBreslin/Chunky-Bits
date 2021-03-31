use std::{
    collections::HashMap,
    hash::Hash,
    sync::Arc,
};

use futures::stream::{
    FuturesOrdered,
    StreamExt,
};
use rand::{
    self,
    Rng,
};
use reed_solomon_erasure::{
    galois_8,
    ReedSolomon,
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    self,
    sync::Mutex,
};

use crate::file::{
    error::*,
    hash::*,
    *,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilePart {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption: Option<Encryption>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunksize: Option<usize>,
    pub data: Vec<HashWithLocation<Sha256Hash>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub parity: Vec<HashWithLocation<Sha256Hash>>,
}

impl FilePart {
    pub(crate) async fn read(&self) -> Result<Vec<u8>, FileReadError> {
        let r: ReedSolomon<galois_8::Field> = ReedSolomon::new(self.data.len(), self.parity.len())?;
        let all_chunks_owned = self
            .data
            .iter()
            .chain(self.parity.iter())
            .cloned()
            .enumerate()
            .collect::<Vec<_>>();
        let all_chunks = Arc::new(Mutex::new(all_chunks_owned));
        let mut indexed_chunks = self
            .data
            .iter()
            .map(|_| {
                let all_chunks = all_chunks.clone();
                async move {
                    loop {
                        let mut all_chunks = all_chunks.as_ref().lock().await;
                        if all_chunks.is_empty() {
                            return None;
                        }
                        let sample = rand::thread_rng().gen_range(0..all_chunks.len());
                        let (index, mut chunk) = all_chunks.remove(sample);
                        drop(all_chunks);
                        for location in chunk.locations.drain(..) {
                            if let Ok(data) = location.read().await {
                                let (data, hash) = Sha256Hash::from_vec_async(data).await;
                                if hash == chunk.sha256 {
                                    return Some((index, data));
                                }
                            }
                        }
                    }
                }
            })
            .collect::<FuturesOrdered<_>>()
            .collect::<Vec<Option<(usize, Vec<u8>)>>>()
            .await;
        let mut all_read_chunks: Vec<Option<Vec<u8>>> = self
            .data
            .iter()
            .chain(self.parity.iter())
            .map(|_| None)
            .collect();
        for indexed_chunk in indexed_chunks.drain(..) {
            if let Some((index, chunk)) = indexed_chunk {
                *all_read_chunks.get_mut(index).unwrap() = Some(chunk);
            }
        }
        if !all_read_chunks
            .iter()
            .take(self.data.len())
            .all(Option::is_some)
        {
            r.reconstruct(&mut all_read_chunks)?;
        }
        let mut output = Vec::<u8>::new();
        for buf in all_read_chunks.drain(..).take(self.data.len()) {
            output.append(&mut buf.unwrap())
        }
        Ok(output)
    }

    pub(crate) async fn verify(&self) -> HashMap<&'_ Location, Integrity> {
        let mut out = HashMap::new();
        for hash_with_location in self.data.iter().chain(self.parity.iter()) {
            let ref hash = hash_with_location.sha256;
            for location in hash_with_location.locations.iter() {
                let integrity: Integrity = {
                    match location.read().await {
                        Ok(data) => {
                            if hash.verify(data).await.1 {
                                Integrity::Valid
                            } else {
                                Integrity::Invalid
                            }
                        },
                        Err(_err) => Integrity::Unavailable,
                    }
                };
                out.insert(location, integrity);
            }
        }
        out
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Integrity {
    Valid,
    Invalid,
    Unavailable,
}
