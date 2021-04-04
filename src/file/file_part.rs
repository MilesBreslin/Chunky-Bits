use std::{
    cell::RefCell,
    collections::{
        HashMap,
        VecDeque,
    },
    hash::Hash,
    sync::Arc,
};

use futures::{
    future,
    future::FutureExt,
    select,
    stream::{
        FuturesOrdered,
        FuturesUnordered,
        StreamExt,
    },
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
    sync::{
        oneshot,
        Mutex,
    },
};

use crate::{
    error::{
        FileReadError,
        FileWriteError,
        ShardError,
    },
    file::{
        hash::Sha256Hash,
        CollectionDestination,
        Encryption,
        HashWithLocation,
        Location,
        ShardWriter,
    },
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

    pub async fn resilver<D>(
        &mut self,
        destination: Arc<D>,
    ) -> Result<HashMap<&'_ Sha256Hash, Result<Vec<&'_ Location>, ShardError>>, FileWriteError>
    where
        D: CollectionDestination + Send + Sync + 'static,
    {
        let FilePart {
            ref mut data,
            ref mut parity,
            ..
        } = self;

        // Calculate these as local variables to allow sending across threads
        let data_len = data.len();
        let parity_len = parity.len();
        let total_chunks = data_len + parity_len;

        // Orig, Repair, and Writer channels
        // Manager channels need to be seperated since they will be consumed seperately
        let mut manager_orig_channels = VecDeque::with_capacity(total_chunks);
        let mut manager_repair_channels = VecDeque::with_capacity(total_chunks);
        let mut manager_writer_channels = VecDeque::with_capacity(total_chunks);
        let mut reader_channels = VecDeque::with_capacity(total_chunks);
        for _index in 0..total_chunks {
            let (orig_tx, orig_rx) = oneshot::channel::<Vec<u8>>();
            let (repair_tx, repair_rx) = oneshot::channel::<Vec<u8>>();
            let (writer_tx, writer_rx) = oneshot::channel::<D::Writer>();
            manager_orig_channels.push_back(orig_rx);
            manager_repair_channels.push_back(repair_tx);
            manager_writer_channels.push_back(writer_tx);
            reader_channels.push_back((orig_tx, repair_rx, writer_rx));
        }

        let r: ReedSolomon<galois_8::Field> = ReedSolomon::new(data_len, parity_len)?;
        let manager_join = tokio::spawn(async move {
            let mut chunks: Vec<Option<Vec<u8>>> = vec![None; data_len + parity_len];
            let mut succeeded_chunks: Vec<Option<bool>> = vec![None; total_chunks];
            let mut incomming_chunks = manager_orig_channels
                .into_iter()
                .enumerate()
                .map(|(index, rx)| async move { (index, rx.await) })
                .collect::<FuturesUnordered<_>>();
            while let Some((index, bytes)) = incomming_chunks.next().await {
                if let Ok(bytes) = bytes {
                    if let Some(destination) = chunks.get_mut(index) {
                        destination.get_or_insert(bytes);
                    }
                    if let Some(success) = succeeded_chunks.get_mut(index) {
                        *success = Some(true);
                    }
                } else if let Some(success) = succeeded_chunks.get_mut(index) {
                    success.get_or_insert(false);
                }

                let valid_chunks = chunks.iter().filter(|opt| opt.is_some()).count();
                let failed_chunks = succeeded_chunks
                    .iter()
                    .filter_map(|opt| match opt {
                        Some(false) => Some(()),
                        _ => None,
                    })
                    .count();
                if valid_chunks >= data_len && failed_chunks > 0 {
                    tokio::task::spawn_blocking(move || {
                        println!("Write chunks");
                        r.reconstruct(&mut chunks)?;
                        for (chunk, repair_tx) in
                            chunks.into_iter().zip(manager_repair_channels.into_iter())
                        {
                            let chunk = chunk.unwrap();
                            let _ = repair_tx.send(chunk);
                        }
                        Ok::<(), FileWriteError>(())
                    })
                    .await
                    .unwrap()?;
                    break;
                }
            }
            while let Some((index, bytes)) = incomming_chunks.next().await {
                if let Ok(_) = bytes {
                    if let Some(success) = succeeded_chunks.get_mut(index) {
                        *success = Some(true);
                    }
                } else if let Some(success) = succeeded_chunks.get_mut(index) {
                    success.get_or_insert(false);
                }
            }
            let failed_chunks = succeeded_chunks
                .iter()
                .filter_map(|opt| match opt {
                    Some(false) => Some(()),
                    _ => None,
                })
                .count();
            println!("failed_chunks: {}", failed_chunks);
            let mut writers = destination.get_writers(failed_chunks)?;
            let iter = succeeded_chunks
                .into_iter()
                .zip(manager_writer_channels.into_iter());
            for (status, writer_channel) in iter {
                if let Some(false) = status {
                    let writer = writers.pop().unwrap();
                    let _ = writer_channel.send(writer);
                }
            }
            Ok::<(), FileWriteError>(())
        });
        let mut data_join = data
            .iter_mut()
            .chain(parity.iter_mut())
            .map(
                |&mut HashWithLocation {
                     ref mut locations,
                     ref sha256,
                     ..
                 }| {
                    let (orig_tx, repair_rx, writer_rx) = reader_channels.pop_front().unwrap();
                    let orig_tx = RefCell::new(Some(orig_tx));
                    async move {
                        let valid_locations: Vec<&Location> = locations
                            .iter()
                            .map(|location| {
                                let orig_tx = &orig_tx;
                                async move {
                                    match location.read().await {
                                        Ok(bytes)
                                            if Sha256Hash::from_buf_async(&bytes).await
                                                == *sha256 =>
                                        {
                                            let mut orig_tx = orig_tx.replace(None);
                                            let _result =
                                                orig_tx.take().map(|orig_tx| orig_tx.send(bytes));
                                            Some(location)
                                        }
                                        Ok(_) => None,
                                        _ => None,
                                    }
                                }
                            })
                            .collect::<FuturesOrdered<_>>()
                            .filter_map(future::ready)
                            .collect()
                            .await;
                        drop(orig_tx);
                        if valid_locations.len() == 0 {
                            if let (Ok(repaired), Ok(mut writer)) =
                                (repair_rx.await, writer_rx.await)
                            {
                                debug_assert_eq!(
                                    Sha256Hash::from_buf_async(&repaired).await,
                                    *sha256,
                                );
                                let written_locations =
                                    writer.write_shard(&format!("{}", sha256), &repaired).await;
                                match written_locations {
                                    Ok(written_locations) => {
                                        let written_locations_len = written_locations.len();
                                        locations.extend(written_locations);
                                        Some((
                                            sha256,
                                            Ok(locations
                                                .iter()
                                                .rev()
                                                .take(written_locations_len)
                                                .collect()),
                                        ))
                                    },
                                    Err(err) => Some((sha256, Err(err))),
                                }
                            } else {
                                Some((sha256, Err(ShardError::NotEnoughChunks)))
                            }
                        } else {
                            None
                        }
                    }
                },
            )
            .collect::<FuturesUnordered<_>>()
            .filter_map(future::ready)
            .collect();
        let mut manager_join = manager_join.fuse();
        loop {
            select! {
                data = data_join => {
                    return Ok(data);
                },
                manager = manager_join => {
                    manager.unwrap()?;
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Integrity {
    Valid,
    Invalid,
    Unavailable,
}
