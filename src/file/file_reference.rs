use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::Arc,
};

use futures::{
    future::FutureExt,
    stream::{
        FuturesOrdered,
        FuturesUnordered,
        StreamExt,
    },
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
    io::{
        AsyncRead,
        AsyncReadExt,
        AsyncWrite,
        AsyncWriteExt,
    },
    sync::mpsc,
    task::JoinHandle,
};

use crate::{
    error::{
        FileReadError,
        FileWriteError,
    },
    file::{
        hash::Sha256Hash,
        CollectionDestination,
        Compression,
        FilePart,
        HashWithLocation,
        Integrity,
        Location,
        ShardWriter,
    },
};

#[derive(Debug, Serialize, Deserialize)]
pub struct FileReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub length: Option<u64>,
    pub parts: Vec<FilePart>,
}

impl FileReference {
    pub async fn from_reader<R, D>(
        reader: &mut R,
        destination: Arc<D>,
        chunksize: usize,
        data: usize,
        parity: usize,
        concurrency: NonZeroUsize,
    ) -> Result<Self, FileWriteError>
    where
        R: AsyncRead + Unpin,
        D: CollectionDestination + Send + Sync + 'static,
    {
        let mut concurrent_parts: usize = concurrency.into();
        // For each read of the channel, 1 part is allowed to start writing
        // Each part on completion will write a result to it
        let (err_sender, mut err_receiver) =
            mpsc::channel::<Result<(), FileWriteError>>(concurrent_parts);

        // A vec of task join handles that will be read at the end
        // Errors will be reported both here and via the channel
        let mut parts_fut = Vec::<JoinHandle<Option<FilePart>>>::new();

        let r: Arc<ReedSolomon<galois_8::Field>> =
            Arc::new(ReedSolomon::new(data, parity).unwrap());
        let mut done = false;
        let mut total_bytes: u64 = 0;
        let mut write_error: Result<(), FileWriteError> = Ok(());
        'file: while !done {
            // Wait to be allowed to read/write a part
            if concurrent_parts == 0 {
                if let Some(result) = err_receiver.recv().await {
                    if let Err(err) = result {
                        write_error = Err(err);
                        break 'file;
                    }
                    concurrent_parts += 1;
                }
            }
            // Clear any other reads
            while let Some(Some(result)) = err_receiver.recv().now_or_never() {
                if let Err(err) = result {
                    write_error = Err(err);
                    break 'file;
                }
                concurrent_parts += 1;
            }
            // Reduce allowed concurrent parts
            concurrent_parts -= 1;
            // data * chunksize all initialized to 0
            let mut data_buf: Vec<u8> = vec![0; data * chunksize];
            let mut bytes_read: usize = 0;
            // read_exact, but handle end-of-file
            {
                let mut buf = &mut data_buf[..];
                while buf.len() != 0 {
                    match reader.read(&mut buf).await {
                        Ok(bytes) if bytes > 0 => {
                            bytes_read += bytes;
                            buf = &mut buf[bytes..];
                        },
                        Ok(_) => {
                            done = true;
                            break;
                        },
                        Err(err) => {
                            write_error = Err(err.into());
                            break 'file;
                        },
                    }
                }
            }
            total_bytes += bytes_read as u64;
            if bytes_read == 0 {
                break;
            } else {
                // Clone some values that will be moved into the task
                let r = r.clone();
                let destination = destination.clone();
                let err_sender = err_sender.clone();
                parts_fut.push(tokio::spawn(async move {
                    // Run as seperate future
                    // Allows use of return statements and ? operator
                    let result = async move {
                        // Move data_buf into scope and make it immutable
                        let data_buf = data_buf.as_slice();
                        // Divide the bytes into bytes_read/data rounded up chunks
                        let buf_length = (bytes_read + data - 1) / data;
                        let data_chunks: Vec<&[u8]> = (0..data)
                            .map(|index| -> &[u8] {
                                &data_buf[(buf_length * index)..(buf_length * (index + 1))]
                            })
                            .collect();

                        // Zero out parity chunks
                        let mut parity_chunks: Vec<Vec<u8>> = vec![vec![0; buf_length]; parity];

                        // Calculate parity
                        r.encode_sep::<&[u8], Vec<u8>>(&data_chunks, &mut parity_chunks)?;

                        // Get some writers
                        let mut writers = destination.get_writers(data + parity)?;

                        // Hash and write all chunks
                        let mut write_results_iter = data_chunks
                            .iter()
                            .map(|slice| -> &[u8] { *slice })
                            .chain(parity_chunks.iter().map(|vec| vec.as_slice()))
                            .zip(writers.drain(..))
                            .enumerate()
                            .map(|(index, (data, mut writer))| async move {
                                let hash = Sha256Hash::from_buf(&data);
                                let hash_with_location = writer
                                    .write_shard(&format!("{}", hash), &data)
                                    .await
                                    .map(|locations| HashWithLocation {
                                        sha256: hash,
                                        locations: locations,
                                    });
                                (index, hash_with_location)
                            })
                            .collect::<FuturesUnordered<_>>();
                        // Collect the stream manually to handle errors out-of-order
                        let mut hashes_with_location: Vec<Option<HashWithLocation<Sha256Hash>>> =
                            vec![None; data + parity];
                        while let Some((index, result)) = write_results_iter.next().await {
                            let result = result?;
                            hashes_with_location[index] = Some(result);
                        }

                        Ok(FilePart {
                            encryption: None,
                            chunksize: Some(buf_length),
                            data: hashes_with_location
                                .drain(..data)
                                .map(Option::unwrap)
                                .collect(),
                            parity: hashes_with_location.drain(..).map(Option::unwrap).collect(),
                        })
                    };
                    // Send the error to the error channel
                    // Return option of value
                    let result: Result<_, FileWriteError> = result.await;
                    match result {
                        Ok(value) => {
                            let _result = err_sender.send(Ok(())).await;
                            Some(value)
                        },
                        Err(err) => {
                            let _result = err_sender.send(Err(err)).await;
                            None
                        },
                    }
                }))
            }
        }
        // Drop the sender owned by this thread. Will block indefinitely otherwise
        drop(err_sender);
        // If there has been an error abort all tasks and quit
        loop {
            // If no known error, check for new errors and break if done
            if let Ok(_) = &write_error {
                match err_receiver.recv().await {
                    None => {
                        break;
                    },
                    Some(Err(err)) => {
                        write_error = Err(err);
                    },
                    Some(_) => {
                        continue;
                    },
                }
            }
            // If error, abort everything and return
            if let Err(err) = write_error {
                for task in parts_fut {
                    task.abort();
                }
                return Err(err);
            }
        }
        // Read the tasks return values into a new vec
        let mut parts_fut = parts_fut.drain(..);
        let mut parts = Vec::with_capacity(parts_fut.len());
        while let Some(part) = parts_fut.next() {
            let part = part.await?;
            parts.push(part.unwrap());
        }
        Ok(FileReference {
            content_type: None,
            compression: None,
            length: Some(total_bytes),
            parts: parts,
        })
    }

    pub async fn to_writer<W>(&self, writer: &mut W) -> Result<(), FileReadError>
    where
        W: AsyncWrite + Unpin,
    {
        let mut bytes_written: u64 = 0;
        for file_part in &self.parts {
            if self.length.map(|len| bytes_written >= len).unwrap_or(false) {
                break;
            }
            let buf = file_part.read().await?;
            if let Some(file_length) = self.length {
                let bytes_remaining: u64 = file_length - bytes_written;
                let mut w_buf: &[u8] = &buf;
                if bytes_remaining < w_buf.len() as u64 {
                    w_buf = &buf[..bytes_remaining as usize];
                }
                bytes_written += w_buf.len() as u64;
                writer.write_all(&w_buf).await?;
            } else {
                writer.write_all(&buf).await?;
            }
        }
        Ok(())
    }

    pub async fn verify(&self) -> Vec<HashMap<&'_ Location, Integrity>> {
        self.parts
            .iter()
            .map(FilePart::verify)
            .collect::<FuturesOrdered<_>>()
            .collect()
            .await
    }
}
