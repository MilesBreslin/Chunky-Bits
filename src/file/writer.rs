use std::sync::Arc;

use futures::{
    future::FutureExt,
    stream::{
        FuturesOrdered,
        StreamExt,
    },
};
use reed_solomon_erasure::{
    galois_8,
    ReedSolomon,
};
use tokio::{
    io::{
        AsyncRead,
        AsyncReadExt,
    },
    select,
    sync::{
        mpsc,
        oneshot,
        Semaphore,
    },
};

use crate::{
    error::FileWriteError,
    file::{
        CollectionDestination,
        FilePart,
        FileReference,
    },
};

#[derive(Clone)]
pub struct FileWriteBuilder<D> {
    destination: D,
    state: FileWriteBuilderState,
}

#[derive(Copy, Clone)]
struct FileWriteBuilderState {
    chunk_size: usize,
    data: usize,
    parity: usize,
    concurrency: usize,
}

impl Default for FileWriteBuilderState {
    fn default() -> Self {
        FileWriteBuilderState {
            chunk_size: 1 << 20,
            data: 3,
            parity: 2,
            concurrency: 10,
        }
    }
}

impl<D: Default> Default for FileWriteBuilder<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D> FileWriteBuilder<D> {
    pub fn new() -> FileWriteBuilder<D>
    where
        D: Default,
    {
        FileWriteBuilder {
            destination: Default::default(),
            state: Default::default(),
        }
    }

    pub fn destination<DN>(self, destination: DN) -> FileWriteBuilder<DN>
    where
        DN: CollectionDestination + Clone + Send + Sync + 'static,
    {
        FileWriteBuilder {
            destination,
            state: self.state,
        }
    }

    pub fn chunk_size(self, chunk_size: usize) -> Self {
        let mut new = self;
        new.state.chunk_size = chunk_size;
        new
    }

    pub fn data_chunks(self, chunks: usize) -> Self {
        let mut new = self;
        new.state.data = chunks;
        new
    }

    pub fn parity_chunks(self, chunks: usize) -> Self {
        let mut new = self;
        new.state.parity = chunks;
        new
    }

    pub fn concurrency(self, concurrency: usize) -> Self {
        let mut new = self;
        new.state.concurrency = concurrency;
        new
    }
}

impl<D> FileWriteBuilder<D>
where
    D: CollectionDestination + Clone + Send + Sync + 'static,
{
    pub async fn write(
        &self,
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<FileReference, FileWriteError> {
        let destination = self.destination.clone();
        let FileWriteBuilderState {
            chunk_size,
            data,
            parity,
            concurrency,
        } = self.state;
        assert!(concurrency > 1);

        let concurrency_limiter = Arc::new(Semaphore::new(concurrency));
        let encoder: Arc<ReedSolomon<galois_8::Field>> = Arc::new(ReedSolomon::new(data, parity)?);

        let (error_tx, mut error_rx) = mpsc::channel::<FileWriteError>(1);
        let (part_tx, mut part_rx) = mpsc::channel::<oneshot::Receiver<FilePart>>(1);

        // Collect all parts in order
        let parts = tokio::spawn(async move {
            let mut parts = Vec::new();
            let mut pending_parts = FuturesOrdered::new();
            let mut more_parts = true;
            loop {
                select! {
                    pending_part = part_rx.recv(), if more_parts => {
                        match pending_part {
                            Some(pending_part) => {
                                pending_parts.push_back(pending_part);
                            },
                            None => {
                                more_parts = false
                            },
                        };
                    },
                    part = pending_parts.next(), if !pending_parts.is_empty() => {
                        if let Some(Ok(part)) = part {
                            parts.push(part);
                        }
                    },
                    else => {
                        break;
                    }
                }
            }
            parts
        });

        let read_file = (async move {
            let mut done = false;
            let mut total_bytes: u64 = 0;
            'file: while !done {
                let permit = concurrency_limiter.clone().acquire_owned().await.unwrap();

                let mut data_buf: Vec<u8> = vec![0; data * chunk_size];
                let mut bytes_read: usize = 0;
                // read_exact, but handle end-of-file
                {
                    let mut buf = &mut data_buf[..];
                    while !buf.is_empty() {
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
                                error_tx.send(err.into()).await.unwrap();
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
                    let encoder = encoder.clone();
                    let destination = destination.clone();
                    let error_tx = error_tx.clone();
                    let part_tx = {
                        let (tx, rx) = oneshot::channel();
                        part_tx.send(rx).await.unwrap();
                        tx
                    };
                    tokio::spawn(async move {
                        let _ = permit;
                        let result = FilePart::write_with_encoder(
                            encoder,
                            destination,
                            data_buf,
                            bytes_read,
                            data,
                            parity,
                        )
                        .await;
                        match result {
                            Ok(file_part) => {
                                let _ = part_tx.send(file_part);
                            },
                            Err(err) => {
                                let _ = error_tx.send(err).await;
                            },
                        }
                    });
                }
            }
            total_bytes
        })
        .fuse();
        tokio::pin!(read_file);

        let total_bytes = select! {
            // Read bytes without spawning a new thread to relax Send/Sync requirements
            total_bytes = &mut read_file => total_bytes,
            err = error_rx.recv() => {
                if let Some(err) = err {
                    return Err(err);
                }
                read_file.await
            },
        };
        if let Some(err) = error_rx.recv().await {
            return Err(err);
        }
        let parts = parts.await.unwrap();
        Ok(FileReference {
            content_type: None,
            compression: None,
            length: Some(total_bytes),
            parts,
        })
    }
}
