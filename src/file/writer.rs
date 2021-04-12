use std::sync::Arc;

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
use tokio::{
    io::{
        AsyncRead,
        AsyncReadExt,
    },
    select,
    sync::{
        mpsc,
        oneshot,
    },
    task::JoinHandle,
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
    destination: Arc<D>,
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

impl<D> FileWriteBuilder<D> {
    pub fn new() -> FileWriteBuilder<D>
    where
        D: Default,
    {
        FileWriteBuilder {
            destination: Arc::new(Default::default()),
            state: Default::default(),
        }
    }

    pub fn destination<DN>(self, destination: DN) -> FileWriteBuilder<DN>
    where
        DN: CollectionDestination + Send + Sync + 'static,
    {
        self.destination_arc(Arc::new(destination))
    }

    pub fn destination_arc<DN>(self, destination: Arc<DN>) -> FileWriteBuilder<DN>
    where
        DN: CollectionDestination + Send + Sync + 'static,
    {
        FileWriteBuilder {
            destination: destination,
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
    D: CollectionDestination + Send + Sync + 'static,
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
        } = self.state.clone();
        assert!(concurrency > 1);

        let encoder: Arc<ReedSolomon<galois_8::Field>> = Arc::new(ReedSolomon::new(data, parity)?);

        let (error_tx, mut error_rx) = mpsc::channel::<FileWriteError>(1);
        let (part_tx, mut part_rx) = mpsc::channel::<oneshot::Receiver<FilePart>>(1);
        let (task_tx, mut task_rx) = mpsc::channel::<JoinHandle<()>>(1);

        // Limit concurrent tasks
        tokio::spawn(async move {
            let mut pending_tasks = FuturesUnordered::new();
            while let Some(task) = task_rx.recv().await {
                pending_tasks.push(task);
                if pending_tasks.len() >= concurrency - 1 {
                    let _ = pending_tasks.next().await;
                }
            }
        });

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
                                pending_parts.push(pending_part);
                            },
                            None => {
                                more_parts = false
                            },
                        };
                    },
                    part = pending_parts.next(), if pending_parts.len() > 0 => {
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
                // Wait for existing concurrent tasks
                let task_tx = match task_tx.reserve().await {
                    Ok(task_tx) => task_tx,
                    Err(_) => {
                        return 0;
                    },
                };

                let mut data_buf: Vec<u8> = vec![0; data * chunk_size];
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
                    task_tx.send(tokio::spawn(async move {
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
                    }));
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
        loop {
            match error_rx.recv().await {
                Some(err) => {
                    return Err(err);
                },
                None => {
                    break;
                },
            }
        }
        let parts = parts.await.unwrap();
        Ok(FileReference {
            content_type: None,
            compression: None,
            length: Some(total_bytes),
            parts: parts,
        })
    }
}
