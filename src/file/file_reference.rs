use std::{
    fmt,
    sync::Arc,
};

use futures::{
    future::FutureExt,
    stream::{
        self,
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
    select,
    sync::{
        mpsc,
        oneshot,
    },
    task::JoinHandle,
};

use crate::{
    error::{
        FileReadError,
        FileWriteError,
        LocationError,
    },
    file::{
        Chunk,
        CollectionDestination,
        Compression,
        FilePart,
        Location,
        ResilverPartReport,
        VerifyPartReport,
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

#[derive(Clone)]
pub struct FileReferenceBuilder<D> {
    destination: Arc<D>,
    state: FileReferenceBuilderState,
}

#[derive(Copy, Clone)]
struct FileReferenceBuilderState {
    chunk_size: usize,
    data: usize,
    parity: usize,
    concurrency: usize,
}

impl Default for FileReferenceBuilderState {
    fn default() -> Self {
        FileReferenceBuilderState {
            chunk_size: 1 << 20,
            data: 3,
            parity: 2,
            concurrency: 10,
        }
    }
}

impl<D> FileReferenceBuilder<D> {
    pub fn destination<DN>(self, destination: DN) -> FileReferenceBuilder<DN>
    where
        DN: CollectionDestination + Send + Sync + 'static,
    {
        self.destination_arc(Arc::new(destination))
    }

    pub fn destination_arc<DN>(self, destination: Arc<DN>) -> FileReferenceBuilder<DN>
    where
        DN: CollectionDestination + Send + Sync + 'static,
    {
        FileReferenceBuilder {
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

impl<D> FileReferenceBuilder<D>
where
    D: CollectionDestination + Send + Sync + 'static,
{
    pub async fn write(
        &self,
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<FileReference, FileWriteError> {
        let destination = self.destination.clone();
        let FileReferenceBuilderState {
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

impl FileReference {
    pub fn write_builder() -> FileReferenceBuilder<()> {
        FileReferenceBuilder {
            destination: Arc::new(()),
            state: Default::default(),
        }
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

    pub async fn verify(&self) -> VerifyFileReport<'_> {
        VerifyFileReport {
            part_reports: self
                .parts
                .iter()
                .map(FilePart::verify)
                .collect::<FuturesOrdered<_>>()
                .collect()
                .await,
        }
    }

    pub async fn resilver<D>(&mut self, destination: Arc<D>) -> ResilverFileReport<'_>
    where
        D: CollectionDestination + Send + Sync + 'static,
    {
        let part_reports = stream::iter(
            self.parts
                .iter_mut()
                .map(|part| part.resilver(destination.clone())),
        )
        .buffered(10)
        .collect()
        .await;
        ResilverFileReport { part_reports }
    }
}

macro_rules! report_common {
    ($report_type:ident) => {
        impl<'a> $report_type<'a> {
            /// Does the FilePart at least 1 valid location for each chunk
            pub fn is_ok(&self) -> bool {
                self.part_reports.iter().all(|report| report.is_ok())
            }

            pub fn is_available(&self) -> bool {
                self.part_reports.iter().all(|report| report.is_available())
            }

            pub fn chunks(&self) -> impl Iterator<Item = &Chunk> {
                self.part_reports.iter().flat_map(|report| report.chunks())
            }

            pub fn healthy_chunks(&self) -> impl Iterator<Item = &Chunk> {
                self.part_reports
                    .iter()
                    .flat_map(|report| report.healthy_chunks())
            }

            pub fn unhealthy_chunks(&self) -> impl Iterator<Item = &Chunk> {
                self.part_reports
                    .iter()
                    .flat_map(|report| report.unhealthy_chunks())
            }

            pub fn failed_read_chunks(&self) -> impl Iterator<Item = &Chunk> {
                self.part_reports
                    .iter()
                    .flat_map(|report| report.failed_read_chunks())
            }

            pub fn unavailable_locations(
                &self,
            ) -> impl Iterator<Item = (&Location, &LocationError)> {
                self.part_reports
                    .iter()
                    .flat_map(|report| report.unavailable_locations())
            }

            pub fn invalid_locations(&self) -> impl Iterator<Item = &Location> {
                self.part_reports
                    .iter()
                    .flat_map(|report| report.invalid_locations())
            }
        }
    };
}

pub struct VerifyFileReport<'a> {
    part_reports: Vec<VerifyPartReport<'a>>,
}

impl VerifyFileReport<'_> {
    pub fn full_report(&self) -> impl fmt::Display + '_ {
        VerifyFileFullReport(self)
    }
}

report_common!(VerifyFileReport);

struct VerifyFileFullReport<'a>(&'a VerifyFileReport<'a>);

impl fmt::Display for VerifyFileFullReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for report in self.0.part_reports.iter() {
            write!(f, "{}", report.full_report())?;
        }
        Ok(())
    }
}

pub struct ResilverFileReport<'a> {
    part_reports: Vec<ResilverPartReport<'a>>,
}

impl fmt::Display for ResilverFileReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Resilvered {}/{} chunks with {} errors, resulting in {}/{} healthy chunks",
            self.successful_writes().count(),
            self.failed_read_chunks().count(),
            self.rebuild_errors().count() + self.failed_writes().count(),
            self.healthy_chunks().count(),
            self.chunks().count(),
        )
    }
}

report_common!(ResilverFileReport);

impl<'a> ResilverFileReport<'a> {
    pub fn rebuild_errors(&self) -> impl Iterator<Item = Result<(), &FileWriteError>> {
        self.part_reports
            .iter()
            .map(|report| report.rebuild_error())
    }

    pub fn new_locations(&self) -> impl Iterator<Item = &Location> {
        self.part_reports
            .iter()
            .flat_map(|report| report.new_locations())
    }

    pub fn successful_writes(&self) -> impl Iterator<Item = &[&Location]> {
        self.part_reports
            .iter()
            .flat_map(|report| report.successful_writes())
    }

    pub fn failed_writes(&self) -> impl Iterator<Item = &FileWriteError> {
        self.part_reports
            .iter()
            .flat_map(|report| report.failed_writes())
    }

    pub fn parts_reports(&self) -> impl Iterator<Item = &ResilverPartReport> {
        self.part_reports.iter()
    }
}
