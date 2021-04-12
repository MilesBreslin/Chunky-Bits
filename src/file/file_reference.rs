use std::{
    fmt,
    sync::Arc,
};

use futures::stream::{
    self,
    FuturesOrdered,
    StreamExt,
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::io::{
    AsyncRead,
    AsyncWrite,
    AsyncWriteExt,
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
        FileReferenceBuilder,
        Location,
        ResilverPartReport,
        VerifyPartReport,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileReference {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    pub length: Option<u64>,
    pub parts: Vec<FilePart>,
}

impl FileReference {
    pub fn write_builder() -> FileReferenceBuilder<()> {
        FileReferenceBuilder::new()
    }

    pub fn reader(&self) -> impl AsyncRead {
        let file = self.clone();
        let (reader, mut writer) = tokio::io::duplex(1 << 24);
        tokio::spawn(async move { file.to_writer(&mut writer).await });
        reader
    }

    async fn to_writer<W>(&self, writer: &mut W) -> Result<(), FileReadError>
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
