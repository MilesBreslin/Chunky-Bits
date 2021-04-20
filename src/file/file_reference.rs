use std::{
    fmt,
    ops::Deref,
    pin::Pin,
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

use crate::{
    error::{
        FileWriteError,
        LocationError,
    },
    file::{
        Chunk,
        CollectionDestination,
        Compression,
        FilePart,
        FileReadBuilder,
        FileWriteBuilder,
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
    pub fn write_builder() -> FileWriteBuilder<()> {
        FileWriteBuilder::new()
    }

    pub fn read_builder(&self) -> FileReadBuilder<&FileReference> {
        FileReadBuilder::new(self)
    }

    pub fn read_builder_owned(self) -> FileReadBuilder<FileReference> {
        FileReadBuilder::new(self)
    }

    pub async fn verify_owned(self) -> VerifyFileReportOwned {
        let file = Box::pin(self);
        let file_ref: Pin<&FileReference> = file.as_ref();
        let report: VerifyFileReport = file_ref.verify().await;
        let report: VerifyFileReport<'static> = unsafe { std::mem::transmute(report) };
        VerifyFileReportOwned { report, file }
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

    pub async fn resilver_owned<D>(self, destination: Arc<D>) -> ResilverFileReportOwned
    where
        D: CollectionDestination + Send + Sync + 'static,
    {
        let mut file = Box::pin(self);
        let mut file_ref: Pin<&mut FileReference> = file.as_mut();
        let report: ResilverFileReport = file_ref.resilver(destination).await;
        let report: ResilverFileReport<'static> = unsafe { std::mem::transmute(report) };
        ResilverFileReportOwned { report, file }
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

// Owned reports are not allowed to produce references to 'static, only '_
macro_rules! report_owned {
    ($target:ident, $owned:ident) => {
        pub struct $owned {
            file: Pin<Box<FileReference>>,
            report: $target<'static>,
        }

        impl Deref for $owned {
            type Target = $target<'static>;

            fn deref(&self) -> &Self::Target {
                &self.as_ref()
            }
        }

        impl AsRef<$target<'static>> for $owned {
            fn as_ref(&self) -> &$target<'static> {
                &self.report
            }
        }

        impl AsRef<FileReference> for $owned {
            fn as_ref(&self) -> &FileReference {
                &self.file
            }
        }
    };
}

report_owned!(ResilverFileReport, ResilverFileReportOwned);
report_owned!(VerifyFileReport, VerifyFileReportOwned);

macro_rules! report_common {
    ($report_type:ident) => {
        impl $report_type<'_> {
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

impl fmt::Display for VerifyFileReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Verified {}/{} chunks",
            self.healthy_chunks().count(),
            self.chunks().count(),
        )
    }
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

impl ResilverFileReport<'_> {
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
