use std::{
    fmt,
    ops::Deref,
    pin::Pin,
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
        FileIntegrity,
        FilePart,
        FileReadBuilder,
        FileWriteBuilder,
        Integrity,
        Location,
        LocationIntegrity,
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
    pub fn len_bytes(&self) -> u64 {
        self.length.unwrap_or_else(|| {
            self.parts
                .iter()
                .map(FilePart::len_bytes)
                .fold(0, |acc, len| acc + len as u64)
        })
    }

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
        VerifyFileReportOwned { file, report }
    }

    pub async fn verify(&self) -> VerifyFileReport<'_> {
        VerifyFileReport(
            self.parts
                .iter()
                .map(FilePart::verify)
                .collect::<FuturesOrdered<_>>()
                .collect()
                .await,
        )
    }

    pub async fn resilver_owned<D>(self, destination: D) -> ResilverFileReportOwned
    where
        D: CollectionDestination + Clone + Send + Sync + 'static,
    {
        let mut file = Box::pin(self);
        let mut file_ref: Pin<&mut FileReference> = file.as_mut();
        let report: ResilverFileReport = file_ref.resilver(destination).await;
        let report: ResilverFileReport<'static> = unsafe { std::mem::transmute(report) };
        ResilverFileReportOwned { file, report }
    }

    pub async fn resilver<D>(&mut self, destination: D) -> ResilverFileReport<'_>
    where
        D: CollectionDestination + Clone + Send + Sync + 'static,
    {
        let part_reports = stream::iter(
            self.parts
                .iter_mut()
                .map(|part| part.resilver(destination.clone())),
        )
        .buffered(10)
        .collect()
        .await;
        ResilverFileReport(part_reports)
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
            pub fn is_ideal(&self) -> bool {
                self.integrity().is_ideal()
            }

            pub fn is_available(&self) -> bool {
                self.integrity().is_available()
            }

            pub fn integrity(&self) -> FileIntegrity {
                self.part_reports()
                    .fold(FileIntegrity::Valid, |current, part_report| {
                        let part_integrity = part_report.integrity();
                        if part_integrity > current {
                            part_integrity
                        } else {
                            current
                        }
                    })
            }

            pub fn total_parts(&self) -> usize {
                self.0.len()
            }

            pub fn total_chunks(&self) -> usize {
                self.part_reports()
                    .map(|report| report.total_chunks())
                    .sum()
            }

            pub fn healthy_parts(&self) -> impl Iterator<Item = &FilePart> {
                self.part_reports().filter_map(|report| {
                    if let Some(_) = report.unhealthy_chunks().next() {
                        None
                    } else {
                        Some(report.as_ref())
                    }
                })
            }

            pub fn healthy_chunks(&self) -> impl Iterator<Item = &Chunk> {
                self.part_reports()
                    .flat_map(|report| report.healthy_chunks())
            }

            pub fn unhealthy_chunks(&self) -> impl Iterator<Item = &Chunk> {
                self.part_reports()
                    .flat_map(|report| report.unhealthy_chunks())
            }

            pub fn unavailable_locations(
                &self,
            ) -> impl Iterator<Item = (&Location, &LocationError)> {
                self.part_reports()
                    .flat_map(|report| report.unavailable_locations())
            }

            pub fn invalid_locations(&self) -> impl Iterator<Item = &Location> {
                self.part_reports()
                    .flat_map(|part_report| part_report.invalid_locations())
            }

            pub fn locations_with_integrity(
                &self,
            ) -> impl Iterator<Item = (&Location, LocationIntegrity)> {
                self.part_reports()
                    .flat_map(|part_report| part_report.locations_with_integrity())
            }

            pub fn display_full_report(&self) -> impl fmt::Display + '_ {
                struct DisplayFullReport<'a>(&'a $report_type<'a>);

                impl fmt::Display for DisplayFullReport<'_> {
                    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        writeln!(f, "file\t{}\n", self.0.integrity())?;
                        for part_report in self.0.part_reports() {
                            writeln!(f, "{}", part_report.display_full_report())?;
                        }
                        Ok(())
                    }
                }

                DisplayFullReport(self)
            }
        }
    };
}

pub struct VerifyFileReport<'a>(Vec<VerifyPartReport<'a>>);

impl fmt::Display for VerifyFileReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}: {}/{} unhealthy parts",
            self.integrity(),
            self.healthy_parts().count(),
            self.total_parts(),
        )
    }
}

impl VerifyFileReport<'_> {
    pub fn full_report(
        &self,
    ) -> impl Iterator<
        Item = (
            FileIntegrity,
            impl Iterator<
                Item = (
                    &Chunk,
                    LocationIntegrity,
                    impl Iterator<Item = (&Location, LocationIntegrity, Option<&LocationError>)>,
                ),
            >,
        ),
    > {
        self.part_reports().map(VerifyPartReport::full_report)
    }

    pub fn part_reports(&self) -> impl Iterator<Item = &VerifyPartReport> {
        self.0.iter()
    }
}

report_common!(VerifyFileReport);

pub struct ResilverFileReport<'a>(Vec<ResilverPartReport<'a>>);

impl fmt::Display for ResilverFileReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}: {}/{} parts modified",
            self.integrity(),
            self.resilvered_parts().count(),
            self.total_parts(),
        )
    }
}

report_common!(ResilverFileReport);

impl ResilverFileReport<'_> {
    pub fn rebuild_errors(&self) -> impl Iterator<Item = Result<(), &FileWriteError>> {
        self.part_reports().map(|report| report.rebuild_error())
    }

    pub fn new_locations(&self) -> impl Iterator<Item = &Location> {
        self.part_reports()
            .flat_map(|report| report.new_locations())
    }

    pub fn successful_writes(&self) -> impl Iterator<Item = &[&Location]> {
        self.part_reports()
            .flat_map(|report| report.successful_writes())
    }

    pub fn failed_writes(&self) -> impl Iterator<Item = &FileWriteError> {
        self.part_reports()
            .flat_map(|report| report.failed_writes())
    }

    pub fn resilvered_parts(&self) -> impl Iterator<Item = &FilePart> {
        self.part_reports().map(|part_report| part_report.as_ref())
    }

    pub fn part_reports(&self) -> impl Iterator<Item = &ResilverPartReport> {
        self.0.iter()
    }
}
