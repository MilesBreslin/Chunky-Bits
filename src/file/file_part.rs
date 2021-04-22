use std::{
    fmt,
    hash::Hash,
    sync::Arc,
};

use futures::stream::{
    FuturesOrdered,
    FuturesUnordered,
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
    pin,
    select,
    sync::{
        oneshot,
        Mutex,
    },
};

use crate::{
    error::{
        FileReadError,
        FileWriteError,
        LocationError,
        ShardError,
    },
    file::{
        hash::{
            DataHasher,
            DataVerifier,
            Sha256Hash,
        },
        Chunk,
        CollectionDestination,
        Encryption,
        Location,
        LocationContext,
        ShardWriter,
    },
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FilePart {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption: Option<Encryption>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunksize: Option<usize>,
    pub data: Vec<Chunk>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub parity: Vec<Chunk>,
}

impl FilePart {
    pub(crate) async fn read_with_context(
        &self,
        cx: &LocationContext,
    ) -> Result<Vec<u8>, FileReadError> {
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
                            if let Ok(data) = location.read_with_context(cx).await {
                                let (equality, data) = chunk.hash.verify_async(data).await.unwrap();
                                if equality {
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

    pub(crate) async fn write_with_encoder<D>(
        encoder: impl AsRef<ReedSolomon<galois_8::Field>> + Send + Sync,
        destination: impl AsRef<D>,
        data_buf: impl AsRef<[u8]>,
        length: usize,
        data: usize,
        parity: usize,
    ) -> Result<FilePart, FileWriteError>
    where
        D: CollectionDestination + Send + Sync + 'static,
    {
        // Move data_buf into scope and make it immutable
        let data_buf = data_buf.as_ref();
        assert!(length <= data_buf.len());
        // Divide the bytes into bytes_read/data rounded up chunks
        let buf_length = (length + data - 1) / data;
        let data_chunks: Vec<&[u8]> = (0..data)
            .map(|index| -> &[u8] { &data_buf[(buf_length * index)..(buf_length * (index + 1))] })
            .collect();

        // Zero out parity chunks
        let mut parity_chunks: Vec<Vec<u8>> = vec![vec![0; buf_length]; parity];

        // Calculate parity
        encoder
            .as_ref()
            .encode_sep::<&[u8], Vec<u8>>(&data_chunks, &mut parity_chunks)?;

        // Get some writers
        let mut writers = destination.as_ref().get_writers(data + parity)?;

        let (mut error_tx, error_rx): (Vec<_>, FuturesUnordered<_>) = (0..(data + parity))
            .map(|_| oneshot::channel::<FileWriteError>())
            .unzip();
        let error_rx = error_rx.filter_map(|err| async move { err.ok() });
        pin!(error_rx);

        // Hash and write all chunks
        let chunk_stream = data_chunks
            .iter()
            .map(|slice| -> &[u8] { *slice })
            .chain(parity_chunks.iter().map(|vec| vec.as_slice()))
            .zip(writers.drain(..))
            .map(|(data, mut writer)| {
                let error_tx = error_tx.pop().unwrap();
                async move {
                    let hash = Sha256Hash::from_buf(&data).into();
                    let locations = writer.write_shard(&hash, &data).await;
                    match locations {
                        Ok(locations) => Some(Chunk {
                            hash: hash,
                            locations,
                        }),
                        Err(err) => {
                            let _ = error_tx.send(err.into());
                            None
                        },
                    }
                }
            })
            .collect::<FuturesOrdered<_>>()
            .filter_map(|chunk| async move { chunk });
        pin!(chunk_stream);

        let mut data_chunks = Vec::with_capacity(data);
        let mut parity_chunks = Vec::with_capacity(parity);
        while parity_chunks.len() < parity {
            select! {
                chunk = chunk_stream.next() => {
                    if let Some(chunk) = chunk {
                        if data_chunks.len() < data {
                            data_chunks.push(chunk);
                        } else {
                            parity_chunks.push(chunk);
                        }
                    }
                },
                err = error_rx.next() => {
                    if let Some(err) = err {
                        return Err(err);
                    }
                }
            }
        }
        Ok(FilePart {
            encryption: None,
            chunksize: Some(buf_length),
            data: data_chunks,
            parity: parity_chunks,
        })
    }

    pub(crate) async fn verify(&self) -> VerifyPartReport<'_> {
        VerifyPartReport {
            file_part: self,
            read_results: self
                .data
                .iter()
                .chain(self.parity.iter())
                .flat_map(|chunk| {
                    chunk.locations.iter().map(move |location| async move {
                        let result = match location.read().await {
                            Ok(bytes) => {
                                let (equality, _) = chunk.hash.verify_async(bytes).await.unwrap();
                                Ok(equality)
                            },
                            Err(err) => Err(err),
                        };
                        (chunk, location, result)
                    })
                })
                .collect::<FuturesOrdered<_>>()
                .collect()
                .await,
        }
    }

    pub async fn resilver<D>(&mut self, destination: Arc<D>) -> ResilverPartReport<'_>
    where
        D: CollectionDestination + Send + Sync + 'static,
    {
        let FilePart {
            ref mut data,
            ref mut parity,
            ..
        } = self;

        let (mut data_bufs, read_report): (
            Vec<Option<Vec<u8>>>,
            Vec<Vec<Result<bool, LocationError>>>,
        ) = data
            .iter()
            .chain(parity.iter())
            .map(|chunk| async move {
                let Chunk {
                    ref locations,
                    ref hash,
                } = chunk;

                let mut locations_report = Vec::with_capacity(locations.len());
                let mut chunk_bytes = None;
                for location in locations.iter() {
                    locations_report.push(match location.read().await {
                        Ok(bytes) => {
                            let (local_hash, bytes) = hash.from_buf_async(bytes).await.unwrap();
                            let equality = local_hash.eq(hash);
                            if equality {
                                chunk_bytes.get_or_insert(bytes);
                            }
                            Ok(equality)
                        },
                        Err(err) => Err(err),
                    })
                }
                (chunk_bytes, locations_report)
            })
            .collect::<FuturesOrdered<_>>()
            .unzip()
            .await;

        let mut write_error = Ok(());
        let mut write_results = Vec::new();
        let chunk_status: Vec<bool> = data_bufs.iter().map(|opt| opt.is_some()).collect();
        if data_bufs.iter().any(|opt| opt.is_none()) {
            write_error = match ReedSolomon::<galois_8::Field>::new(data.len(), parity.len()) {
                Ok(encoder) => encoder.reconstruct(&mut data_bufs).map_err(Into::into),
                Err(err) => Err(err.into()),
            };
            let chunks_request = chunk_status
                .iter()
                .zip(data.iter().chain(parity.iter()))
                .flat_map(|(status, chunk)| {
                    chunk
                        .locations
                        .iter()
                        .enumerate()
                        .filter_map(move |(index, location)| match (status, index) {
                            (true, _) => Some(Some(location)),
                            (false, 0) => Some(None),
                            (false, _) => None,
                        })
                })
                .collect::<Vec<Option<&Location>>>();
            match destination.as_ref().get_used_writers(&chunks_request) {
                Ok(mut writers) => {
                    let iter_mut = data
                        .iter_mut()
                        .chain(parity.iter_mut())
                        .zip(data_bufs.into_iter())
                        .zip(chunk_status.iter())
                        .filter_map(|((chunk, bytes), status)| (!status).then(|| (chunk, bytes)));
                    let mut inner_write_results: Vec<Result<usize, ShardError>> = Vec::new();
                    for (chunk, bytes) in iter_mut {
                        let mut writer = writers.pop().unwrap();
                        if let Some(bytes) = bytes {
                            match writer.write_shard(&chunk.hash, &bytes).await {
                                Ok(locations) => {
                                    inner_write_results.push(Ok(locations.len()));
                                    chunk.locations.extend(locations);
                                },
                                Err(err) => inner_write_results.push(Err(err)),
                            };
                        }
                    }
                    let iter_immut = self
                        .data
                        .iter()
                        .chain(self.parity.iter())
                        .zip(chunk_status.iter())
                        .filter_map(|(chunk, status)| (!status).then(|| chunk))
                        .zip(inner_write_results.into_iter());
                    for (chunk, result) in iter_immut {
                        write_results.push((
                            chunk,
                            result
                                .map(|len| chunk.locations.iter().rev().take(len).collect())
                                .map_err(Into::into),
                        ));
                    }
                },
                Err(err) => {
                    write_error = Err(err.into());
                },
            }
        }
        let read_results = read_report
            .into_iter()
            .zip(self.data.iter().chain(self.parity.iter()))
            .flat_map(|(report, chunk)| {
                report
                    .into_iter()
                    .zip(chunk.locations.iter())
                    .map(move |(report, location)| (chunk, location, report))
            })
            .collect();
        ResilverPartReport {
            file_part: self,
            write_error,
            write_results,
            read_results,
        }
    }
}

pub trait Integrity {
    fn is_ideal(&self) -> bool;
    fn is_available(&self) -> bool;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum LocationIntegrity {
    Valid,
    Resilvered,
    Invalid,
    Unavailable,
}

impl fmt::Display for LocationIntegrity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.serialize(f)
    }
}

impl Integrity for LocationIntegrity {
    fn is_ideal(&self) -> bool {
        use LocationIntegrity::*;
        match self {
            Valid | Resilvered => true,
            Invalid | Unavailable => false,
        }
    }

    fn is_available(&self) -> bool {
        self.is_ideal()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum FileIntegrity {
    Valid,
    Resilvered,
    Degraded,
    Unavailable,
}

impl fmt::Display for FileIntegrity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.serialize(f)
    }
}

impl Integrity for FileIntegrity {
    fn is_ideal(&self) -> bool {
        use FileIntegrity::*;
        match self {
            Valid | Resilvered => true,
            Degraded | Unavailable => false,
        }
    }

    fn is_available(&self) -> bool {
        use FileIntegrity::*;
        match self {
            Valid | Resilvered | Degraded => true,
            Unavailable => false,
        }
    }
}

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

            pub fn total_chunks(&self) -> usize {
                let FilePart { data, parity, .. } = &self.file_part;
                data.len() + parity.len()
            }

            fn chunks(&self) -> impl Iterator<Item = &Chunk> {
                self.file_part
                    .data
                    .iter()
                    .chain(self.file_part.parity.iter())
            }

            pub fn healthy_chunks(&self) -> impl Iterator<Item = &Chunk> {
                self.chunks()
                    .filter(move |chunk| self.chunk_is_healthy(chunk))
            }

            pub fn unhealthy_chunks(&self) -> impl Iterator<Item = &Chunk> {
                self.chunks()
                    .filter(move |chunk| !self.chunk_is_healthy(chunk))
            }

            pub fn unavailable_locations(
                &self,
            ) -> impl Iterator<Item = (&Location, &LocationError)> {
                self.read_results
                    .iter()
                    .filter_map(|(_, location, result)| match result {
                        Err(err) => Some((*location, err)),
                        Ok(_) => None,
                    })
            }

            pub fn invalid_locations(&self) -> impl Iterator<Item = &Location> {
                self.read_location_with_integrity()
                    .filter_map(|(_, location, integrity)| {
                        (integrity == LocationIntegrity::Invalid).then(|| location)
                    })
            }

            pub fn locations_with_integrity(
                &self,
            ) -> impl Iterator<Item = (&Location, LocationIntegrity)> {
                self.read_location_with_integrity()
                    .map(|(_, location, integrity)| (location, integrity))
            }

            fn read_location_result(
                &self,
                location: &Location,
            ) -> Option<&Result<bool, LocationError>> {
                self.read_results
                    .iter()
                    .filter_map(move |(_, read_location, result)| {
                        let read_location: &Location = *read_location;
                        (location as *const _ == read_location as *const _).then(|| result)
                    })
                    .next()
            }

            fn read_location_with_integrity(
                &self,
            ) -> impl Iterator<Item = (&Chunk, &Location, LocationIntegrity)> {
                self.read_results.iter().map(|(chunk, location, result)| {
                    (*chunk, *location, Self::location_err_to_integrity(result))
                })
            }

            fn location_err_to_integrity(err: &Result<bool, LocationError>) -> LocationIntegrity {
                match err {
                    Ok(true) => LocationIntegrity::Valid,
                    Ok(false) => LocationIntegrity::Invalid,
                    _ => LocationIntegrity::Unavailable,
                }
            }

            fn chunk_is_healthy(&self, chunk: &Chunk) -> bool {
                self.chunk_integrity(chunk) == LocationIntegrity::Valid
            }

            fn chunk_read_results<'a>(
                &'a self,
                chunk: &'a Chunk,
            ) -> impl Iterator<Item = (&'a Location, &'a Result<bool, LocationError>)> {
                self.read_results
                    .iter()
                    .filter_map(move |(read_chunk, location, result)| {
                        let read_chunk: &Chunk = *read_chunk;
                        (chunk as *const _ == read_chunk as *const _).then(|| (*location, result))
                    })
            }
        }

        impl AsRef<FilePart> for $report_type<'_> {
            fn as_ref(&self) -> &FilePart {
                &self.file_part
            }
        }
    };
}

pub struct VerifyPartReport<'a> {
    file_part: &'a FilePart,
    read_results: Vec<(&'a Chunk, &'a Location, Result<bool, LocationError>)>,
}

impl fmt::Display for VerifyPartReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}: {}/{} unhealthy chunks",
            self.integrity(),
            self.unhealthy_chunks().count(),
            self.total_chunks(),
        )
    }
}

impl VerifyPartReport<'_> {
    pub fn integrity(&self) -> FileIntegrity {
        let data_chunks = self.file_part.data.len();
        let parity_chunks = self.file_part.parity.len();
        let total_chunks = data_chunks + parity_chunks;
        match self.healthy_chunks().count() {
            x if x == total_chunks => FileIntegrity::Valid,
            x if x >= data_chunks => FileIntegrity::Degraded,
            _ => FileIntegrity::Unavailable,
        }
    }

    fn chunk_integrity(&self, chunk: &Chunk) -> LocationIntegrity {
        let mut chunk_integrity = LocationIntegrity::Unavailable;
        let mut iter = self
            .chunk_read_results(chunk)
            .map(|(_, res)| Self::location_err_to_integrity(res));
        while let Some(integrity) = iter.next() {
            if integrity < chunk_integrity {
                chunk_integrity = integrity
            }
            if integrity == LocationIntegrity::Valid {
                break;
            }
        }
        chunk_integrity
    }

    pub fn full_report(
        &self,
    ) -> (
        FileIntegrity,
        impl Iterator<
            Item = (
                &Chunk,
                LocationIntegrity,
                impl Iterator<Item = (&Location, LocationIntegrity, Option<&LocationError>)>,
            ),
        >,
    ) {
        (
            self.integrity(),
            self.chunks().map(move |chunk| {
                (
                    chunk,
                    self.chunk_integrity(chunk),
                    chunk.locations.iter().map(move |location| {
                        let result = self.read_location_result(location).unwrap();
                        let integrity = Self::location_err_to_integrity(result);
                        let error = result.as_ref().err();
                        (location, integrity, error)
                    }),
                )
            }),
        )
    }

    pub fn display_full_report(&self) -> impl fmt::Display + '_ {
        VerifyPartFullReport(self)
    }
}

report_common!(VerifyPartReport);

struct VerifyPartFullReport<'a>(&'a VerifyPartReport<'a>);

impl fmt::Display for VerifyPartFullReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (part_integrity, chunks) = self.0.full_report();
        writeln!(f, "part\t{}", part_integrity)?;
        for (chunk, chunk_integrity, locations) in chunks {
            writeln!(f, "chunk\t{}\t{}", chunk_integrity, chunk.hash)?;
            for (location, loc_integrity, error) in locations {
                if let Some(error) = error {
                    writeln!(f, "location\t{}\t{}\t{}", loc_integrity, location, error)?;
                } else {
                    writeln!(f, "location\t{}\t{}", loc_integrity, location)?;
                }
            }
        }
        Ok(())
    }
}

pub struct ResilverPartReport<'a> {
    file_part: &'a FilePart,
    write_error: Result<(), FileWriteError>,
    write_results: Vec<(&'a Chunk, Result<Vec<&'a Location>, FileWriteError>)>,
    read_results: Vec<(&'a Chunk, &'a Location, Result<bool, LocationError>)>,
}

impl fmt::Display for ResilverPartReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}: {}/{} chunks modified",
            self.integrity(),
            self.successful_writes().count(),
            self.total_chunks(),
        )
    }
}

report_common!(ResilverPartReport);

impl<'a> ResilverPartReport<'a> {
    pub fn integrity(&self) -> FileIntegrity {
        let data_chunks = self.file_part.data.len();
        let parity_chunks = self.file_part.parity.len();
        let total_chunks = data_chunks + parity_chunks;
        match self.healthy_chunks().count() {
            x if x == total_chunks => {
                if self.successful_writes().count() > 1 {
                    FileIntegrity::Resilvered
                } else {
                    FileIntegrity::Valid
                }
            },
            x if x >= data_chunks => FileIntegrity::Degraded,
            _ => FileIntegrity::Unavailable,
        }
    }

    pub fn rebuild_error(&self) -> Result<(), &FileWriteError> {
        match &self.write_error {
            Ok(()) => Ok(()),
            Err(err) => Err(err),
        }
    }

    pub fn new_locations(&self) -> impl Iterator<Item = &Location> {
        self.successful_writes()
            .flat_map(|locations| locations.iter().map(|location| *location))
    }

    pub fn successful_writes(&self) -> impl Iterator<Item = &[&Location]> {
        self.write_results
            .iter()
            .filter_map(|(_, result)| match result {
                Ok(locations) => Some(locations.as_slice()),
                Err(_) => None,
            })
    }

    pub fn failed_writes(&self) -> impl Iterator<Item = &FileWriteError> {
        self.write_results
            .iter()
            .filter_map(|(_, result)| match result {
                Ok(_) => None,
                Err(err) => Some(err),
            })
    }

    fn chunk_integrity(&self, chunk: &Chunk) -> LocationIntegrity {
        let mut chunk_integrity = LocationIntegrity::Unavailable;
        let mut iter = self
            .chunk_read_results(chunk)
            .map(|(_, res)| Self::location_err_to_integrity(res));
        while let Some(integrity) = iter.next() {
            if integrity < chunk_integrity {
                chunk_integrity = integrity
            }
            if integrity == LocationIntegrity::Valid {
                return integrity;
            }
        }
        let mut successful_writes =
            self.write_results
                .iter()
                .filter_map(|(write_chunk, result)| {
                    let write_chunk: &Chunk = write_chunk;
                    (chunk as *const _ == write_chunk as *const _)
                        .then(|| result.as_ref().ok())
                        .flatten()
                });
        if let Some(_) = successful_writes.next() {
            return LocationIntegrity::Valid;
        };
        chunk_integrity
    }

    pub fn full_report(
        &self,
    ) -> (
        FileIntegrity,
        &Result<(), FileWriteError>,
        impl Iterator<
            Item = (
                &Chunk,
                LocationIntegrity,
                impl Iterator<Item = (&Location, LocationIntegrity, Option<&LocationError>)>,
                impl Iterator<Item = &FileWriteError>,
            ),
        >,
    ) {
        (
            self.integrity(),
            &self.write_error,
            self.chunks().map(move |chunk| {
                (
                    chunk,
                    self.chunk_integrity(chunk),
                    chunk.locations.iter().map(move |location| {
                        let integrity;
                        let error: Option<&LocationError>;
                        if let Some(result) = self.read_location_result(location) {
                            integrity = Self::location_err_to_integrity(result);
                            error = result.as_ref().err();
                        } else {
                            integrity = LocationIntegrity::Valid;
                            error = None;
                        }
                        (location, integrity, error)
                    }),
                    self.failed_writes(),
                )
            }),
        )
    }

    pub fn display_full_report(&self) -> impl fmt::Display + '_ {
        ResilverPartFullReport(self)
    }
}

struct ResilverPartFullReport<'a>(&'a ResilverPartReport<'a>);

impl fmt::Display for ResilverPartFullReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (part_integrity, write_error, chunks) = self.0.full_report();
        write!(f, "part\t{}", part_integrity)?;
        if let Err(error) = write_error {
            write!(f, "\t{}", error)?;
        }
        write!(f, "\n")?;
        for (chunk, chunk_integrity, locations, failed_locations) in chunks {
            writeln!(f, "chunk\t{}\t{}", chunk_integrity, chunk.hash)?;
            for (location, loc_integrity, error) in locations {
                if let Some(error) = error {
                    writeln!(f, "location\t{}\t{}\t{}", loc_integrity, location, error)?;
                } else {
                    writeln!(f, "location\t{}\t{}", loc_integrity, location)?;
                }
            }
            for error in failed_locations {
                writeln!(f, "error\t{}", error)?;
            }
        }
        Ok(())
    }
}
