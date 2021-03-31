use crate::file::Location;
use std::{
    error::Error,
    io,
    fmt::{
        self,
        Formatter,
        Display,
    },
};

#[derive(Debug)]
/// An error type for file writes
pub enum FileWriteError {
    Erasure(reed_solomon_erasure::Error),
    JoinError(tokio::task::JoinError),
    NotEnoughWriters,
    ReaderError(io::Error),
    WriterError(ShardWriterError),
}

impl Display for FileWriteError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use FileWriteError::*;
        match self {
            Erasure(e) => write!(f, "Erasure: {}", e),
            JoinError(e) => write!(f, "Tokio Join: {}", e),
            NotEnoughWriters => write!(f, "Not Enough Writers"),
            ReaderError(e) => write!(f, "Reader: {}", e),
            WriterError(e) => write!(f, "Writer: {}", e),
        }
    }
}

impl Error for FileWriteError {}

impl From<reed_solomon_erasure::Error> for FileWriteError {
    fn from(err: reed_solomon_erasure::Error) -> Self {
        FileWriteError::Erasure(err)
    }
}

impl From<tokio::task::JoinError> for FileWriteError {
    fn from(err: tokio::task::JoinError) -> Self {
        FileWriteError::JoinError(err)
    }
}

impl From<io::Error> for FileWriteError {
    fn from(err: io::Error) -> Self {
        FileWriteError::ReaderError(err)
    }
}

impl From<ShardWriterError> for FileWriteError {
    fn from(err: ShardWriterError) -> Self {
        FileWriteError::WriterError(err)
    }
}

#[derive(Debug)]
/// When writing a shard, the location of the failure should also be known
pub struct ShardWriterError {
    pub location: Location,
    pub error: LocationWriteError,
}

impl Display for ShardWriterError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "<{}> {}", self.location, self.error)
    }
}

impl Error for ShardWriterError {}

#[derive(Debug)]
/// Different types of location write errors
pub enum LocationWriteError {
    IoError(io::Error),
    HttpStatus(reqwest::StatusCode),
    HttpError(reqwest::Error),
}

impl Display for LocationWriteError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use LocationWriteError::*;
        match self {
            IoError(e) => write!(f, "IO: {}", e),
            HttpStatus(status) => write!(f, "Http Status: {}", status),
            HttpError(e) => write!(f, "Http Error: {}", e),
        }
    }
}

impl Error for LocationWriteError {}

impl From<reqwest::Error> for LocationWriteError {
    fn from(err: reqwest::Error) -> Self {
        match err.status() {
            Some(status) if !status.is_success() => {
                LocationWriteError::HttpStatus(status)
            },
            _ => {
                LocationWriteError::HttpError(err)
            }
        }
    }
}

impl From<io::Error> for LocationWriteError {
    fn from(err: io::Error) -> Self {
        LocationWriteError::IoError(err)
    }
}