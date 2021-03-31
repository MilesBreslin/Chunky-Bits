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
pub enum FileWriteError {
    Erasure(reed_solomon_erasure::Error),
    NotEnoughWriters,
    ReaderError(io::Error),
    WriterError {
        location: Location,
        error: ShardWriterError,
    },
}

impl Display for FileWriteError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use FileWriteError::*;
        match self {
            Erasure(e) => write!(f, "Erasure: {}", e),
            NotEnoughWriters => write!(f, "Not Enough Writers"),
            ReaderError(e) => write!(f, "Reader: {}", e),
            WriterError{location, error} => write!(f, "Writer <{}>: {}", location, error),
        }
    }
}

impl Error for FileWriteError {}

#[derive(Debug)]
pub enum ShardWriterError {
    IoError(io::Error),
    HttpStatus(reqwest::StatusCode),
    HttpError(reqwest::Error),
}

impl Display for ShardWriterError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use ShardWriterError::*;
        match self {
            IoError(e) => write!(f, "IO: {}", e),
            HttpStatus(status) => write!(f, "Http Status: {}", status),
            HttpError(e) => write!(f, "Http Error: {}", e),
        }
    }
}

impl Error for ShardWriterError {}