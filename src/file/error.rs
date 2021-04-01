use std::{
    error::Error,
    fmt::{
        self,
        Display,
        Formatter,
    },
    io,
};

use crate::file::Location;

macro_rules! impl_from_err {
    ($error_type:ty => $variant:ident for $parent:ident) => {
        impl From<$error_type> for $parent {
            fn from(err: $error_type) -> Self {
                $parent::$variant(err)
            }
        }
    };
    ({ $($error_type:ty => $variant:ident),* } for $parent:ident) => {
        $(
            impl_from_err!($error_type => $variant for $parent);
        )*
    };
    ({ $($error_type:ty => $variant:ident),*, } for $parent:ident) => {
        $(
            impl_from_err!($error_type => $variant for $parent);
        )*
    };
}

#[derive(Debug)]
/// An error type for file writes
pub enum FileWriteError {
    Erasure(reed_solomon_erasure::Error),
    JoinError(tokio::task::JoinError),
    NotEnoughWriters,
    ReaderError(io::Error),
    WriterError(ShardError),
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

impl_from_err! {
    {
        reed_solomon_erasure::Error => Erasure,
        tokio::task::JoinError => JoinError,
        io::Error => ReaderError,
        ShardError => WriterError,
    } for FileWriteError
}

#[derive(Debug)]
/// When writing a shard, the location of the failure should also be known
pub struct ShardError {
    pub location: Location,
    pub error: LocationError,
}

impl Display for ShardError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "<{}> {}", self.location, self.error)
    }
}

impl Error for ShardError {}

#[derive(Debug)]
/// Different types of location errors
pub enum LocationError {
    IoError(io::Error),
    HttpStatus(reqwest::StatusCode),
    HttpError(reqwest::Error),
}

impl Display for LocationError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use LocationError::*;
        match self {
            IoError(e) => write!(f, "IO: {}", e),
            HttpStatus(status) => write!(f, "Http Status: {}", status),
            HttpError(e) => write!(f, "Http Error: {}", e),
        }
    }
}

impl Error for LocationError {}

impl From<reqwest::Error> for LocationError {
    fn from(err: reqwest::Error) -> Self {
        match err.status() {
            Some(status) if !status.is_success() => LocationError::HttpStatus(status),
            _ => LocationError::HttpError(err),
        }
    }
}

impl_from_err! {
    {
        io::Error => IoError,
        reqwest::StatusCode => HttpStatus,
    } for LocationError
}

#[derive(Debug)]
pub enum FileReadError {
    Erasure(reed_solomon_erasure::Error),
    FilePart(ShardError),
    WriterError(io::Error),
}

impl_from_err! {
    {
        reed_solomon_erasure::Error => Erasure,
        ShardError => FilePart,
        io::Error => WriterError,
    } for FileReadError
}

#[derive(Debug)]
pub enum ClusterError {
    FileWrite(FileWriteError),
    FileMetadataRead(MetadataReadError),
    FileRead(FileReadError),
}

impl_from_err! {
    {
        FileWriteError => FileWrite,
        MetadataReadError => FileMetadataRead,
        FileReadError => FileRead,
    } for ClusterError
}

#[derive(Debug)]
pub enum SerdeError {
    Json(serde_json::Error),
    Yaml(serde_yaml::Error),
}

impl Display for SerdeError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use SerdeError::*;
        match self {
            Json(e) => write!(f, "Json: {}", e),
            Yaml(e) => write!(f, "Yaml: {}", e),
        }
    }
}

impl std::error::Error for SerdeError {}

impl_from_err! {
    {
        serde_json::Error => Json,
        serde_yaml::Error => Yaml,
    } for SerdeError
}

#[derive(Debug)]
pub enum MetadataReadError {
    PostExec(io::Error),
    FileRead(LocationError),
    Serde(SerdeError),
}

impl_from_err! {
    {
        LocationError => FileRead,
        SerdeError => Serde,
    } for MetadataReadError
}

#[derive(Debug)]
pub enum HttpUrlError {
    Parse(url::ParseError),
    NotHttp,
}

impl_from_err! {
    {
        url::ParseError => Parse,
    } for HttpUrlError
}

impl Display for HttpUrlError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use HttpUrlError::*;
        match self {
            Parse(e) => write!(f, "Parse: {}", e),
            NotHttp => write!(f, "Not HTTP"),
        }
    }
}

impl Error for HttpUrlError {}