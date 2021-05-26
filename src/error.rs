use std::{
    convert::Infallible,
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
        impl From<Infallible> for $parent {
            fn from(e: Infallible) -> Self {
                match e {}
            }
        }
    };
    ({ $($error_type:ty => $variant:ident),*, } for $parent:ident) => {
        impl_from_err!{
            {
                $($error_type => $variant),*
            } for $parent
        }
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
pub enum ShardError {
    NotEnoughAvailability,
    NotEnoughChunks,
    LocationError {
        location: Location,
        error: LocationError,
    },
}

impl Display for ShardError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use ShardError::*;
        match self {
            NotEnoughAvailability => write!(f, "Not enough writer availability"),
            NotEnoughChunks => write!(f, "Not enough chunks"),
            LocationError { location, error } => write!(f, "<{}> {}", location, error),
        }
    }
}

impl Error for ShardError {}

#[derive(Debug)]
/// Different types of location errors
pub enum LocationError {
    IoError(io::Error),
    HttpStatus(reqwest::StatusCode),
    HttpError(reqwest::Error),
    WriteToRange,
}

impl Display for LocationError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use LocationError::*;
        match self {
            IoError(e) => write!(f, "IO: {}", e),
            HttpStatus(status) => write!(f, "Http Status: {}", status),
            HttpError(e) => write!(f, "Http Error: {}", e),
            WriteToRange => write!(f, "Attempted to write to a location with a specified range"),
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

impl Display for FileReadError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use FileReadError::*;
        match self {
            Erasure(e) => write!(f, "Erasure: {}", e),
            FilePart(e) => write!(f, "FilePart: {}", e),
            WriterError(e) => write!(f, "WriterError: {}", e),
        }
    }
}

impl_from_err! {
    {
        reed_solomon_erasure::Error => Erasure,
        ShardError => FilePart,
        io::Error => WriterError,
    } for FileReadError
}

impl Error for FileReadError {}

#[derive(Debug)]
pub enum ClusterError {
    FileWrite(FileWriteError),
    FileMetadataRead(MetadataReadError),
    FileRead(FileReadError),
}

impl Display for ClusterError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use ClusterError::*;
        match self {
            FileWrite(e) => write!(f, "FileWrite: {}", e),
            FileMetadataRead(e) => write!(f, "FileMetadataRead: {:?}", e),
            FileRead(e) => write!(f, "FileRead: {}", e),
        }
    }
}

impl Error for ClusterError {}

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
    ExitCode(i32),
    FileRead(LocationError),
    InvalidLocation(LocationParseError),
    IoError(io::Error),
    PostExec(io::Error),
    Serde(SerdeError),
    Signal,
}

impl_from_err! {
    {
        LocationParseError => InvalidLocation,
        LocationError => FileRead,
        SerdeError => Serde,
    } for MetadataReadError
}

impl Display for MetadataReadError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use MetadataReadError::*;
        match self {
            ExitCode(code) => write!(f, "Exit Code: {}", code),
            FileRead(e) => write!(f, "File Read: {}", e),
            InvalidLocation(e) => write!(f, "Invalid Location: {}", e),
            IoError(e) => write!(f, "IO Error: {}", e),
            PostExec(e) => write!(f, "Post-Execution: {}", e),
            Serde(e) => write!(f, "Serde: {}", e),
            Signal => write!(f, "Signal"),
        }
    }
}

impl std::error::Error for MetadataReadError {}

#[derive(Debug)]
pub enum LocationParseError {
    Parse(url::ParseError),
    FilePathNotAbsolute,
    InvalidScheme,
    NotHttp,
}

impl_from_err! {
    {
        url::ParseError => Parse,
    } for LocationParseError
}

impl Display for LocationParseError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        use LocationParseError::*;
        match self {
            Parse(e) => write!(f, "Parse: {}", e),
            FilePathNotAbsolute => write!(f, "File path not absolute"),
            InvalidScheme => write!(f, "Invalid scheme"),
            NotHttp => write!(f, "Not HTTP"),
        }
    }
}

impl Error for LocationParseError {}
