pub mod cluster;
pub mod file;
use std::convert::Infallible;

use tokio::{
    fs::{
        self,
        File,
    },
    io::{
        self,
        AsyncWrite,
        AsyncWriteExt,
    },
    task,
};

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    JoinError(task::JoinError),
    Erasure(reed_solomon_erasure::Error),
    Reqwest(reqwest::Error),
    HttpStatus(reqwest::StatusCode),
    ExpiredWriter,
    NotEnoughWriters,
    UnknownError,
    Unimplemented,
    NotHttp,
    UrlParseError(url::ParseError),
    Json(serde_json::Error),
    Yaml(serde_yaml::Error),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}
impl From<task::JoinError> for Error {
    fn from(e: task::JoinError) -> Self {
        Error::JoinError(e)
    }
}
impl From<reed_solomon_erasure::Error> for Error {
    fn from(e: reed_solomon_erasure::Error) -> Self {
        Error::Erasure(e)
    }
}
impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Reqwest(e)
    }
}
impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Error::UrlParseError(e)
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e)
    }
}
impl From<serde_yaml::Error> for Error {
    fn from(e: serde_yaml::Error) -> Self {
        Error::Yaml(e)
    }
}
impl From<Infallible> for Error {
    fn from(_: Infallible) -> Self {
        panic!("Infallible")
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for Error {}
