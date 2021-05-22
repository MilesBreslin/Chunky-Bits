use std::{
    convert::Infallible,
    str::FromStr,
    sync::Arc,
};

use futures::stream::{
    Stream,
    StreamExt,
};
use tokio::io;
use warp::{
    http::{
        Response,
        StatusCode,
    },
    hyper::body::Body,
    reject::Rejection,
    Filter,
};

use crate::{
    cluster::Cluster,
    error::MetadataReadError,
};

async fn index_get(
    cluster: Arc<Cluster>,
    path: warp::path::FullPath,
    range: Option<HttpRange>,
) -> Result<Response<Body>, Infallible> {
    Ok(match cluster.get_file_ref(path.as_str()).await {
        Ok(file_ref) => {
            let mut response_builder = Response::builder().status(StatusCode::OK);
            let mut file_reader = file_ref.read_builder_owned();

            if let Some(HttpRange { start, end }) = range {
                match (start, end) {
                    (Some(start), Some(end)) => {
                        file_reader = file_reader.seek(start).take(end - start);
                    },
                    (Some(start), None) => {
                        file_reader = file_reader.seek(start);
                    },
                    (None, Some(end)) => {
                        let file_len = file_reader.file_reference().len_bytes();
                        if end > file_len {
                            return Ok(Response::builder()
                                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                                .body(Body::empty())
                                .unwrap());
                        }
                        let start = file_len - end;
                        file_reader = file_reader.seek(start).take(end);
                    },
                    (None, None) => panic!("Invalid range"),
                }
                if file_reader.len_bytes() == 0 {
                    return Ok(Response::builder()
                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                        .body(Body::empty())
                        .unwrap());
                }
                let seek = file_reader.get_seek();
                let end = seek + file_reader.len_bytes();
                response_builder = response_builder.header(
                    "Content-Range",
                    format!(
                        "{}-{}/{}",
                        seek,
                        end,
                        file_reader.file_reference().len_bytes(),
                    ),
                );
                response_builder = response_builder.status(StatusCode::PARTIAL_CONTENT);
            }

            response_builder = response_builder.header("Content-Length", file_reader.len_bytes());

            if let Some(content_type) = &file_reader.file_reference().content_type {
                response_builder = response_builder.header("Content-Type", content_type);
            }

            let stream = file_reader.stream_reader_owned();
            response_builder.body(Body::wrap_stream(stream)).unwrap()
        },
        Err(MetadataReadError::FileRead(_)) => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap(),
        _ => Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::empty())
            .unwrap(),
    })
}

async fn index_put(
    cluster: Arc<Cluster>,
    path: warp::path::FullPath,
    content_type: Option<String>,
    body: impl Stream<Item = Result<impl bytes::buf::Buf, warp::Error>> + Unpin,
) -> Result<impl warp::Reply, Infallible> {
    let profile = cluster.get_profile(None).unwrap();
    let write = cluster
        .write_file(
            path.as_str(),
            &mut tokio_util::io::StreamReader::new(
                body.map(|res| -> io::Result<_> { Ok(res.unwrap()) }),
            ),
            profile,
            content_type,
        )
        .await;
    Ok(match write {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    })
}

pub fn cluster_filter_get(
    cluster: impl Into<Arc<Cluster>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    let cluster: Arc<Cluster> = cluster.into();
    warp::get()
        .or(warp::head())
        .map(move |_| cluster.clone())
        .and(warp::path::full())
        .and(get_http_range())
        .and_then(index_get)
}

pub fn cluster_filter_put(
    cluster: impl Into<Arc<Cluster>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    let cluster: Arc<Cluster> = cluster.into();
    warp::put()
        .map(move || cluster.clone())
        .and(warp::path::full())
        .and(warp::header::optional::<String>("content-type"))
        .and(warp::body::stream())
        .and_then(index_put)
}

pub fn cluster_filter(
    cluster: impl Into<Arc<Cluster>>,
) -> impl Filter<Extract = impl warp::Reply, Error = Rejection> + Clone {
    let cluster: Arc<Cluster> = cluster.into();
    cluster_filter_get(cluster.clone()).or(cluster_filter_put(cluster))
}

#[derive(Debug, Default)]
pub(crate) struct HttpRange {
    start: Option<u64>,
    end: Option<u64>,
}

#[derive(Debug)]
pub(crate) enum HttpRangeError {
    InvalidFormat,
    InvalidInteger,
    InvalidLength,
    MultiRange,
    NoRangeSpecified,
    UnknownUnit,
}

impl FromStr for HttpRange {
    type Err = HttpRangeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('=') {
            Some(("bytes", suffix)) => {
                if let Some(_) = suffix.split_once(',') {
                    return Err(HttpRangeError::MultiRange);
                }
                let mut split_iter = suffix.split('-');
                match (split_iter.next(), split_iter.next(), split_iter.next()) {
                    (Some(start), Some(end), None) => {
                        let start = if !start.is_empty() {
                            Some(u64::from_str(start))
                        } else {
                            None
                        };
                        let end = if !end.is_empty() {
                            Some(u64::from_str(end))
                        } else {
                            None
                        };
                        let (start, end) = match (start.transpose(), end.transpose()) {
                            (Ok(start), Ok(end)) => (start, end),
                            _ => {
                                return Err(HttpRangeError::InvalidInteger);
                            },
                        };
                        match (start, end) {
                            (Some(start), Some(end)) => {
                                if start >= end {
                                    return Err(HttpRangeError::InvalidLength);
                                }
                            },
                            (None, None) => {
                                return Err(HttpRangeError::NoRangeSpecified);
                            },
                            _ => {},
                        }
                        Ok(HttpRange { start, end })
                    },
                    _ => {
                        return Err(HttpRangeError::InvalidFormat);
                    },
                }
            },
            Some((_unit, _suffix)) => Err(HttpRangeError::UnknownUnit),
            None => Err(HttpRangeError::InvalidFormat),
        }
    }
}

pub(crate) fn get_http_range(
) -> impl Filter<Extract = (Option<HttpRange>,), Error = Rejection> + Clone {
    warp::header::optional::<HttpRange>("range")
}
