use std::{
    convert::Infallible,
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
) -> Result<Response<Body>, Infallible> {
    Ok(match cluster.get_file_ref(path.as_str()).await {
        Ok(file_ref) => {
            let length = file_ref.length.clone();
            let content_type = file_ref.content_type.clone();
            let stream = file_ref.read_builder_owned().stream_reader_owned();
            let mut response_builder = Response::builder().status(StatusCode::OK);
            if let Some(length) = length {
                response_builder = response_builder.header("Content-Length", length);
            }
            if let Some(content_type) = content_type {
                response_builder = response_builder.header("Content-Type", content_type);
            }
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
