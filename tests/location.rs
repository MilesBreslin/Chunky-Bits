use std::{
    error::Error,
    str::FromStr,
};

use chunky_bits::file::Location;
use tempfile::tempdir;
use tokio::io::AsyncReadExt;
use url::Url;

const DEFAULT_PAYLOAD: &[u8] = "HELLO WORLD".as_bytes();

mod http_server {
    use std::{
        collections::HashMap,
        convert::Infallible,
        net::SocketAddr,
        ops::Deref,
        sync::Arc,
    };

    use bytes::Bytes;
    use tokio::{
        sync::{
            oneshot,
            Mutex,
        },
        task::JoinHandle,
    };
    use warp::{
        path::FullPath,
        Filter,
    };

    use super::*;

    pub struct HttpServer(Url, oneshot::Sender<()>, JoinHandle<()>);
    impl Deref for HttpServer {
        type Target = Url;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }
    impl HttpServer {
        pub async fn kill(self) {
            let HttpServer(_, tx, handle) = self;
            drop(tx);
            let _ = handle.await;
        }
    }
    /// Port is required since the tests run in parallel
    pub fn start(port: usize) -> HttpServer {
        let url = Url::parse(&format!("http://127.0.0.1:{}", port)).unwrap();
        let addr: SocketAddr = url.socket_addrs(|| None).unwrap().first().unwrap().clone();
        let content: Arc<Mutex<HashMap<String, Vec<u8>>>> = Default::default();
        let content_put = content.clone();
        let get_filter = warp::get()
            .map(move || content.clone())
            .and(warp::path::full())
            .and_then(
                |content: Arc<Mutex<HashMap<String, Vec<u8>>>>, path: FullPath| async move {
                    let content = content.lock().await;
                    match content.get(&path.as_str().to_string()) {
                        Some(bytes) => Ok::<Vec<u8>, Infallible>(bytes.clone()),
                        None => Ok(DEFAULT_PAYLOAD.to_owned()),
                    }
                },
            );
        let put_filter =
            warp::put()
                .map(move || content_put.clone())
                .and(warp::path::full())
                .and(warp::body::bytes())
                .and_then(
                    |content: Arc<Mutex<HashMap<String, Vec<u8>>>>,
                     path: FullPath,
                     bytes: Bytes| async move {
                        let mut content = content.lock().await;
                        content.insert(path.as_str().to_string(), bytes.to_vec());
                        Ok::<_, Infallible>(warp::reply())
                    },
                );
        let (tx, rx) = oneshot::channel::<()>();
        let (_, server) = warp::serve(get_filter.or(put_filter))
            .try_bind_with_graceful_shutdown(addr, async {
                let _ = rx.await;
            })
            .unwrap();

        let handle = tokio::spawn(server);

        HttpServer(url, tx, handle)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn location_fs_read() -> Result<(), Box<dyn Error>> {
    let location = Location::from_str("/bin/sh")?;
    let bytes = location.read().await?;
    assert!(bytes.len() > 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn location_fs_write() -> Result<(), Box<dyn Error>> {
    let payload = DEFAULT_PAYLOAD;
    let dir = tempdir()?;
    let dir_location = Location::from(dir.path());
    let location = dir_location.write_subfile("TESTFILE", payload).await?;
    let payload_read = location.read().await?;
    dir.close()?;
    assert_eq!(
        format!("{}/TESTFILE", dir_location),
        format!("{}", location),
    );
    assert!(location.is_child_of(&dir_location));
    assert_eq!(payload, payload_read);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn location_fs_reader_writer() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let location = Location::from_str(&format!("{}/hello", dir.path().display()))?;

    let payload = DEFAULT_PAYLOAD;
    let mut reader = payload.clone();
    let len = location
        .write_from_reader_with_context(&Default::default(), &mut reader)
        .await
        .unwrap();
    assert_eq!(len, payload.len() as u64);

    let mut reader = location.reader_with_context(&Default::default()).await?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, payload);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn location_http_read() -> Result<(), Box<dyn Error>> {
    let server = http_server::start(64000);
    let location = Location::from_str(&format!("{}", *server))?;
    let bytes = location.read().await?;
    assert_eq!(bytes, DEFAULT_PAYLOAD);
    server.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn location_http_write() -> Result<(), Box<dyn Error>> {
    let server = http_server::start(64001);
    let location = Location::from_str(&format!("{}hello", *server))?;
    let bytes = location.read().await?;
    assert_eq!(bytes, DEFAULT_PAYLOAD);

    let new_bytes = "NEW DATA".as_bytes();
    location.write(new_bytes).await.unwrap();
    let bytes = location.read().await.unwrap();
    assert_eq!(bytes, new_bytes);
    server.kill().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn location_http_reader_writer() -> Result<(), Box<dyn Error>> {
    let server = http_server::start(64003);
    let location = Location::from_str(&format!("{}hello", *server))?;
    let mut reader = location.reader_with_context(&Default::default()).await?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, DEFAULT_PAYLOAD);

    let new_bytes = "NEW DATA".as_bytes();
    let mut reader = new_bytes.clone();
    let len = location
        .write_from_reader_with_context(&Default::default(), &mut reader)
        .await
        .unwrap();
    assert_eq!(len, new_bytes.len() as u64);
    let mut reader = location.reader_with_context(&Default::default()).await?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(bytes, new_bytes);
    server.kill().await;
    Ok(())
}
