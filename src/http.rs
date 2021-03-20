use std::{
    convert::TryFrom,
    fmt,
    time::Duration,
};

use async_trait::async_trait;
use reqwest::{
    Client,
    ClientBuilder,
    RequestBuilder,
};
use serde::{
    Deserialize,
    Serialize,
};
use url::Url;

use crate::{
    file::{
        CollectionDestination,
        HttpUrl,
        Location,
        ShardWriter,
    },
    Error,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(from = "HttpUrl")]
#[serde(into = "HttpUrl")]
pub struct HttpFiles {
    base: HttpUrl,
    client: Client,
}

impl HttpFiles {
    pub fn new(url: HttpUrl) -> Self {
        Self {
            base: url,
            client: ClientBuilder::new()
                .connect_timeout(Duration::from_secs(5))
                .build()
                .unwrap(),
        }
    }
}

impl From<HttpUrl> for HttpFiles {
    fn from(u: HttpUrl) -> Self {
        Self::new(u)
    }
}

impl Into<HttpUrl> for HttpFiles {
    fn into(self) -> HttpUrl {
        self.base
    }
}

#[async_trait]
impl CollectionDestination for HttpFiles {
    async fn get_writers<T: fmt::Display + Send + Sync + 'static>(
        &self,
        addrs: &[T],
    ) -> Result<
        Vec<(
            Box<dyn ShardWriter + Unpin + Send + Sync + 'static>,
            Vec<Location>,
        )>,
        Error,
    > {
        let mut fds: Vec<(
            Box<dyn ShardWriter + Unpin + Send + Sync + 'static>,
            Vec<Location>,
        )> = vec![];
        for addr in addrs {
            let mut url: Url = self.base.clone().into();
            let addr = format!("{}", addr);
            url.path_segments_mut().unwrap().push(&addr);
            fds.push((
                Box::new(PutWriter {
                    inner: Some(self.client.put(url.clone())),
                }),
                vec![Location::Http(HttpUrl::try_from(url).unwrap())],
            ))
        }
        Ok(fds)
    }
}

struct PutWriter {
    inner: Option<RequestBuilder>,
}

#[async_trait]
impl ShardWriter for PutWriter {
    async fn write_shard(&mut self, bytes: &[u8]) -> Result<(), Error> {
        match self.inner.take() {
            Some(cli) => {
                let response = cli
                    .body(bytes.iter().copied().collect::<Vec<u8>>())
                    .send()
                    .await?;
                let status = response.status();
                if status.is_success() {
                    Ok(())
                } else {
                    Err(Error::HttpStatus(status))
                }
            },
            None => Err(Error::ExpiredWriter),
        }
    }
}
