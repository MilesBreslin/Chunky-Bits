use std::{
    fmt,
    path::{
        Path,
        PathBuf,
    },
};

use async_trait::async_trait;
use serde::{
    Deserialize,
    Serialize,
};
use tokio::fs::File;

use crate::{
    file::{
        CollectionDestination,
        Location,
        ShardWriter,
    },
    Error,
};

#[derive(Clone, Serialize, Deserialize)]
pub struct LocalFiles(PathBuf);

impl LocalFiles {
    pub fn new<T>(path: &T) -> Self
    where
        T: AsRef<Path>,
    {
        LocalFiles(path.as_ref().to_path_buf())
    }
}

#[async_trait]
impl CollectionDestination for LocalFiles {
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
            let addr = format!("{}/{}", &self.0.display(), addr);
            fds.push((Box::new(File::create(&addr).await?), vec![Location::Local(
                addr.into(),
            )]))
        }
        Ok(fds)
    }
}
