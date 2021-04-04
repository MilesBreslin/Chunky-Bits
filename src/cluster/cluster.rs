use std::{
    convert::TryInto,
    num::NonZeroUsize,
    path::{
        Path,
        PathBuf,
    },
    sync::Arc,
};

use futures::{
    future::{
        BoxFuture,
        FutureExt,
    },
    stream::{
        FuturesUnordered,
        StreamExt,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::io::AsyncRead;

use crate::{
    cluster::{
        ClusterNodes,
        ClusterProfile,
        ClusterProfiles,
        FileOrDirectory,
        MetadataFormat,
        MetadataTypes,
    },
    error::{
        ClusterError,
        LocationParseError,
        MetadataReadError,
    },
    file::{
        self,
        CollectionDestination,
        FileReference,
        Location,
    },
};

#[derive(Clone, Serialize, Deserialize)]
pub struct Cluster {
    #[serde(alias = "destination")]
    #[serde(alias = "nodes")]
    #[serde(alias = "node")]
    pub destinations: ClusterNodes,
    #[serde(alias = "metadata")]
    pub metadata: MetadataTypes,
    pub profiles: ClusterProfiles,
}

impl Cluster {
    pub async fn from_location(
        location: impl TryInto<Location, Error = impl Into<LocationParseError>>,
    ) -> Result<Cluster, MetadataReadError> {
        MetadataFormat::Yaml.from_location(location).await
    }

    pub async fn write_file<R>(
        &self,
        path: impl AsRef<Path>,
        reader: &mut R,
        profile: &ClusterProfile,
        content_type: Option<String>,
    ) -> Result<(), ClusterError>
    where
        R: AsyncRead + Unpin,
    {
        let destination = self.get_destination(profile).await;
        let mut file_ref = file::FileReference::from_reader(
            reader,
            Arc::new(destination),
            1 << profile.get_chunk_size(),
            profile.get_data_chunks(),
            profile.get_parity_chunks(),
            NonZeroUsize::new(50).unwrap(),
        )
        .await?;
        file_ref.content_type = content_type;
        self.metadata.write(path, &file_ref).await.unwrap();
        Ok(())
    }

    pub async fn get_file_ref(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<FileReference, MetadataReadError> {
        self.metadata.read(path).await
    }

    pub async fn read_file(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<impl AsyncRead + Unpin, MetadataReadError> {
        let file_ref = self.get_file_ref(path).await?;
        let (reader, mut writer) = tokio::io::duplex(1 << 24);
        tokio::spawn(async move { file_ref.to_writer(&mut writer).await });
        Ok(reader)
    }

    pub async fn get_destination(&self, profile: &ClusterProfile) -> impl CollectionDestination {
        self.destinations.clone().with_profile(profile.clone())
    }

    pub fn get_profile<'a>(
        &self,
        profile: impl Into<Option<&'a str>>,
    ) -> Option<&'_ ClusterProfile> {
        self.profiles.get(profile)
    }

    pub async fn list_files(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Vec<FileOrDirectory>, MetadataReadError> {
        self.metadata.list(path).await
    }

    pub async fn list_files_recursive(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<Vec<FileOrDirectory>, MetadataReadError> {
        self._list_files_recursive_inner(path.as_ref().to_owned())
            .await
    }

    fn _list_files_recursive_inner(
        &self,
        path: PathBuf,
    ) -> BoxFuture<'_, Result<Vec<FileOrDirectory>, MetadataReadError>> {
        async move {
            let mut items = self.list_files(&path).await?;
            let mut recursions = FuturesUnordered::<BoxFuture<_>>::new();
            for item in items.clone() {
                if let FileOrDirectory::Directory(path) = item {
                    recursions.push(self._list_files_recursive_inner(path).boxed());
                }
            }
            while let Some(result) = recursions.next().await {
                items.extend(result?);
            }
            Ok(items)
        }
        .boxed()
    }
}
