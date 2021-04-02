use std::{
    convert::TryInto,
    num::NonZeroUsize,
    sync::Arc,
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
        filename: &str,
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
        self.metadata.write(&filename, &file_ref).await.unwrap();
        Ok(())
    }

    pub async fn get_file_ref(&self, filename: &str) -> Result<FileReference, MetadataReadError> {
        self.metadata.read(&filename).await
    }

    pub async fn read_file(
        &self,
        filename: &str,
    ) -> Result<impl AsyncRead + Unpin, MetadataReadError> {
        let file_ref = self.get_file_ref(filename).await?;
        let (reader, mut writer) = tokio::io::duplex(1 << 24);
        tokio::spawn(async move { file_ref.to_writer(&mut writer).await });
        Ok(reader)
    }

    pub async fn get_destination(&self, profile: &ClusterProfile) -> impl CollectionDestination {
        self.destinations.clone().with_profile(profile.clone())
    }

    pub fn get_profile<'a, T>(&self, profile: T) -> Option<&'_ ClusterProfile>
    where
        T: Into<Option<&'a str>>,
    {
        self.profiles.get(profile)
    }
}
