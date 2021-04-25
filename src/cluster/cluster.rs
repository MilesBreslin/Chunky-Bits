use std::{
    convert::TryInto,
    path::Path,
};

use futures::stream::Stream;
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    io,
    io::AsyncRead,
};

use crate::{
    cluster::{
        ClusterNodes,
        ClusterProfile,
        ClusterProfiles,
        Destination,
        DestinationInner,
        FileOrDirectory,
        MetadataFormat,
        MetadataTypes,
        Tunables,
    },
    error::{
        ClusterError,
        LocationParseError,
        MetadataReadError,
    },
    file::{
        new_profiler,
        FileReference,
        FileWriteBuilder,
        Location,
        ProfileReport,
        ProfileReporter,
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
    #[serde(default)]
    #[serde(alias = "tunable")]
    #[serde(alias = "tuning")]
    pub tunables: Tunables,
}

impl Cluster {
    pub async fn from_location(
        location: impl TryInto<Location, Error = impl Into<LocationParseError>>,
    ) -> Result<Cluster, MetadataReadError> {
        MetadataFormat::Yaml.from_location(location).await
    }

    pub fn get_file_writer(&self, profile: &ClusterProfile) -> FileWriteBuilder<Destination> {
        let destination = self.get_destination(profile);
        FileReference::write_builder()
            .destination(destination)
            .chunk_size((1_usize) << profile.get_chunk_size())
            .data_chunks(profile.get_data_chunks())
    }

    pub async fn write_file_ref(
        &self,
        path: impl AsRef<Path>,
        file_ref: &FileReference,
    ) -> Result<(), ClusterError> {
        self.metadata.write(path, &file_ref).await?;
        Ok(())
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
        let mut file_ref = self.get_file_writer(profile).write(reader).await?;
        file_ref.content_type = content_type;
        self.metadata.write(path, &file_ref).await.unwrap();
        Ok(())
    }

    pub async fn write_file_with_report<R>(
        &self,
        path: impl AsRef<Path>,
        reader: &mut R,
        profile: &ClusterProfile,
        content_type: Option<String>,
    ) -> (ProfileReport, Result<(), ClusterError>)
    where
        R: AsyncRead + Unpin,
    {
        let (reporter, destination) = self.get_destination_with_profiler(profile);
        let result = FileReference::write_builder()
            .destination(destination)
            .chunk_size((1_usize) << profile.get_chunk_size())
            .data_chunks(profile.get_data_chunks())
            .parity_chunks(profile.get_parity_chunks())
            .write(reader)
            .await;
        match result {
            Ok(mut file_ref) => {
                file_ref.content_type = content_type;
                self.metadata.write(path, &file_ref).await.unwrap();
                (reporter.profile().await, Ok(()))
            },
            Err(err) => (reporter.profile().await, Err(err.into())),
        }
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
        let reader = file_ref.read_builder_owned().reader_owned();
        Ok(reader)
    }

    pub fn get_destination(&self, profile: &ClusterProfile) -> Destination {
        let inner = DestinationInner {
            nodes: self.destinations.clone(),
            location_context: self.tunables.as_ref().clone(),
            profile: profile.clone(),
        };
        Destination(inner.into())
    }

    pub fn get_destination_with_profiler(
        &self,
        profile: &ClusterProfile,
    ) -> (ProfileReporter, Destination) {
        let (profiler, reporter) = new_profiler();
        let location_context = self
            .tunables
            .generate_location_context_builder()
            .profiler(profiler)
            .build();
        (
            reporter,
            Destination(
                DestinationInner {
                    nodes: self.destinations.clone(),
                    location_context,
                    profile: profile.clone(),
                }
                .into(),
            ),
        )
    }

    pub fn get_profile<'a>(
        &self,
        profile: impl Into<Option<&'a str>>,
    ) -> Option<&'_ ClusterProfile> {
        self.profiles.get(profile)
    }

    pub async fn list_files(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = io::Result<FileOrDirectory>> + 'static, MetadataReadError> {
        self.metadata.list(path).await
    }
}
