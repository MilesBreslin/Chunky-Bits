use std::{
    collections::HashSet,
    convert::TryInto,
    path::{
        Path,
        PathBuf,
    },
};

use futures::{
    future::{
        BoxFuture,
        FutureExt,
    },
    stream::{
        self,
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
        hash::AnyHash,
        Chunk,
        CollectionDestination,
        FilePart,
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
        let mut file_ref = FileReference::write_builder()
            .destination(destination)
            .chunk_size((1 as usize) << profile.get_chunk_size())
            .data_chunks(profile.get_data_chunks())
            .parity_chunks(profile.get_parity_chunks())
            .write(reader)
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

    pub async fn get_destination(
        &self,
        profile: &ClusterProfile,
    ) -> impl CollectionDestination + Send + Sync {
        self.destinations.clone().with_profile(profile.clone())
    }

    pub fn get_profile<'a>(
        &self,
        profile: impl Into<Option<&'a str>>,
    ) -> Option<&'_ ClusterProfile> {
        self.profiles.get(profile)
    }

    pub async fn get_all_hashes(&self) -> Result<HashSet<AnyHash>, MetadataReadError> {
        let files: HashSet<PathBuf> = self
            .list_files_recursive("/")
            .await?
            .into_iter()
            .filter_map(|file_or_directory| match file_or_directory {
                FileOrDirectory::File(path) => Some(path),
                _ => None,
            })
            .collect();
        let mut file_hashes = stream::iter(files.iter().map(|file| self.get_file_ref(file)))
            .buffered(10)
            .flat_map(|file_result| {
                let file_ref = match file_result {
                    Ok(file_ref) => file_ref,
                    Err(err) => {
                        return stream::once(async move { Err(err) }).boxed();
                    },
                };
                let FileReference { parts, .. } = file_ref;
                stream::iter(parts.into_iter().flat_map(|FilePart { data, parity, .. }| {
                    data.into_iter()
                        .chain(parity.into_iter())
                        .map(|Chunk { hash, .. }| Ok(hash))
                }))
                .boxed()
            });
        let mut out = HashSet::new();
        while let Some(hash_result) = file_hashes.next().await {
            match hash_result {
                Ok(hash) => {
                    out.insert(hash);
                },
                Err(err) => {
                    return Err(err);
                },
            }
        }
        Ok(out)
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
