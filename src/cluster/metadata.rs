use std::{
    convert::TryInto,
    fmt,
    path::{
        Component,
        Path,
        PathBuf,
    },
    sync::Arc,
};

use futures::stream::{
    Stream,
    StreamExt,
};
use serde::{
    de::DeserializeOwned,
    Deserialize,
    Serialize,
};
use tokio::{
    fs,
    io,
    process::Command,
};
use tokio_stream::wrappers::ReadDirStream;

use crate::{
    error::{
        LocationError,
        LocationParseError,
        MetadataReadError,
        SerdeError,
    },
    file::Location,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum MetadataTypes {
    Path(MetadataPath),
}

impl MetadataTypes {
    pub async fn write<T>(
        &self,
        path: impl AsRef<Path>,
        payload: &T,
    ) -> Result<(), MetadataReadError>
    where
        T: Serialize,
    {
        match self {
            MetadataTypes::Path(meta_path) => meta_path.write(path, payload).await,
        }
    }

    pub async fn read<T>(&self, path: impl AsRef<Path>) -> Result<T, MetadataReadError>
    where
        T: DeserializeOwned,
    {
        match self {
            MetadataTypes::Path(meta_path) => meta_path.read(path).await,
        }
    }

    pub async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = io::Result<FileOrDirectory>> + 'static, MetadataReadError> {
        match self {
            MetadataTypes::Path(meta_path) => {
                Ok(meta_path.list(path).await.map_err(LocationError::from)?)
            },
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MetadataPath {
    #[serde(default)]
    pub format: MetadataFormat,
    pub path: PathBuf,
    pub put_script: Option<String>,
    #[serde(default)]
    pub fail_on_script_error: bool,
}

impl MetadataPath {
    pub async fn write<T>(
        &self,
        path: impl AsRef<Path>,
        payload: &T,
    ) -> Result<(), MetadataReadError>
    where
        T: Serialize,
    {
        let path = self.sub_path(path);
        let payload = self.format.to_string(payload)?;
        fs::write(path, payload)
            .await
            .map_err(LocationError::from)?;
        if let Some(put_script) = &self.put_script {
            let res = Command::new("/bin/sh")
                .arg("-c")
                .arg(put_script)
                .current_dir(&self.path)
                .spawn()
                .unwrap()
                .wait()
                .await;
            if self.fail_on_script_error {
                if let Err(err) = res {
                    return Err(MetadataReadError::PostExec(err));
                }
            }
        }
        Ok(())
    }

    pub async fn read<T>(&self, path: impl AsRef<Path>) -> Result<T, MetadataReadError>
    where
        T: DeserializeOwned,
    {
        let path = self.sub_path(path);
        let bytes = fs::read(path).await.map_err(LocationError::from)?;
        Ok(self.format.from_bytes(&bytes)?)
    }

    pub async fn list(
        &self,
        path: &Path,
    ) -> io::Result<impl Stream<Item = io::Result<FileOrDirectory>> + 'static> {
        let path = self.sub_path(path);
        let self_arc = Arc::new(self.clone());
        let dir_reader = fs::read_dir(&path).await?;
        let s = ReadDirStream::new(dir_reader)
            .map(move |entry_res| (entry_res, self_arc.clone()))
            .filter_map(|(entry_res, self_arc)| async move {
                let entry = match entry_res {
                    Ok(e) => e,
                    Err(err) => return Some(Err(err)),
                };
                match FileOrDirectory::from_local_path(entry.path().clone()).await {
                    Ok(mut file_or_dir) => {
                        // Remove parent path prefix
                        let sub_path: &PathBuf = file_or_dir.as_ref();
                        let mut parent_components = self_arc.path.components();
                        let mut sub_components = sub_path.components().peekable();
                        loop {
                            match (parent_components.next(), sub_components.peek()) {
                                (Some(x), Some(y)) if x == *y => {
                                    sub_components.next();
                                },
                                (Some(_), None) => {
                                    panic!("Parent path length exceeds child length");
                                },
                                _ => {
                                    break;
                                },
                            }
                        }
                        let new_sub_path: PathBuf = sub_components.collect();
                        let sub_path: &mut PathBuf = file_or_dir.as_mut();
                        *sub_path = new_sub_path;
                        Some(Ok::<_, io::Error>(file_or_dir))
                    },
                    Err(err) if err.kind() == io::ErrorKind::NotFound => None,
                    Err(err) => Some(Err(err)),
                }
            });
        Ok(s)
    }

    fn sub_path(&self, path: impl AsRef<Path>) -> PathBuf {
        let mut new_path = self.path.clone();
        new_path.extend(
            path.as_ref()
                .components()
                .filter(|c| matches!(c, Component::Normal(_))),
        );
        new_path
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MetadataFormat {
    Json,
    JsonPretty,
    JsonStrict,
    Yaml,
}

impl Default for MetadataFormat {
    fn default() -> Self {
        MetadataFormat::JsonPretty
    }
}

impl MetadataFormat {
    pub fn to_string<T>(&self, payload: &T) -> Result<String, SerdeError>
    where
        T: Serialize,
    {
        use MetadataFormat::*;
        Ok(match self {
            Json | JsonStrict => serde_json::to_string(payload)?,
            JsonPretty => serde_json::to_string_pretty(payload)?,
            Yaml => serde_yaml::to_string(payload)?,
        })
    }

    pub fn from_bytes<T, U>(&self, v: &T) -> Result<U, SerdeError>
    where
        T: AsRef<[u8]>,
        U: DeserializeOwned,
    {
        use MetadataFormat::*;
        Ok(match self {
            JsonStrict => serde_json::from_slice(v.as_ref())?,
            Json | JsonPretty | Yaml => serde_yaml::from_slice(v.as_ref())?,
        })
    }

    pub async fn from_location<T>(
        &self,
        location: impl TryInto<Location, Error = impl Into<LocationParseError>>,
    ) -> Result<T, MetadataReadError>
    where
        T: DeserializeOwned,
    {
        let location: Location = TryInto::try_into(location).map_err(|err| err.into())?;
        let bytes = location.read().await?;
        Ok(self.from_bytes(&bytes)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum FileOrDirectory {
    Directory(PathBuf),
    File(PathBuf),
}

impl fmt::Display for FileOrDirectory {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let path: &PathBuf = self.as_ref();
        write!(f, "{}", path.display())
    }
}

impl FileOrDirectory {
    pub async fn from_local_path(path: PathBuf) -> io::Result<Self> {
        let metadata = fs::metadata(&path).await?;
        if metadata.is_dir() {
            Ok(FileOrDirectory::Directory(path))
        } else if metadata.is_file() {
            Ok(FileOrDirectory::File(path))
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                "Not a file or directory",
            ))
        }
    }
}

impl AsRef<PathBuf> for FileOrDirectory {
    fn as_ref(&self) -> &PathBuf {
        use FileOrDirectory::*;
        match self {
            File(path) => &path,
            Directory(path) => &path,
        }
    }
}

impl AsMut<PathBuf> for FileOrDirectory {
    fn as_mut(&mut self) -> &mut PathBuf {
        use FileOrDirectory::*;
        match self {
            File(ref mut path) => path,
            Directory(ref mut path) => path,
        }
    }
}

impl From<FileOrDirectory> for PathBuf {
    fn from(f: FileOrDirectory) -> Self {
        use FileOrDirectory::*;
        match f {
            File(path) => path,
            Directory(path) => path,
        }
    }
}
