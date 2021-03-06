use std::{
    convert::TryInto,
    fmt,
    ops::Deref,
    path::{
        Component,
        Path,
        PathBuf,
    },
};

use futures::{
    future,
    stream::{
        self,
        Stream,
        StreamExt,
    },
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
#[serde(rename_all = "kebab-case")]
#[serde(tag = "type")]
pub enum MetadataTypes {
    Path(MetadataPath),
    Git(MetadataGit),
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
            MetadataTypes::Git(meta_git) => meta_git.write(path, payload).await,
        }
    }

    pub async fn read<T>(&self, path: impl AsRef<Path>) -> Result<T, MetadataReadError>
    where
        T: DeserializeOwned,
    {
        match self {
            MetadataTypes::Path(meta_path) => meta_path.read(path).await,
            MetadataTypes::Git(meta_git) => meta_git.read(path).await,
        }
    }

    pub async fn list(
        &self,
        path: &Path,
    ) -> Result<impl Stream<Item = io::Result<FileOrDirectory>> + 'static, MetadataReadError> {
        match self {
            MetadataTypes::Path(meta_path) => Ok(meta_path
                .list(path)
                .await
                .map_err(LocationError::from)?
                .boxed()),
            MetadataTypes::Git(meta_git) => Ok(meta_git
                .list(path)
                .await
                .map_err(LocationError::from)?
                .boxed()),
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
                match res {
                    Ok(status) => match status.code() {
                        Some(0) => {},
                        Some(code) => return Err(MetadataReadError::ExitCode(code)),
                        None => return Err(MetadataReadError::Signal),
                    },
                    Err(err) => {
                        return Err(MetadataReadError::PostExec(err));
                    },
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
        let self_owned = self.clone();
        let path = self.sub_path(path);
        let stream = FileOrDirectory::list(&path).await?.map(move |result| {
            result.map(|mut f_or_d| {
                self_owned.to_pub_path(&mut f_or_d);
                f_or_d
            })
        });
        Ok(stream)
    }

    fn to_pub_path(&self, f: &mut FileOrDirectory) {
        let sub_path: &PathBuf = f.as_ref();
        let new_sub_path: PathBuf = self.pub_path(sub_path);
        let sub_path: &mut PathBuf = f.as_mut();
        *sub_path = new_sub_path;
    }

    fn pub_path(&self, sub_path: &Path) -> PathBuf {
        let mut parent_components = self.path.components();
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
        if sub_components.peek().is_none() {
            PathBuf::from(".")
        } else {
            sub_components.collect()
        }
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(from = "MetadataGitSerde")]
#[serde(into = "MetadataGitSerde")]
pub struct MetadataGit {
    meta_path: MetadataPath,
}

impl Deref for MetadataGit {
    type Target = MetadataPath;

    fn deref(&self) -> &Self::Target {
        &self.meta_path
    }
}

impl MetadataGit {
    pub async fn write<T>(
        &self,
        path: impl AsRef<Path>,
        payload: &T,
    ) -> Result<(), MetadataReadError>
    where
        T: Serialize,
    {
        let path = Self::check_sub_git_dir(path).map_err(|err| MetadataReadError::IoError(err))?;
        let orig_path: PathBuf = path.as_ref().to_owned();
        let path = self.sub_path(path);
        let payload = self.format.to_string(payload)?;
        fs::write(&path, payload)
            .await
            .map_err(LocationError::from)?;
        let res = Command::new("git")
            .arg("add")
            .arg(&orig_path)
            .current_dir(&self.path)
            .spawn()
            .unwrap()
            .wait()
            .await;
        match res {
            Ok(status) => match status.code() {
                Some(0) => {},
                Some(code) => return Err(MetadataReadError::ExitCode(code)),
                None => return Err(MetadataReadError::Signal),
            },
            Err(err) => {
                return Err(MetadataReadError::PostExec(err));
            },
        }
        let res = Command::new("git")
            .arg("commit")
            .arg("-m")
            .arg(format!("Write {}", orig_path.display()))
            .current_dir(&self.path)
            .spawn()
            .unwrap()
            .wait()
            .await;
        match res {
            Ok(status) => match status.code() {
                Some(0) => {},
                Some(code) => return Err(MetadataReadError::ExitCode(code)),
                None => return Err(MetadataReadError::Signal),
            },
            Err(err) => {
                return Err(MetadataReadError::PostExec(err));
            },
        }
        Ok(())
    }

    pub async fn list(
        &self,
        path: &Path,
    ) -> io::Result<impl Stream<Item = io::Result<FileOrDirectory>> + 'static> {
        Self::check_sub_git_dir(path)?;
        let stream = self.meta_path.list(path).await?;
        Ok(stream.filter_map(|result| async move {
            match result {
                Ok(file_or_dir) => Ok(Self::check_sub_git_dir(file_or_dir).ok()).transpose(),
                Err(err) => Some(Err(err)),
            }
        }))
    }

    pub async fn read<T>(&self, path: impl AsRef<Path>) -> Result<T, MetadataReadError>
    where
        T: DeserializeOwned,
    {
        let path = Self::check_sub_git_dir(path).map_err(|err| MetadataReadError::IoError(err))?;
        self.meta_path.read(path).await
    }

    fn check_sub_git_dir<T>(path: T) -> io::Result<T>
    where
        T: AsRef<Path>,
    {
        if Self::is_sub_git_dir(path.as_ref()) {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Access to .git is denied",
            ))
        } else {
            Ok(path)
        }
    }

    fn is_sub_git_dir(path: impl AsRef<Path>) -> bool {
        for component in path.as_ref().components() {
            match component {
                Component::Normal(part) if part.eq(".git") => {
                    return true;
                },
                Component::Normal(_) => {
                    return false;
                },
                _ => {},
            }
        }
        false
    }
}

#[derive(Serialize, Deserialize)]
struct MetadataGitSerde {
    #[serde(default)]
    pub format: MetadataFormat,
    pub path: PathBuf,
}

impl From<MetadataGitSerde> for MetadataGit {
    fn from(meta: MetadataGitSerde) -> Self {
        let MetadataGitSerde { format, path } = meta;
        let meta_path = MetadataPath {
            format,
            path,
            put_script: None,
            fail_on_script_error: false,
        };
        MetadataGit { meta_path }
    }
}

impl From<MetadataGit> for MetadataGitSerde {
    fn from(meta: MetadataGit) -> Self {
        let MetadataGit { meta_path } = meta;
        let MetadataPath {
            format,
            path,
            put_script: _,
            fail_on_script_error: _,
        } = meta_path;
        MetadataGitSerde { format, path }
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

    pub async fn list(
        path: &Path,
    ) -> io::Result<impl Stream<Item = io::Result<FileOrDirectory>> + 'static> {
        let top_level = FileOrDirectory::from_local_path(path.to_owned()).await?;
        let children = if let FileOrDirectory::Directory(_) = &top_level {
            let dir_reader = fs::read_dir(&path).await?;
            ReadDirStream::new(dir_reader)
                .filter_map(|entry_res| async move {
                    let entry = match entry_res {
                        Ok(e) => e,
                        Err(err) => return Some(Err(err)),
                    };
                    match FileOrDirectory::from_local_path(entry.path().clone()).await {
                        Ok(file_or_dir) => Some(Ok::<_, io::Error>(file_or_dir)),
                        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
                        Err(err) => Some(Err(err)),
                    }
                })
                .boxed()
        } else {
            stream::empty().boxed()
        };
        Ok(stream::once(future::ready(Ok(top_level))).chain(children))
    }
}

impl AsRef<Path> for FileOrDirectory {
    fn as_ref(&self) -> &Path {
        let path: &PathBuf = self.as_ref();
        &path
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
