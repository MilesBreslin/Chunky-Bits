use std::{
    convert::TryFrom,
    error::Error,
    fmt,
    path::{
        Path,
        PathBuf,
    },
    pin::Pin,
    str::FromStr,
    string::ToString,
    sync::Arc,
};

use chunky_bits::{
    cluster::FileOrDirectory,
    file::{
        FileReference,
        Location,
        ResilverFileReportOwned,
    },
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::io::AsyncRead;

use crate::{
    config::Config,
    error_message::ErrorMessage,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String")]
#[serde(into = "String")]
pub enum ClusterLocation {
    ClusterFile { cluster: String, path: PathBuf },
    FileRef(Location),
    Other(Location),
}

impl ClusterLocation {
    pub async fn get_reader(
        &self,
        config: &Config,
    ) -> Result<impl AsyncRead + Unpin + '_, Box<dyn Error>> {
        use ClusterLocation::*;
        let result: Result<Pin<Box<dyn AsyncRead + Unpin>>, Box<dyn Error>>;
        match self {
            ClusterFile { cluster, path } => {
                let cluster = config.get_cluster(&cluster).await?;
                let file_ref = cluster.get_file_ref(&path).await?;
                let reader = file_ref.read_builder_owned().reader_owned();
                result = Ok(Box::pin(reader));
            },
            FileRef(loc) => {
                let bytes = loc.read().await?;
                let file_ref: FileReference = serde_yaml::from_slice(&bytes)?;
                let reader = file_ref.read_builder_owned().reader_owned();
                result = Ok(Box::pin(reader));
            },
            Other(loc) => {
                let reader = loc.reader_with_context(&Default::default()).await?;
                result = Ok(Box::pin(reader));
            },
        };
        result
    }

    pub async fn write_from_reader(
        &self,
        config: &Config,
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<u64, Box<dyn Error>> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                let profile_name = config.get_profile(&cluster_name).await;
                let profile = cluster
                    .get_profile(profile_name.as_ref().map(String::as_str))
                    .ok_or_else(|| {
                        ErrorMessage::from(format!("Profile not found: {}", profile_name.unwrap()))
                    })?;
                let file_ref = cluster.get_file_writer(&profile).write(reader).await?;
                cluster.write_file_ref(path, &file_ref).await?;
                Ok(file_ref.length.unwrap())
            },
            FileRef(loc) => {
                let file_ref = FileReference::write_builder().write(reader).await?;
                let file_str = serde_json::to_string_pretty(&file_ref)?;
                loc.write(file_str.as_bytes()).await?;
                Ok(file_ref.length.unwrap())
            },
            Other(loc) => Ok(loc
                .write_from_reader_with_context(&Default::default(), reader)
                .await?),
        }
    }

    pub async fn list_files(
        &self,
        config: &Config,
    ) -> Result<Vec<FileOrDirectory>, Box<dyn Error>> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                return Ok(cluster.list_files(path).await?);
            },
            _ => {
                todo!();
            },
        }
    }

    pub async fn resilver(
        &self,
        config: &Config,
    ) -> Result<ResilverFileReportOwned, Box<dyn Error>> {
        use ClusterLocation::*;
        match self {
            ClusterFile {
                cluster: cluster_name,
                path,
            } => {
                let cluster = config.get_cluster(&cluster_name).await?;
                let profile_name = config.get_profile(&cluster_name).await;
                let profile = cluster
                    .get_profile(profile_name.as_ref().map(String::as_str))
                    .ok_or_else(|| {
                        ErrorMessage::from(format!("Profile not found: {}", profile_name.unwrap()))
                    })?;
                let destination = cluster.get_destination(&profile);
                let file_ref = cluster.get_file_ref(&path).await?;
                let destination = Arc::new(destination);
                let report = file_ref.resilver_owned(destination).await;
                Ok(report)
            },
            _ => Err(ErrorMessage::from("Resilver is only supported on cluster files").into()),
        }
    }
}

impl FromStr for ClusterLocation {
    type Err = Box<dyn Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut split = s.split("#");
        match (split.next(), split.next(), split.next()) {
            (Some("@"), Some(path), None) => {
                Ok(ClusterLocation::FileRef(Location::from_str(path)?))
            },
            (Some(cluster), Some(path), None)
                if !(cluster
                    .chars()
                    .next()
                    .as_ref()
                    .map_or(false, char::is_ascii_alphanumeric)
                    && cluster.len() == 1) =>
            {
                Ok(ClusterLocation::ClusterFile {
                    cluster: cluster.to_string(),
                    path: path.into(),
                })
            },
            (Some(cluster), Some(_), None) => {
                Err(ErrorMessage::from(format!("Invalid cluster name: {}", cluster)).into())
            },
            (Some(_), None, None) => Ok(Location::from_str(s).map(Into::into)?),
            _ => Err(ErrorMessage::from(format!("Invalid cluster location format: {}", s)).into()),
        }
    }
}

impl fmt::Display for ClusterLocation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use ClusterLocation::*;
        match self {
            ClusterFile { cluster, path } => write!(f, "{}#{}", cluster, path.display()),
            FileRef(location) => write!(f, "@#{}", location),
            Other(location) => write!(f, "{}", location),
        }
    }
}

impl Into<String> for ClusterLocation {
    fn into(self) -> String {
        self.to_string()
    }
}

impl From<Location> for ClusterLocation {
    fn from(loc: Location) -> Self {
        ClusterLocation::Other(loc)
    }
}

macro_rules! impl_try_from_string {
    ($type:ty) => {
        impl TryFrom<$type> for ClusterLocation {
            type Error = Box<dyn Error>;

            fn try_from(s: $type) -> Result<Self, Self::Error> {
                FromStr::from_str(AsRef::<str>::as_ref(&s))
            }
        }
    };
}
impl_try_from_string!(&str);
impl_try_from_string!(String);

macro_rules! impl_from_path {
    ($type:ty) => {
        impl From<$type> for ClusterLocation {
            fn from(p: $type) -> Self {
                ClusterLocation::Other(p.into())
            }
        }
    };
}
impl_from_path!(&Path);
impl_from_path!(PathBuf);
