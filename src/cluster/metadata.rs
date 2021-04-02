use std::{
    convert::TryInto,
    path::PathBuf,
};

use serde::{
    de::DeserializeOwned,
    Deserialize,
    Serialize,
};
use tokio::{
    fs,
    process::Command,
};

use crate::file::{
    error::{
        HttpUrlError,
        LocationError,
        MetadataReadError,
        SerdeError,
    },
    Location,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[serde(tag = "type")]
pub enum MetadataTypes {
    Path(MetadataPath),
}

impl MetadataTypes {
    pub async fn write<T, U>(&self, filename: &T, payload: &U) -> Result<(), MetadataReadError>
    where
        T: std::borrow::Borrow<str>,
        U: Serialize,
    {
        match self {
            MetadataTypes::Path(meta_path) => meta_path.write(filename, payload).await,
        }
    }

    pub async fn read<T, U>(&self, filename: &T) -> Result<U, MetadataReadError>
    where
        T: std::borrow::Borrow<str>,
        U: DeserializeOwned,
    {
        match self {
            MetadataTypes::Path(meta_path) => meta_path.read(filename).await,
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
    pub async fn write<T, U>(&self, filename: &T, payload: &U) -> Result<(), MetadataReadError>
    where
        T: std::borrow::Borrow<str>,
        U: Serialize,
    {
        let payload = self.format.to_string(payload)?;
        fs::write(
            &format!("{}/{}", self.path.display(), filename.borrow()),
            payload,
        )
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

    pub async fn read<T, U>(&self, filename: &T) -> Result<U, MetadataReadError>
    where
        T: std::borrow::Borrow<str>,
        U: DeserializeOwned,
    {
        let bytes = fs::read(&format!("{}/{}", self.path.display(), filename.borrow()))
            .await
            .map_err(LocationError::from)?;
        Ok(self.format.from_bytes(&bytes)?)
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
        location: impl TryInto<Location, Error = impl Into<HttpUrlError>>,
    ) -> Result<T, MetadataReadError>
    where
        T: DeserializeOwned,
    {
        let location: Location = TryInto::try_into(location).map_err(|err| err.into())?;
        let bytes = location.read().await?;
        Ok(self.from_bytes(&bytes)?)
    }
}
