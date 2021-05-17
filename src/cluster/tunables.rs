use serde::{
    Deserialize,
    Serialize,
};

use crate::file::{
    LocationContext,
    LocationContextBuilder,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(from = "TunablesInner")]
#[serde(into = "TunablesInner")]
pub struct Tunables {
    inner: TunablesInner,
    location_context: LocationContext,
}

impl AsRef<LocationContext> for Tunables {
    fn as_ref(&self) -> &LocationContext {
        &self.location_context
    }
}

impl From<TunablesInner> for Tunables {
    fn from(inner: TunablesInner) -> Self {
        Tunables {
            location_context: inner.generate_location_context_builder().build(),
            inner,
        }
    }
}

impl From<Tunables> for TunablesInner {
    fn from(t: Tunables) -> Self {
        t.inner
    }
}

impl Default for Tunables {
    fn default() -> Self {
        TunablesInner::default().into()
    }
}

impl Tunables {
    pub(super) fn generate_location_context_builder(&self) -> LocationContextBuilder {
        self.inner.generate_location_context_builder()
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct TunablesInner {
    #[serde(default = "TunablesInner::https_only")]
    https_only: bool,
    #[serde(default = "TunablesInner::on_conflict")]
    on_conflict: OnConflict,
    #[serde(default = "TunablesInner::user_agent")]
    user_agent: Option<String>,
}

macro_rules! default_getters {
    ($($field:ident: $type:ty),*,) => {
        impl TunablesInner {
            $(
                fn $field() -> $type {
                    <TunablesInner as Default>::default().$field
                }
            )*
        }
    };
}

default_getters! {
    https_only: bool,
    user_agent: Option<String>,
    on_conflict: OnConflict,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum OnConflict {
    Ignore,
    Overwrite,
}

impl Default for TunablesInner {
    fn default() -> Self {
        TunablesInner {
            https_only: false,
            user_agent: None,
            on_conflict: OnConflict::Ignore,
        }
    }
}

impl TunablesInner {
    fn generate_location_context_builder(&self) -> LocationContextBuilder {
        let http_client = {
            let mut builder = reqwest::Client::builder();
            builder = builder.https_only(self.https_only);
            if let Some(user_agent) = &self.user_agent {
                builder = builder.user_agent(user_agent.clone())
            }
            builder.build().unwrap()
        };
        let builder = LocationContext::builder().http_client(http_client);
        let builder = match &self.on_conflict {
            OnConflict::Ignore => builder.conflict_ignore(),
            OnConflict::Overwrite => builder.conflict_overwrite(),
        };
        builder
    }
}
