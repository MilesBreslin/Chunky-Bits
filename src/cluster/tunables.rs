use serde::{
    Deserialize,
    Serialize,
};

use crate::file::LocationContext;

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
        let ref inner_ref = inner;
        let http_client = {
            let mut builder = reqwest::Client::builder();
            builder = builder.https_only(inner_ref.https_only);
            if let Some(user_agent) = &inner_ref.user_agent {
                builder = builder.user_agent(user_agent.clone())
            }
            builder.build().unwrap()
        };
        let location_context = LocationContext::builder().http_client(http_client).build();
        Tunables {
            location_context,
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

#[derive(Clone, Serialize, Deserialize)]
struct TunablesInner {
    #[serde(default = "TunablesInner::https_only")]
    https_only: bool,
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
}

impl Default for TunablesInner {
    fn default() -> Self {
        TunablesInner {
            https_only: false,
            user_agent: None,
        }
    }
}
