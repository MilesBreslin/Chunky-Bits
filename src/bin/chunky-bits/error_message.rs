use std::fmt::Display;

use anyhow::Result;

pub trait PrefixError {
    type Ok;
    fn prefix_err<P: Display>(self, prefix: P) -> Result<Self::Ok>;
}

impl<T, E> PrefixError for Result<T, E>
where
    E: Into<anyhow::Error>,
{
    type Ok = T;

    fn prefix_err<P: Display>(self, prefix: P) -> Result<T> {
        self.map_err(|err| {
            let err: anyhow::Error = err.into();
            let context = format!("{}: {}", prefix, err);
            err.context(context)
        })
    }
}
