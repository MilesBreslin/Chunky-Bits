use std::{
    error::Error,
    fmt::{
        self,
        Display,
        Formatter,
    },
};

use anyhow::Result;

#[derive(Debug)]
pub struct ErrorMessage(String);
impl ErrorMessage {
    pub fn new(err: impl Display) -> Self {
        ErrorMessage(err.to_string())
    }

    pub fn with_prefix<T, U>(prefix: U) -> impl Fn(T) -> ErrorMessage
    where
        T: Display,
        U: Display,
    {
        move |msg| ErrorMessage(format!("{}: {}", prefix, msg))
    }
}
impl<T: AsRef<str>> From<T> for ErrorMessage {
    fn from(msg: T) -> Self {
        ErrorMessage(msg.as_ref().to_string())
    }
}
impl Display for ErrorMessage {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl Error for ErrorMessage {}

pub trait PrefixError {
    type Ok;
    fn prefix_err<P: Display>(self, prefix: P) -> Result<Self::Ok>;
}

impl<T, E> PrefixError for Result<T, E>
where
    E: Display,
{
    type Ok = T;

    fn prefix_err<P: Display>(self, prefix: P) -> Result<T> {
        self.map_err(ErrorMessage::with_prefix(prefix))
            .map_err(anyhow::Error::new)
    }
}
