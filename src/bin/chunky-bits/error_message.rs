use std::{
    error::Error,
    fmt::{
        self,
        Display,
        Formatter,
    },
};

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
