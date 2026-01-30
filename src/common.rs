pub type Value = Vec<u8>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("capnp error: {0}")]
    Capnp(String),
}

impl From<capnp::Error> for Error {
    fn from(err: capnp::Error) -> Self {
        Error::Capnp(err.to_string())
    }
}
