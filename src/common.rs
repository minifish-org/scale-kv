pub const PAGE_SIZE: usize = 16 * 1024;
pub const KEY_SIZE: usize = 16;
pub const VALUE_SIZE: usize = 1024;

pub type PageId = u64;
pub type Page = Vec<u8>;
pub type Value = Vec<u8>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("capnp error: {0}")]
    Capnp(String),
    #[error("invalid page size: {0} (expected {1})")]
    InvalidPageSize(usize, usize),
    #[error("invalid value size: {0} (max {1})")]
    InvalidValueSize(usize, usize),
    #[error("invalid key size: {0} (expected {1})")]
    InvalidKeySize(usize, usize),
    #[error("in-memory page missing for page_id {0}")]
    InMemoryPageMissing(u64),
}

impl From<capnp::Error> for Error {
    fn from(err: capnp::Error) -> Self {
        Error::Capnp(err.to_string())
    }
}
