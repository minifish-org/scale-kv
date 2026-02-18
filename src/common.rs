use bytes::Bytes;

pub const PAGE_SIZE: usize = 16 * 1024;
pub const KEY_SIZE: usize = 16;
pub const VALUE_SIZE: usize = 1024;

pub type PageId = u64;
pub type Page = Bytes;
pub type Value = Vec<u8>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    InvalidInput,
    Conflict,
    Timeout,
    Backpressure,
    Unavailable,
    Internal,
}

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
    #[error("transaction timed out")]
    TxnTimeout,
}

impl From<capnp::Error> for Error {
    fn from(err: capnp::Error) -> Self {
        Error::Capnp(err.to_string())
    }
}

impl Error {
    pub fn category(&self) -> ErrorCategory {
        match self {
            Error::InvalidPageSize(_, _)
            | Error::InvalidValueSize(_, _)
            | Error::InvalidKeySize(_, _) => ErrorCategory::InvalidInput,
            Error::TxnTimeout => ErrorCategory::Timeout,
            Error::InMemoryPageMissing(_) => ErrorCategory::Internal,
            Error::Capnp(_) => ErrorCategory::Unavailable,
            Error::Io(err) => match err.kind() {
                std::io::ErrorKind::InvalidInput
                | std::io::ErrorKind::AlreadyExists
                | std::io::ErrorKind::NotFound => ErrorCategory::InvalidInput,
                std::io::ErrorKind::WouldBlock => ErrorCategory::Backpressure,
                std::io::ErrorKind::TimedOut => ErrorCategory::Timeout,
                std::io::ErrorKind::Interrupted
                | std::io::ErrorKind::BrokenPipe
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::AddrInUse
                | std::io::ErrorKind::AddrNotAvailable
                | std::io::ErrorKind::NetworkDown
                | std::io::ErrorKind::NetworkUnreachable
                | std::io::ErrorKind::HostUnreachable => ErrorCategory::Unavailable,
                _ => ErrorCategory::Internal,
            },
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self.category(),
            ErrorCategory::Timeout | ErrorCategory::Backpressure | ErrorCategory::Unavailable
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_category_and_retryable() {
        let e = Error::Io(std::io::Error::new(std::io::ErrorKind::WouldBlock, "bp"));
        assert_eq!(e.category(), ErrorCategory::Backpressure);
        assert!(e.is_retryable());

        let e = Error::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "missing"));
        assert_eq!(e.category(), ErrorCategory::InvalidInput);
        assert!(!e.is_retryable());

        let e = Error::InvalidKeySize(1, 16);
        assert_eq!(e.category(), ErrorCategory::InvalidInput);
        assert!(!e.is_retryable());
    }
}
