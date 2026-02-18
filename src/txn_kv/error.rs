#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnErrorCategory {
    InvalidInput,
    Conflict,
    Timeout,
    Backpressure,
    Unavailable,
    Internal,
}

#[derive(Debug, thiserror::Error)]
pub enum TxnError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid key size: {0} (expected {1})")]
    InvalidKeySize(usize, usize),
    #[error("invalid value size: {0} (max {1})")]
    InvalidValueSize(usize, usize),
    #[error("write-write conflict")]
    WriteWriteConflict,
    #[error("corrupt wal: {0}")]
    CorruptWal(String),
    #[error("quorum not met: required {required}, succeeded {succeeded}")]
    QuorumNotMet { required: usize, succeeded: usize },
    #[error("invalid quorum {quorum} for {replicas} replicas")]
    InvalidQuorum { quorum: usize, replicas: usize },
    #[error("transaction timed out")]
    TxnTimeout,
}

pub type Result<T> = std::result::Result<T, TxnError>;

impl TxnError {
    pub fn category(&self) -> TxnErrorCategory {
        match self {
            TxnError::InvalidKeySize(_, _)
            | TxnError::InvalidValueSize(_, _)
            | TxnError::InvalidQuorum { .. } => TxnErrorCategory::InvalidInput,
            TxnError::WriteWriteConflict => TxnErrorCategory::Conflict,
            TxnError::TxnTimeout => TxnErrorCategory::Timeout,
            TxnError::QuorumNotMet { .. } => TxnErrorCategory::Unavailable,
            TxnError::CorruptWal(_) => TxnErrorCategory::Internal,
            TxnError::Io(err) => match err.kind() {
                std::io::ErrorKind::InvalidInput
                | std::io::ErrorKind::AlreadyExists
                | std::io::ErrorKind::NotFound => TxnErrorCategory::InvalidInput,
                std::io::ErrorKind::WouldBlock => TxnErrorCategory::Backpressure,
                std::io::ErrorKind::TimedOut => TxnErrorCategory::Timeout,
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
                | std::io::ErrorKind::HostUnreachable => TxnErrorCategory::Unavailable,
                _ => TxnErrorCategory::Internal,
            },
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self.category(),
            TxnErrorCategory::Timeout
                | TxnErrorCategory::Backpressure
                | TxnErrorCategory::Unavailable
                | TxnErrorCategory::Conflict
        )
    }
}
