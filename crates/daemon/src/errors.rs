//! Transfer error categorization
//!
//! Provides typed error categories for transfer operations, enabling
//! retry decisions, error propagation to UI, and peer scoring.

use std::fmt;
use std::io;

/// Categorized transfer errors for retry and reporting decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferError {
    /// Network operation timed out (transient, retry).
    Timeout,
    /// Connection lost or reset (transient, retry with backoff).
    ConnectionLost,
    /// Peer explicitly refused the request (permanent for this peer).
    PeerRefused,
    /// Received invalid or malformed data (permanent, report peer).
    InvalidData(String),
    /// Local or remote storage is full (permanent until space freed).
    StorageFull,
    /// Requested content/piece not found (permanent).
    ContentNotFound,
}

impl TransferError {
    /// Whether this error is transient and may resolve on retry.
    pub fn is_transient(&self) -> bool {
        matches!(self, TransferError::Timeout | TransferError::ConnectionLost)
    }

    /// Whether this error should trigger a retry attempt.
    pub fn should_retry(&self) -> bool {
        self.is_transient()
    }

    /// Convert to a short error type string for event reporting.
    pub fn error_type(&self) -> &'static str {
        match self {
            TransferError::Timeout => "timeout",
            TransferError::ConnectionLost => "connection_lost",
            TransferError::PeerRefused => "peer_refused",
            TransferError::InvalidData(_) => "invalid_data",
            TransferError::StorageFull => "storage_full",
            TransferError::ContentNotFound => "content_not_found",
        }
    }

    /// Map an I/O error to the appropriate transfer error category.
    pub fn from_io(err: &io::Error) -> Self {
        match err.kind() {
            io::ErrorKind::TimedOut => TransferError::Timeout,
            io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::BrokenPipe
            | io::ErrorKind::UnexpectedEof => TransferError::ConnectionLost,
            io::ErrorKind::ConnectionRefused => TransferError::PeerRefused,
            io::ErrorKind::StorageFull => TransferError::StorageFull,
            _ => TransferError::ConnectionLost,
        }
    }
}

impl fmt::Display for TransferError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransferError::Timeout => write!(f, "operation timed out"),
            TransferError::ConnectionLost => write!(f, "connection lost"),
            TransferError::PeerRefused => write!(f, "peer refused request"),
            TransferError::InvalidData(msg) => write!(f, "invalid data: {}", msg),
            TransferError::StorageFull => write!(f, "storage full"),
            TransferError::ContentNotFound => write!(f, "content not found"),
        }
    }
}

impl std::error::Error for TransferError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transient_errors() {
        assert!(TransferError::Timeout.is_transient());
        assert!(TransferError::Timeout.should_retry());
        assert!(TransferError::ConnectionLost.is_transient());
        assert!(TransferError::ConnectionLost.should_retry());
    }

    #[test]
    fn test_permanent_errors() {
        assert!(!TransferError::PeerRefused.is_transient());
        assert!(!TransferError::PeerRefused.should_retry());
        assert!(!TransferError::InvalidData("bad".into()).is_transient());
        assert!(!TransferError::StorageFull.should_retry());
        assert!(!TransferError::ContentNotFound.should_retry());
    }

    #[test]
    fn test_io_mapping() {
        let timeout = io::Error::new(io::ErrorKind::TimedOut, "timeout");
        assert_eq!(TransferError::from_io(&timeout), TransferError::Timeout);

        let reset = io::Error::new(io::ErrorKind::ConnectionReset, "reset");
        assert_eq!(TransferError::from_io(&reset), TransferError::ConnectionLost);

        let refused = io::Error::new(io::ErrorKind::ConnectionRefused, "refused");
        assert_eq!(TransferError::from_io(&refused), TransferError::PeerRefused);
    }

    #[test]
    fn test_error_type_strings() {
        assert_eq!(TransferError::Timeout.error_type(), "timeout");
        assert_eq!(TransferError::ConnectionLost.error_type(), "connection_lost");
        assert_eq!(TransferError::InvalidData("x".into()).error_type(), "invalid_data");
    }
}
