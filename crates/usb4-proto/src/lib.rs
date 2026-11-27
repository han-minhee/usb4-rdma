#![forbid(unsafe_code)]

pub mod framer;
pub mod protocol;
pub mod reassembly;

pub type NodeId = u32;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid protocol: {0}")]
    Protocol(&'static str),
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("bounded resource exhausted")]
    Exhausted,
    #[error("stale or invalid handle")]
    Stale,
    #[error("peer or link failed")]
    Disconnected,
    #[error("operation timed out")]
    Timeout,
    #[error("unsupported memory access flags for this registration mode")]
    UnsupportedAccess,
    #[error("remote memory access or bounds violation")]
    AccessViolation,
    #[error("remote operation failed (status={0})")]
    RemoteError(i32),
    #[error("invalid argument: {0}")]
    InvalidArg(&'static str),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
