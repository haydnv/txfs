//! A transactional filesystem cache layer based on [`freqfs`].
//! See the "examples" directory for usage examples.

use std::{fmt, io};

pub use dir::{Dir, DirEntry, Key, VERSIONS};
pub use file::{File, FileVersionRead, FileVersionWrite};
pub use hr_id::Id;

mod dir;
mod file;

/// An error encountered during a transactional filesystem operation
pub enum Error {
    Conflict(txn_lock::Error),
    IO(io::Error),
    NotFound(String),
    Parse(hr_id::ParseError),
}

impl From<hr_id::ParseError> for Error {
    fn from(cause: hr_id::ParseError) -> Self {
        Self::Parse(cause)
    }
}

impl From<io::Error> for Error {
    fn from(cause: io::Error) -> Self {
        Self::IO(cause)
    }
}

impl From<txn_lock::Error> for Error {
    fn from(cause: txn_lock::Error) -> Self {
        Self::Conflict(cause)
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Conflict(cause) => cause.fmt(f),
            Self::IO(cause) => cause.fmt(f),
            Self::NotFound(locator) => write!(f, "not found: {locator}"),
            Self::Parse(cause) => cause.fmt(f),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for Error {}

/// The result of a transactional filesystem operation
pub type Result<T> = std::result::Result<T, Error>;
