use std::{fmt, io};

mod dir;
mod file;

pub use dir::{Dir, DirEntry, Key};
pub use file::{File, FileVersionRead, FileVersionWrite};

/// The type of error encountered during a transactional filesystem operation
pub enum ErrorKind {
    NotFound,
    Conflict,
    IO,
}

impl fmt::Debug for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::NotFound => "not found",
            Self::Conflict => "conflict",
            Self::IO => "file IO error",
        })
    }
}

/// An error encountered during a transactional filesystem operation
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    /// Destructure this error information
    pub fn into_inner(self) -> (ErrorKind, String) {
        (self.kind, self.message)
    }
}

impl From<txn_lock::Error> for Error {
    fn from(cause: txn_lock::Error) -> Self {
        Self {
            kind: ErrorKind::Conflict,
            message: cause.to_string(),
        }
    }
}

impl From<io::Error> for Error {
    fn from(cause: io::Error) -> Self {
        let kind = match cause.kind() {
            io::ErrorKind::NotFound => ErrorKind::NotFound,
            io::ErrorKind::WouldBlock => ErrorKind::Conflict,
            _ => ErrorKind::IO,
        };

        Self {
            kind,
            message: cause.to_string(),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for Error {}

/// The result of a transactional filesystem operation
pub type Result<T> = std::result::Result<T, Error>;
