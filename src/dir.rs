use std::hash::Hash;
use std::{fmt, io};

use freqfs::{DirLock, FileLoad};
use futures::{join, TryFutureExt};
use safecast::AsType;
use txn_lock::map::{Entry as TxnMapEntry, TxnMapLock, TxnMapValueReadGuardMap};

use super::file::*;
use super::{Error, Result};

const VERSIONS: &str = ".txfs";

enum DirEntry<TxnId, FE> {
    Dir(Dir<TxnId, FE>),
    File(File<TxnId, FE>),
}

impl<TxnId, FE> Clone for DirEntry<TxnId, FE> {
    fn clone(&self) -> Self {
        match self {
            Self::Dir(dir) => Self::Dir(dir.clone()),
            Self::File(file) => Self::File(file.clone()),
        }
    }
}

impl<TxnId, FE> fmt::Debug for DirEntry<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Dir(dir) => dir.fmt(f),
            Self::File(file) => file.fmt(f),
        }
    }
}

/// A transactional directory
pub struct Dir<TxnId, FE> {
    canon: DirLock<FE>,
    versions: DirLock<FE>,
    entries: TxnMapLock<TxnId, String, DirEntry<TxnId, FE>>,
}

impl<TxnId, FE> Clone for Dir<TxnId, FE> {
    fn clone(&self) -> Self {
        Self {
            canon: self.canon.clone(),
            versions: self.versions.clone(),
            entries: self.entries.clone(),
        }
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: fmt::Debug + Hash + Ord + Copy,
    FE: FileLoad,
{
    /// Load a transactional [`Dir`] from a [`freqfs::DirLock`].
    pub fn load(txn_id: TxnId, canon: DirLock<FE>) -> Result<Self> {
        let versions = {
            let mut canon = canon.try_write()?;
            if let Some(versions) = canon.get_dir(VERSIONS) {
                #[cfg(feature = "logging")]
                log::warn!("{:?} has dangling versions", canon);
                versions.clone()
            } else {
                canon.create_dir(VERSIONS.to_string())?
            }
        };

        Ok(Self {
            canon,
            versions,
            entries: TxnMapLock::new(txn_id),
        })
    }

    /// Create a new [`Dir`] with the given `name` at `txn_id`.
    pub async fn create_dir(&self, txn_id: TxnId, name: String) -> Result<Self> {
        // this write permit ensures that there is no other pending entry with this name
        let entry = match self.entries.entry(txn_id, name.clone()).await? {
            TxnMapEntry::Occupied(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("directory {name}"),
                )
                .into())
            }
            TxnMapEntry::Vacant(entry) => entry,
        };

        let mut canon = self.canon.write().await;

        // so it's safe to get or create this directory
        let sub_dir = canon.get_or_create_dir(name)?;

        // but any abandoned (un-committed) writes must be discarded
        sub_dir.try_write().expect("sub-dir").truncate().await;

        let sub_dir = Self::load(txn_id, sub_dir)?;
        entry.insert(DirEntry::Dir(sub_dir.clone()));
        Ok(sub_dir)
    }

    /// Create a new [`File`] with the given `name`, `contents`, and `size` at `txn_id`.
    pub async fn create_file<F>(
        &self,
        txn_id: TxnId,
        name: String,
        contents: F,
        size: usize,
    ) -> Result<File<TxnId, FE>>
    where
        FE: AsType<F>,
    {
        // this write permit ensures that there is no other pending entry with this name
        let entry = match self.entries.entry(txn_id, name.clone()).await? {
            TxnMapEntry::Occupied(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("directory {name}"),
                )
                .into())
            }
            TxnMapEntry::Vacant(entry) => entry,
        };

        let (mut canon, mut versions) = join!(self.canon.write(), self.versions.write());

        // but any prior version must be discarded
        canon.delete(&name).await;

        // so that it's safe to create a new file with its own versions
        let file = canon.create_file(name.clone(), contents, size)?;
        let file_versions = versions.get_or_create_dir(name)?;

        // again, make sure to discard any un-committed versions
        file_versions
            .try_write()
            .expect("file version dir")
            .truncate()
            .await;

        let file = File::load(txn_id, file, file_versions);
        entry.insert(DirEntry::File(file.clone()));
        Ok(file)
    }

    /// Delete the entry at `name` at `txn_id` and return `true` if it was present.
    pub async fn delete(&self, txn_id: TxnId, name: &str) -> Result<bool> {
        self.entries
            .remove(txn_id, name)
            .map_ok(|entry| entry.is_some())
            .map_err(Error::from)
            .await
    }

    pub async fn get_dir(
        &self,
        txn_id: TxnId,
        name: &str,
    ) -> Result<Option<TxnMapValueReadGuardMap<String, Self>>> {
        if let Some(entry) = self.entries.get(txn_id, name).map_err(Error::from).await? {
            entry
                .try_map(|entry| match entry {
                    DirEntry::Dir(dir) => Ok(dir.clone()),
                    DirEntry::File(file) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("not a directory: {:?}", file),
                    )
                    .into()),
                })
                .map(Some)
        } else {
            Ok(None)
        }
    }

    pub async fn get_file(
        &self,
        txn_id: TxnId,
        name: &str,
    ) -> Result<Option<TxnMapValueReadGuardMap<String, File<TxnId, FE>>>> {
        if let Some(entry) = self.entries.get(txn_id, name).map_err(Error::from).await? {
            entry
                .try_map(|entry| match entry {
                    DirEntry::Dir(dir) => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("not a file: {:?}", dir),
                    )
                    .into()),
                    DirEntry::File(file) => Ok(file.clone()),
                })
                .map(Some)
        } else {
            Ok(None)
        }
    }

    pub async fn read_file<F>(
        &self,
        txn_id: TxnId,
        name: &str,
    ) -> Result<FileVersionReadOwned<TxnId, FE, F>>
    where
        FE: AsType<F>,
    {
        if let Some(file) = self.get_file(txn_id, name).await? {
            file.read_owned(txn_id).await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, format!("file not found: {name}")).into())
        }
    }

    pub async fn write_file<F>(
        &self,
        txn_id: TxnId,
        name: &str,
    ) -> Result<FileVersionWriteOwned<TxnId, FE, F>>
    where
        FE: AsType<F>,
    {
        if let Some(file) = self.get_file(txn_id, name).await? {
            file.write_owned(txn_id).await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, format!("file not found: {name}")).into())
        }
    }
}

impl<TxnId, FE> fmt::Debug for Dir<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "transactional {:?}", self.canon)
    }
}
