use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use freqfs::*;
use get_size::GetSize;
use safecast::AsType;
use txn_lock::scalar::{TxnLock, TxnLockReadGuard, TxnLockWriteGuard};

use super::Result;

/// A read guard on a version of a transactional [`File`]
pub struct FileVersionRead<TxnId, FE, F> {
    _modified: TxnLockReadGuard<TxnId>,
    version: FileReadGuardOwned<FE, F>,
}

impl<TxnId, FE, F> Deref for FileVersionRead<TxnId, FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.version.deref()
    }
}

/// A write guard on a version of a transactional [`File`]
pub struct FileVersionWrite<TxnId, FE, F> {
    _modified: TxnLockWriteGuard<TxnId>,
    version: FileWriteGuardOwned<FE, F>,
}

impl<TxnId, FE, F> Deref for FileVersionWrite<TxnId, FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.version.deref()
    }
}

impl<TxnId, FE, F> DerefMut for FileVersionWrite<TxnId, FE, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.version.deref_mut()
    }
}

/// A transactional file
pub struct File<TxnId, FE> {
    last_modified: TxnLock<TxnId, TxnId>,
    canon: FileLock<FE>,
    versions: DirLock<FE>,
    phantom: PhantomData<TxnId>,
}

impl<TxnId, FE> Clone for File<TxnId, FE> {
    fn clone(&self) -> Self {
        Self {
            last_modified: self.last_modified.clone(),
            canon: self.canon.clone(),
            versions: self.versions.clone(),
            phantom: PhantomData,
        }
    }
}

impl<TxnId, FE> File<TxnId, FE>
where
    TxnId: Name + fmt::Display + fmt::Debug + Hash + Ord + Copy,
    FE: FileLoad,
{
    pub(super) fn load(txn_id: TxnId, canon: FileLock<FE>, versions: DirLock<FE>) -> Self {
        debug_assert_eq!(
            canon.path().parent(),
            versions.try_read().expect("versions").path().parent(),
        );

        Self {
            last_modified: TxnLock::new(txn_id),
            canon,
            versions,
            phantom: PhantomData,
        }
    }

    /// Lock this file for reading at the given `txn_id`.
    pub async fn read<F>(&self, txn_id: TxnId) -> Result<FileVersionRead<TxnId, FE, F>>
    where
        FE: AsType<F>,
    {
        let last_modified = self.last_modified.read(txn_id).await?;
        let version = if last_modified == txn_id {
            let versions = self.versions.read().await;
            versions.read_file_owned(&txn_id).await?
        } else {
            self.canon.read_owned().await?
        };

        Ok(FileVersionRead {
            _modified: last_modified,
            version,
        })
    }

    /// Lock this file for reading at the given `txn_id` without borrowing.
    pub async fn into_read<F>(self, txn_id: TxnId) -> Result<FileVersionRead<TxnId, FE, F>>
    where
        FE: AsType<F>,
    {
        self.read(txn_id).await
    }

    /// Lock this file for writing at the given `txn_id`.
    pub async fn write<F>(&self, txn_id: TxnId) -> Result<FileVersionWrite<TxnId, FE, F>>
    where
        FE: AsType<F>,
        F: Clone + GetSize,
    {
        let mut last_modified = self.last_modified.write(txn_id).await?;

        let version = if last_modified == txn_id {
            let versions = self.versions.read().await;
            versions.write_file_owned(&txn_id).await?
        } else {
            let canon = self.canon.read().await?;
            let mut versions = self.versions.write().await;
            let file =
                versions.create_file(txn_id.to_string(), F::clone(&*canon), canon.get_size())?;

            *last_modified = txn_id;

            file.try_into_write()?
        };

        Ok(FileVersionWrite {
            _modified: last_modified,
            version,
        })
    }

    /// Lock this file for writing at the given `txn_id` without borrowing.
    pub async fn into_write<F>(self, txn_id: TxnId) -> Result<FileVersionWrite<TxnId, FE, F>>
    where
        FE: AsType<F>,
        F: Clone + GetSize,
    {
        self.write(txn_id).await
    }
}

impl<TxnId, FE> fmt::Debug for File<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "transactional {:?}", self.canon)
    }
}
