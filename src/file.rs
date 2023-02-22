use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

use freqfs::{
    DirLock, FileLoad, FileLock, FileReadGuard, FileReadGuardOwned, FileWriteGuard,
    FileWriteGuardOwned,
};
use safecast::AsType;
use txn_lock::map::{TxnMapValueReadGuard, TxnMapValueWriteGuard};
use txn_lock::scalar::TxnLock;

use super::Result;

/// A read guard on a version of a transactional [`File`]
pub struct FileVersionRead<'a, TxnId, F> {
    _commit: TxnMapValueReadGuard<TxnId, TxnId>,
    guard: FileReadGuard<'a, F>,
}

impl<'a, TxnId, F> Deref for FileVersionRead<'a, TxnId, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

/// An owned read guard on a version of a transactional [`File`]
pub struct FileVersionReadOwned<TxnId, FE, F> {
    _commit: TxnMapValueReadGuard<TxnId, TxnId>,
    guard: FileReadGuardOwned<FE, F>,
}

impl<TxnId, FE, F> Deref for FileVersionReadOwned<TxnId, FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

/// A write guard on a version of a transactional [`File`]
pub struct FileVersionWrite<'a, TxnId, F> {
    _commit: TxnMapValueWriteGuard<TxnId, TxnId>,
    guard: FileWriteGuard<'a, F>,
}

impl<'a, TxnId, F> Deref for FileVersionWrite<'a, TxnId, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<'a, TxnId, F> DerefMut for FileVersionWrite<'a, TxnId, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

/// An owned write guard on a version of a transactional [`File`]
pub struct FileVersionWriteOwned<TxnId, FE, F> {
    _commit: TxnMapValueWriteGuard<TxnId, TxnId>,
    guard: FileWriteGuardOwned<FE, F>,
}

impl<TxnId, FE, F> Deref for FileVersionWriteOwned<TxnId, FE, F> {
    type Target = F;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<TxnId, FE, F> DerefMut for FileVersionWriteOwned<TxnId, FE, F> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
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

impl<TxnId: Copy + Ord + Hash + fmt::Debug, FE: FileLoad> File<TxnId, FE> {
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

    /// Lock this file to read at the given `txn_id`.
    pub async fn read<'a, F>(&'a self, _txn_id: TxnId) -> Result<FileVersionRead<'a, TxnId, F>>
    where
        F: 'a,
        FE: AsType<F>,
    {
        todo!()
    }

    /// Lock this file to read at the given `txn_id`.
    pub async fn read_owned<F>(&self, _txn_id: TxnId) -> Result<FileVersionReadOwned<TxnId, FE, F>>
    where
        FE: AsType<F>,
    {
        todo!()
    }

    /// Lock this file to read at the given `txn_id` without borrowing.
    pub async fn into_read<F>(self, _txn_id: TxnId) -> Result<FileVersionReadOwned<TxnId, FE, F>>
    where
        FE: AsType<F>,
    {
        todo!()
    }

    /// Lock this file to write at the given `txn_id`.
    pub async fn write<'a, F>(&'a self, _txn_id: TxnId) -> Result<FileVersionRead<'a, TxnId, F>>
    where
        F: 'a,
        FE: AsType<F>,
    {
        todo!()
    }

    /// Lock this file to read at the given `txn_id`.
    pub async fn write_owned<F>(
        &self,
        _txn_id: TxnId,
    ) -> Result<FileVersionWriteOwned<TxnId, FE, F>>
    where
        FE: AsType<F>,
    {
        todo!()
    }

    /// Lock this file to read at the given `txn_id` without borrowing.
    pub async fn into_write<F>(self, _txn_id: TxnId) -> Result<FileVersionWriteOwned<TxnId, FE, F>>
    where
        FE: AsType<F>,
    {
        todo!()
    }
}

impl<TxnId, FE> fmt::Debug for File<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "transactional {:?}", self.canon)
    }
}
