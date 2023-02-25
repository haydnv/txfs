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
    FE: FileLoad + GetSize + Clone,
{
    pub(super) async fn load(
        txn_id: TxnId,
        canon: FileLock<FE>,
        versions: DirLock<FE>,
    ) -> Result<Self> {
        debug_assert_eq!(
            canon.path().parent(),
            versions.try_read().expect("versions").path().parent(),
        );

        {
            let txn_id = txn_id.to_string();
            let contents = canon.read().await?;
            let mut versions = versions.write().await;

            if let Some(version) = versions.get_file(&txn_id) {
                *version.write().await? = FE::clone(&*contents);
            } else {
                versions.create_file(
                    txn_id.to_string(),
                    FE::clone(&*contents),
                    contents.get_size(),
                )?;
            }
        }

        Ok(Self {
            last_modified: TxnLock::new(txn_id),
            canon,
            versions,
            phantom: PhantomData,
        })
    }

    /// Lock this file for reading at the given `txn_id`.
    pub async fn read<F>(&self, txn_id: TxnId) -> Result<FileVersionRead<TxnId, FE, F>>
    where
        FE: AsType<F>,
    {
        let last_modified = self.last_modified.read(txn_id).await?;
        let versions = self.versions.read().await;
        let version = versions.read_file_owned(&*last_modified).await?;

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
        let mut versions = self.versions.write().await;

        let version = if last_modified < txn_id {
            let canon = versions.read_file_owned(&*last_modified).await?;
            *last_modified = txn_id;

            let version = F::clone(&*canon);
            let size = version.get_size();

            versions.create_file(txn_id.to_string(), version, size)?
        } else {
            versions.get_file(&*last_modified).expect("version").clone()
        };

        Ok(FileVersionWrite {
            _modified: last_modified,
            version: version.write_owned().await?,
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

impl<TxnId, FE> File<TxnId, FE>
where
    TxnId: Name + Hash + Ord + PartialOrd<str> + fmt::Debug + Copy + Send + Sync,
    FE: FileLoad,
    FE: Clone,
{
    /// Commit the state of this file at `txn_id`.
    /// This will un-block any pending future write locks.
    /// If this file was modified at `txn_id`, it will replace the canonical version with
    /// the modified version and sync with the host filesystem.
    pub async fn commit(&self, txn_id: TxnId) {
        let versions = self.versions.read().await;

        if let Some(version) = versions.get(&txn_id) {
            let version = match &*version {
                DirEntry::File(file) => file.read().await.expect("version"),
                DirEntry::Dir(dir) => panic!("not a file: {:?}", dir),
            };

            *self.canon.write().await.expect("canon") = FE::clone(&*version);
            self.canon.sync().await.expect("sync");
        }

        self.last_modified.commit(txn_id);
    }

    pub async fn rollback(&self, txn_id: &TxnId) {
        let mut versions = self.versions.write().await;
        versions.delete(txn_id).await;
        self.last_modified.rollback(txn_id);
    }

    pub async fn finalize(&self, txn_id: TxnId) {
        let mut versions = self.versions.write().await;

        let to_delete = versions
            .names()
            .filter(|version_id| txn_id >= *version_id.as_str())
            .cloned()
            .collect::<Vec<_>>();

        for version_id in to_delete {
            versions.delete(&version_id).await;
        }

        self.last_modified.finalize(txn_id)
    }
}

impl<TxnId, FE> fmt::Debug for File<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "transactional {:?}", self.canon)
    }
}
