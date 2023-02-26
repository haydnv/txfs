use std::collections::HashMap;
use std::hash::Hash;
use std::pin::Pin;
use std::{fmt, io};

use freqfs::{DirLock, FileLoad, Name};
use futures::future::{self, Future, TryFutureExt};
use futures::join;
use futures::stream::{FuturesUnordered, StreamExt};
use get_size::GetSize;
use safecast::AsType;
use txn_lock::map::{
    Entry as TxnMapEntry, TxnMapLock, TxnMapValueReadGuard, TxnMapValueReadGuardMap,
};

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

impl<TxnId, FE> DirEntry<TxnId, FE> {
    fn is_file(&self) -> bool {
        match self {
            Self::File(_) => true,
            _ => false,
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

impl<TxnId, FE> Dir<TxnId, FE> {
    /// Destructure this [`Dir`] into its underlying [`DirLock`].
    /// The caller of this method must implement transactional state management explicitly.
    pub fn into_inner(self) -> DirLock<FE> {
        self.canon
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: Name + Hash + Ord + Copy + fmt::Display + fmt::Debug + 'static,
    FE: FileLoad + GetSize + Clone,
{
    /// Load a transactional [`Dir`] from a [`freqfs::DirLock`].
    pub fn load(txn_id: TxnId, canon: DirLock<FE>) -> Pin<Box<dyn Future<Output = Result<Self>>>> {
        Box::pin(async move {
            let (contents, versions) = {
                let mut dir = canon.try_write()?;

                let versions = dir.get_or_create_dir(VERSIONS.to_string())?;

                let contents = {
                    let mut versions = versions.try_write()?;
                    versions.truncate();

                    let mut contents = HashMap::new();

                    for (name, entry) in dir.iter() {
                        let entry = match entry.clone() {
                            freqfs::DirEntry::Dir(dir) => {
                                Self::load(txn_id, dir).map_ok(DirEntry::Dir).await?
                            }
                            freqfs::DirEntry::File(file) => {
                                let file_versions = versions.create_dir(name.clone())?;

                                File::load(txn_id, file, file_versions)
                                    .map_ok(DirEntry::File)
                                    .await?
                            }
                        };

                        contents.insert(name.clone(), entry);
                    }

                    contents
                };

                (contents, versions)
            };

            Ok(Self {
                canon,
                versions,
                entries: TxnMapLock::with_contents(txn_id, contents),
            })
        })
    }

    /// Create a new [`Dir`] with the given `name` at `txn_id`.
    pub async fn create_dir(&self, txn_id: TxnId, name: String) -> Result<Self> {
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

        let sub_dir = canon.get_or_create_dir(name.clone())?;
        let sub_dir = Self::load(txn_id, sub_dir).await?;

        entry.insert(DirEntry::Dir(sub_dir.clone()));

        Ok(sub_dir)
    }

    /// Delete the entry at `name` at `txn_id` and return `true` if it was present.
    pub async fn delete(&self, txn_id: TxnId, name: String) -> Result<bool> {
        if let Some(entry) = self.entries.remove(txn_id, name.as_str()).await? {
            if let DirEntry::Dir(_dir) = &*entry {
                todo!("truncate dir");
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get a sub-directory in this [`Dir`] at the given `txn_id`.
    pub async fn get_dir(
        &self,
        txn_id: TxnId,
        name: &str,
    ) -> Result<Option<TxnMapValueReadGuardMap<String, Self>>> {
        if let Some(entry) = self.entries.get(txn_id, name).map_err(Error::from).await? {
            expect_dir(entry).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Get a sub-directory in this [`Dir`] at the given `txn_id` synchronously, if possible.
    pub fn try_get_dir(
        &self,
        txn_id: TxnId,
        name: &str,
    ) -> Result<Option<TxnMapValueReadGuardMap<String, Self>>> {
        if let Some(entry) = self.entries.try_get(txn_id, name).map_err(Error::from)? {
            expect_dir(entry).map(Some)
        } else {
            Ok(None)
        }
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: Name + fmt::Display + fmt::Debug + Hash + Ord + Copy,
    FE: FileLoad + GetSize + Clone,
{
    /// Create a new [`File`] with the given `name`, `contents` at `txn_id`.
    pub async fn create_file<F>(
        &self,
        txn_id: TxnId,
        name: String,
        contents: F,
    ) -> Result<File<TxnId, FE>>
    where
        FE: AsType<F>,
        F: Clone + GetSize,
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
        let size = contents.get_size();
        let file = canon.create_file(name.clone(), contents, size)?;
        let file_versions = versions.get_or_create_dir(name)?;

        // again, make sure to discard any un-committed versions
        file_versions
            .try_write()
            .expect("file version dir")
            .truncate()
            .await;

        let file = File::load(txn_id, file, file_versions).await?;
        entry.insert(DirEntry::File(file.clone()));
        Ok(file)
    }

    /// Get a [`File`] present in this [`Dir`] at the given `txn_id`.
    pub async fn get_file(
        &self,
        txn_id: TxnId,
        name: &str,
    ) -> Result<Option<TxnMapValueReadGuardMap<String, File<TxnId, FE>>>> {
        if let Some(entry) = self.entries.get(txn_id, name).map_err(Error::from).await? {
            expect_file(entry).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Get a [`File`] present in this [`Dir`] at the given `txn_id` synchronously, if possible.
    pub fn try_get_file(
        &self,
        txn_id: TxnId,
        name: &str,
    ) -> Result<Option<TxnMapValueReadGuardMap<String, File<TxnId, FE>>>> {
        if let Some(entry) = self.entries.try_get(txn_id, name).map_err(Error::from)? {
            expect_file(entry).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Convenience method to lock a file in this [`Dir`] for reading at the given `txn_id`.
    /// This returns an owned read guard or an error if the file is not found.
    pub async fn read_file<F>(
        &self,
        txn_id: TxnId,
        name: &str,
    ) -> Result<FileVersionRead<TxnId, FE, F>>
    where
        FE: AsType<F>,
    {
        if let Some(file) = self.get_file(txn_id, name).await? {
            file.read(txn_id).await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, format!("file not found: {name}")).into())
        }
    }

    /// Convenience method to lock a file in this [`Dir`] for writing at the given `txn_id`.
    /// This returns an owned write guard or an error if the file is not found.
    pub async fn write_file<F>(
        &self,
        txn_id: TxnId,
        name: &str,
    ) -> Result<FileVersionWrite<TxnId, FE, F>>
    where
        FE: AsType<F>,
        F: Clone + GetSize,
    {
        if let Some(file) = self.get_file(txn_id, name).await? {
            file.write(txn_id).await
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, format!("file not found: {name}")).into())
        }
    }
}

impl<TxnId, FE> Dir<TxnId, FE>
where
    TxnId: Name + PartialOrd<str> + Hash + Copy + Ord + fmt::Debug + Send + Sync,
    FE: FileLoad + Clone,
{
    /// Commit the state of this [`Dir`] at `txn_id`.
    pub fn commit<'a>(&'a self, txn_id: TxnId) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async move {
            let (contents, deltas) = self.entries.read_and_commit(txn_id).await;
            let commits = FuturesUnordered::new();

            for (_name, entry) in &*contents {
                let entry = DirEntry::clone(&*entry);
                commits.push(async move {
                    match entry {
                        DirEntry::Dir(dir) => dir.commit(txn_id).await,
                        DirEntry::File(file) => file.commit(txn_id).await,
                    }
                });
            }

            let mut needs_sync = false;
            if let Some(deltas) = deltas {
                let mut canon = self.canon.write().await;

                for (name, entry) in deltas {
                    if entry.is_none() {
                        assert!(!contents.contains_key(&**name));

                        if let Some(entry) = canon.get(&**name) {
                            if entry.is_file() {
                                canon.delete(&**name).await;
                                needs_sync = true;
                            }
                        }
                    }
                }
            };

            commits.fold((), |(), ()| future::ready(())).await;

            if needs_sync {
                self.canon.sync().await.expect("sync");
            }
        })
    }

    /// Roll back the state of this [`Dir`] at `txn_id`.
    pub fn rollback<'a>(&'a self, txn_id: TxnId) -> Pin<Box<dyn Future<Output = ()> + 'a>> {
        Box::pin(async move {
            let (contents, deltas) = self.entries.read_and_rollback(txn_id).await;
            let rollbacks = FuturesUnordered::new();

            for (_name, entry) in &*contents {
                let entry = DirEntry::clone(&*entry);

                rollbacks.push(async move {
                    match entry {
                        DirEntry::Dir(dir) => dir.rollback(txn_id).await,
                        DirEntry::File(file) => file.rollback(txn_id).await,
                    }
                });
            }

            let mut needs_sync = false;
            if let Some(deltas) = deltas {
                let mut canon = self.canon.write().await;

                for (name, entry) in deltas {
                    if let Some(entry) = entry {
                        assert!(contents.contains_key(&**name));

                        if entry.is_file() {
                            needs_sync = needs_sync || canon.delete(&**name).await;
                        }
                    }
                }
            }

            rollbacks.fold((), |(), ()| future::ready(())).await;

            if needs_sync {
                self.canon.sync().await.expect("sync");
            }
        })
    }

    /// Finalize the state of this [`Dir`] at `txn_id`.
    pub async fn finalize(&self, txn_id: TxnId) {
        self.entries.finalize(txn_id);
    }
}

impl<TxnId, FE> fmt::Debug for Dir<TxnId, FE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "transactional {:?}", self.canon)
    }
}

#[inline]
fn expect_dir<TxnId, FE>(
    entry: TxnMapValueReadGuard<String, DirEntry<TxnId, FE>>,
) -> Result<TxnMapValueReadGuardMap<String, Dir<TxnId, FE>>> {
    entry.try_map(|entry| match entry {
        DirEntry::Dir(dir) => Ok(dir.clone()),
        DirEntry::File(file) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("not a directory: {:?}", file),
        )
        .into()),
    })
}

#[inline]
fn expect_file<TxnId, FE>(
    entry: TxnMapValueReadGuard<String, DirEntry<TxnId, FE>>,
) -> Result<TxnMapValueReadGuardMap<String, File<TxnId, FE>>> {
    entry.try_map(|entry| match entry {
        DirEntry::Dir(dir) => {
            Err(io::Error::new(io::ErrorKind::InvalidData, format!("not a file: {:?}", dir)).into())
        }
        DirEntry::File(file) => Ok(file.clone()),
    })
}
