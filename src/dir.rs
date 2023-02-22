use std::hash::Hash;
use std::{fmt, io};

use freqfs::{DirLock, FileLoad, Name};
use futures::join;
use safecast::AsType;
use txn_lock::map::{Entry, TxnMapLock};
use txn_lock::scalar::TxnLock;

use super::file::*;
use super::Result;

const VERSIONS: &str = ".txfs";

/// A transactional directory
#[derive(Clone)]
pub struct Dir<TxnId, FE> {
    commits: TxnMapLock<TxnId, String, TxnLock<TxnId, TxnId>>,
    versions: DirLock<FE>,
    canon: DirLock<FE>,
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
            commits: TxnMapLock::new(txn_id),
            versions,
            canon,
        })
    }

    pub async fn create_dir(&self, txn_id: TxnId, name: String) -> Result<Self> {
        // holding this write permit ensures that there is no other pending entry with this name
        let _entry = match self.commits.entry(txn_id, name.clone()).await? {
            Entry::Occupied(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("directory {name}"),
                )
                .into())
            }
            Entry::Vacant(entry) => entry.insert(TxnLock::new(txn_id)),
        };

        let mut canon = self.canon.write().await;

        // so it's safe to get or create this directory
        let sub_dir = canon.get_or_create_dir(name)?;

        // but any abandoned (un-committed) writes must be discarded
        sub_dir.try_write().expect("sub-dir").truncate().await;

        Self::load(txn_id, sub_dir)
    }

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
        // holding this write permit ensures that there is no other pending entry with this name
        let _entry = match self.commits.entry(txn_id, name.clone()).await? {
            Entry::Occupied(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("directory {name}"),
                )
                .into())
            }
            Entry::Vacant(entry) => entry.insert(TxnLock::new(txn_id)),
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

        Ok(File::load(file, file_versions))
    }

    pub fn delete<Q: Name + ?Sized>(&self, _txn_id: TxnId, _name: &Q) -> Result<bool> {
        todo!()
    }

    pub fn get_dir<Q: Name + ?Sized>(&self, _txn_id: TxnId, _name: &Q) -> Result<Option<Self>> {
        todo!()
    }

    pub fn get_file<Q>(&self, _txn_id: TxnId, _name: &Q) -> Result<Option<File<TxnId, FE>>>
    where
        Q: Name + ?Sized,
    {
        todo!()
    }

    pub async fn read_file<'a, Q, F>(
        &'a self,
        _txn_id: TxnId,
        _name: &Q,
    ) -> Result<Option<FileVersionRead<'a, TxnId, F>>>
    where
        Q: Name + ?Sized,
        F: 'a,
        FE: AsType<F>,
    {
        todo!()
    }

    pub async fn read_file_owned<Q, F>(
        &self,
        _txn_id: TxnId,
        _name: &Q,
    ) -> Result<Option<FileVersionReadOwned<TxnId, FE, F>>>
    where
        Q: Name + ?Sized,
        FE: AsType<F>,
    {
        todo!()
    }

    pub async fn write_file<'a, Q, F>(
        &'a self,
        _txn_id: TxnId,
        _name: &Q,
    ) -> Result<Option<FileVersionWrite<'a, TxnId, F>>>
    where
        Q: Name + ?Sized,
        F: 'a,
        FE: AsType<F>,
    {
        todo!()
    }

    pub async fn write_file_owned<Q, F>(
        &self,
        _txn_id: TxnId,
        _name: &Q,
    ) -> Result<Option<FileVersionWriteOwned<TxnId, FE, F>>>
    where
        Q: Name + ?Sized,
        FE: AsType<F>,
    {
        todo!()
    }
}
