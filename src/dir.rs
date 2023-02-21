use std::fmt;
use std::hash::Hash;

use freqfs::{DirLock, FileLoad, Name};
use safecast::AsType;
use txn_lock::map::TxnMapLock;
use txn_lock::scalar::TxnLock;

use super::file::{
    File, FileVersionRead, FileVersionReadOwned, FileVersionWrite, FileVersionWriteOwned,
};
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

    pub fn create_dir(&self, _txn_id: TxnId, _name: String) -> Result<Self> {
        todo!()
    }

    pub fn create_file(&self, _txn_id: TxnId, _name: String) -> Result<File<TxnId, FE>> {
        todo!()
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
