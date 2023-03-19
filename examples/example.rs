use std::io;
use std::path::PathBuf;

use destream::en;
use freqfs::{Cache, DirLock};
use rand::Rng;
use safecast::as_type;
use tokio::fs;
use txfs::Dir;

#[derive(Clone)]
enum File {
    Bin(Vec<u8>),
    Text(String),
}

impl<'en> en::ToStream<'en> for File {
    fn to_stream<E: en::Encoder<'en>>(&'en self, encoder: E) -> Result<E::Ok, E::Error> {
        match self {
            Self::Bin(bytes) => bytes.to_stream(encoder),
            Self::Text(string) => string.to_stream(encoder),
        }
    }
}

as_type!(File, Bin, Vec<u8>);
as_type!(File, Text, String);

async fn setup_tmp_dir() -> Result<PathBuf, io::Error> {
    let mut rng = rand::thread_rng();
    loop {
        let rand: u32 = rng.gen();
        let path = PathBuf::from(format!("/tmp/test_txfs_{}", rand));
        if !path.exists() {
            fs::create_dir(&path).await?;
            break Ok(path);
        }
    }
}

async fn run_example(cache: DirLock<File>) -> Result<(), txfs::Error> {
    let first_txn_id = 1;
    let _root = Dir::load(first_txn_id, cache).await?;

    // TODO

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), txfs::Error> {
    let path = setup_tmp_dir().await?;

    // initialize the cache
    let cache = Cache::new(40, None);

    // load the directory and file paths into memory (not file contents, yet)
    let root = cache.load(path.clone())?;

    // all I/O under the cache directory at `path` MUST now go through the cache methods
    // otherwise concurrent filesystem access may cause errors
    run_example(root).await?;

    Ok(())
}
