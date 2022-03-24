use std::path::Path;
use std::str;

use super::{Storage, StorateIter};
use crate::{KvError, Kvpair, Value};

use sled::{Db, IVec};

pub struct SledDB(Db);

impl SledDB {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }
}

/// 把 Option<Result<T, E>> flip 成 Result<Option<T>, E>
fn flip<T, E>(v: Option<Result<T, E>>) -> Result<Option<T>, E> {
    v.map_or(Ok(None), |x| x.map(Some))
}

impl Storage for SledDB {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let tree = self.0.open_tree(table)?;
        let value = tree.get(key)?.map(|v| v.try_into());
        flip(value)
    }

    fn set(
        &self,
        table: &str,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Result<Option<Value>, KvError> {
        let tree = self.0.open_tree(table)?;
        let iv: IVec = value.into().try_into()?;
        let old = tree.insert(key.into(), iv)?.map(|v| v.try_into());
        flip(old)
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let tree = self.0.open_tree(table)?;
        tree.contains_key(key).map_err(|e| e.into())
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let tree = self.0.open_tree(table)?;
        let value = tree.remove(key)?.map(|v| v.try_into());
        flip(value)
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let tree = self.0.open_tree(table)?;
        let pairs = tree.into_iter().map(|v| v.into()).collect();
        Ok(pairs)
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let tree = self.0.open_tree(table)?;
        Ok(Box::new(StorateIter::new(tree.into_iter())))
    }
}

impl From<sled::Result<(IVec, IVec)>> for Kvpair {
    fn from(v: sled::Result<(IVec, IVec)>) -> Self {
        match v {
            Ok((k, v)) => match v.try_into() {
                Ok(v) => Kvpair::new(str::from_utf8(k.as_ref()).unwrap(), v),
                Err(_) => Kvpair::default(),
            },
            Err(_) => Kvpair::default(),
        }
    }
}
