// Copyright (C) 2019-2021 Aleo Systems Inc.
// This file is part of the snarkOS library.

// The snarkOS library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// The snarkOS library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with the snarkOS library. If not, see <https://www.gnu.org/licenses/>.

use crate::{AtomicTransaction, DatabaseTransaction, Op, Storage, StorageError, SyncStorage, NUM_COLS};

use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc}; // only used for testing

#[allow(clippy::type_complexity)]
struct MemDbInner {
    pub cols: Vec<HashMap<Box<[u8]>, Box<[u8]>>>,
}

impl SyncStorage for MemDbInner {
    fn get(&self, col: u32, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.cols[col as usize].get(key).map(|v| v.to_vec()))
    }

    fn get_col(&self, col: u32) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, StorageError> {
        Ok(self.cols[col as usize].clone().into_iter().collect())
    }

    fn get_keys(&self, col: u32) -> Result<Vec<Box<[u8]>>, StorageError> {
        Ok(self.cols[col as usize].keys().cloned().collect())
    }

    fn put(&mut self, col: u32, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.cols[col as usize].insert(key.as_ref().into(), value.as_ref().into());
        Ok(())
    }

    fn batch(&mut self, batch: DatabaseTransaction) -> Result<(), StorageError> {
        if batch.0.is_empty() {
            return Ok(());
        }

        for operation in batch.0 {
            match operation {
                Op::Insert { col, key, value } => {
                    self.cols[col as usize].insert(key.into(), value.into());
                }
                Op::Delete { col, key } => {
                    self.cols[col as usize].remove(&Box::from(key));
                }
            }
        }

        Ok(())
    }

    fn exists(&self, col: u32, key: &[u8]) -> Result<bool, StorageError> {
        Ok(self.cols[col as usize].contains_key(key))
    }
}

#[derive(Clone)]
pub struct MemDb(Arc<RwLock<MemDbInner>>);

impl MemDb {
    pub fn open() -> Self {
        Self(Arc::new(RwLock::new(MemDbInner {
            cols: vec![Default::default(); NUM_COLS as usize],
        })))
    }
}

#[async_trait::async_trait]
impl Storage for MemDb {
    async fn get(&self, col: u32, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.0.read().get(col, key)
    }

    #[allow(clippy::type_complexity)]
    async fn get_col(&self, col: u32) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, StorageError> {
        self.0.read().get_col(col)
    }

    async fn get_keys(&self, col: u32) -> Result<Vec<Box<[u8]>>, StorageError> {
        self.0.read().get_keys(col)
    }

    async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        col: u32,
        key: K,
        value: V,
    ) -> Result<(), StorageError> {
        self.0.write().put(col, key.as_ref(), value.as_ref())
    }

    async fn batch(&self, transaction: DatabaseTransaction) -> Result<(), StorageError> {
        self.0.write().batch(transaction)
    }

    async fn exists(&self, col: u32, key: &[u8]) -> Result<bool, StorageError> {
        self.0.read().exists(col, key)
    }

    async fn atomic<T: Send + 'static>(
        &self,
        atomic: Box<AtomicTransaction<T>>,
    ) -> Result<Option<Box<T>>, StorageError> {
        let mut locked = self.0.write();
        atomic(&mut *locked)
    }
}
