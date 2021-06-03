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

use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, DB};
use std::{any::Any, path::Path};
use tokio::sync::{mpsc, oneshot};

fn convert_err(err: rocksdb::Error) -> StorageError {
    StorageError::Crate("rocksdb", err.to_string())
}

#[derive(Clone)]
pub struct RocksDb {
    sender: mpsc::Sender<DbOperation>,
}

type AtomicTransactionDyn =
    dyn FnOnce(&mut dyn SyncStorage) -> Result<Option<Box<dyn Any + Send>>, StorageError> + Send + Sync;

enum DbOperation {
    Get {
        col: u32,
        key: Vec<u8>,
        response: oneshot::Sender<Result<Option<Vec<u8>>, StorageError>>,
    },
    Exists {
        col: u32,
        key: Vec<u8>,
        response: oneshot::Sender<Result<bool, StorageError>>,
    },
    GetCol {
        col: u32,
        response: oneshot::Sender<Result<Vec<(Box<[u8]>, Box<[u8]>)>, StorageError>>,
    },
    GetKeys {
        col: u32,
        response: oneshot::Sender<Result<Vec<Box<[u8]>>, StorageError>>,
    },
    Put {
        col: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    Batch {
        transaction: DatabaseTransaction,
        response: oneshot::Sender<Result<(), StorageError>>,
    },
    Atomic {
        operation: Box<AtomicTransactionDyn>,
        response: oneshot::Sender<Result<Option<Box<dyn Any + Send>>, StorageError>>,
    },
}

struct RocksDbInner {
    cf_names: Vec<String>,
    db: DB,
    receiver: mpsc::Receiver<DbOperation>,
}

impl RocksDbInner {
    /// Returns the column family reference from a given index.
    /// If the given index does not exist, returns [None](std::option::Option).
    fn get_cf_ref(&self, index: u32) -> &ColumnFamily {
        self.db
            .cf_handle(&self.cf_names[index as usize])
            .expect("the column family exists")
    }

    fn thread(mut self) {
        while let Some(received) = self.receiver.blocking_recv() {
            match received {
                DbOperation::Get { col, key, response } => {
                    let result = self.get(col, &key[..]);
                    response.send(result).ok();
                }
                DbOperation::Exists { col, key, response } => {
                    let result = self.exists(col, &key[..]);
                    response.send(result).ok();
                }
                DbOperation::GetCol { col, response } => {
                    let result = self.get_col(col);
                    response.send(result).ok();
                }
                DbOperation::GetKeys { col, response } => {
                    let result = self.get_keys(col);
                    response.send(result).ok();
                }
                DbOperation::Put {
                    col,
                    key,
                    value,
                    response,
                } => {
                    let result = self.put(col, &key[..], &value[..]);
                    response.send(result).ok();
                }
                DbOperation::Batch { transaction, response } => {
                    let result = self.batch(transaction);
                    response.send(result).ok();
                }
                DbOperation::Atomic { operation, response } => {
                    let result = operation(&mut self);
                    response.send(result).ok();
                }
            }
        }
    }
}

impl SyncStorage for RocksDbInner {
    fn get(&self, col: u32, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.db.get_cf(self.get_cf_ref(col), &key[..]).map_err(convert_err)
    }

    #[allow(clippy::type_complexity)]
    fn get_col(&self, col: u32) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, StorageError> {
        Ok(self.db.iterator_cf(self.get_cf_ref(col), IteratorMode::Start).collect())
    }

    fn get_keys(&self, col: u32) -> Result<Vec<Box<[u8]>>, StorageError> {
        Ok(self
            .db
            .iterator_cf(self.get_cf_ref(col), IteratorMode::Start)
            .map(|(k, _v)| k)
            .collect())
    }

    fn put(&mut self, col: u32, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.db.put_cf(self.get_cf_ref(col), key, value).map_err(convert_err)
    }

    fn batch(&mut self, transaction: DatabaseTransaction) -> Result<(), StorageError> {
        let mut batch = WriteBatch::default();

        for operation in transaction.0 {
            match operation {
                Op::Insert { col, key, value } => {
                    let cf = self.get_cf_ref(col);
                    batch.put_cf(cf, &key, value);
                }
                Op::Delete { col, key } => {
                    let cf = self.get_cf_ref(col);
                    batch.delete_cf(cf, &key);
                }
            };
        }
        self.db.write(batch).map_err(convert_err)
    }

    fn exists(&self, col: u32, key: &[u8]) -> Result<bool, StorageError> {
        self.db
            .get_pinned_cf(self.get_cf_ref(col), &key[..])
            .map_err(convert_err)
            .map(|x| x.is_some())
    }
}

impl RocksDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageError> {
        RocksDb::open_cf(path, NUM_COLS)
    }
}

fn stale_db<E>(_: E) -> StorageError {
    StorageError::Message("stale db".to_string())
}

#[async_trait::async_trait]
impl Storage for RocksDb {
    async fn get(&self, col: u32, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(DbOperation::Get {
                col,
                key: key.to_vec(),
                response: sender,
            })
            .await
            .map_err(stale_db)?;
        receiver.await.map_err(stale_db)?
    }

    #[allow(clippy::type_complexity)]
    async fn get_col(&self, col: u32) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, StorageError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(DbOperation::GetCol { col, response: sender })
            .await
            .map_err(stale_db)?;
        receiver.await.map_err(stale_db)?
    }

    async fn get_keys(&self, col: u32) -> Result<Vec<Box<[u8]>>, StorageError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(DbOperation::GetKeys { col, response: sender })
            .await
            .map_err(stale_db)?;
        receiver.await.map_err(stale_db)?
    }

    async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        col: u32,
        key: K,
        value: V,
    ) -> Result<(), StorageError> {
        let (sender, receiver) = oneshot::channel();
        let key = key.as_ref().to_vec();
        let value = value.as_ref().to_vec();
        self.sender
            .send(DbOperation::Put {
                col,
                key,
                value,
                response: sender,
            })
            .await
            .map_err(stale_db)?;
        receiver.await.map_err(stale_db)?
    }

    async fn batch(&self, transaction: DatabaseTransaction) -> Result<(), StorageError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(DbOperation::Batch {
                transaction,
                response: sender,
            })
            .await
            .map_err(stale_db)?;
        receiver.await.map_err(stale_db)?
    }

    async fn exists(&self, col: u32, key: &[u8]) -> Result<bool, StorageError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(DbOperation::Exists {
                col,
                key: key.to_vec(),
                response: sender,
            })
            .await
            .map_err(stale_db)?;
        receiver.await.map_err(stale_db)?
    }

    async fn atomic<T: Send + 'static>(
        &self,
        atomic: Box<AtomicTransaction<T>>,
    ) -> Result<Option<Box<T>>, StorageError> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(DbOperation::Atomic {
                operation: Box::new(move |db| atomic(db).map(|a| a.map(|a| a as Box<dyn Any + Send + 'static>))),
                response: sender,
            })
            .await
            .map_err(stale_db)?;
        receiver
            .await
            .map_err(stale_db)?
            .map(|a| a.map(|a| a.downcast().unwrap()))
    }
}

impl RocksDb {
    /// Opens storage from the given path with its given names. If storage does not exists,
    /// it creates a new storage file at the given path with its given names, and opens it.
    /// If RocksDB fails to open, returns [StorageError](snarkvm_errors::storage::StorageError).
    pub fn open_cf<P: AsRef<Path>>(path: P, num_cfs: u32) -> Result<Self, StorageError> {
        let mut cfs = Vec::with_capacity(num_cfs as usize);
        let mut cf_names: Vec<String> = Vec::with_capacity(cfs.len());

        for column in 0..num_cfs {
            let column_name = format!("col{}", column.to_string());

            let mut cf_opts = Options::default();
            cf_opts.set_max_write_buffer_number(16);

            cfs.push(ColumnFamilyDescriptor::new(&column_name, cf_opts));
            cf_names.push(column_name);
        }

        let mut storage_opts = Options::default();
        storage_opts.increase_parallelism(3);
        storage_opts.create_missing_column_families(true);
        storage_opts.create_if_missing(true);

        let storage = DB::open_cf_descriptors(&storage_opts, path, cfs).map_err(convert_err)?;

        let (sender, receiver) = mpsc::channel(256);
        std::thread::spawn(move || {
            RocksDbInner {
                receiver,
                db: storage,
                cf_names,
            }
            .thread();
        });

        Ok(Self { sender })
    }
}
