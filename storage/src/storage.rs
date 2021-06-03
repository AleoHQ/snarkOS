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

pub use snarkvm_objects::errors::StorageError;

use std::fmt::Debug;

pub type AtomicTransaction<T> = dyn FnOnce(&mut dyn SyncStorage) -> Result<Option<Box<T>>, StorageError> + Send + Sync;

#[async_trait::async_trait]
pub trait Storage: Sized + Clone + Sync + Send + 'static {
    /// Returns the value with the given key and belonging to the given column.
    async fn get(&self, col: u32, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;

    /// Returns all the keys and values belonging to the given column.
    #[allow(clippy::type_complexity)]
    async fn get_col(&self, col: u32) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, StorageError>;

    /// Returns all the keys belonging to the given column.
    async fn get_keys(&self, col: u32) -> Result<Vec<Box<[u8]>>, StorageError>;

    /// Stores the given key and value in the specified column.
    async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        col: u32,
        key: K,
        value: V,
    ) -> Result<(), StorageError>;

    /// Executes the given `DatabaseTransaction` as a batch operation.
    async fn batch(&self, batch: DatabaseTransaction) -> Result<(), StorageError>;

    /// Returns `true` if the given key exists in the speficied column.
    async fn exists(&self, col: u32, key: &[u8]) -> Result<bool, StorageError>;

    async fn atomic<T: Send + 'static>(
        &self,
        atomic: Box<AtomicTransaction<T>>,
    ) -> Result<Option<Box<T>>, StorageError>;
}

pub trait SyncStorage: Sync + Send + 'static {
    /// Returns the value with the given key and belonging to the given column.
    fn get(&self, col: u32, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError>;

    /// Returns all the keys and values belonging to the given column.
    #[allow(clippy::type_complexity)]
    fn get_col(&self, col: u32) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, StorageError>;

    /// Returns all the keys belonging to the given column.
    fn get_keys(&self, col: u32) -> Result<Vec<Box<[u8]>>, StorageError>;

    /// Stores the given key and value in the specified column.
    fn put(&mut self, col: u32, key: &[u8], value: &[u8]) -> Result<(), StorageError>;

    /// Executes the given `DatabaseTransaction` as a batch operation.
    fn batch(&mut self, batch: DatabaseTransaction) -> Result<(), StorageError>;

    /// Returns `true` if the given key exists in the speficied column.
    fn exists(&self, col: u32, key: &[u8]) -> Result<bool, StorageError>;
}

/// Database operation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    Insert { col: u32, key: Vec<u8>, value: Vec<u8> },
    Delete { col: u32, key: Vec<u8> },
}

/// Batched transaction of database operations.
#[derive(Default, Clone, PartialEq)]
pub struct DatabaseTransaction(pub Vec<Op>);

impl DatabaseTransaction {
    /// Create new transaction.
    pub fn new() -> Self {
        Self(vec![])
    }

    /// Add an operation.
    pub fn push(&mut self, op: Op) {
        self.0.push(op)
    }

    /// Add a vector of operations.
    pub fn push_vec(&mut self, ops: Vec<Op>) {
        self.0.extend(ops)
    }
}
