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

//! Transactions memory pool
//!
//! `MemoryPool` keeps a vector of transactions seen by the miner.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::error::ConsensusError;
use mpmc_map::MpmcMap;
use snarkos_storage::Ledger;
use snarkvm_algorithms::traits::LoadableMerkleParameters;
use snarkvm_dpc::{BlockHeader, LedgerScheme, Storage, TransactionScheme, Transactions};
use snarkvm_utilities::{
    bytes::{FromBytes, ToBytes},
    has_duplicates,
    to_bytes_le,
};

/// Stores a transaction and it's size in the memory pool.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry<T: TransactionScheme> {
    pub size_in_bytes: usize,
    pub transaction: T,
}

/// Stores transactions received by the server.
/// Transaction entries will eventually be fetched by the miner and assembled into blocks.
#[derive(Debug)]
pub struct MemoryPool<T: TransactionScheme + Send + Sync + 'static> {
    /// The mapping of all unconfirmed transaction IDs to their corresponding transaction data.
    pub transactions: MpmcMap<Vec<u8>, Entry<T>>,
    /// The total size in bytes of the current memory pool.
    pub total_size_in_bytes: AtomicUsize,
}

impl<T: TransactionScheme + Send + Sync + 'static> Clone for MemoryPool<T> {
    fn clone(&self) -> Self {
        Self {
            transactions: self.transactions.clone(),
            total_size_in_bytes: AtomicUsize::new(self.total_size_in_bytes.load(Ordering::SeqCst)),
        }
    }
}

const BLOCK_HEADER_SIZE: usize = BlockHeader::size();
const COINBASE_TRANSACTION_SIZE: usize = 1490; // TODO Find the value for actual coinbase transaction size

impl<T: TransactionScheme + Send + Sync + 'static> MemoryPool<T> {
    /// Initialize a new memory pool with no transactions
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Load the memory pool from previously stored state in storage
    pub async fn from_storage<P: LoadableMerkleParameters, S: Storage>(
        storage: &Ledger<T, P, S>,
    ) -> Result<Self, ConsensusError> {
        let memory_pool = Self::new();

        if let Ok(Some(serialized_transactions)) = storage.get_memory_pool() {
            if let Ok(transaction_bytes) = Transactions::<T>::read_le(&serialized_transactions[..]) {
                for transaction in transaction_bytes.0 {
                    let size = transaction.size();
                    let entry = Entry {
                        transaction,
                        size_in_bytes: size,
                    };
                    memory_pool.insert(storage, entry).await?;
                }
            }
        }

        Ok(memory_pool)
    }

    /// Store the memory pool state to the database
    #[inline]
    pub fn store<P: LoadableMerkleParameters, S: Storage>(
        &self,
        storage: &Ledger<T, P, S>,
    ) -> Result<(), ConsensusError> {
        let mut transactions = Transactions::<T>::new();

        for (_transaction_id, entry) in self.transactions.inner().iter() {
            transactions.push(entry.transaction.clone())
        }

        let serialized_transactions = to_bytes_le![transactions]?.to_vec();

        storage.store_to_memory_pool(serialized_transactions)?;

        Ok(())
    }

    /// Adds entry to memory pool if valid in the current ledger.
    pub async fn insert<P: LoadableMerkleParameters, S: Storage>(
        &self,
        storage: &Ledger<T, P, S>,
        entry: Entry<T>,
    ) -> Result<Option<Vec<u8>>, ConsensusError> {
        let transaction_serial_numbers = entry.transaction.old_serial_numbers();
        let transaction_commitments = entry.transaction.new_commitments();
        let transaction_memo = entry.transaction.memorandum();

        if has_duplicates(transaction_serial_numbers)
            || has_duplicates(transaction_commitments)
            || self.contains(&entry)
        {
            return Ok(None);
        }

        let mut holding_serial_numbers = vec![];
        let mut holding_commitments = vec![];
        let mut holding_memos = Vec::with_capacity(self.transactions.inner().len());

        let txns = self.transactions.inner();
        for (_, tx) in txns.iter() {
            holding_serial_numbers.extend(tx.transaction.old_serial_numbers());
            holding_commitments.extend(tx.transaction.new_commitments());
            holding_memos.push(tx.transaction.memorandum());
        }

        for sn in transaction_serial_numbers {
            if storage.contains_sn(sn) || holding_serial_numbers.contains(&sn) {
                return Ok(None);
            }
        }

        for cm in transaction_commitments {
            if storage.contains_cm(cm) || holding_commitments.contains(&cm) {
                return Ok(None);
            }
        }

        if storage.contains_memo(transaction_memo) || holding_memos.contains(&transaction_memo) {
            return Ok(None);
        }

        let transaction_id = entry.transaction.transaction_id()?.to_vec();

        self.total_size_in_bytes
            .fetch_add(entry.size_in_bytes, Ordering::SeqCst);
        self.transactions.insert(transaction_id.clone(), entry).await;

        Ok(Some(transaction_id))
    }

    /// Cleanse the memory pool of outdated transactions.
    #[inline]
    pub async fn cleanse<P: LoadableMerkleParameters, S: Storage>(
        &self,
        storage: &Ledger<T, P, S>,
    ) -> Result<(), ConsensusError> {
        let new_memory_pool = Self::new();

        for (_, entry) in self.clone().transactions.inner().iter() {
            new_memory_pool.insert(storage, entry.clone()).await?;
        }

        self.total_size_in_bytes.store(
            new_memory_pool.total_size_in_bytes.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.transactions.reset(new_memory_pool.transactions.inner_full()).await;

        Ok(())
    }

    /// Removes transaction from memory pool or error.
    #[inline]
    pub async fn remove(&self, entry: &Entry<T>) -> Result<Option<Vec<u8>>, ConsensusError> {
        if self.contains(entry) {
            self.total_size_in_bytes
                .fetch_sub(entry.size_in_bytes, Ordering::SeqCst);

            let transaction_id = entry.transaction.transaction_id()?.to_vec();

            self.transactions.remove(transaction_id.to_vec()).await;

            return Ok(Some(transaction_id));
        }

        Ok(None)
    }

    /// Removes transaction from memory pool based on the transaction id.
    #[inline]
    pub async fn remove_by_hash(&self, transaction_id: &[u8]) -> Result<Option<Entry<T>>, ConsensusError> {
        match self.transactions.get(transaction_id) {
            Some(entry) => {
                self.total_size_in_bytes
                    .fetch_sub(entry.size_in_bytes, Ordering::SeqCst);

                self.transactions.remove(transaction_id.to_vec()).await;

                Ok(Some(entry.clone()))
            }
            None => Ok(None),
        }
    }

    /// Returns whether or not the memory pool contains the entry.
    #[inline]
    pub fn contains(&self, entry: &Entry<T>) -> bool {
        match &entry.transaction.transaction_id() {
            Ok(transaction_id) => self.transactions.contains_key(&transaction_id.to_vec()),
            Err(_) => false,
        }
    }

    /// Get candidate transactions for a new block.
    pub fn get_candidates<P: LoadableMerkleParameters, S: Storage>(
        &self,
        storage: &Ledger<T, P, S>,
        max_size: usize,
    ) -> Result<Transactions<T>, ConsensusError> {
        let max_size = max_size - (BLOCK_HEADER_SIZE + COINBASE_TRANSACTION_SIZE);

        let mut block_size = 0;
        let mut transactions = Transactions::new();

        // TODO Change naive transaction selection
        for (_transaction_id, entry) in self.transactions.inner().iter() {
            if block_size + entry.size_in_bytes <= max_size {
                if storage.transaction_conflicts(&entry.transaction) || transactions.conflicts(&entry.transaction) {
                    continue;
                }

                block_size += entry.size_in_bytes;
                transactions.push(entry.transaction.clone());
            }
        }

        Ok(transactions)
    }
}

impl<T: TransactionScheme + Send + Sync + 'static> Default for MemoryPool<T> {
    fn default() -> Self {
        Self {
            total_size_in_bytes: AtomicUsize::new(0),
            transactions: MpmcMap::<Vec<u8>, Entry<T>>::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkos_testing::sync::*;
    use snarkvm_dpc::{testnet1::instantiated::Testnet1Transaction, Block};

    // MemoryPool tests use TRANSACTION_2 because memory pools shouldn't store coinbase transactions

    #[tokio::test]
    async fn push() {
        let blockchain = FIXTURE_VK.ledger();

        let mem_pool = MemoryPool::new();
        let transaction = Testnet1Transaction::read_le(&TRANSACTION_2[..]).unwrap();
        let size = TRANSACTION_2.len();

        mem_pool
            .insert(&blockchain, Entry {
                size_in_bytes: size,
                transaction: transaction.clone(),
            })
            .await
            .unwrap();

        assert_eq!(size, mem_pool.total_size_in_bytes.load(Ordering::SeqCst));
        assert_eq!(1, mem_pool.transactions.len());

        // Duplicate pushes don't do anything

        mem_pool
            .insert(&blockchain, Entry {
                size_in_bytes: size,
                transaction,
            })
            .await
            .unwrap();

        assert_eq!(size, mem_pool.total_size_in_bytes.load(Ordering::SeqCst));
        assert_eq!(1, mem_pool.transactions.len());
    }

    #[tokio::test]
    async fn remove_entry() {
        let blockchain = FIXTURE_VK.ledger();

        let mem_pool = MemoryPool::new();
        let transaction = Testnet1Transaction::read_le(&TRANSACTION_2[..]).unwrap();
        let size = TRANSACTION_2.len();

        let entry = Entry::<Testnet1Transaction> {
            size_in_bytes: size,
            transaction,
        };

        mem_pool.insert(&blockchain, entry.clone()).await.unwrap();

        assert_eq!(1, mem_pool.transactions.len());
        assert_eq!(size, mem_pool.total_size_in_bytes.load(Ordering::SeqCst));

        mem_pool.remove(&entry).await.unwrap();

        assert_eq!(0, mem_pool.transactions.len());
        assert_eq!(0, mem_pool.total_size_in_bytes.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn remove_transaction_by_hash() {
        let blockchain = FIXTURE_VK.ledger();

        let mem_pool = MemoryPool::new();
        let transaction = Testnet1Transaction::read_le(&TRANSACTION_2[..]).unwrap();
        let size = TRANSACTION_2.len();

        mem_pool
            .insert(&blockchain, Entry {
                size_in_bytes: size,
                transaction: transaction.clone(),
            })
            .await
            .unwrap();

        assert_eq!(1, mem_pool.transactions.len());
        assert_eq!(size, mem_pool.total_size_in_bytes.load(Ordering::SeqCst));

        mem_pool
            .remove_by_hash(&transaction.transaction_id().unwrap().to_vec())
            .await
            .unwrap();

        assert_eq!(0, mem_pool.transactions.len());
        assert_eq!(0, mem_pool.total_size_in_bytes.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn get_candidates() {
        let blockchain = FIXTURE_VK.ledger();

        let mem_pool = MemoryPool::new();
        let transaction = Testnet1Transaction::read_le(&TRANSACTION_2[..]).unwrap();

        let size = to_bytes_le![transaction].unwrap().len();

        let expected_transaction = transaction.clone();
        mem_pool
            .insert(&blockchain, Entry {
                size_in_bytes: size,
                transaction,
            })
            .await
            .unwrap();

        let max_block_size = size + BLOCK_HEADER_SIZE + COINBASE_TRANSACTION_SIZE;

        let candidates = mem_pool.get_candidates(&blockchain, max_block_size).unwrap();

        assert!(candidates.contains(&expected_transaction));
    }

    #[tokio::test]
    async fn store_memory_pool() {
        let blockchain = FIXTURE_VK.ledger();

        let mem_pool = MemoryPool::new();
        let transaction = Testnet1Transaction::read_le(&TRANSACTION_2[..]).unwrap();
        mem_pool
            .insert(&blockchain, Entry {
                size_in_bytes: TRANSACTION_2.len(),
                transaction,
            })
            .await
            .unwrap();

        assert_eq!(1, mem_pool.transactions.len());

        mem_pool.store(&blockchain).unwrap();

        let new_mem_pool = MemoryPool::from_storage(&blockchain).await.unwrap();

        assert_eq!(
            mem_pool.total_size_in_bytes.load(Ordering::SeqCst),
            new_mem_pool.total_size_in_bytes.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn cleanse_memory_pool() {
        let blockchain = FIXTURE_VK.ledger();

        let mem_pool = MemoryPool::new();
        let transaction = Testnet1Transaction::read_le(&TRANSACTION_2[..]).unwrap();
        mem_pool
            .insert(&blockchain, Entry {
                size_in_bytes: TRANSACTION_2.len(),
                transaction,
            })
            .await
            .unwrap();

        assert_eq!(1, mem_pool.transactions.len());

        mem_pool.store(&blockchain).unwrap();

        let block_1 = Block::<Testnet1Transaction>::read_le(&BLOCK_1[..]).unwrap();
        let block_2 = Block::<Testnet1Transaction>::read_le(&BLOCK_2[..]).unwrap();

        blockchain.insert_and_commit(&block_1).unwrap();
        blockchain.insert_and_commit(&block_2).unwrap();

        mem_pool.cleanse(&blockchain).await.unwrap();

        assert_eq!(0, mem_pool.transactions.len());
        assert_eq!(0, mem_pool.total_size_in_bytes.load(Ordering::SeqCst));
    }
}
