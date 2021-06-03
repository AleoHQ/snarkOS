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

use crate::{DatabaseTransaction, Op, Storage, StorageError, *};
use snarkvm_algorithms::traits::LoadableMerkleParameters;
use snarkvm_objects::{Block, BlockError, BlockHeader, BlockHeaderHash, DPCTransactions, Transaction};
use snarkvm_utilities::{to_bytes, FromBytes, ToBytes};

use std::sync::atomic::Ordering;

impl<T: Transaction + Send + 'static, P: LoadableMerkleParameters, S: Storage> Ledger<T, P, S> {
    /// Get the latest block in the chain.
    pub async fn get_latest_block(&self) -> Result<Block<T>, StorageError> {
        self.get_block_from_block_number(self.get_current_block_height()).await
    }

    /// Get a block given the block hash.
    pub async fn get_block(&self, block_hash: &BlockHeaderHash) -> Result<Block<T>, StorageError> {
        Ok(Block {
            header: self.get_block_header(block_hash).await?,
            transactions: self.get_block_transactions(block_hash).await?,
        })
    }

    /// Get a block given the block number.
    pub async fn get_block_from_block_number(&self, block_number: u32) -> Result<Block<T>, StorageError> {
        if block_number > self.get_current_block_height() {
            return Err(StorageError::BlockError(BlockError::InvalidBlockNumber(block_number)));
        }

        self.storage
            .atomic(Box::new(move |db| {
                let block_hash = match db.get(COL_BLOCK_LOCATOR, &block_number.to_le_bytes())? {
                    Some(block_header_hash) => BlockHeaderHash::new(block_header_hash),
                    None => return Err(StorageError::MissingBlockHash(block_number)),
                };
                let header = match db.get(COL_BLOCK_HEADER, &block_hash.0)? {
                    Some(block_header_bytes) => BlockHeader::read(&block_header_bytes[..])?,
                    None => return Err(StorageError::MissingBlockHeader(block_hash.to_string())),
                };
                let transactions = match db.get(COL_BLOCK_TRANSACTIONS, &block_hash.0)? {
                    Some(encoded_block_transactions) => DPCTransactions::<T>::read(&encoded_block_transactions[..])?,
                    None => return Err(StorageError::MissingBlockTransactions(block_hash.to_string())),
                };
                Ok(Some(Box::new(Block { header, transactions })))
            }))
            .await
            .map(|x| *x.unwrap())
    }

    /// Get the block hash given a block number.
    pub async fn get_block_hash(&self, block_number: u32) -> Result<BlockHeaderHash, StorageError> {
        match self.storage.get(COL_BLOCK_LOCATOR, &block_number.to_le_bytes()).await? {
            Some(block_header_hash) => Ok(BlockHeaderHash::new(block_header_hash)),
            None => Err(StorageError::MissingBlockHash(block_number)),
        }
    }

    /// Get the block number given a block hash.
    pub async fn get_block_number(&self, block_hash: &BlockHeaderHash) -> Result<u32, StorageError> {
        match self.storage.get(COL_BLOCK_LOCATOR, &block_hash.0).await? {
            Some(block_num_bytes) => Ok(bytes_to_u32(&block_num_bytes)),
            None => Err(StorageError::MissingBlockNumber(block_hash.to_string())),
        }
    }

    /// Get the list of transaction ids given a block hash.
    pub async fn get_block_transactions(
        &self,
        block_hash: &BlockHeaderHash,
    ) -> Result<DPCTransactions<T>, StorageError> {
        match self.storage.get(COL_BLOCK_TRANSACTIONS, &block_hash.0).await? {
            Some(encoded_block_transactions) => Ok(DPCTransactions::read(&encoded_block_transactions[..])?),
            None => Err(StorageError::MissingBlockTransactions(block_hash.to_string())),
        }
    }

    /// Find the potential child block hashes given a parent block header.
    pub async fn get_child_block_hashes(
        &self,
        parent_header: &BlockHeaderHash,
    ) -> Result<Vec<BlockHeaderHash>, StorageError> {
        match self.storage.get(COL_CHILD_HASHES, &parent_header.0).await? {
            Some(encoded_child_block_hashes) => Ok(bincode::deserialize(&encoded_child_block_hashes[..])?),
            None => Ok(vec![]),
        }
    }

    /// Returns the block number of a conflicting block that has already been mined.
    pub async fn already_mined(&self, block: &Block<T>) -> Result<Option<u32>, StorageError> {
        // look up new block's previous block by hash
        // if the block after previous_block_number exists, then someone has already mined this new block
        let previous_block_number = self.get_block_number(&block.header.previous_block_hash).await?;

        let existing_block_number = previous_block_number + 1;

        if self.get_block_from_block_number(existing_block_number).await.is_ok() {
            // the storage has a conflicting block with the same previous_block_hash
            Ok(Some(existing_block_number))
        } else {
            // the new block has no conflicts
            Ok(None)
        }
    }

    /// Remove a block and it's related data from the storage.
    pub async fn remove_block(&self, block_hash: BlockHeaderHash) -> Result<(), StorageError> {
        if self.is_canon(&block_hash).await {
            return Err(StorageError::InvalidBlockRemovalCanon(block_hash.to_string()));
        }

        let mut database_transaction = DatabaseTransaction::new();

        // Remove block transactions

        database_transaction.push(Op::Delete {
            col: COL_BLOCK_TRANSACTIONS,
            key: block_hash.0.to_vec(),
        });

        for transaction in self.get_block_transactions(&block_hash).await?.0 {
            database_transaction.push(Op::Delete {
                col: COL_TRANSACTION_LOCATION,
                key: transaction.transaction_id()?.to_vec(),
            });
        }

        // Remove parent's reference to this block

        let block_header = self.get_block_header(&block_hash).await?;

        let mut child_hashes = self.get_child_block_hashes(&block_header.previous_block_hash).await?;

        if child_hashes.contains(&block_hash) {
            // Remove the block hash from the parent's potential children
            for (index, child) in child_hashes.iter().enumerate() {
                if child == &block_hash {
                    child_hashes.remove(index);

                    database_transaction.push(Op::Insert {
                        col: COL_CHILD_HASHES,
                        key: block_header.previous_block_hash.0.to_vec(),
                        value: bincode::serialize(&child_hashes)?,
                    });
                    break;
                }
            }
        }

        self.storage.batch(database_transaction).await
    }

    /// De-commit the latest block and return its header hash.
    pub async fn decommit_latest_block(&self) -> Result<BlockHeaderHash, StorageError> {
        let current_block_height = self.get_current_block_height();
        if current_block_height == 0 {
            return Err(StorageError::InvalidBlockDecommit);
        }

        let update_best_block_num = current_block_height - 1;
        let block_hash: BlockHeaderHash = self.get_block_hash(current_block_height).await?;

        let mut database_transaction = DatabaseTransaction::new();

        database_transaction.push(Op::Insert {
            col: COL_META,
            key: KEY_BEST_BLOCK_NUMBER.as_bytes().to_vec(),
            value: update_best_block_num.to_le_bytes().to_vec(),
        });

        database_transaction.push(Op::Delete {
            col: COL_DIGEST,
            key: self.current_digest().await?,
        });

        let mut sn_index = self.current_sn_index().await?;
        let mut cm_index = self.current_cm_index().await?;
        let mut memo_index = self.current_memo_index().await?;

        let mut database_transaction = DatabaseTransaction::new();

        for transaction in self.get_block_transactions(&block_hash).await?.0 {
            for sn in transaction.old_serial_numbers() {
                database_transaction.push(Op::Delete {
                    col: COL_SERIAL_NUMBER,
                    key: to_bytes![sn]?.to_vec(),
                });
                sn_index -= 1;
            }

            for cm in transaction.new_commitments() {
                database_transaction.push(Op::Delete {
                    col: COL_COMMITMENT,
                    key: to_bytes![cm]?.to_vec(),
                });
                cm_index -= 1;
            }

            database_transaction.push(Op::Delete {
                col: COL_MEMO,
                key: to_bytes![transaction.memorandum()]?.to_vec(),
            });
            memo_index -= 1;
        }

        // Update the database state for current indexes

        database_transaction.push(Op::Insert {
            col: COL_META,
            key: KEY_CURR_SN_INDEX.as_bytes().to_vec(),
            value: (sn_index as u32).to_le_bytes().to_vec(),
        });
        database_transaction.push(Op::Insert {
            col: COL_META,
            key: KEY_CURR_CM_INDEX.as_bytes().to_vec(),
            value: (cm_index as u32).to_le_bytes().to_vec(),
        });
        database_transaction.push(Op::Insert {
            col: COL_META,
            key: KEY_CURR_MEMO_INDEX.as_bytes().to_vec(),
            value: (memo_index as u32).to_le_bytes().to_vec(),
        });

        database_transaction.push(Op::Delete {
            col: COL_BLOCK_LOCATOR,
            key: current_block_height.to_le_bytes().to_vec(),
        });

        database_transaction.push(Op::Delete {
            col: COL_BLOCK_LOCATOR,
            key: block_hash.0.to_vec(),
        });

        self.storage.batch(database_transaction).await?;

        self.current_block_height.fetch_sub(1, Ordering::SeqCst);

        self.update_merkle_tree().await?;

        Ok(block_hash)
    }

    /// Remove the latest block.
    pub async fn remove_latest_block(&self) -> Result<(), StorageError> {
        let block_hash = self.decommit_latest_block().await?;
        self.remove_block(block_hash).await
    }

    /// Remove the latest `num_blocks` blocks.
    pub async fn remove_latest_blocks(&self, num_blocks: u32) -> Result<(), StorageError> {
        let current_block_height = self.get_current_block_height();
        if num_blocks > current_block_height {
            return Err(StorageError::InvalidBlockRemovalNum(num_blocks, current_block_height));
        }

        for _ in 0..num_blocks {
            self.remove_latest_block().await?;
        }

        Ok(())
    }
}
