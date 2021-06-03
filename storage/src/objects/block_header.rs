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

use crate::{Ledger, Storage, StorageError, COL_BLOCK_HEADER};
use snarkvm_algorithms::traits::LoadableMerkleParameters;
use snarkvm_objects::{Block, BlockHeader, BlockHeaderHash, Transaction};
use snarkvm_utilities::FromBytes;

impl<T: Transaction + Send + 'static, P: LoadableMerkleParameters, S: Storage> Ledger<T, P, S> {
    /// Returns true if the block for the given block header hash exists.
    pub async fn block_hash_exists(&self, block_hash: &BlockHeaderHash) -> bool {
        if self.is_empty().await {
            return false;
        }

        self.get_block_header(block_hash).await.is_ok()
    }

    /// Get a block header given the block hash.
    pub async fn get_block_header(&self, block_hash: &BlockHeaderHash) -> Result<BlockHeader, StorageError> {
        match self.storage.get(COL_BLOCK_HEADER, &block_hash.0).await? {
            Some(block_header_bytes) => Ok(BlockHeader::read(&block_header_bytes[..])?),
            None => Err(StorageError::MissingBlockHeader(block_hash.to_string())),
        }
    }

    /// Returns true if the block corresponding to this block's previous_block_hash exists.
    pub async fn previous_block_hash_exists(&self, block: &Block<T>) -> bool {
        self.block_hash_exists(&block.header.previous_block_hash).await
    }

    /// Returns the latest shared block header hash.
    /// If the block locator hashes are for a side chain, returns the common point of fork.
    /// If the block locator hashes are for the canon chain, returns the latest block header hash.
    pub async fn get_latest_shared_hash(
        &self,
        block_locator_hashes: Vec<BlockHeaderHash>,
    ) -> Result<BlockHeaderHash, StorageError> {
        for block_hash in block_locator_hashes {
            if self.is_canon(&block_hash).await {
                return Ok(block_hash);
            }
        }

        self.get_block_hash(0).await
    }

    /// Returns a list of block locator hashes. The purpose of this method is to detect
    /// wrong branches in the caller's canon chain.
    pub async fn get_block_locator_hashes(&self) -> Result<Vec<BlockHeaderHash>, StorageError> {
        // Start from the latest block and work backwards
        let mut index = self.get_current_block_height();

        // Update the step size with each iteration
        let mut step = 1;

        // The output list of block locator hashes
        let mut block_locator_hashes = vec![];

        while index > 0 {
            block_locator_hashes.push(self.get_block_hash(index).await?);
            if block_locator_hashes.len() >= 20 {
                step *= 2;
            }

            // Check whether it is appropriate to terminate
            if index < step {
                // If the genesis block has not already been include, add it to the final output
                if index != 1 {
                    block_locator_hashes.push(self.get_block_hash(0).await?);
                }
                break;
            }

            index -= step;
        }

        Ok(block_locator_hashes)
    }
}
