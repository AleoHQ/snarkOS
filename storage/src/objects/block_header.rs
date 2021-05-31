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

use crate::{Ledger, COL_BLOCK_HEADER};
use snarkvm_algorithms::traits::LoadableMerkleParameters;
use snarkvm_dpc::{errors::StorageError, Block, BlockHeader, BlockHeaderHash, Storage, TransactionScheme};
use snarkvm_utilities::FromBytes;

impl<T: TransactionScheme, P: LoadableMerkleParameters, S: Storage> Ledger<T, P, S> {
    /// Returns true if the block for the given block header hash exists.
    pub fn block_hash_exists(&self, block_hash: &BlockHeaderHash) -> bool {
        if self.is_empty() {
            return false;
        }

        self.get_block_header(block_hash).is_ok()
    }

    /// Get a block header given the block hash.
    pub fn get_block_header(&self, block_hash: &BlockHeaderHash) -> Result<BlockHeader, StorageError> {
        match self.storage.get(COL_BLOCK_HEADER, &block_hash.0)? {
            Some(block_header_bytes) => Ok(BlockHeader::read(&block_header_bytes[..])?),
            None => Err(StorageError::MissingBlockHeader(block_hash.to_string())),
        }
    }

    /// Returns true if the block corresponding to this block's previous_block_hash exists.
    pub fn previous_block_hash_exists(&self, block: &Block<T>) -> bool {
        self.block_hash_exists(&block.header.previous_block_hash)
    }

    /// Returns the latest shared block header hash.
    /// If the block locator hashes are for a side chain, returns the common point of fork.
    /// If the block locator hashes are for the canon chain, returns the latest block header hash.
    pub fn get_latest_shared_hash(
        &self,
        block_locator_hashes: Vec<BlockHeaderHash>,
    ) -> Result<BlockHeaderHash, StorageError> {
        for block_hash in block_locator_hashes {
            if self.is_canon(&block_hash) {
                return Ok(block_hash);
            }
        }

        self.get_block_hash(0)
    }

    /// Returns a list of block locator hashes. The purpose of this method is to detect
    /// wrong branches in the caller's canon chain.
    pub fn get_block_locator_hashes(&self) -> Result<Vec<BlockHeaderHash>, StorageError> {
        // Start from the latest block and work backwards
        let mut index = self.get_current_block_height();

        // The number of "chunks" into which the height is to be divided after the quadratic
        // step used initially grows too high.
        let divisor = std::cmp::min(16u32, std::cmp::max(index, 1));

        // Two different steps used to seek further hashes: a quadratic one and a proportional one.
        let mut quadratic_step = 4;
        let mut proportional_step = index / divisor;
        let mut proportional_step_updated = false;

        // The output list of block locator hashes
        let mut block_locator_hashes = vec![];

        while index > 0 {
            block_locator_hashes.push(self.get_block_hash(index)?);

            // Start by using quadratic steps.
            if proportional_step > quadratic_step {
                index = index.saturating_sub(quadratic_step);
                quadratic_step *= 2;
            } else {
                // Update the proportional step and use it from now on.
                if !proportional_step_updated {
                    proportional_step_updated = true;
                    proportional_step = index / divisor;
                }
                index = index.saturating_sub(proportional_step);
            }
        }
        block_locator_hashes.push(self.get_block_hash(0)?);

        Ok(block_locator_hashes)
    }
}
