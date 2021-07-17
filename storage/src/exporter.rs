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

use crate::{bytes_to_u32, Ledger, COL_BLOCK_LOCATOR};
use snarkvm_algorithms::traits::LoadableMerkleParameters;
use snarkvm_dpc::{BlockHeaderHash, Storage, TransactionScheme};
use snarkvm_utilities::ToBytes;

use parking_lot::Mutex;
use rayon::prelude::*;
use tracing::*;

use std::{
    cmp::{self, Ordering},
    collections::BinaryHeap,
    fs,
    hash::{Hash, Hasher},
    io::BufWriter,
    path::Path,
};

struct BlockLocatorPair(Box<[u8]>, Box<[u8]>);

impl PartialEq for BlockLocatorPair {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() == other.0.len() {
            // (key0, value0) == (key1, value1) || (value0, key0) == (value1, key1)
            self.0 == other.0
        } else {
            // (key0, value0) == (value1, key1) || (value0, key0) == (key1, value1)
            self.0 == other.1
        }
    }
}
impl Eq for BlockLocatorPair {}

impl Hash for BlockLocatorPair {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Just hash the block number, it's unique
        if self.0.len() < self.1.len() {
            // (number, hash)
            self.0.hash(state);
        } else {
            // (hash, number)
            self.1.hash(state);
        }
    }
}

impl Ord for BlockLocatorPair {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0.len() == other.0.len() {
            // (key0, value0) == (key1, value1) || (value0, key0) == (value1, key1)
            self.0.cmp(&other.0)
        } else {
            // (key0, value0) == (value1, key1) || (value0, key0) == (key1, value1)
            self.0.cmp(&other.1)
        }
    }
}

impl PartialOrd for BlockLocatorPair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: TransactionScheme + Send + Sync, P: LoadableMerkleParameters, S: Storage + Sync> Ledger<T, P, S> {
    /// Serializes the node's stored canon blocks into a single file written to `location`; `limit` specifies the limit
    /// on the number of blocks to export, with `0` being no limit (a full export). Returns the number of exported
    /// blocks.
    pub fn export_canon_blocks(&self, limit: u32, location: &Path) -> Result<usize, anyhow::Error> {
        info!("Exporting the node's canon blocks to {}", location.display());

        let locator_col = self.storage.get_col(COL_BLOCK_LOCATOR)?;

        let numbers_and_hashes = locator_col
            .into_iter()
            .filter(|(locator_key, locator_value)| locator_key.len() < locator_value.len())
            .map(|(block_number_bytes, block_hash)| (bytes_to_u32(&block_number_bytes), block_hash))
            .collect::<BinaryHeap<_>>()
            .into_sorted_vec();

        let number_to_export = if limit == 0 { u32::MAX } else { limit } as usize;

        let blocks = Mutex::new(Vec::with_capacity(cmp::min(numbers_and_hashes.len(), number_to_export)));

        // Skip the genesis block, as it's always known.
        numbers_and_hashes
            .into_par_iter()
            .skip(1)
            .take(number_to_export)
            .for_each(|(block_number, block_hash)| {
                let hash = BlockHeaderHash::new(block_hash.into_vec());
                let block = self
                    .get_block(&hash)
                    .expect("Can't export blocks; one of the blocks was not found!");
                blocks.lock().push((block_number, block));
            });

        let mut blocks = blocks.into_inner();

        blocks.par_sort_unstable_by_key(|(block_number, _block)| *block_number);

        let mut target_file = BufWriter::new(fs::File::create(location)?);
        for (_block_number, block) in &blocks {
            block.write_le(&mut target_file)?;
        }

        Ok(blocks.len())
    }
}
