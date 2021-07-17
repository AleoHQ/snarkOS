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

use crate::*;
use arc_swap::ArcSwap;
use snarkos_parameters::GenesisBlock;
use snarkvm_algorithms::{merkle_tree::MerkleTree, traits::LoadableMerkleParameters};
use snarkvm_dpc::{errors::StorageError, Block, DatabaseTransaction, LedgerScheme, Op, Storage, TransactionScheme};
use snarkvm_parameters::{traits::genesis::Genesis, LedgerMerkleTreeParameters, Parameter};
use snarkvm_utilities::bytes::FromBytes;

use std::{
    fs,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

pub type BlockHeight = u32;

pub struct Ledger<T: TransactionScheme, P: LoadableMerkleParameters, S: Storage> {
    pub current_block_height: AtomicU32,
    pub ledger_parameters: Arc<P>,
    pub cm_merkle_tree: ArcSwap<MerkleTree<P>>,
    pub storage: S,
    pub _transaction: PhantomData<T>,
}

impl<T: TransactionScheme, P: LoadableMerkleParameters, S: Storage> Ledger<T, P, S> {
    /// Create a fresh blockchain, optionally at the specified path.
    /// Warning: if specified, any existing storage at that location is removed.
    pub fn new_empty<PATH: AsRef<Path>>(path: Option<PATH>) -> Result<Self, StorageError> {
        if let Some(ref path) = path {
            let _ = fs::remove_dir_all(path);

            Self::open_at_path(path)
        } else {
            let crh = P::H::from(FromBytes::read_le(&LedgerMerkleTreeParameters::load_bytes()?[..])?);
            let ledger_parameters = Arc::new(P::from(crh));

            let genesis_block: Block<T> = FromBytes::read_le(GenesisBlock::load_bytes().as_slice())?;

            Ok(Self::new(None, ledger_parameters, genesis_block).expect("Ledger could not be instantiated"))
        }
    }

    /// Open the blockchain storage at a particular path.
    pub fn open_at_path<PATH: AsRef<Path>>(path: PATH) -> Result<Self, StorageError> {
        fs::create_dir_all(path.as_ref())?;

        Self::load_ledger_state(path, true)
    }

    /// Open the blockchain storage at a particular path as a secondary read-only instance.
    pub fn open_secondary_at_path<PATH: AsRef<Path>>(path: PATH) -> Result<Self, StorageError> {
        fs::create_dir_all(path.as_ref())?;

        Self::load_ledger_state(path, false)
    }

    /// Returns true if there are no blocks in the ledger.
    pub fn is_empty(&self) -> bool {
        self.get_latest_block().is_err()
    }

    /// Get the latest block height of the chain.
    pub fn get_current_block_height(&self) -> BlockHeight {
        self.current_block_height.load(Ordering::SeqCst)
    }

    /// Get the latest number of blocks in the chain.
    pub fn get_block_count(&self) -> BlockHeight {
        self.get_current_block_height() + 1
    }

    /// Get the height of the best block on the chain.
    pub fn get_best_block_number(&self) -> Result<BlockHeight, StorageError> {
        let best_block_number_bytes = self
            .storage
            .get(COL_META, KEY_BEST_BLOCK_NUMBER.as_bytes())?
            .ok_or_else(|| StorageError::Message("Can't obtain the best block's number".into()))?;

        Ok(bytes_to_u32(&best_block_number_bytes))
    }

    /// Get the stored old connected peers.
    pub fn get_peer_book(&self) -> Result<Option<Vec<u8>>, StorageError> {
        self.storage.get(COL_META, &KEY_PEER_BOOK.as_bytes().to_vec())
    }

    /// Store the connected peers.
    pub fn save_peer_book_to_storage(&self, peers_serialized: Vec<u8>) -> Result<(), StorageError> {
        let op = Op::Insert {
            col: COL_META,
            key: KEY_PEER_BOOK.as_bytes().to_vec(),
            value: peers_serialized,
        };
        self.storage.batch(DatabaseTransaction(vec![op]))
    }

    /// Returns a `Ledger` with the latest state loaded from storage at a given path as
    /// a primary or secondary ledger. A secondary ledger runs as a read-only instance.
    fn load_ledger_state<PATH: AsRef<Path>>(path: PATH, primary: bool) -> Result<Self, StorageError> {
        let mut secondary_path_os_string = path.as_ref().to_path_buf().into_os_string();
        secondary_path_os_string.push("_secondary");

        let secondary_path = PathBuf::from(secondary_path_os_string);

        let latest_block_number = {
            let storage = match primary {
                true => S::open(Some(path.as_ref()), None)?,
                false => S::open(Some(path.as_ref()), Some(&secondary_path))?,
            };
            storage.get(COL_META, KEY_BEST_BLOCK_NUMBER.as_bytes())?
        };

        let crh = P::H::from(FromBytes::read_le(&LedgerMerkleTreeParameters::load_bytes()?[..])?);
        let ledger_parameters = Arc::new(P::from(crh));

        match latest_block_number {
            Some(val) => {
                let storage = match primary {
                    true => S::open(Some(path.as_ref()), None)?,
                    false => S::open(Some(path.as_ref()), Some(&secondary_path))?,
                };

                // Build commitment merkle tree

                let mut cm_and_indices = vec![];

                let cms = storage.get_col(COL_COMMITMENT)?;

                for (commitment_key, index_value) in cms {
                    let commitment: T::Commitment = FromBytes::read_le(&commitment_key[..])?;
                    let index = bytes_to_u32(&index_value) as usize;

                    cm_and_indices.push((commitment, index));
                }

                cm_and_indices.sort_by(|&(_, i), &(_, j)| i.cmp(&j));
                let commitments = cm_and_indices.into_iter().map(|(cm, _)| cm).collect::<Vec<_>>();

                let merkle_tree = MerkleTree::new(ledger_parameters.clone(), &commitments[..])?;

                Ok(Self {
                    current_block_height: AtomicU32::new(bytes_to_u32(&val)),
                    storage,
                    cm_merkle_tree: ArcSwap::new(Arc::new(merkle_tree)),
                    ledger_parameters,
                    _transaction: PhantomData,
                })
            }
            None => {
                // Add genesis block to database

                let genesis_block: Block<T> = FromBytes::read_le(GenesisBlock::load_bytes().as_slice())?;

                let ledger_storage = Self::new(Some(path.as_ref()), ledger_parameters, genesis_block)
                    .expect("Ledger could not be instantiated");

                // If there did not exist a primary ledger at the path,
                // then create one and then open the secondary instance.
                if !primary {
                    return Self::load_ledger_state(path, primary);
                }

                Ok(ledger_storage)
            }
        }
    }

    /// Attempt to catch the secondary read-only storage instance with the primary instance.
    pub fn catch_up_secondary(&self, update_merkle_tree: bool, primary_height: u32) -> Result<(), StorageError> {
        let secondary_height = self.get_current_block_height();

        // If the primary block height is greater than the secondary block height, attempt to catch up,
        // update the block height and potentially the merkle tree.
        if primary_height > secondary_height {
            // Sync the secondary and primary instances
            if self.storage.try_catch_up_with_primary().is_ok() {
                // Update the latest block height of the secondary instance.
                self.current_block_height.store(primary_height, Ordering::SeqCst);

                // Optional `cm_merkle_tree` regeneration because not all usages of
                // the secondary instance requires it.
                if update_merkle_tree {
                    // Update the Merkle tree of the secondary instance.
                    self.rebuild_merkle_tree(vec![])?;
                }
            }
        }

        Ok(())
    }
}
