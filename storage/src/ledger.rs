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
use arc_swap::ArcSwap;
use snarkos_parameters::GenesisBlock;
use snarkvm_algorithms::{merkle_tree::MerkleTree, traits::LoadableMerkleParameters};
use snarkvm_objects::{Block, Transaction};
use snarkvm_parameters::{traits::genesis::Genesis, LedgerMerkleTreeParameters, Parameter};
use snarkvm_utilities::bytes::FromBytes;

use std::{
    marker::PhantomData,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

pub type BlockHeight = u32;

pub struct Ledger<T: Transaction, P: LoadableMerkleParameters, S: Storage> {
    pub current_block_height: AtomicU32,
    pub ledger_parameters: Arc<P>,
    pub cm_merkle_tree: ArcSwap<MerkleTree<P>>,
    pub storage: S,
    pub _transaction: PhantomData<T>,
}

impl<T: Transaction + Send + 'static, P: LoadableMerkleParameters, S: Storage> Ledger<T, P, S> {
    /// Returns true if there are no blocks in the ledger.
    pub async fn is_empty(&self) -> bool {
        self.get_latest_block().await.is_err()
    }

    /// Get the latest block height of the chain.
    pub fn get_current_block_height(&self) -> BlockHeight {
        self.current_block_height.load(Ordering::SeqCst)
    }

    /// Get the latest number of blocks in the chain.
    pub fn get_block_count(&self) -> BlockHeight {
        self.get_current_block_height() + 1
    }

    /// Get the stored old connected peers.
    pub async fn get_peer_book(&self) -> Result<Option<Vec<u8>>, StorageError> {
        self.storage.get(COL_META, &KEY_PEER_BOOK.as_bytes().to_vec()).await
    }

    /// Store the connected peers.
    pub async fn save_peer_book_to_storage(&self, peers_serialized: Vec<u8>) -> Result<(), StorageError> {
        let op = Op::Insert {
            col: COL_META,
            key: KEY_PEER_BOOK.as_bytes().to_vec(),
            value: peers_serialized,
        };
        self.storage.batch(DatabaseTransaction(vec![op])).await
    }

    /// Returns a `Ledger` with the latest state loaded from storage at a given path as
    /// a primary or secondary ledger. A secondary ledger runs as a read-only instance.
    pub async fn load_ledger_state(storage: S) -> Result<Self, StorageError> {
        let latest_block_number = { storage.get(COL_META, KEY_BEST_BLOCK_NUMBER.as_bytes()).await? };

        let crh = P::H::from(FromBytes::read(&LedgerMerkleTreeParameters::load_bytes()?[..])?);
        let ledger_parameters = Arc::new(P::from(crh));

        match latest_block_number {
            Some(val) => {
                // Build commitment merkle tree

                let mut cm_and_indices = vec![];

                let cms = storage.get_col(COL_COMMITMENT).await?;

                for (commitment_key, index_value) in cms {
                    let commitment: T::Commitment = FromBytes::read(&commitment_key[..])?;
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

                let genesis_block: Block<T> = FromBytes::read(GenesisBlock::load_bytes().as_slice())?;

                let ledger_storage = Self::new(storage, ledger_parameters, genesis_block)
                    .await
                    .expect("Ledger could not be instantiated");

                Ok(ledger_storage)
            }
        }
    }
}
