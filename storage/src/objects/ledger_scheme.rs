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
use snarkvm_algorithms::{merkle_tree::*, traits::LoadableMerkleParameters};
use snarkvm_dpc::LedgerError;
use snarkvm_objects::{Block, Transaction};
use snarkvm_utilities::{
    bytes::{FromBytes, ToBytes},
    to_bytes,
};

use crate::Storage;
use std::{marker::PhantomData, sync::Arc};

impl<T: Transaction + Send + 'static, P: LoadableMerkleParameters, S: Storage> Ledger<T, P, S> {
    /// Instantiates a new ledger with a genesis block.
    pub async fn new(storage: S, parameters: Arc<P>, genesis_block: Block<T>) -> anyhow::Result<Self> {
        if let Some(block_num) = storage.get(COL_META, KEY_BEST_BLOCK_NUMBER.as_bytes()).await? {
            if bytes_to_u32(&block_num) != 0 {
                return Err(LedgerError::ExistingDatabase.into());
            }
        }

        let leaves: &[[u8; 32]] = &[];
        let empty_cm_merkle_tree = MerkleTree::<P>::new(parameters.clone(), leaves)?;

        let ledger_storage = Self {
            current_block_height: Default::default(),
            storage,
            cm_merkle_tree: ArcSwap::new(Arc::new(empty_cm_merkle_tree)),
            ledger_parameters: parameters,
            _transaction: PhantomData,
        };

        ledger_storage.insert_and_commit(&genesis_block).await?;

        Ok(ledger_storage)
    }

    /// Return a digest of the latest ledger Merkle tree.
    pub async fn digest(&self) -> Option<MerkleTreeDigest<P>> {
        let digest: MerkleTreeDigest<P> = FromBytes::read(&self.current_digest().await.unwrap()[..]).unwrap();
        Some(digest)
    }

    /// Check that st_{ts} is a valid digest for some (past) ledger state.
    pub async fn validate_digest(&self, digest: &MerkleTreeDigest<P>) -> bool {
        self.storage
            .exists(COL_DIGEST, &to_bytes![digest].unwrap())
            .await
            .unwrap_or(false)
    }

    /// Returns true if the given commitment exists in the ledger.
    pub async fn contains_cm(&self, cm: &T::Commitment) -> bool {
        self.storage
            .exists(COL_COMMITMENT, &to_bytes![cm].unwrap())
            .await
            .unwrap_or(false)
    }

    /// Returns true if the given serial number exists in the ledger.
    pub async fn contains_sn(&self, sn: &T::SerialNumber) -> bool {
        self.storage
            .exists(COL_SERIAL_NUMBER, &to_bytes![sn].unwrap())
            .await
            .unwrap_or(false)
    }

    /// Returns true if the given memo exists in the ledger.
    pub async fn contains_memo(&self, memo: &T::Memorandum) -> bool {
        self.storage
            .exists(COL_MEMO, &to_bytes![memo].unwrap())
            .await
            .unwrap_or(false)
    }

    /// Returns the Merkle path to the latest ledger digest
    /// for a given commitment, if it exists in the ledger.
    pub async fn prove_cm(&self, cm: &T::Commitment) -> anyhow::Result<MerklePath<P>> {
        let cm_index = self
            .get_cm_index(&to_bytes![cm]?)
            .await?
            .ok_or(LedgerError::InvalidCmIndex)?;
        let result = self.cm_merkle_tree.load().generate_proof(cm_index, cm)?;

        Ok(result)
    }

    /// Returns true if the given Merkle path is a valid witness for
    /// the given ledger digest and commitment.
    pub fn verify_cm(
        _parameters: &Arc<P>,
        digest: &MerkleTreeDigest<P>,
        cm: &T::Commitment,
        witness: &MerklePath<P>,
    ) -> bool {
        witness.verify(&digest, cm).unwrap()
    }
}
