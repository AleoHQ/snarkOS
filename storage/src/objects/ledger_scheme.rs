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
use snarkvm_dpc::{Block, LedgerError, LedgerScheme, Storage, TransactionScheme};
use snarkvm_utilities::{
    bytes::{FromBytes, ToBytes},
    to_bytes_le,
};

use std::{fs, marker::PhantomData, path::Path, sync::Arc};

impl<T: TransactionScheme, P: LoadableMerkleParameters, S: Storage> LedgerScheme for Ledger<T, P, S> {
    type Block = Block<Self::Transaction>;
    type Commitment = T::Commitment;
    type MerkleParameters = P;
    type MerklePath = MerklePath<Self::MerkleParameters>;
    type MerkleTreeDigest = MerkleTreeDigest<Self::MerkleParameters>;
    type SerialNumber = T::SerialNumber;
    type Transaction = T;

    /// Instantiates a new ledger with a genesis block.
    fn new(
        path: Option<&Path>,
        parameters: Arc<Self::MerkleParameters>,
        genesis_block: Self::Block,
    ) -> anyhow::Result<Self> {
        let storage = if let Some(path) = path {
            fs::create_dir_all(&path).map_err(|err| LedgerError::Message(err.to_string()))?;

            S::open(Some(path), None)
        } else {
            S::open(None, None) // this must mean we're using an in-memory storage
        }?;

        if let Some(block_num) = storage.get(COL_META, KEY_BEST_BLOCK_NUMBER.as_bytes())? {
            if bytes_to_u32(&block_num) != 0 {
                return Err(LedgerError::ExistingDatabase.into());
            }
        }

        let leaves: &[[u8; 32]] = &[];
        let empty_cm_merkle_tree = MerkleTree::<Self::MerkleParameters>::new(parameters.clone(), leaves)?;

        let ledger_storage = Self {
            current_block_height: Default::default(),
            storage,
            cm_merkle_tree: ArcSwap::new(Arc::new(empty_cm_merkle_tree)),
            ledger_parameters: parameters,
            _transaction: PhantomData,
        };

        ledger_storage.insert_and_commit(&genesis_block)?;

        Ok(ledger_storage)
    }

    /// Returns the number of blocks including the genesis block
    fn len(&self) -> usize {
        self.get_current_block_height() as usize + 1
    }

    /// Return the parameters used to construct the ledger Merkle tree.
    fn parameters(&self) -> &Arc<Self::MerkleParameters> {
        &self.ledger_parameters
    }

    /// Return a digest of the latest ledger Merkle tree.
    fn digest(&self) -> Option<Self::MerkleTreeDigest> {
        let digest: Self::MerkleTreeDigest = FromBytes::read_le(&self.current_digest().unwrap()[..]).unwrap();
        Some(digest)
    }

    /// Check that st_{ts} is a valid digest for some (past) ledger state.
    fn validate_digest(&self, digest: &Self::MerkleTreeDigest) -> bool {
        self.storage.exists(COL_DIGEST, &to_bytes_le![digest].unwrap())
    }

    /// Returns true if the given commitment exists in the ledger.
    fn contains_cm(&self, cm: &Self::Commitment) -> bool {
        self.storage.exists(COL_COMMITMENT, &to_bytes_le![cm].unwrap())
    }

    /// Returns true if the given serial number exists in the ledger.
    fn contains_sn(&self, sn: &Self::SerialNumber) -> bool {
        self.storage.exists(COL_SERIAL_NUMBER, &to_bytes_le![sn].unwrap())
    }

    /// Returns true if the given memo exists in the ledger.
    fn contains_memo(&self, memo: &<Self::Transaction as TransactionScheme>::Memorandum) -> bool {
        self.storage.exists(COL_MEMO, &to_bytes_le![memo].unwrap())
    }

    /// Returns the Merkle path to the latest ledger digest
    /// for a given commitment, if it exists in the ledger.
    fn prove_cm(&self, cm: &Self::Commitment) -> anyhow::Result<Self::MerklePath> {
        let cm_index = self
            .get_cm_index(&to_bytes_le![cm]?)?
            .ok_or(LedgerError::InvalidCmIndex)?;
        let result = self.cm_merkle_tree.load().generate_proof(cm_index, cm)?;

        Ok(result)
    }

    /// Returns true if the given Merkle path is a valid witness for
    /// the given ledger digest and commitment.
    fn verify_cm(
        _parameters: &Arc<Self::MerkleParameters>,
        digest: &Self::MerkleTreeDigest,
        cm: &Self::Commitment,
        witness: &Self::MerklePath,
    ) -> bool {
        witness.verify(digest, cm).unwrap()
    }
}
