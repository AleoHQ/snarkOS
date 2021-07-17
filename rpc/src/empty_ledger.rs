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

use snarkvm_algorithms::{merkle_tree::*, traits::LoadableMerkleParameters};
use snarkvm_dpc::{Block, LedgerScheme, TransactionScheme};

use std::{marker::PhantomData, path::Path, sync::Arc};

pub struct EmptyLedger<T: TransactionScheme, P: LoadableMerkleParameters> {
    parameters: Arc<P>,
    _transaction: PhantomData<T>,
}

impl<T: TransactionScheme, P: LoadableMerkleParameters> LedgerScheme for EmptyLedger<T, P> {
    type Block = Block<Self::Transaction>;
    type Commitment = T::Commitment;
    type MerkleParameters = P;
    type MerklePath = MerklePath<Self::MerkleParameters>;
    type MerkleTreeDigest = MerkleTreeDigest<Self::MerkleParameters>;
    type SerialNumber = T::SerialNumber;
    type Transaction = T;

    /// Instantiates a new ledger with a genesis block.
    fn new(
        _path: Option<&Path>,
        parameters: Arc<Self::MerkleParameters>,
        _genesis_block: Self::Block,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            parameters,
            _transaction: PhantomData,
        })
    }

    /// Returns the number of blocks including the genesis block.
    fn len(&self) -> usize {
        0
    }

    /// Return the parameters used to construct the ledger Merkle tree.
    fn parameters(&self) -> &Arc<Self::MerkleParameters> {
        &self.parameters
    }

    /// Return a digest of the latest ledger Merkle tree.
    fn digest(&self) -> Option<Self::MerkleTreeDigest> {
        None
    }

    /// Check that st_{ts} is a valid digest for some (past) ledger state.
    fn validate_digest(&self, _digest: &Self::MerkleTreeDigest) -> bool {
        true
    }

    /// Returns true if the given commitment exists in the ledger.
    fn contains_cm(&self, _cm: &Self::Commitment) -> bool {
        false
    }

    /// Returns true if the given serial number exists in the ledger.
    fn contains_sn(&self, _sn: &Self::SerialNumber) -> bool {
        false
    }

    /// Returns true if the given memo exists in the ledger.
    fn contains_memo(&self, _memo: &<Self::Transaction as TransactionScheme>::Memorandum) -> bool {
        false
    }

    /// Returns the Merkle path to the latest ledger digest
    /// for a given commitment, if it exists in the ledger.
    fn prove_cm(&self, _cm: &Self::Commitment) -> anyhow::Result<Self::MerklePath> {
        unimplemented!()
    }

    /// Returns true if the given Merkle path is a valid witness for
    /// the given ledger digest and commitment.
    fn verify_cm(
        _parameters: &Arc<Self::MerkleParameters>,
        _digest: &Self::MerkleTreeDigest,
        _cm: &Self::Commitment,
        _witness: &Self::MerklePath,
    ) -> bool {
        true
    }
}
