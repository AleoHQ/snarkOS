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
use futures::executor::block_on;
use snarkvm_algorithms::{merkle_tree::*, traits::LoadableMerkleParameters};
use snarkvm_objects::{Block, LedgerScheme, Transaction};

use crate::Storage;
use std::sync::Arc;

pub struct LedgerSchemeCompat<T: Transaction + Send + 'static, P: LoadableMerkleParameters, S: Storage>(
    pub Arc<Ledger<T, P, S>>,
);

impl<T: Transaction + Send + 'static, P: LoadableMerkleParameters, S: Storage> LedgerScheme
    for LedgerSchemeCompat<T, P, S>
{
    type Block = Block<Self::Transaction>;
    type Commitment = T::Commitment;
    type MerkleParameters = P;
    type MerklePath = MerklePath<Self::MerkleParameters>;
    type MerkleTreeDigest = MerkleTreeDigest<Self::MerkleParameters>;
    type SerialNumber = T::SerialNumber;
    type Transaction = T;

    fn new(
        _path: Option<&std::path::Path>,
        _parameters: Arc<Self::MerkleParameters>,
        _genesis_block: Self::Block,
    ) -> anyhow::Result<Self> {
        unimplemented!("LedgerScheme::new")
    }

    fn len(&self) -> usize {
        self.0.get_current_block_height() as usize
    }

    fn parameters(&self) -> &Arc<Self::MerkleParameters> {
        &self.0.ledger_parameters
    }

    fn digest(&self) -> Option<Self::MerkleTreeDigest> {
        block_on(self.0.digest())
    }

    fn validate_digest(&self, digest: &Self::MerkleTreeDigest) -> bool {
        block_on(self.0.validate_digest(digest))
    }

    fn contains_cm(&self, cm: &Self::Commitment) -> bool {
        block_on(self.0.contains_cm(cm))
    }

    fn contains_sn(&self, sn: &Self::SerialNumber) -> bool {
        block_on(self.0.contains_sn(sn))
    }

    fn contains_memo(&self, memo: &T::Memorandum) -> bool {
        block_on(self.0.contains_memo(memo))
    }

    fn prove_cm(&self, cm: &Self::Commitment) -> anyhow::Result<Self::MerklePath> {
        block_on(self.0.prove_cm(cm))
    }

    fn verify_cm(
        parameters: &Arc<Self::MerkleParameters>,
        digest: &Self::MerkleTreeDigest,
        cm: &Self::Commitment,
        witness: &Self::MerklePath,
    ) -> bool {
        Ledger::<T, P, S>::verify_cm(parameters, digest, cm, witness)
    }
}
