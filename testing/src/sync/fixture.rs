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

use crate::{
    dpc::{generate_test_accounts, setup_or_load_parameters},
    storage::*,
};
use snarkos_consensus::MerkleTreeLedger;
use snarkos_parameters::GenesisBlock;
use snarkos_storage::LedgerStorage;
use snarkvm_dpc::{
    testnet1::{instantiated::*, NoopProgram},
    Account,
    Block,
    Storage,
};
use snarkvm_parameters::traits::genesis::Genesis;
use snarkvm_utilities::bytes::FromBytes;

use once_cell::sync::Lazy;
use rand::SeedableRng;
use rand_chacha::ChaChaRng;
use std::{marker::PhantomData, sync::Arc};

pub static FIXTURE: Lazy<Fixture<LedgerStorage>> = Lazy::new(|| setup(false));
pub static FIXTURE_VK: Lazy<Fixture<LedgerStorage>> = Lazy::new(|| setup(true));

// helper for setting up e2e tests
pub struct Fixture<S: Storage> {
    pub dpc: Arc<Testnet1DPC>,
    pub test_accounts: [Account<Components>; 3],
    pub ledger_parameters: Arc<CommitmentMerkleParameters>,
    pub genesis_block: Block<Testnet1Transaction>,
    pub program: NoopProgram<Components>,
    pub rng: ChaChaRng,
    _storage: PhantomData<S>,
}

impl<S: Storage> Fixture<S> {
    pub fn ledger(&self) -> MerkleTreeLedger<S> {
        initialize_test_blockchain(self.ledger_parameters.clone(), self.genesis_block.clone())
    }
}

fn setup<S: Storage>(verify_only: bool) -> Fixture<S> {
    let mut rng = ChaChaRng::seed_from_u64(1231275789u64);

    // Generate or load parameters for the ledger, commitment schemes, and CRH
    let (ledger_parameters, dpc) = setup_or_load_parameters::<_, S>(verify_only, &mut rng);

    // Generate addresses
    let test_accounts = generate_test_accounts::<_, S>(&dpc, &mut rng);

    let genesis_block: Block<Testnet1Transaction> = FromBytes::read_le(GenesisBlock::load_bytes().as_slice()).unwrap();

    let program = dpc.noop_program.clone();

    let dpc = Arc::new(dpc);

    Fixture {
        dpc,
        test_accounts,
        ledger_parameters,
        genesis_block,
        program,
        rng,
        _storage: PhantomData,
    }
}
