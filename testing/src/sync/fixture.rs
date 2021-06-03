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
use snarkvm_algorithms::CRH;
use snarkvm_dpc::{
    base_dpc::{instantiated::*, BaseDPCComponents, NoopProgram},
    Account,
    DPCScheme,
};
use snarkvm_objects::Block;
use snarkvm_parameters::traits::genesis::Genesis;
use snarkvm_utilities::{
    bytes::{FromBytes, ToBytes},
    to_bytes,
};

use once_cell::sync::Lazy;
use rand::SeedableRng;
use rand_xorshift::XorShiftRng;
use std::sync::Arc;

pub static FIXTURE: Lazy<Fixture> = Lazy::new(|| setup(false));
pub static FIXTURE_VK: Lazy<Fixture> = Lazy::new(|| setup(true));

// helper for setting up e2e tests
pub struct Fixture {
    pub parameters: <InstantiatedDPC as DPCScheme<CommitmentMerkleParameters>>::NetworkParameters,
    pub test_accounts: [Account<Components>; 3],
    pub ledger_parameters: Arc<CommitmentMerkleParameters>,
    pub genesis_block: Block<Tx>,
    pub program: NoopProgram<Components, <Components as BaseDPCComponents>::NoopProgramSNARK>,
    pub rng: XorShiftRng,
}

impl Fixture {
    pub async fn ledger(&self) -> MerkleTreeLedger<LedgerStorage> {
        initialize_test_blockchain(self.ledger_parameters.clone(), self.genesis_block.clone()).await
    }
}

fn setup(verify_only: bool) -> Fixture {
    let mut rng = XorShiftRng::seed_from_u64(1231275789u64);

    // Generate or load parameters for the ledger, commitment schemes, and CRH
    let (ledger_parameters, parameters) = setup_or_load_parameters(verify_only, &mut rng);

    // Generate addresses
    let test_accounts = generate_test_accounts(&parameters, &mut rng);

    let genesis_block: Block<Tx> = FromBytes::read(GenesisBlock::load_bytes().as_slice()).unwrap();

    let program_vk_hash = to_bytes![
        ProgramVerificationKeyCRH::hash(
            &parameters.system_parameters.program_verification_key_crh,
            &to_bytes![parameters.noop_program_snark_parameters().verification_key].unwrap()
        )
        .unwrap()
    ]
    .unwrap();

    let program = NoopProgram::new(program_vk_hash);

    Fixture {
        parameters,
        test_accounts,
        ledger_parameters,
        genesis_block,
        program,
        rng,
    }
}
