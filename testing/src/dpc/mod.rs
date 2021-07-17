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

use snarkos_consensus::MerkleTreeLedger;
use snarkvm_algorithms::{MerkleParameters, CRH};
use snarkvm_dpc::{testnet1::instantiated::*, Account, DPCScheme, Storage};
use snarkvm_parameters::{LedgerMerkleTreeParameters, Parameter};
use snarkvm_utilities::FromBytes;

use rand::{CryptoRng, Rng};
use std::sync::Arc;

pub fn setup_or_load_parameters<R: Rng + CryptoRng, S: Storage>(
    verify_only: bool,
    rng: &mut R,
) -> (Arc<CommitmentMerkleParameters>, Testnet1DPC) {
    // TODO (howardwu): Resolve this inconsistency on import structure with a new model once MerkleParameters are refactored.
    let crh_parameters =
        <MerkleTreeCRH as CRH>::Parameters::read_le(&LedgerMerkleTreeParameters::load_bytes().unwrap()[..])
            .expect("read bytes as hash for MerkleParameters in ledger");
    let merkle_tree_hash_parameters = <CommitmentMerkleParameters as MerkleParameters>::H::from(crh_parameters);
    let ledger_merkle_tree_parameters = Arc::new(From::from(merkle_tree_hash_parameters));

    let dpc = match <Testnet1DPC as DPCScheme<MerkleTreeLedger<S>>>::load(verify_only) {
        Ok(parameters) => parameters,
        Err(err) => {
            println!("error - {}, re-running parameter Setup", err);
            <Testnet1DPC as DPCScheme<MerkleTreeLedger<S>>>::setup(&ledger_merkle_tree_parameters, rng)
                .expect("DPC setup failed")
        }
    };

    (ledger_merkle_tree_parameters, dpc)
}

pub fn generate_test_accounts<R: Rng + CryptoRng, S: Storage>(
    dpc: &Testnet1DPC,
    rng: &mut R,
) -> [Account<Components>; 3] {
    // TODO (howardwu): Remove DPCScheme<MerkleTreeLedger<S>> usage after decoupling ledger.
    let genesis_account = <Testnet1DPC as DPCScheme<MerkleTreeLedger<S>>>::create_account(dpc, rng).unwrap();
    let account_1 = <Testnet1DPC as DPCScheme<MerkleTreeLedger<S>>>::create_account(dpc, rng).unwrap();
    let account_2 = <Testnet1DPC as DPCScheme<MerkleTreeLedger<S>>>::create_account(dpc, rng).unwrap();

    [genesis_account, account_1, account_2]
}
