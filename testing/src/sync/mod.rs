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

use snarkos_consensus::{ConsensusParameters, MerkleTreeLedger};
use snarkos_storage::LedgerStorage;
use snarkvm_algorithms::CRH;
use snarkvm_dpc::{instantiated::Components, DPCComponents};
use snarkvm_objects::{Network, Transaction, TransactionError};
use snarkvm_parameters::{InnerSNARKVKCRHParameters, InnerSNARKVKParameters, Parameter};
use snarkvm_posw::PoswMarlin;
use snarkvm_utilities::{to_bytes, FromBytes, ToBytes};

use once_cell::sync::Lazy;
use std::{
    io::{Read, Result as IoResult, Write},
    sync::Arc,
};

mod e2e;
pub use e2e::*;

mod fixture;
pub use fixture::*;

pub static TEST_CONSENSUS_PARAMS: Lazy<ConsensusParameters> = Lazy::new(|| {
    let inner_snark_verification_key_crh_parameters: <<Components as DPCComponents>::InnerSNARKVerificationKeyCRH as CRH>::Parameters = FromBytes::read(InnerSNARKVKCRHParameters::load_bytes().unwrap().as_slice()).unwrap();

    let inner_snark_verification_key_crh: <Components as DPCComponents>::InnerSNARKVerificationKeyCRH =
        From::from(inner_snark_verification_key_crh_parameters);

    let inner_snark_id = to_bytes![
        inner_snark_verification_key_crh
            .hash(&InnerSNARKVKParameters::load_bytes().unwrap())
            .unwrap()
    ]
    .unwrap();

    ConsensusParameters {
        max_block_size: 1_000_000usize,
        max_nonce: u32::max_value(),
        target_block_time: 2i64, //unix seconds
        network_id: Network::Mainnet,
        verifier: PoswMarlin::verify_only().unwrap(),
        authorized_inner_snark_ids: vec![inner_snark_id],
    }
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestTx;

impl Transaction for TestTx {
    type Commitment = [u8; 32];
    type Digest = [u8; 32];
    type EncryptedRecord = [u8; 32];
    type InnerSNARKID = [u8; 32];
    type LocalDataRoot = [u8; 32];
    type Memorandum = [u8; 32];
    type ProgramCommitment = [u8; 32];
    type SerialNumber = [u8; 32];
    type ValueBalance = i64;

    fn transaction_id(&self) -> Result<[u8; 32], TransactionError> {
        Ok([0u8; 32])
    }

    fn network_id(&self) -> u8 {
        0
    }

    fn ledger_digest(&self) -> &Self::Digest {
        &[0u8; 32]
    }

    fn inner_circuit_id(&self) -> &Self::InnerSNARKID {
        &[0u8; 32]
    }

    fn old_serial_numbers(&self) -> &[Self::SerialNumber] {
        &[[0u8; 32]]
    }

    fn new_commitments(&self) -> &[Self::Commitment] {
        &[[0u8; 32]]
    }

    fn program_commitment(&self) -> &Self::ProgramCommitment {
        &[0u8; 32]
    }

    fn local_data_root(&self) -> &Self::LocalDataRoot {
        &[0u8; 32]
    }

    fn value_balance(&self) -> i64 {
        0
    }

    fn memorandum(&self) -> &Self::Memorandum {
        &[0u8; 32]
    }

    fn encrypted_records(&self) -> &[Self::EncryptedRecord] {
        &[[0u8; 32]]
    }

    fn size(&self) -> usize {
        0
    }
}

impl ToBytes for TestTx {
    #[inline]
    fn write<W: Write>(&self, mut _writer: W) -> IoResult<()> {
        Ok(())
    }
}

impl FromBytes for TestTx {
    #[inline]
    fn read<R: Read>(mut _reader: R) -> IoResult<Self> {
        Ok(Self)
    }
}

pub async fn create_test_consensus() -> snarkos_consensus::Consensus<LedgerStorage> {
    create_test_consensus_from_ledger(Arc::new(FIXTURE_VK.ledger().await))
}

pub fn create_test_consensus_from_ledger(
    ledger: Arc<MerkleTreeLedger<LedgerStorage>>,
) -> snarkos_consensus::Consensus<LedgerStorage> {
    snarkos_consensus::Consensus {
        ledger,
        memory_pool: Default::default(),
        parameters: TEST_CONSENSUS_PARAMS.clone(),
        public_parameters: FIXTURE.parameters.clone(),
    }
}
