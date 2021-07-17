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

use crate::empty_ledger::EmptyLedger;
use snarkvm_algorithms::CRH;
use snarkvm_dpc::{
    testnet1::{
        instantiated::{CommitmentMerkleParameters, Components, Testnet1DPC, Testnet1Transaction},
        Payload,
        Record,
        TransactionKernel as TransactionKernelNative,
    },
    Address,
    DPCComponents,
    DPCScheme,
    PrivateKey,
    RecordScheme,
    *,
};
use snarkvm_utilities::{to_bytes_le, ToBytes};

use rand::{CryptoRng, Rng};
use std::{fmt, str::FromStr};

pub type MerkleTreeLedger = EmptyLedger<Testnet1Transaction, CommitmentMerkleParameters>;

#[derive(Clone, Debug)]
pub struct TransactionInput {
    pub(crate) private_key: PrivateKey<Components>,
    pub(crate) record: Record<Components>,
}

#[derive(Clone, Debug)]
pub struct TransactionOutput {
    pub(crate) recipient: Address<Components>,
    pub(crate) amount: u64,
    // TODO (raychu86): Add support for payloads and birth/death program ids.
    // pub(crate) payload: Option<Vec<u8>>,
}

pub struct TransactionKernel {
    pub(crate) transaction_kernel: TransactionKernelNative<Components>,
}

// TODO (raychu86) Look into genericizing this model into `dpc`.
#[derive(Clone, Debug, Default)]
pub struct TransactionKernelBuilder {
    /// Transaction inputs
    pub(crate) inputs: Vec<TransactionInput>,

    /// Transaction outputs
    pub(crate) outputs: Vec<TransactionOutput>,

    /// Network ID
    pub(crate) network_id: u8,

    /// Transaction memo
    pub(crate) memo: Option<[u8; 32]>,
}

impl TransactionKernelBuilder {
    pub fn new() -> Self {
        // TODO (raychu86) update the default to `0` for mainnet.
        Self {
            inputs: vec![],
            outputs: vec![],
            network_id: 1,
            memo: None,
        }
    }

    ///
    /// Returns a new transaction builder with the added transaction input.
    /// Otherwise, returns a `DPCError`.
    ///
    pub fn add_input(self, private_key: PrivateKey<Components>, record: Record<Components>) -> Result<Self, DPCError> {
        // Check that the transaction is limited to `Components::NUM_INPUT_RECORDS` inputs.
        if self.inputs.len() > Components::NUM_INPUT_RECORDS {
            return Err(DPCError::InvalidNumberOfInputs(
                self.inputs.len() + 1,
                Components::NUM_INPUT_RECORDS,
            ));
        }

        // Construct the transaction input.
        let input = TransactionInput { private_key, record };

        // Update the current builder instance.
        let mut builder = self;
        builder.inputs.push(input);

        Ok(builder)
    }

    ///
    /// Returns a new transaction builder with the added transaction output.
    /// Otherwise, returns a `DPCError`.
    ///
    pub fn add_output(self, recipient: Address<Components>, amount: u64) -> Result<Self, DPCError> {
        // Check that the transaction is limited to `Components::NUM_OUTPUT_RECORDS` outputs.
        if self.outputs.len() > Components::NUM_OUTPUT_RECORDS {
            return Err(DPCError::InvalidNumberOfOutputs(
                self.outputs.len() + 1,
                Components::NUM_OUTPUT_RECORDS,
            ));
        }

        // Construct the transaction output.
        let output = TransactionOutput { recipient, amount };

        // Update the current builder instance.
        let mut builder = self;
        builder.outputs.push(output);

        Ok(builder)
    }

    ///
    /// Returns a new transaction builder with the updated network id.
    ///
    pub fn network_id(self, network_id: u8) -> Self {
        let mut builder = self;
        builder.network_id = network_id;

        builder
    }

    ///
    /// Returns a new transaction builder with the updated network id.
    ///
    pub fn memo(self, memo: [u8; 32]) -> Self {
        let mut builder = self;
        builder.memo = Some(memo);

        builder
    }

    ///
    /// Returns the transaction kernel (offline transaction) derived from the provided builder
    /// attributes.
    ///
    /// Otherwise, returns `DPCError`.
    ///
    pub fn build<R: Rng + CryptoRng>(&self, rng: &mut R) -> Result<TransactionKernel, DPCError> {
        // Check that the transaction is limited to `Components::NUM_INPUT_RECORDS` inputs.
        match self.inputs.len() {
            1 | 2 => {}
            num_inputs => {
                return Err(DPCError::InvalidNumberOfInputs(
                    num_inputs,
                    Components::NUM_INPUT_RECORDS,
                ));
            }
        }

        // Check that the transaction has at least one output and is limited to `Components::NUM_OUTPUT_RECORDS` outputs.
        match self.outputs.len() {
            0 => return Err(DPCError::Message("Transaction kernel is missing outputs".to_string())),
            1 | 2 => {}
            num_inputs => {
                return Err(DPCError::InvalidNumberOfInputs(
                    num_inputs,
                    Components::NUM_INPUT_RECORDS,
                ));
            }
        }

        // Construct the parameters from the given transaction inputs.
        let mut spenders = vec![];
        let mut records_to_spend = vec![];

        for input in &self.inputs {
            spenders.push(input.private_key.clone());
            records_to_spend.push(input.record.clone());
        }

        // Construct the parameters from the given transaction outputs.
        let mut recipients = vec![];
        let mut recipient_amounts = vec![];

        for output in &self.outputs {
            recipients.push(output.recipient.clone());
            recipient_amounts.push(output.amount);
        }

        // Construct the transaction kernel
        TransactionKernel::new(
            spenders,
            records_to_spend,
            recipients,
            recipient_amounts,
            self.network_id,
            rng,
        )
    }
}

impl TransactionKernel {
    /// Returns an offline transaction kernel
    pub(crate) fn new<R: Rng + CryptoRng>(
        spenders: Vec<PrivateKey<Components>>,
        records_to_spend: Vec<Record<Components>>,
        recipients: Vec<Address<Components>>,
        recipient_amounts: Vec<u64>,
        _network_id: u8, // TODO (howardwu): Keep this around to use for network modularization.
        rng: &mut R,
    ) -> Result<Self, DPCError> {
        let dpc = <Testnet1DPC as DPCScheme<MerkleTreeLedger>>::load(false).unwrap();

        assert!(!spenders.is_empty());
        assert_eq!(spenders.len(), records_to_spend.len());

        assert!(!recipients.is_empty());
        assert_eq!(recipients.len(), recipient_amounts.len());

        // Construct the new records
        let mut old_records = vec![];
        for record in records_to_spend {
            old_records.push(record);
        }

        let mut old_private_keys = vec![];
        for private_key in spenders {
            old_private_keys.push(private_key);
        }

        while old_records.len() < Components::NUM_INPUT_RECORDS {
            let sn_randomness: [u8; 32] = rng.gen();
            let old_sn_nonce = dpc.system_parameters.serial_number_nonce.hash(&sn_randomness)?;

            let private_key = old_private_keys[0].clone();
            let address = Address::<Components>::from_private_key(
                &dpc.system_parameters.account_signature,
                &dpc.system_parameters.account_commitment,
                &dpc.system_parameters.account_encryption,
                &private_key,
            )?;

            let dummy_record = Record::<Components>::new(
                &dpc.system_parameters.record_commitment,
                address,
                true, // The input record is dummy
                0,
                Default::default(),
                dpc.noop_program.id(),
                dpc.noop_program.id(),
                old_sn_nonce,
                rng,
            )?;

            old_records.push(dummy_record);
            old_private_keys.push(private_key);
        }

        assert_eq!(old_records.len(), Components::NUM_INPUT_RECORDS);

        // Enforce that the old record addresses correspond with the private keys
        for (private_key, record) in old_private_keys.iter().zip(&old_records) {
            let address = Address::<Components>::from_private_key(
                &dpc.system_parameters.account_signature,
                &dpc.system_parameters.account_commitment,
                &dpc.system_parameters.account_encryption,
                private_key,
            )?;

            assert_eq!(&address, record.owner());
        }

        assert_eq!(old_records.len(), Components::NUM_INPUT_RECORDS);
        assert_eq!(old_private_keys.len(), Components::NUM_INPUT_RECORDS);

        // Decode new recipient data
        let mut new_record_owners = vec![];
        let mut new_is_dummy_flags = vec![];
        let mut new_values = vec![];
        for (recipient, amount) in recipients.iter().zip(recipient_amounts) {
            new_record_owners.push(recipient.clone());
            new_is_dummy_flags.push(false);
            new_values.push(amount);
        }

        // Fill any unused new_record indices with dummy output values
        while new_record_owners.len() < Components::NUM_OUTPUT_RECORDS {
            new_record_owners.push(new_record_owners[0].clone());
            new_is_dummy_flags.push(true);
            new_values.push(0);
        }

        assert_eq!(new_record_owners.len(), Components::NUM_OUTPUT_RECORDS);
        assert_eq!(new_is_dummy_flags.len(), Components::NUM_OUTPUT_RECORDS);
        assert_eq!(new_values.len(), Components::NUM_OUTPUT_RECORDS);

        let new_birth_program_ids = vec![dpc.noop_program.id(); Components::NUM_OUTPUT_RECORDS];
        let new_death_program_ids = vec![dpc.noop_program.id(); Components::NUM_OUTPUT_RECORDS];
        let new_payloads: Vec<Payload> = vec![Default::default(); Components::NUM_OUTPUT_RECORDS];

        // Generate a random memo
        let memo = rng.gen();

        // Generate transaction

        let mut joint_serial_numbers = vec![];
        for i in 0..Components::NUM_INPUT_RECORDS {
            let (sn, _) =
                old_records[i].to_serial_number(&dpc.system_parameters.account_signature, &old_private_keys[i])?;
            joint_serial_numbers.extend_from_slice(&to_bytes_le![sn]?);
        }

        let mut new_records = vec![];
        for j in 0..Components::NUM_OUTPUT_RECORDS {
            new_records.push(Record::new_full(
                &dpc.system_parameters.serial_number_nonce,
                &dpc.system_parameters.record_commitment,
                new_record_owners[j].clone(),
                new_is_dummy_flags[j],
                new_values[j],
                new_payloads[j].clone(),
                new_birth_program_ids[j].clone(),
                new_death_program_ids[j].clone(),
                j as u8,
                joint_serial_numbers.clone(),
                rng,
            )?);
        }

        // Offline execution to generate a DPC transaction
        let transaction_kernel = <Testnet1DPC as DPCScheme<MerkleTreeLedger>>::execute_offline_phase::<R>(
            &dpc,
            &old_private_keys,
            old_records,
            new_records,
            memo,
            rng,
        )?;

        Ok(Self { transaction_kernel })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut output = vec![];
        self.transaction_kernel
            .write_le(&mut output)
            .expect("serialization to bytes failed");
        output
    }
}

impl FromStr for TransactionKernel {
    type Err = DPCError;

    fn from_str(transaction_kernel: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            transaction_kernel: TransactionKernelNative::<Components>::from_str(transaction_kernel)?,
        })
    }
}

impl fmt::Display for TransactionKernel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.transaction_kernel.to_string())
    }
}
