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

use crate::{error::ConsensusError, CompatMerkleTreeLedger, ConsensusParameters, MemoryPool, MerkleTreeLedger, Tx};
use snarkos_storage::{BlockPath, LedgerSchemeCompat, Storage};
use snarkvm_algorithms::CRH;
use snarkvm_dpc::{
    instantiated::{Components, InstantiatedDPC, SerialNumberNonce, NUM_OUTPUT_RECORDS},
    record_payload::RecordPayload,
    Account,
    AccountAddress,
    AccountPrivateKey,
    AccountScheme,
    DPCComponents,
    DPCRecord,
    DPCScheme,
    PublicParameters,
    TransactionKernel,
};
use snarkvm_objects::{AleoAmount, Block, DPCTransactions};
use snarkvm_posw::txids_to_roots;
use snarkvm_utilities::{to_bytes, ToBytes};

use rand::Rng;

use std::sync::Arc;

pub struct Consensus<S: Storage> {
    pub parameters: ConsensusParameters,
    pub public_parameters: PublicParameters<Components>,
    pub ledger: Arc<MerkleTreeLedger<S>>,
    pub memory_pool: MemoryPool<Tx>,
}

impl<S: Storage> Consensus<S> {
    /// Check if the transaction is valid.
    pub async fn verify_transaction(&self, transaction: &Tx) -> Result<bool, ConsensusError> {
        if !self
            .parameters
            .authorized_inner_snark_ids
            .contains(&to_bytes![transaction.inner_circuit_id]?)
        {
            return Ok(false);
        }

        let parameters = self.public_parameters.clone();
        let ledger = self.ledger.clone();
        let transaction = transaction.clone();
        let result = tokio::task::spawn_blocking(move || {
            InstantiatedDPC::verify(&parameters, &transaction, &LedgerSchemeCompat(ledger))
        })
        .await
        .expect("verify task panicked")?;
        Ok(result)
    }

    /// Check if the transactions are valid.
    pub async fn verify_transactions(&self, transactions: &[Tx]) -> Result<bool, ConsensusError> {
        for tx in transactions {
            debug!("verifying: {:?}", tx);
            if !self.verify_transaction(tx).await? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Check if the block is valid.
    /// Verify transactions and transaction fees.
    pub async fn verify_block(&self, block: &Block<Tx>) -> Result<bool, ConsensusError> {
        let transaction_ids: Vec<_> = block.transactions.to_transaction_ids()?;
        let (merkle_root, pedersen_merkle_root, _) = txids_to_roots(&transaction_ids);

        // Verify the block header
        if !crate::is_genesis(&block.header) {
            let parent_block = self.ledger.get_latest_block().await?;
            if let Err(err) =
                self.parameters
                    .verify_header(&block.header, &parent_block.header, &merkle_root, &pedersen_merkle_root)
            {
                error!("block header failed to verify: {:?}", err);
                return Ok(false);
            }
        }
        // Verify block amounts and check that there is a single coinbase transaction

        let mut coinbase_transaction_count = 0;
        let mut total_value_balance = AleoAmount::ZERO;

        for transaction in block.transactions.iter() {
            let value_balance = transaction.value_balance;

            if value_balance.is_negative() {
                coinbase_transaction_count += 1;
            }

            total_value_balance = total_value_balance.add(value_balance);
        }

        // Check that there is only 1 coinbase transaction
        if coinbase_transaction_count > 1 {
            error!("multiple coinbase transactions");
            return Ok(false);
        }

        // Check that the block value balances are correct
        let expected_block_reward = crate::get_block_reward(self.ledger.get_current_block_height()).0;
        if total_value_balance.0 + expected_block_reward != 0 {
            trace!("total_value_balance: {:?}", total_value_balance);
            trace!("expected_block_reward: {:?}", expected_block_reward);

            return Ok(false);
        }

        // Check that all the transaction proofs verify
        self.verify_transactions(&block.transactions.0).await
    }

    /// Receive a block from an external source and process it based on ledger state.
    pub async fn receive_block(&self, block: &Block<Tx>) -> Result<(), ConsensusError> {
        // Block is an unknown orphan
        if !self.ledger.previous_block_hash_exists(block).await
            && !self.ledger.is_previous_block_canon(&block.header).await
        {
            debug!("Processing a block that is an unknown orphan");

            // There are two possible cases for an unknown orphan.
            // 1) The block is a genesis block, or
            // 2) The block is unknown and does not correspond with the canon chain.
            if crate::is_genesis(&block.header) && self.ledger.is_empty().await {
                self.process_block(&block).await?;
            } else {
                self.ledger.insert_only(block).await?;
            }
        } else {
            // If the block is not an unknown orphan, find the origin of the block
            match self.ledger.get_block_path(&block.header).await? {
                BlockPath::ExistingBlock => {
                    debug!("Received a pre-existing block");
                    return Err(ConsensusError::PreExistingBlock);
                }
                BlockPath::CanonChain(block_height) => {
                    debug!("Processing a block that is on canon chain. Height {}", block_height);

                    self.process_block(block).await?;

                    // Attempt to fast forward the block state if the node already stores
                    // the children of the new canon block.
                    let (_, child_path) =
                        MerkleTreeLedger::<S>::longest_child_path(self.ledger.storage.clone(), block.header.get_hash())
                            .await?;
                    for child_block_hash in child_path {
                        let new_block = self.ledger.get_block(&child_block_hash).await?;
                        self.process_block(&new_block).await?;
                    }
                }
                BlockPath::SideChain(side_chain_path) => {
                    debug!(
                        "Processing a block that is on side chain. Height {}",
                        side_chain_path.new_block_number
                    );

                    // If the side chain is now longer than the canon chain,
                    // perform a fork to the side chain.
                    if side_chain_path.new_block_number > self.ledger.get_current_block_height() {
                        debug!(
                            "Determined side chain is longer than canon chain by {} blocks",
                            side_chain_path.new_block_number - self.ledger.get_current_block_height()
                        );
                        warn!("A valid fork has been detected. Performing a fork to the side chain.");

                        // Fork to superior side chain
                        self.ledger.revert_for_fork(&side_chain_path).await?;

                        if !side_chain_path.path.is_empty() {
                            for block_hash in side_chain_path.path {
                                if block_hash == block.header.get_hash() {
                                    self.process_block(&block).await?
                                } else {
                                    let new_block = self.ledger.get_block(&block_hash).await?;
                                    self.process_block(&new_block).await?;
                                }
                            }
                        }
                    } else {
                        // If the sidechain is not longer than the main canon chain, simply store the block
                        self.ledger.insert_only(block).await?;
                    }
                }
            };
        }

        Ok(())
    }

    /// Return whether or not the given block is valid and insert it.
    /// 1. Verify that the block header is valid.
    /// 2. Verify that the transactions are valid.
    /// 3. Insert/canonize block.
    pub async fn process_block(&self, block: &Block<Tx>) -> Result<(), ConsensusError> {
        if self.ledger.is_canon(&block.header.get_hash()).await {
            return Ok(());
        }

        // 1. Verify that the block valid
        if !self.verify_block(block).await? {
            return Err(ConsensusError::InvalidBlock(block.header.get_hash().0.to_vec()));
        }

        // 2. Insert/canonize block
        self.ledger.insert_and_commit(block).await?;

        // 3. Remove transactions from the mempool
        for transaction_id in block.transactions.to_transaction_ids()? {
            self.memory_pool.remove_by_hash(&transaction_id).await?;
        }

        Ok(())
    }

    /// Generate a transaction by spending old records and specifying new record attributes
    #[allow(clippy::too_many_arguments)]
    pub async fn create_transaction<R: Rng + Send + 'static>(
        &self,
        old_records: Vec<DPCRecord<Components>>,
        old_account_private_keys: Vec<AccountPrivateKey<Components>>,
        new_record_owners: Vec<AccountAddress<Components>>,
        new_birth_program_ids: Vec<Vec<u8>>,
        new_death_program_ids: Vec<Vec<u8>>,
        new_is_dummy_flags: Vec<bool>,
        new_values: Vec<u64>,
        new_payloads: Vec<RecordPayload>,
        memo: [u8; 32],
        mut rng: R,
    ) -> Result<(Vec<DPCRecord<Components>>, Tx), ConsensusError> {
        // Offline execution to generate a DPC transaction
        let transaction_kernel = <InstantiatedDPC as DPCScheme<CompatMerkleTreeLedger<S>>>::execute_offline(
            self.public_parameters.system_parameters.clone(),
            old_records,
            old_account_private_keys,
            new_record_owners,
            &new_is_dummy_flags,
            &new_values,
            new_payloads,
            new_birth_program_ids,
            new_death_program_ids,
            memo,
            self.parameters.network_id.id(),
            &mut rng,
        )?;

        self.execute_transaction(transaction_kernel, rng).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn execute_transaction<R: Rng + Send + 'static>(
        &self,
        transaction_kernel: TransactionKernel<Components>,
        mut rng: R,
    ) -> Result<(Vec<DPCRecord<Components>>, Tx), ConsensusError> {
        // Construct the program proofs
        let (old_death_program_proofs, new_birth_program_proofs) = ConsensusParameters::generate_program_proofs::<R, S>(
            &self.public_parameters,
            &transaction_kernel,
            &mut rng,
        )?;

        // Online execution to generate a DPC transaction
        let ledger = self.ledger.clone();
        let parameters = self.public_parameters.clone();

        let (new_records, transaction) = tokio::task::spawn_blocking(move || {
            InstantiatedDPC::execute_online(
                &parameters,
                transaction_kernel,
                old_death_program_proofs,
                new_birth_program_proofs,
                &LedgerSchemeCompat(ledger),
                &mut rng,
            )
        })
        .await
        .expect("execute_online task panicked")?;

        Ok((new_records, transaction))
    }

    /// Generate a coinbase transaction given candidate block transactions
    #[allow(clippy::too_many_arguments)]
    pub async fn create_coinbase_transaction<R: Rng + Send + 'static>(
        &self,
        block_num: u32,
        transactions: &DPCTransactions<Tx>,
        program_vk_hash: Vec<u8>,
        new_birth_program_ids: Vec<Vec<u8>>,
        new_death_program_ids: Vec<Vec<u8>>,
        recipient: AccountAddress<Components>,
        mut rng: R,
    ) -> Result<(Vec<DPCRecord<Components>>, Tx), ConsensusError> {
        let mut total_value_balance = crate::get_block_reward(block_num);

        for transaction in transactions.iter() {
            let tx_value_balance = transaction.value_balance;

            if tx_value_balance.is_negative() {
                return Err(ConsensusError::CoinbaseTransactionAlreadyExists());
            }

            total_value_balance = total_value_balance.add(transaction.value_balance);
        }

        // Generate a new account that owns the dummy input records
        let new_account = Account::new(
            &self.public_parameters.system_parameters.account_signature,
            &self.public_parameters.system_parameters.account_commitment,
            &self.public_parameters.system_parameters.account_encryption,
            &mut rng,
        )
        .unwrap();

        // Generate dummy input records having as address the genesis address.
        let old_account_private_keys = vec![new_account.private_key.clone(); Components::NUM_INPUT_RECORDS];
        let mut old_records = Vec::with_capacity(Components::NUM_INPUT_RECORDS);
        for _ in 0..Components::NUM_INPUT_RECORDS {
            let sn_nonce_input: [u8; 4] = rng.gen();

            let old_sn_nonce = SerialNumberNonce::hash(
                &self.public_parameters.system_parameters.serial_number_nonce,
                &sn_nonce_input,
            )?;

            let old_record = InstantiatedDPC::generate_record(
                &self.public_parameters.system_parameters,
                old_sn_nonce,
                new_account.address.clone(),
                true, // The input record is dummy
                0,
                RecordPayload::default(),
                // Filler program input
                program_vk_hash.clone(),
                program_vk_hash.clone(),
                &mut rng,
            )?;

            old_records.push(old_record);
        }

        let new_record_owners = vec![recipient; Components::NUM_OUTPUT_RECORDS];
        let new_is_dummy_flags = [vec![false], vec![true; Components::NUM_OUTPUT_RECORDS - 1]].concat();
        let new_values = [vec![total_value_balance.0 as u64], vec![
            0;
            Components::NUM_OUTPUT_RECORDS
                - 1
        ]]
        .concat();
        let new_payloads = vec![RecordPayload::default(); NUM_OUTPUT_RECORDS];

        let memo: [u8; 32] = rng.gen();

        self.create_transaction(
            old_records,
            old_account_private_keys,
            new_record_owners,
            new_birth_program_ids,
            new_death_program_ids,
            new_is_dummy_flags,
            new_values,
            new_payloads,
            memo,
            rng,
        )
        .await
    }
}
