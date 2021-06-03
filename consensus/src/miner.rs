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

use crate::{error::ConsensusError, Consensus};
use snarkos_storage::Storage;
use snarkvm_algorithms::CRH;
use snarkvm_dpc::{
    base_dpc::{instantiated::*, record::DPCRecord},
    AccountAddress,
    Record,
};
use snarkvm_objects::{dpc::DPCTransactions, Block, BlockHeader, Transaction};
use snarkvm_posw::{txids_to_roots, PoswMarlin};
use snarkvm_utilities::{bytes::ToBytes, to_bytes};

use chrono::Utc;
use rand::{prelude::StdRng, thread_rng, Rng, SeedableRng};
use std::sync::Arc;

/// Compiles transactions into blocks to be submitted to the network.
/// Uses a proof of work based algorithm to find valid blocks.
pub struct Miner<S: Storage> {
    /// The coinbase address that mining rewards are assigned to.
    address: AccountAddress<Components>,
    /// The sync parameters for the network of this miner.
    pub consensus: Arc<Consensus<S>>,
    /// The mining instance that is initialized with a proving key.
    miner: PoswMarlin,
}

impl<S: Storage> Miner<S> {
    /// Creates a new instance of `Miner`.
    pub fn new(address: AccountAddress<Components>, consensus: Arc<Consensus<S>>) -> Self {
        Self {
            address,
            consensus,
            // Load the miner with the proving key, this should never fail
            miner: PoswMarlin::load().expect("could not instantiate the miner"),
        }
    }

    /// Fetches new transactions from the memory pool.
    pub async fn fetch_memory_pool_transactions(&self) -> Result<DPCTransactions<Tx>, ConsensusError> {
        let max_block_size = self.consensus.parameters.max_block_size;

        self.consensus
            .memory_pool
            .get_candidates(&self.consensus.ledger, max_block_size)
            .await
    }

    /// Add a coinbase transaction to a list of candidate block transactions
    pub async fn add_coinbase_transaction<R: Rng + Send + 'static>(
        &self,
        transactions: &mut DPCTransactions<Tx>,
        rng: R,
    ) -> Result<Vec<DPCRecord<Components>>, ConsensusError> {
        let program_vk_hash = to_bytes![ProgramVerificationKeyCRH::hash(
            &self
                .consensus
                .public_parameters
                .system_parameters
                .program_verification_key_crh,
            &to_bytes![
                self.consensus
                    .public_parameters
                    .noop_program_snark_parameters
                    .verification_key
            ]?
        )?]?;

        let new_birth_programs = vec![program_vk_hash.clone(); NUM_OUTPUT_RECORDS];
        let new_death_programs = vec![program_vk_hash.clone(); NUM_OUTPUT_RECORDS];

        for transaction in transactions.iter() {
            if self.consensus.parameters.network_id != transaction.network {
                return Err(ConsensusError::ConflictingNetworkId(
                    self.consensus.parameters.network_id.id(),
                    transaction.network.id(),
                ));
            }
        }

        let (records, tx) = self
            .consensus
            .create_coinbase_transaction(
                self.consensus.ledger.get_current_block_height() + 1,
                transactions,
                program_vk_hash,
                new_birth_programs,
                new_death_programs,
                self.address.clone(),
                rng,
            )
            .await?;

        transactions.push(tx);
        Ok(records)
    }

    /// Acquires the storage lock and returns the previous block header and verified transactions.
    #[allow(clippy::type_complexity)]
    pub async fn establish_block(
        &self,
        transactions: &DPCTransactions<Tx>,
    ) -> Result<(BlockHeader, DPCTransactions<Tx>, Vec<DPCRecord<Components>>), ConsensusError> {
        let mut transactions = transactions.clone();
        let coinbase_records = self
            .add_coinbase_transaction(&mut transactions, StdRng::from_rng(thread_rng()).unwrap())
            .await?;

        // Verify transactions
        // assert!(InstantiatedDPC::verify_transactions(
        //     &self.consensus.public_parameters,
        //     &transactions.0,
        //     &*self.consensus.ledger,
        // )?);

        let previous_block_header = self.consensus.ledger.get_latest_block().await?.header;

        Ok((previous_block_header, transactions, coinbase_records))
    }

    /// Run proof of work to find block.
    /// Returns BlockHeader with nonce solution.
    pub fn find_block<T: Transaction>(
        &self,
        transactions: &DPCTransactions<T>,
        parent_header: &BlockHeader,
    ) -> Result<BlockHeader, ConsensusError> {
        let txids = transactions.to_transaction_ids()?;
        let (merkle_root_hash, pedersen_merkle_root_hash, subroots) = txids_to_roots(&txids);

        let time = Utc::now().timestamp();
        let difficulty_target = self.consensus.parameters.get_block_difficulty(parent_header, time);

        // TODO: Switch this to use a user-provided RNG
        let (nonce, proof) = self.miner.mine(
            &subroots,
            difficulty_target,
            &mut thread_rng(),
            self.consensus.parameters.max_nonce,
        )?;

        Ok(BlockHeader {
            previous_block_hash: parent_header.get_hash(),
            merkle_root_hash,
            pedersen_merkle_root_hash,
            time,
            difficulty_target,
            nonce,
            proof: proof.into(),
        })
    }

    /// Returns a mined block.
    /// Calls methods to fetch transactions, run proof of work, and add the block into the chain for storage.
    pub async fn mine_block(&self) -> Result<(Block<Tx>, Vec<DPCRecord<Components>>), ConsensusError> {
        let candidate_transactions = self.fetch_memory_pool_transactions().await?;

        debug!("The miner is creating a block");

        let (previous_block_header, transactions, coinbase_records) =
            self.establish_block(&candidate_transactions).await?;

        debug!("The miner generated a coinbase transaction");

        for (index, record) in coinbase_records.iter().enumerate() {
            let record_commitment = hex::encode(&to_bytes![record.commitment()]?);
            debug!("Coinbase record {:?} commitment: {:?}", index, record_commitment);
        }

        let header = self.find_block(&transactions, &previous_block_header)?;

        debug!("The Miner found a block");

        let block = Block { header, transactions };

        self.consensus.receive_block(&block).await?;

        // Store the non-dummy coinbase records.
        let mut records_to_store = vec![];
        for record in &coinbase_records {
            if !record.is_dummy() {
                records_to_store.push(record.clone());
            }
        }
        self.consensus.ledger.store_records(&records_to_store).await?;

        Ok((block, coinbase_records))
    }
}
