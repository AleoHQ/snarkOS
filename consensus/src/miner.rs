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
use snarkvm_dpc::{
    testnet1::{instantiated::*, Record},
    Address,
    Block,
    BlockHeader,
    DPCComponents,
    DPCScheme,
    ProgramScheme,
    RecordScheme,
    Storage,
    TransactionScheme,
    Transactions,
};
use snarkvm_posw::{txids_to_roots, PoswMarlin};
use snarkvm_utilities::{to_bytes_le, ToBytes};

use chrono::Utc;
use rand::{thread_rng, CryptoRng, Rng};
use std::sync::Arc;

/// Compiles transactions into blocks to be submitted to the network.
/// Uses a proof of work based algorithm to find valid blocks.
pub struct Miner<S: Storage> {
    /// The coinbase address that mining rewards are assigned to.
    address: Address<Components>,
    /// The sync parameters for the network of this miner.
    pub consensus: Arc<Consensus<S>>,
    /// The mining instance that is initialized with a proving key.
    miner: PoswMarlin,
}

impl<S: Storage> Miner<S> {
    /// Creates a new instance of `Miner`.
    pub fn new(address: Address<Components>, consensus: Arc<Consensus<S>>) -> Self {
        Self {
            address,
            consensus,
            // Load the miner with the proving key, this should never fail
            miner: PoswMarlin::load().expect("could not instantiate the miner"),
        }
    }

    /// Fetches new transactions from the memory pool.
    pub fn fetch_memory_pool_transactions(&self) -> Result<Transactions<Testnet1Transaction>, ConsensusError> {
        let max_block_size = self.consensus.parameters.max_block_size;

        self.consensus
            .memory_pool
            .get_candidates(&self.consensus.ledger, max_block_size)
    }

    /// Add a coinbase transaction to a list of candidate block transactions
    pub fn add_coinbase_transaction<R: Rng + CryptoRng>(
        &self,
        transactions: &mut Transactions<Testnet1Transaction>,
        rng: &mut R,
    ) -> Result<Vec<Record<Components>>, ConsensusError> {
        for transaction in transactions.iter() {
            if self.consensus.parameters.network_id != transaction.network {
                return Err(ConsensusError::ConflictingNetworkId(
                    self.consensus.parameters.network_id.id(),
                    transaction.network.id(),
                ));
            }
        }

        let (records, tx) = self.consensus.create_coinbase_transaction(
            self.consensus.ledger.get_current_block_height() + 1,
            transactions,
            self.consensus.dpc.noop_program.id(),
            vec![self.consensus.dpc.noop_program.id(); Components::NUM_OUTPUT_RECORDS],
            vec![self.consensus.dpc.noop_program.id(); Components::NUM_OUTPUT_RECORDS],
            self.address.clone(),
            rng,
        )?;

        transactions.push(tx);
        Ok(records)
    }

    /// Acquires the storage lock and returns the previous block header and verified transactions.
    #[allow(clippy::type_complexity)]
    pub fn establish_block(
        &self,
        transactions: &Transactions<Testnet1Transaction>,
    ) -> Result<(BlockHeader, Transactions<Testnet1Transaction>, Vec<Record<Components>>), ConsensusError> {
        let rng = &mut thread_rng();
        let mut transactions = transactions.clone();
        let coinbase_records = self.add_coinbase_transaction(&mut transactions, rng)?;

        // Verify transactions
        assert!(Testnet1DPC::verify_transactions(
            &self.consensus.dpc,
            &transactions.0,
            &*self.consensus.ledger,
        ));

        let previous_block_header = self.consensus.ledger.get_latest_block()?.header;

        Ok((previous_block_header, transactions, coinbase_records))
    }

    /// Run proof of work to find block.
    /// Returns BlockHeader with nonce solution.
    pub fn find_block<T: TransactionScheme>(
        &self,
        transactions: &Transactions<T>,
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
    pub async fn mine_block(&self) -> Result<(Block<Testnet1Transaction>, Vec<Record<Components>>), ConsensusError> {
        let candidate_transactions = self.fetch_memory_pool_transactions()?;

        debug!("The miner is creating a block");

        let (previous_block_header, transactions, coinbase_records) = self.establish_block(&candidate_transactions)?;

        debug!("The miner generated a coinbase transaction");

        for (index, record) in coinbase_records.iter().enumerate() {
            let record_commitment = hex::encode(&to_bytes_le![record.commitment()]?);
            debug!("Coinbase record {:?} commitment: {:?}", index, record_commitment);
        }

        let header = self.find_block(&transactions, &previous_block_header)?;

        debug!("The Miner found a block");

        let block = Block { header, transactions };

        self.consensus.receive_block(&block, false).await?;

        // Store the non-dummy coinbase records.
        let mut records_to_store = vec![];
        for record in &coinbase_records {
            if !record.is_dummy() {
                records_to_store.push(record.clone());
            }
        }
        self.consensus.ledger.store_records(&records_to_store)?;

        Ok((block, coinbase_records))
    }
}
