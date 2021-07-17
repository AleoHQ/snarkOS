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

//! Implementation of public RPC endpoints.
//!
//! See [RpcFunctions](../trait.RpcFunctions.html) for documentation of public endpoints.

use crate::{error::RpcError, rpc_trait::RpcFunctions, rpc_types::*};
use snarkos_consensus::{get_block_reward, memory_pool::Entry, ConsensusParameters, MemoryPool, MerkleTreeLedger};
use snarkos_metrics::{snapshots::NodeStats, stats::NODE_STATS};
use snarkos_network::{KnownNetwork, NetworkMetrics, Node, Sync};
use snarkvm_dpc::{
    testnet1::instantiated::{Testnet1DPC, Testnet1Transaction},
    BlockHeaderHash,
    Storage,
    TransactionScheme,
};
use snarkvm_utilities::{
    bytes::{FromBytes, ToBytes},
    to_bytes_le,
    CanonicalSerialize,
};

use chrono::Utc;

use std::{ops::Deref, sync::Arc};

/// Implements JSON-RPC HTTP endpoint functions for a node.
/// The constructor is given Arc::clone() copies of all needed node components.
#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct RpcImpl<S: Storage + Send + core::marker::Sync + 'static>(Arc<RpcInner<S>>);

impl<S: Storage + Send + core::marker::Sync + 'static> Deref for RpcImpl<S> {
    type Target = RpcInner<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct RpcInner<S: Storage + Send + core::marker::Sync + 'static> {
    /// Blockchain database storage.
    pub(crate) storage: Arc<MerkleTreeLedger<S>>,

    /// RPC credentials for accessing guarded endpoints
    pub(crate) credentials: Option<RpcCredentials>,

    /// A clone of the network Node
    pub(crate) node: Node<S>,
}

impl<S: Storage + Send + core::marker::Sync + 'static> RpcImpl<S> {
    /// Creates a new struct for calling public and private RPC endpoints.
    pub fn new(storage: Arc<MerkleTreeLedger<S>>, credentials: Option<RpcCredentials>, node: Node<S>) -> Self {
        Self(Arc::new(RpcInner {
            storage,
            credentials,
            node,
        }))
    }

    pub fn sync_handler(&self) -> Result<&Arc<Sync<S>>, RpcError> {
        self.node.sync().ok_or(RpcError::NoConsensus)
    }

    pub fn consensus_parameters(&self) -> Result<&ConsensusParameters, RpcError> {
        Ok(self.sync_handler()?.consensus_parameters())
    }

    pub fn dpc(&self) -> Result<&Testnet1DPC, RpcError> {
        Ok(self.sync_handler()?.dpc_parameters())
    }

    pub fn memory_pool(&self) -> Result<&MemoryPool<Testnet1Transaction>, RpcError> {
        Ok(self.sync_handler()?.memory_pool())
    }

    pub fn known_network(&self) -> Result<&KnownNetwork, RpcError> {
        self.node.known_network().ok_or(RpcError::NoKnownNetwork)
    }
}

impl<S: Storage + Send + core::marker::Sync + 'static> RpcFunctions for RpcImpl<S> {
    /// Returns information about a block from a block hash.
    fn get_block(&self, block_hash_string: String) -> Result<BlockInfo, RpcError> {
        let block_hash = hex::decode(&block_hash_string)?;
        if block_hash.len() != 32 {
            return Err(RpcError::InvalidBlockHash(block_hash_string));
        }

        let storage = &self.storage;

        let primary_height = self.sync_handler()?.current_block_height();
        storage.catch_up_secondary(false, primary_height)?;

        let block_header_hash = BlockHeaderHash::new(block_hash);
        let height = match storage.get_block_number(&block_header_hash) {
            Ok(block_num) => match storage.is_canon(&block_header_hash) {
                true => Some(block_num),
                false => None,
            },
            Err(_) => None,
        };

        let confirmations = match height {
            Some(block_height) => storage.get_current_block_height() - block_height,
            None => 0,
        };

        if let Ok(block) = storage.get_block(&block_header_hash) {
            let mut transactions = Vec::with_capacity(block.transactions.len());

            for transaction in block.transactions.iter() {
                transactions.push(hex::encode(&transaction.transaction_id()?));
            }

            Ok(BlockInfo {
                hash: block_hash_string,
                height,
                confirmations,
                size: block.serialize()?.len(),
                previous_block_hash: block.header.previous_block_hash.to_string(),
                merkle_root: block.header.merkle_root_hash.to_string(),
                pedersen_merkle_root_hash: block.header.pedersen_merkle_root_hash.to_string(),
                proof: block.header.proof.to_string(),
                time: block.header.time,
                difficulty_target: block.header.difficulty_target,
                nonce: block.header.nonce,
                transactions,
            })
        } else {
            Err(RpcError::InvalidBlockHash(block_hash_string))
        }
    }

    /// Returns the number of blocks in the canonical chain, including the genesis.
    fn get_block_count(&self) -> Result<u32, RpcError> {
        let primary_height = self.sync_handler()?.current_block_height();
        Ok(primary_height + 1)
    }

    /// Returns the block hash of the head of the canonical chain.
    fn get_best_block_hash(&self) -> Result<String, RpcError> {
        let storage = &self.storage;
        let primary_height = self.sync_handler()?.current_block_height();
        storage.catch_up_secondary(false, primary_height)?;
        let best_block_hash = storage.get_block_hash(storage.get_current_block_height())?;

        Ok(hex::encode(&best_block_hash.0))
    }

    /// Returns the block hash of the index specified if it exists in the canonical chain.
    fn get_block_hash(&self, block_height: u32) -> Result<String, RpcError> {
        let storage = &self.storage;
        let primary_height = self.sync_handler()?.current_block_height();
        storage.catch_up_secondary(false, primary_height)?;
        let block_hash = storage.get_block_hash(block_height)?;

        Ok(hex::encode(&block_hash.0))
    }

    /// Returns the hex encoded bytes of a transaction from its transaction id.
    fn get_raw_transaction(&self, transaction_id: String) -> Result<String, RpcError> {
        let storage = &self.storage;
        let primary_height = self.sync_handler()?.current_block_height();
        storage.catch_up_secondary(false, primary_height)?;
        Ok(hex::encode(
            &storage.get_transaction_bytes(&hex::decode(transaction_id)?)?,
        ))
    }

    /// Returns information about a transaction from a transaction id.
    fn get_transaction_info(&self, transaction_id: String) -> Result<TransactionInfo, RpcError> {
        let transaction_bytes = self.get_raw_transaction(transaction_id)?;
        self.decode_raw_transaction(transaction_bytes)
    }

    /// Returns information about a transaction from serialized transaction bytes.
    fn decode_raw_transaction(&self, transaction_bytes: String) -> Result<TransactionInfo, RpcError> {
        let primary_height = self.sync_handler()?.current_block_height();
        self.storage.catch_up_secondary(false, primary_height)?;
        let transaction_bytes = hex::decode(transaction_bytes)?;
        let transaction = Testnet1Transaction::read_le(&transaction_bytes[..])?;

        let mut old_serial_numbers = Vec::with_capacity(transaction.old_serial_numbers().len());

        for sn in transaction.old_serial_numbers() {
            let mut serial_number: Vec<u8> = vec![];
            CanonicalSerialize::serialize(sn, &mut serial_number).unwrap();
            old_serial_numbers.push(hex::encode(serial_number));
        }

        let mut new_commitments = Vec::with_capacity(transaction.new_commitments().len());

        for cm in transaction.new_commitments() {
            new_commitments.push(hex::encode(to_bytes_le![cm]?));
        }

        let memo = hex::encode(to_bytes_le![transaction.memorandum()]?);

        let mut signatures = Vec::with_capacity(transaction.signatures.len());
        for sig in &transaction.signatures {
            signatures.push(hex::encode(to_bytes_le![sig]?));
        }

        let mut encrypted_records = Vec::with_capacity(transaction.encrypted_records.len());

        for encrypted_record in &transaction.encrypted_records {
            encrypted_records.push(hex::encode(to_bytes_le![encrypted_record]?));
        }

        let transaction_id = transaction.transaction_id()?;
        let storage = &self.storage;
        let block_number = match storage.get_transaction_location(&transaction_id.to_vec())? {
            Some(block_location) => storage
                .get_block_number(&BlockHeaderHash(block_location.block_hash))
                .ok(),
            None => None,
        };

        let transaction_metadata = TransactionMetadata { block_number };

        Ok(TransactionInfo {
            txid: hex::encode(&transaction_id),
            size: transaction_bytes.len(),
            old_serial_numbers,
            new_commitments,
            memo,
            network_id: transaction.network.id(),
            digest: hex::encode(to_bytes_le![transaction.ledger_digest]?),
            transaction_proof: hex::encode(to_bytes_le![transaction.transaction_proof]?),
            program_commitment: hex::encode(to_bytes_le![transaction.program_commitment]?),
            local_data_root: hex::encode(to_bytes_le![transaction.local_data_root]?),
            value_balance: transaction.value_balance.0,
            signatures,
            encrypted_records,
            transaction_metadata,
        })
    }

    /// Send raw transaction bytes to this node to be added into the mempool.
    /// If valid, the transaction will be stored and propagated to all peers.
    /// Returns the transaction id if valid.
    fn send_raw_transaction(&self, transaction_bytes: String) -> Result<String, RpcError> {
        let transaction_bytes = hex::decode(transaction_bytes)?;
        let transaction = Testnet1Transaction::read_le(&transaction_bytes[..])?;
        let transaction_hex_id = hex::encode(transaction.transaction_id()?);

        let storage = &self.storage;

        let primary_height = self.sync_handler()?.current_block_height();
        storage.catch_up_secondary(false, primary_height)?;

        if !self.sync_handler()?.consensus.verify_transaction(&transaction)? {
            // TODO (raychu86) Add more descriptive message. (e.g. tx already exists)
            return Ok("Transaction did not verify".into());
        }

        match !storage.transaction_conflicts(&transaction) {
            true => {
                let entry = Entry::<Testnet1Transaction> {
                    size_in_bytes: transaction_bytes.len(),
                    transaction,
                };

                let self_clone = self.clone();
                tokio::spawn(async move {
                    match self_clone.memory_pool() {
                        Ok(pool) => match pool.insert(&self_clone.storage, entry).await {
                            Ok(Some(_)) => {
                                info!("Transaction added to the memory pool.");
                            }
                            Ok(None) => (),
                            Err(e) => {
                                error!("Failed to insert into memory pool: {:?}", e);
                            }
                        },
                        Err(e) => {
                            error!("Failed to fetch memory pool: {:?}", e);
                        }
                    }
                });

                Ok(transaction_hex_id)
            }
            false => Ok("Transaction contains spent records".into()),
        }
    }

    /// Validate and return if the transaction is valid.
    fn validate_raw_transaction(&self, transaction_bytes: String) -> Result<bool, RpcError> {
        let transaction_bytes = hex::decode(transaction_bytes)?;
        let transaction = Testnet1Transaction::read_le(&transaction_bytes[..])?;

        let storage = &self.storage;

        let primary_height = self.sync_handler()?.current_block_height();
        storage.catch_up_secondary(false, primary_height)?;

        Ok(self.sync_handler()?.consensus.verify_transaction(&transaction)?)
    }

    /// Fetch the number of connected peers this node has.
    fn get_connection_count(&self) -> Result<usize, RpcError> {
        // Create a temporary tokio runtime to make an asynchronous function call
        let number = self.node.peer_book.get_active_peer_count();

        Ok(number as usize)
    }

    /// Returns this nodes connected peers.
    fn get_peer_info(&self) -> Result<PeerInfo, RpcError> {
        // Create a temporary tokio runtime to make an asynchronous function call
        let peers = self.node.peer_book.connected_peers();

        Ok(PeerInfo { peers })
    }

    /// Returns data about the node.
    fn get_node_info(&self) -> Result<NodeInfo, RpcError> {
        Ok(NodeInfo {
            listening_addr: self.node.config.desired_address,
            is_bootnode: self.node.config.is_bootnode(),
            is_miner: self.sync_handler()?.is_miner(),
            is_syncing: self.node.is_syncing_blocks(),
            launched: self.node.launched,
            version: env!("CARGO_PKG_VERSION").into(),
        })
    }

    /// Returns statistics related to the node.
    fn get_node_stats(&self) -> Result<NodeStats, RpcError> {
        let metrics = NODE_STATS.snapshot();

        Ok(metrics)
    }

    /// Returns the current mempool and sync information known by this node.
    fn get_block_template(&self) -> Result<BlockTemplate, RpcError> {
        let storage = &self.storage;

        let primary_height = self.sync_handler()?.current_block_height();
        storage.catch_up_secondary(false, primary_height)?;

        let block_height = storage.get_current_block_height();
        let block = storage.get_block_from_block_number(block_height)?;

        let time = Utc::now().timestamp();

        let full_transactions = self
            .memory_pool()?
            .get_candidates(storage, self.consensus_parameters()?.max_block_size)?;

        let transaction_strings = full_transactions.serialize_as_str()?;

        let mut coinbase_value = get_block_reward(block_height + 1);
        for transaction in full_transactions.iter() {
            coinbase_value = coinbase_value.add(transaction.value_balance())
        }

        Ok(BlockTemplate {
            previous_block_hash: hex::encode(&block.header.get_hash().0),
            block_height: block_height + 1,
            time,
            difficulty_target: self.consensus_parameters()?.get_block_difficulty(&block.header, time),
            transactions: transaction_strings,
            coinbase_value: coinbase_value.0 as u64,
        })
    }

    fn get_network_graph(&self) -> Result<NetworkGraph, RpcError> {
        // Copy the connections as the data must not change throughout the metrics computation.
        let known_network = self.known_network()?;
        let connections = known_network.connections();

        // Collect the edges.
        let edges = connections
            .iter()
            .map(|connection| Edge {
                source: connection.source,
                target: connection.target,
            })
            .collect();

        // Compute the metrics.
        let network_metrics = NetworkMetrics::new(connections);

        // Collect the vertices with the metrics.
        let vertices: Vec<Vertice> = network_metrics
            .centrality
            .iter()
            .map(|(addr, node_centrality)| Vertice {
                addr: *addr,
                is_bootnode: self.node.config.bootnodes().contains(addr),
                degree_centrality: node_centrality.degree_centrality,
                eigenvector_centrality: node_centrality.eigenvector_centrality,
                fiedler_value: node_centrality.fiedler_value,
            })
            .collect();

        let potential_forks = known_network
            .potential_forks()
            .into_iter()
            .map(|(height, members)| PotentialFork { height, members })
            .collect();

        Ok(NetworkGraph {
            node_count: network_metrics.node_count,
            connection_count: network_metrics.connection_count,
            density: network_metrics.density,
            algebraic_connectivity: network_metrics.algebraic_connectivity,
            degree_centrality_delta: network_metrics.degree_centrality_delta,
            potential_forks,
            vertices,
            edges,
        })
    }
}
