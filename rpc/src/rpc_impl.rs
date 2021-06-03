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
use futures::executor::block_on;
use snarkos_consensus::{get_block_reward, memory_pool::Entry, ConsensusParameters, MemoryPool, MerkleTreeLedger};
use snarkos_network::{Node, Sync, NODE_STATS};
use snarkos_storage::Storage;
use snarkvm_dpc::base_dpc::{
    instantiated::{Components, Tx},
    parameters::PublicParameters,
};
use snarkvm_objects::{BlockHeaderHash, Transaction};
use snarkvm_utilities::{
    bytes::{FromBytes, ToBytes},
    to_bytes,
    CanonicalSerialize,
};

use chrono::Utc;

use std::{
    ops::Deref,
    sync::{atomic::Ordering, Arc},
};

/// Implements JSON-RPC HTTP endpoint functions for a node.
/// The constructor is given Arc::clone() copies of all needed node components.
#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct RpcImpl<S: Storage>(Arc<RpcInner<S>>);

impl<S: Storage> Deref for RpcImpl<S> {
    type Target = RpcInner<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct RpcInner<S: Storage> {
    /// Blockchain database storage.
    pub(crate) storage: Arc<MerkleTreeLedger<S>>,

    /// RPC credentials for accessing guarded endpoints
    pub(crate) credentials: Option<RpcCredentials>,

    /// A clone of the network Node
    pub(crate) node: Node<S>,
}

impl<S: Storage> RpcImpl<S> {
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

    pub fn dpc_parameters(&self) -> Result<&PublicParameters<Components>, RpcError> {
        Ok(self.sync_handler()?.dpc_parameters())
    }

    pub fn memory_pool(&self) -> Result<&MemoryPool<Tx>, RpcError> {
        Ok(self.sync_handler()?.memory_pool())
    }
}

impl<S: Storage> RpcFunctions for RpcImpl<S> {
    /// Returns information about a block from a block hash.
    fn get_block(&self, block_hash_string: String) -> Result<BlockInfo, RpcError> {
        let block_hash = hex::decode(&block_hash_string)?;
        if block_hash.len() != 32 {
            return Err(RpcError::InvalidBlockHash(block_hash_string));
        }

        let storage = &self.storage;

        let block_header_hash = BlockHeaderHash::new(block_hash);
        let height = match block_on(storage.get_block_number(&block_header_hash)) {
            Ok(block_num) => match block_on(storage.is_canon(&block_header_hash)) {
                true => Some(block_num),
                false => None,
            },
            Err(_) => None,
        };

        let confirmations = match height {
            Some(block_height) => storage.get_current_block_height() - block_height,
            None => 0,
        };

        if let Ok(block) = block_on(storage.get_block(&block_header_hash)) {
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

    /// Returns the number of blocks in the canonical chain.
    fn get_block_count(&self) -> Result<u32, RpcError> {
        let storage = &self.storage;
        Ok(storage.get_block_count())
    }

    /// Returns the block hash of the head of the canonical chain.
    fn get_best_block_hash(&self) -> Result<String, RpcError> {
        let storage = &self.storage;
        let best_block_hash = block_on(storage.get_block_hash(storage.get_current_block_height()))?;

        Ok(hex::encode(&best_block_hash.0))
    }

    /// Returns the block hash of the index specified if it exists in the canonical chain.
    fn get_block_hash(&self, block_height: u32) -> Result<String, RpcError> {
        let storage = &self.storage;
        let block_hash = block_on(storage.get_block_hash(block_height))?;

        Ok(hex::encode(&block_hash.0))
    }

    /// Returns the hex encoded bytes of a transaction from its transaction id.
    fn get_raw_transaction(&self, transaction_id: String) -> Result<String, RpcError> {
        let storage = &self.storage;
        Ok(hex::encode(&block_on(
            storage.get_transaction_bytes(&hex::decode(transaction_id)?),
        )?))
    }

    /// Returns information about a transaction from a transaction id.
    fn get_transaction_info(&self, transaction_id: String) -> Result<TransactionInfo, RpcError> {
        let transaction_bytes = self.get_raw_transaction(transaction_id)?;
        self.decode_raw_transaction(transaction_bytes)
    }

    /// Returns information about a transaction from serialized transaction bytes.
    fn decode_raw_transaction(&self, transaction_bytes: String) -> Result<TransactionInfo, RpcError> {
        let transaction_bytes = hex::decode(transaction_bytes)?;
        let transaction = Tx::read(&transaction_bytes[..])?;

        let mut old_serial_numbers = Vec::with_capacity(transaction.old_serial_numbers().len());

        for sn in transaction.old_serial_numbers() {
            let mut serial_number: Vec<u8> = vec![];
            CanonicalSerialize::serialize(sn, &mut serial_number).unwrap();
            old_serial_numbers.push(hex::encode(serial_number));
        }

        let mut new_commitments = Vec::with_capacity(transaction.new_commitments().len());

        for cm in transaction.new_commitments() {
            new_commitments.push(hex::encode(to_bytes![cm]?));
        }

        let memo = hex::encode(to_bytes![transaction.memorandum()]?);

        let mut signatures = Vec::with_capacity(transaction.signatures.len());
        for sig in &transaction.signatures {
            signatures.push(hex::encode(to_bytes![sig]?));
        }

        let mut encrypted_records = Vec::with_capacity(transaction.encrypted_records.len());

        for encrypted_record in &transaction.encrypted_records {
            encrypted_records.push(hex::encode(to_bytes![encrypted_record]?));
        }

        let transaction_id = transaction.transaction_id()?;
        let storage = &self.storage;
        let block_number = match block_on(storage.get_transaction_location(&transaction_id.to_vec()))? {
            Some(block_location) => Some(block_on(
                storage.get_block_number(&BlockHeaderHash(block_location.block_hash)),
            )?),
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
            digest: hex::encode(to_bytes![transaction.ledger_digest]?),
            transaction_proof: hex::encode(to_bytes![transaction.transaction_proof]?),
            program_commitment: hex::encode(to_bytes![transaction.program_commitment]?),
            local_data_root: hex::encode(to_bytes![transaction.local_data_root]?),
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
        let transaction = Tx::read(&transaction_bytes[..])?;
        let transaction_hex_id = hex::encode(transaction.transaction_id()?);

        let storage = &self.storage;

        if !block_on(self.sync_handler()?.consensus.verify_transaction(&transaction))? {
            // TODO (raychu86) Add more descriptive message. (e.g. tx already exists)
            return Ok("Transaction did not verify".into());
        }

        match !block_on(storage.transaction_conflicts(&transaction)) {
            true => {
                let entry = Entry::<Tx> {
                    size_in_bytes: transaction_bytes.len(),
                    transaction,
                };

                // this block_on will halt the tokio worker until insert completion -- can cause problems if not in a multi-threaded environment (tests)
                if let Ok(inserted) = block_on(self.memory_pool()?.insert(&storage, entry)) {
                    if inserted.is_some() {
                        info!("Transaction added to the memory pool.");
                        // TODO(ljedrz): checks if needs to be propagated to the network; if need be, this could
                        // be made automatic at the time when a tx from any source is added the memory pool
                    }
                }

                Ok(transaction_hex_id)
            }
            false => Ok("Transaction contains spent records".into()),
        }
    }

    /// Validate and return if the transaction is valid.
    fn validate_raw_transaction(&self, transaction_bytes: String) -> Result<bool, RpcError> {
        let transaction_bytes = hex::decode(transaction_bytes)?;
        let transaction = Tx::read(&transaction_bytes[..])?;

        Ok(block_on(
            self.sync_handler()?.consensus.verify_transaction(&transaction),
        )?)
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
        Ok(NodeStats {
            inbound: NodeInboundStats {
                all_successes: NODE_STATS.inbound.all_successes.load(Ordering::Relaxed),
                all_failures: NODE_STATS.inbound.all_failures.load(Ordering::Relaxed),

                blocks: NODE_STATS.inbound.blocks.load(Ordering::Relaxed),
                getblocks: NODE_STATS.inbound.getblocks.load(Ordering::Relaxed),
                getmemorypool: NODE_STATS.inbound.getmemorypool.load(Ordering::Relaxed),
                getpeers: NODE_STATS.inbound.getpeers.load(Ordering::Relaxed),
                getsync: NODE_STATS.inbound.getsync.load(Ordering::Relaxed),
                memorypool: NODE_STATS.inbound.memorypool.load(Ordering::Relaxed),
                peers: NODE_STATS.inbound.peers.load(Ordering::Relaxed),
                pings: NODE_STATS.inbound.pings.load(Ordering::Relaxed),
                pongs: NODE_STATS.inbound.pongs.load(Ordering::Relaxed),
                syncs: NODE_STATS.inbound.syncs.load(Ordering::Relaxed),
                syncblocks: NODE_STATS.inbound.syncblocks.load(Ordering::Relaxed),
                transactions: NODE_STATS.inbound.transactions.load(Ordering::Relaxed),
                unknown: NODE_STATS.inbound.unknown.load(Ordering::Relaxed),
            },
            outbound: NodeOutboundStats {
                all_successes: NODE_STATS.outbound.all_successes.load(Ordering::Relaxed),
                all_failures: NODE_STATS.outbound.all_failures.load(Ordering::Relaxed),
            },
            connections: NodeConnectionStats {
                all_accepted: NODE_STATS.connections.all_accepted.load(Ordering::Relaxed),
                all_initiated: NODE_STATS.connections.all_initiated.load(Ordering::Relaxed),
                all_rejected: NODE_STATS.connections.all_rejected.load(Ordering::Relaxed),
                connected_peers: self.node.peer_book.get_active_peer_count(),
                disconnected_peers: self.node.peer_book.get_disconnected_peer_count(),
            },
            handshakes: NodeHandshakeStats {
                successes_init: NODE_STATS.handshakes.successes_init.load(Ordering::Relaxed),
                successes_resp: NODE_STATS.handshakes.successes_resp.load(Ordering::Relaxed),
                failures_init: NODE_STATS.handshakes.failures_init.load(Ordering::Relaxed),
                failures_resp: NODE_STATS.handshakes.failures_resp.load(Ordering::Relaxed),
                timeouts_init: NODE_STATS.handshakes.timeouts_init.load(Ordering::Relaxed),
                timeouts_resp: NODE_STATS.handshakes.timeouts_resp.load(Ordering::Relaxed),
            },
            queues: NodeQueueStats {
                inbound: NODE_STATS.queues.inbound.load(Ordering::SeqCst),
                outbound: NODE_STATS.queues.outbound.load(Ordering::SeqCst),
            },
            misc: NodeMiscStats {
                block_height: self
                    .node
                    .sync()
                    .map(|sync| sync.current_block_height() as u64)
                    .unwrap_or(0),
                blocks_mined: NODE_STATS.misc.blocks_mined.load(Ordering::Relaxed),
                duplicate_blocks: NODE_STATS.misc.duplicate_blocks.load(Ordering::Relaxed),
                duplicate_sync_blocks: NODE_STATS.misc.duplicate_sync_blocks.load(Ordering::Relaxed),
                rpc_requests: NODE_STATS.misc.rpc_requests.load(Ordering::Relaxed),
            },
        })
    }

    /// Returns the current mempool and sync information known by this node.
    fn get_block_template(&self) -> Result<BlockTemplate, RpcError> {
        let storage = &self.storage;

        let block_height = storage.get_current_block_height();
        let block = block_on(storage.get_block_from_block_number(block_height))?;

        let time = Utc::now().timestamp();

        let full_transactions = block_on(
            self.memory_pool()?
                .get_candidates(&storage, self.consensus_parameters()?.max_block_size),
        )?;

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
}
