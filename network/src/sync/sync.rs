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

use crate::{Node, State};
use snarkos_consensus::{ConsensusParameters, MemoryPool, MerkleTreeLedger};
use snarkos_storage::BlockHeight;
use snarkvm_dpc::{
    testnet1::instantiated::{Testnet1DPC, Testnet1Transaction},
    Storage,
};

use atomic_instant::AtomicInstant;
use std::{sync::Arc, time::Duration};

/// The sync handler of this node.
pub struct Sync<S: Storage> {
    /// The core sync objects.
    pub consensus: Arc<snarkos_consensus::Consensus<S>>,
    /// If `true`, initializes a mining task on this node.
    is_miner: bool,
    /// The interval between each block sync.
    block_sync_interval: Duration,
    /// The interval between each memory pool sync.
    mempool_sync_interval: Duration,
    /// The last time a block sync was initiated.
    last_block_sync: AtomicInstant,
}

impl<S: Storage + core::marker::Sync + Send + 'static> Sync<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        consensus: Arc<snarkos_consensus::Consensus<S>>,
        is_miner: bool,
        block_sync_interval: Duration,
        mempool_sync_interval: Duration,
    ) -> Self {
        Self {
            consensus,
            is_miner,
            block_sync_interval,
            mempool_sync_interval,
            last_block_sync: AtomicInstant::empty(),
        }
    }

    /// Returns a reference to the storage system of this node.
    #[inline]
    pub fn storage(&self) -> &MerkleTreeLedger<S> {
        &self.consensus.ledger
    }

    /// Returns a reference to the memory pool of this node.
    #[inline]
    pub fn memory_pool(&self) -> &MemoryPool<Testnet1Transaction> {
        &self.consensus.memory_pool
    }

    /// Returns a reference to the sync parameters of this node.
    #[inline]
    pub fn consensus_parameters(&self) -> &ConsensusParameters {
        &self.consensus.parameters
    }

    /// Returns a reference to the DPC parameters of this node.
    #[inline]
    pub fn dpc_parameters(&self) -> &Testnet1DPC {
        &self.consensus.dpc
    }

    /// Returns `true` if this node is a mining node. Otherwise, returns `false`.
    #[inline]
    pub fn is_miner(&self) -> bool {
        self.is_miner
    }

    /// Returns the current block height of the ledger from storage.
    #[inline]
    pub fn current_block_height(&self) -> BlockHeight {
        self.consensus.ledger.get_current_block_height()
    }

    /// Checks whether any previous sync attempt has expired.
    pub fn has_block_sync_expired(&self) -> bool {
        let last_block_sync = self.last_block_sync.as_millis();

        // due to double load, this can technically return twice, but shouldnt happen in practice
        if last_block_sync > 0 {
            self.last_block_sync.elapsed() > Duration::from_secs(crate::BLOCK_SYNC_EXPIRATION_SECS as u64)
        } else {
            // this means it's the very first sync attempt
            true
        }
    }

    /// Returns the interval between each block sync.
    pub fn block_sync_interval(&self) -> Duration {
        self.block_sync_interval
    }

    /// Returns the interval between each memory pool sync.
    pub fn mempool_sync_interval(&self) -> Duration {
        self.mempool_sync_interval
    }

    pub fn max_block_size(&self) -> usize {
        self.consensus.parameters.max_block_size
    }
}

impl<S: Storage + Send + core::marker::Sync + 'static> Node<S> {
    /// Checks whether the node is currently syncing blocks.
    pub fn is_syncing_blocks(&self) -> bool {
        self.state() == State::Syncing
    }

    /// Register that the node is no longer syncing blocks.
    pub fn finished_syncing_blocks(&self) {
        self.set_state(State::Idle);
    }

    /// Register that the node attempted to sync blocks.
    pub fn register_block_sync_attempt(&self) {
        if let Some(sync) = self.sync() {
            sync.last_block_sync.set_now();
        }
        self.set_state(State::Syncing);
    }
}
