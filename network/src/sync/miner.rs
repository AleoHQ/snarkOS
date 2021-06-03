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

use crate::{stats, Node, State};
use futures::executor::block_on;
use snarkos_consensus::Miner;
use snarkos_storage::Storage;
use snarkvm_dpc::{base_dpc::instantiated::*, AccountAddress};

use tokio::task;
use tracing::*;

use std::{sync::Arc, thread, time::Duration};

/// Parameters for spawning a miner that runs proof of work to find a block.
pub struct MinerInstance<S: Storage> {
    miner_address: AccountAddress<Components>,
    node: Node<S>,
}

impl<S: Storage> MinerInstance<S> {
    /// Creates a new MinerInstance for spawning miners.
    pub fn new(miner_address: AccountAddress<Components>, node: Node<S>) -> Self {
        Self { miner_address, node }
    }

    /// Spawns a new miner on a new thread using MinerInstance parameters.
    /// Once a block is found, A block message is sent to all peers.
    /// Calling this function multiple times will spawn additional listeners on separate threads.
    pub fn spawn(self) -> task::JoinHandle<()> {
        let local_address = self.node.local_address().unwrap();
        info!("Initializing Aleo miner - Your miner address is {}", self.miner_address);
        let miner = Miner::new(
            self.miner_address.clone(),
            Arc::clone(&self.node.expect_sync().consensus),
        );
        info!("Miner instantiated; starting to mine blocks");

        let mut mining_failure_count = 0;
        let mining_failure_threshold = 10;

        task::spawn_blocking(move || {
            loop {
                if self.node.is_shutting_down() {
                    debug!("The node is shutting down, stopping mining");
                    break;
                }

                // Don't mine if the node is currently syncing.
                if self.node.state() == State::Syncing {
                    thread::sleep(Duration::from_secs(15));
                    continue;
                } else {
                    self.node.set_state(State::Mining);
                }

                info!("Starting to mine the next block");

                let (block, _coinbase_records) = match block_on(miner.mine_block()) {
                    Ok(mined_block) => mined_block,
                    Err(error) => {
                        // It's possible that the node realized that it needs to sync with another one in the
                        // meantime; don't change to `Idle` if the current status isn't still `Mining`.
                        if self.node.state() == State::Mining {
                            self.node.set_state(State::Idle);
                        }

                        warn!(
                            "Miner failed to mine a block {} time(s). (error message: {}).",
                            mining_failure_count, error
                        );
                        mining_failure_count += 1;

                        if mining_failure_count >= mining_failure_threshold {
                            warn!(
                                "Miner has failed to mine a block {} times. Shutting down miner.",
                                mining_failure_count
                            );
                            break;
                        } else {
                            continue;
                        }
                    }
                };

                // See the `Err` path note above.
                if self.node.state() == State::Mining {
                    self.node.set_state(State::Idle);
                }

                metrics::increment_counter!(stats::MISC_BLOCKS_MINED);

                info!("Mined a new block: {:?}", hex::encode(block.header.get_hash().0));

                let serialized_block = if let Ok(block) = block.serialize() {
                    block
                } else {
                    error!("Our own miner baked an unserializable block!");
                    continue;
                };

                block_on(self.node.propagate_block(serialized_block, local_address));
            }
        })
    }
}
