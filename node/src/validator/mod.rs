// Copyright (C) 2019-2023 Aleo Systems Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod router;

use crate::traits::NodeInterface;
use snarkos_account::Account;
use snarkos_node_consensus::Consensus;
use snarkos_node_messages::{BlockRequest, Message, NodeType, PuzzleResponse, UnconfirmedSolution};
use snarkos_node_narwhal::helpers::init_primary_channels;
use snarkos_node_rest::Rest;
use snarkos_node_router::{Heartbeat, Inbound, Outbound, Router, Routing};
use snarkos_node_tcp::{
    protocols::{Disconnect, Handshake, OnConnect, Reading, Writing},
    P2P,
};
use snarkvm::prelude::{
    block::{Block, Header},
    coinbase::ProverSolution,
    store::ConsensusStorage,
    Ledger,
    Network,
};

use anyhow::Result;
use parking_lot::Mutex;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::task::JoinHandle;

/// A validator is a full node, capable of validating blocks.
#[derive(Clone)]
pub struct Validator<N: Network, C: ConsensusStorage<N>> {
    /// The ledger of the node.
    ledger: Ledger<N, C>,
    /// The consensus module of the node.
    consensus: Consensus<N, C>,
    /// The router of the node.
    router: Router<N>,
    /// The REST server of the node.
    rest: Option<Rest<N, C, Self>>,
    /// The spawned handles.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// The shutdown signal.
    shutdown: Arc<AtomicBool>,
}

impl<N: Network, C: ConsensusStorage<N>> Validator<N, C> {
    /// Initializes a new validator node.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        node_ip: SocketAddr,
        rest_ip: Option<SocketAddr>,
        bft_ip: Option<SocketAddr>,
        account: Account<N>,
        trusted_peers: &[SocketAddr],
        genesis: Block<N>,
        cdn: Option<String>,
        dev: Option<u16>,
    ) -> Result<Self> {
        // Initialize the ledger.
        let ledger = Ledger::load(genesis, dev)?;
        // Initialize the CDN.
        if let Some(base_url) = cdn {
            // Sync the ledger with the CDN.
            if let Err((_, error)) = snarkos_node_cdn::sync_ledger_with_cdn(&base_url, ledger.clone()).await {
                crate::helpers::log_clean_error(dev);
                return Err(error);
            }
        }
        // Initialize the consensus.
        let mut consensus = Consensus::new(account.clone(), ledger.clone(), bft_ip, dev)?;
        // Initialize the primary channels.
        let (primary_sender, primary_receiver) = init_primary_channels::<N>();
        // Start the consensus.
        consensus.run(primary_sender, primary_receiver).await?;

        // Initialize the node router.
        let router = Router::new(
            node_ip,
            NodeType::Validator,
            account,
            trusted_peers,
            Self::MAXIMUM_NUMBER_OF_PEERS as u16,
            dev.is_some(),
        )
        .await?;

        // Initialize the node.
        let mut node = Self {
            ledger: ledger.clone(),
            consensus: consensus.clone(),
            router,
            rest: None,
            handles: Default::default(),
            shutdown: Default::default(),
        };

        // Initialize the REST server.
        if let Some(rest_ip) = rest_ip {
            node.rest = Some(Rest::start(rest_ip, Some(consensus), ledger, Arc::new(node.clone()))?);
        }
        // Initialize the sync pool.
        node.initialize_sync()?;
        // Initialize the routing.
        node.initialize_routing().await;
        // Initialize the signal handler.
        node.handle_signals();
        // Return the node.
        Ok(node)
    }

    /// Returns the ledger.
    pub fn ledger(&self) -> &Ledger<N, C> {
        &self.ledger
    }

    /// Returns the REST server.
    pub fn rest(&self) -> &Option<Rest<N, C, Self>> {
        &self.rest
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> NodeInterface<N> for Validator<N, C> {
    /// Shuts down the node.
    async fn shut_down(&self) {
        info!("Shutting down...");

        // Shut down the sync pool.
        trace!("Shutting down the sync pool...");
        self.shutdown.store(true, Ordering::Relaxed);

        // Abort the tasks.
        trace!("Shutting down the validator...");
        self.handles.lock().iter().for_each(|handle| handle.abort());

        // Shut down the router.
        self.router.shut_down().await;

        // Shut down the ledger.
        trace!("Shutting down the ledger...");
        // self.ledger.shut_down().await;

        info!("Node has shut down.");
    }
}

impl<N: Network, C: ConsensusStorage<N>> Validator<N, C> {
    /// Initializes the sync pool.
    fn initialize_sync(&self) -> Result<()> {
        // Retrieve the canon locators.
        let canon_locators = crate::helpers::get_block_locators(&self.ledger)?;
        // Insert the canon locators into the sync pool.
        self.router.sync().insert_canon_locators(canon_locators).unwrap();

        // Start the sync loop.
        let validator = self.clone();
        self.handles.lock().push(tokio::spawn(async move {
            loop {
                // If the Ctrl-C handler registered the signal, stop the node.
                if validator.shutdown.load(Ordering::Relaxed) {
                    info!("Shutting down block production");
                    break;
                }

                // Sleep briefly to avoid triggering spam detection.
                tokio::time::sleep(Duration::from_secs(1)).await;

                // Prepare the block requests, if any.
                let block_requests = validator.router.sync().prepare_block_requests();
                trace!("Prepared {} block requests", block_requests.len());

                // Process the block requests.
                'outer: for (height, (hash, previous_hash, sync_ips)) in block_requests {
                    // Insert the block request into the sync pool.
                    let result =
                        validator.router.sync().insert_block_request(height, (hash, previous_hash, sync_ips.clone()));

                    // If the block request was inserted, send it to the peers.
                    if result.is_ok() {
                        // Construct the message.
                        let message =
                            Message::BlockRequest(BlockRequest { start_height: height, end_height: height + 1 });
                        // Send the message to the peers.
                        for sync_ip in sync_ips {
                            // If the send fails for any peer, remove the block request from the sync pool.
                            if validator.send(sync_ip, message.clone()).is_none() {
                                // Remove the entire block request.
                                validator.router.sync().remove_block_request(height);
                                // Break out of the loop.
                                break 'outer;
                            }
                        }
                        // Sleep for 10 milliseconds to avoid triggering spam detection.
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }
            }
        }));
        Ok(())
    }

    /// Attempts to advance with blocks from the sync pool.
    fn advance_with_sync_blocks(&self) {
        // Retrieve the latest block height.
        let mut current_height = self.ledger.latest_height();
        // Try to advance the ledger with the sync pool.
        while let Some(block) = self.router.sync().remove_block_response(current_height + 1) {
            // Ensure the block height matches.
            if block.height() != current_height + 1 {
                warn!("Block height mismatch: expected {}, found {}", current_height + 1, block.height());
                break;
            }
            // Check the next block.
            if let Err(error) = self.ledger.check_next_block(&block) {
                warn!("The next block ({}) is invalid - {error}", block.height());
                break;
            }
            // Attempt to advance to the next block.
            if let Err(error) = self.consensus.ledger().advance_to_next_block(&block) {
                warn!("{error}");
                break;
            }
            // Insert the height and hash as canon in the sync pool.
            self.router.sync().insert_canon_locator(block.height(), block.hash());
            // Increment the latest height.
            current_height += 1;
        }
    }
}
