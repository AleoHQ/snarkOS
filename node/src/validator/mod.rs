// Copyright (C) 2019-2022 Aleo Systems Inc.
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

mod router;

use crate::traits::NodeInterface;
use snarkos_account::Account;
use snarkos_node_consensus::Consensus;
use snarkos_node_executor::{spawn_task, spawn_task_loop, Executor, NodeType, Status};
use snarkos_node_ledger::Ledger;
use snarkos_node_messages::{
    BlockRequest,
    BlockResponse,
    Data,
    Message,
    Ping,
    Pong,
    PuzzleResponse,
    UnconfirmedBlock,
    UnconfirmedSolution,
};
use snarkos_node_rest::Rest;
use snarkos_node_router::{Handshake, Inbound, Outbound, Peer, Router, RouterRequest, ALEO_MAXIMUM_FORK_DEPTH};
use snarkos_node_store::ConsensusDB;
use snarkvm::prelude::{Address, Block, CoinbasePuzzle, EpochChallenge, Network, PrivateKey, ProverSolution, ViewKey};

use anyhow::{bail, ensure, Result};
use core::time::Duration;
use sha2::{Digest, Sha256};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

/// The number of blocks in each fast-sync chunk.
const NUM_BLOCKS_PER_CHUNK: u32 = 50;

/// A validator is a full node, capable of validating blocks.
#[derive(Clone)]
pub struct Validator<N: Network> {
    /// The account of the node.
    account: Account<N>,
    /// The consensus module of the node.
    consensus: Consensus<N, ConsensusDB<N>>,
    /// The ledger of the node.
    ledger: Ledger<N, ConsensusDB<N>>,
    /// The router of the node.
    router: Router<N>,
    /// The REST server of the node.
    rest: Option<Arc<Rest<N, ConsensusDB<N>>>>,
    /// The coinbase puzzle.
    coinbase_puzzle: CoinbasePuzzle<N>,
    /// The latest epoch challenge.
    latest_epoch_challenge: Arc<RwLock<Option<EpochChallenge<N>>>>,
    /// The latest block.
    latest_block: Arc<RwLock<Option<Block<N>>>>,
    /// The latest puzzle response.
    latest_puzzle_response: Arc<RwLock<Option<PuzzleResponse<N>>>>,
    /// The shutdown signal.
    shutdown: Arc<AtomicBool>,
}

impl<N: Network> Validator<N> {
    /// Initializes a new validator node.
    pub async fn new(
        node_ip: SocketAddr,
        rest_ip: Option<SocketAddr>,
        private_key: PrivateKey<N>,
        trusted_peers: &[SocketAddr],
        genesis: Option<Block<N>>,
        dev: Option<u16>,
    ) -> Result<Self> {
        // Initialize the node account.
        let account = Account::from(private_key)?;
        // Initialize the ledger.
        let ledger = Ledger::load(genesis, dev)?;
        // Initialize the consensus.
        let consensus = Consensus::new(ledger.clone())?;
        // Initialize the node router.
        let (router, router_receiver) = Router::new::<Self>(node_ip, account.address(), trusted_peers).await?;
        // Initialize the REST server.
        let rest = match rest_ip {
            Some(rest_ip) => {
                Some(Arc::new(Rest::start(rest_ip, account.address(), None, ledger.clone(), router.clone())?))
            }
            None => None,
        };
        // Load the coinbase puzzle.
        let coinbase_puzzle = CoinbasePuzzle::<N>::load()?;
        // Initialize the node.
        let node = Self {
            account,
            consensus,
            ledger,
            router: router.clone(),
            rest,
            coinbase_puzzle,
            latest_epoch_challenge: Default::default(),
            latest_block: Default::default(),
            latest_puzzle_response: Default::default(),
            shutdown: Default::default(),
        };
        // Initialize the router handler.
        router.initialize_handler(node.clone(), router_receiver).await;
        // Initialize the signal handler.
        node.handle_signals();
        // Initialize the standard block sync.
        node.initialize_block_sync(dev).await;
        // Return the node.
        Ok(node)
    }

    /// Returns the ledger.
    pub fn ledger(&self) -> &Ledger<N, ConsensusDB<N>> {
        &self.ledger
    }

    /// Returns the REST server.
    pub fn rest(&self) -> &Option<Arc<Rest<N, ConsensusDB<N>>>> {
        &self.rest
    }
}

#[async_trait]
impl<N: Network> Executor for Validator<N> {
    /// The node type.
    const NODE_TYPE: NodeType = NodeType::Validator;

    /// Disconnects from peers and shuts down the node.
    async fn shut_down(&self) {
        info!("Shutting down...");
        // Update the node status.
        Self::status().update(Status::ShuttingDown);

        // Shut down the ledger.
        trace!("Proceeding to shut down the ledger...");
        self.shutdown.store(true, Ordering::SeqCst);

        // Flush the tasks.
        Self::resources().shut_down();
        trace!("Node has shut down.");
    }
}

impl<N: Network> NodeInterface<N> for Validator<N> {
    /// Returns the node type.
    fn node_type(&self) -> NodeType {
        Self::NODE_TYPE
    }

    /// Returns the node router.
    fn router(&self) -> &Router<N> {
        &self.router
    }

    /// Returns the account private key of the node.
    fn private_key(&self) -> &PrivateKey<N> {
        self.account.private_key()
    }

    /// Returns the account view key of the node.
    fn view_key(&self) -> &ViewKey<N> {
        self.account.view_key()
    }

    /// Returns the account address of the node.
    fn address(&self) -> Address<N> {
        self.account.address()
    }
}

impl<N: Network> Validator<N> {
    /// Fetches the block chunk with the given starting block height from the fast sync server.
    async fn request_fast_sync_blocks(start_height: u32) -> Result<Vec<Block<N>>> {
        // Sha256 hasher.
        pub fn sha256(data: &[u8]) -> [u8; 32] {
            let digest = Sha256::digest(data);
            let mut ret = [0u8; 32];
            ret.copy_from_slice(&digest);
            ret
        }

        // TODO (raychu86): Use a proxy fast-sync server.
        const FAST_SYNC_SERVER: &str = "https://s3.us-west-1.amazonaws.com/testnet3.blocks/phase2/";

        ensure!(start_height % NUM_BLOCKS_PER_CHUNK == 0, "Invalid starting height for fast-sync. ({start_height})");

        // Fetch the end height for the chunk.
        let end_height = start_height + NUM_BLOCKS_PER_CHUNK;

        trace!("Requesting fast-sync blocks from {start_height} to {end_height}...");

        // Specify the URLs for fetching blocks.
        let blocks_url = format!("{FAST_SYNC_SERVER}{start_height}.{end_height}.blocks");
        let blocks_checksum_url = format!("{blocks_url}.sum");

        // Request the blocks from the fast-sync server.
        let blocks_bytes = match reqwest::Client::new().get(&blocks_url).send().await?.bytes().await {
            Ok(bytes) => bytes,
            Err(error) => {
                bail!("Failed to fetch blocks from {blocks_url}: {error}");
            }
        };
        let blocks_checksum = reqwest::Client::new().get(&blocks_checksum_url).send().await?.bytes().await?;
        ensure!(
            sha256(&blocks_bytes) == blocks_checksum.as_ref(),
            "Invalid checksum for fast-sync blocks. ({blocks_url})"
        );

        // Deserialize the blocks.
        let blocks: Vec<Block<N>> = bincode::deserialize(&blocks_bytes)?;

        trace!("Received fast-sync blocks from {start_height} to {end_height}...");

        Ok(blocks)
    }

    /// Attempts to sync the node with the fast sync server. This will return an error if the
    /// node failed to sync or has finished syncing.
    async fn initialize_block_fast_sync(&self) -> Result<()> {
        // Set the sync status to `Syncing`.
        Self::status().update(Status::Syncing);

        info!("Performing fast sync...");

        loop {
            // Fetch the latest block height.
            let latest_height = self.ledger().latest_height();

            // Fetch the number of blocks that you already have in a chunk.
            let num_overlapping_blocks = latest_height.saturating_add(1) % NUM_BLOCKS_PER_CHUNK;

            // Fetch the starting height of the requested chunk of blocks.
            let start_height = latest_height.saturating_add(1).saturating_sub(num_overlapping_blocks);

            // Fetch the blocks from the fast-sync server.
            let new_blocks = Self::request_fast_sync_blocks(start_height).await?;

            // Insert the blocks into the ledger. Skip the blocks that we already own.
            for block in new_blocks.iter() {
                // Skip the block if it already exists in the ledger.
                if self.ledger.contains_block_hash(&block.hash())? {
                    continue;
                }

                // Check that the next block is valid.
                self.consensus.check_next_block(block)?;

                // Attempt to add the block to the ledger.
                self.consensus.advance_to_next_block(block)?;

                info!("Ledger successfully advanced to block {} ({})", block.height(), block.hash());
            }

            // If the Ctrl-C handler registered the signal, stop the node once the current block is complete.
            if self.shutdown.load(Ordering::Relaxed) {
                info!("Shutting down block fast sync");
                return Ok(());
            }
        }
    }

    ///
    /// Initialize the block synchronizer. This will request blocks from connected peers that have
    /// a higher block height than the node's current block height.
    ///
    /// If the node is a non-development node, it will first perform a fast sync before attempting
    /// to sync with peers.
    ///
    async fn initialize_block_sync(&self, dev: Option<u16>) {
        // Initialize the syncing protocol.
        let validator = self.clone();
        spawn_task_loop!(Self, {
            if dev.is_none() {
                // Perform the fast sync.
                let _ = validator.initialize_block_fast_sync().await;
                info!("Fast sync completed, switching to standard sync protocol.");
            }

            // Set the sync status to `Ready`.
            Self::status().update(Status::Ready);

            // Perform the standard block sync protocol.
            loop {
                // If the Ctrl-C handler registered the signal, stop the node once the current block is complete.
                if validator.shutdown.load(Ordering::Relaxed) {
                    info!("Shutting down block sync");
                    break;
                }

                // Fetch the latest block height.
                let latest_height = validator.ledger().latest_height();

                // Get the peer with the highest block height.
                let peer_block_heights = validator.router.connected_peer_block_heights().await;
                let peer = match peer_block_heights.into_iter().max_by(|(_, a), (_, b)| a.cmp(b)) {
                    Some(peer) => Some(peer),
                    None => {
                        // Set the sync status to `Ready`.
                        Self::status().update(Status::Ready);
                        None
                    }
                };

                // If a peer exists, check if the peer is ahead of the node.
                if let Some((peer_ip, peer_block_height)) = peer {
                    // TODO (raychu86): Upgrade to a more sophisticated sync protocol.

                    // If the peer has a greater height than the node, request blocks.
                    if latest_height < peer_block_height {
                        Self::status().update(Status::Syncing);

                        // Specify the block height to request.
                        let start_block_height = latest_height.saturating_add(1);
                        let end_block_height =
                            std::cmp::min(peer_block_height, start_block_height + Self::MAXIMUM_BLOCK_REQUEST);

                        trace!(
                            "Sending block request to peer {peer_ip} for blocks {start_block_height} to {end_block_height}."
                        );

                        // Send the `BlockRequest` message to the peer.
                        let message = Message::BlockRequest(BlockRequest { start_block_height, end_block_height });
                        if let Err(error) = validator.router.process(RouterRequest::MessageSend(peer_ip, message)).await
                        {
                            warn!("[BlockRequest] {}", error);
                        }
                    } else {
                        // Set the sync status to `Ready`.
                        Self::status().update(Status::Ready);
                    }
                }

                // Sleep depending on the sync status.
                if Self::status().is_syncing() {
                    // Sleep for 1 second.
                    tokio::time::sleep(Duration::from_secs(1)).await;
                } else {
                    // Sleep for
                    // 10 seconds.
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            }
        });
    }
}
