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

use crate::{
    helpers::{PeerPair, SyncRequest},
    locators::BlockLocators,
};
use snarkos_node_bft_ledger_service::LedgerService;
use snarkos_node_router::messages::DataBlocks;
use snarkos_node_sync_communication_service::CommunicationService;
use snarkos_node_sync_locators::{CHECKPOINT_INTERVAL, NUM_RECENT_BLOCKS};
use snarkvm::prelude::{block::Block, Network};

use anyhow::{bail, ensure, Result};
use indexmap::{IndexMap, IndexSet};
use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use rand::{prelude::IteratorRandom, CryptoRng, Rng};
use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

#[cfg(not(test))]
pub const REDUNDANCY_FACTOR: usize = 1;
#[cfg(test)]
pub const REDUNDANCY_FACTOR: usize = 3;
const EXTRA_REDUNDANCY_FACTOR: usize = REDUNDANCY_FACTOR * 3;
const NUM_SYNC_CANDIDATE_PEERS: usize = REDUNDANCY_FACTOR * 5;

const BLOCK_REQUEST_TIMEOUT_IN_SECS: u64 = 600; // 600 seconds
const MAX_BLOCK_REQUESTS: usize = 50; // 50 requests
const MAX_BLOCK_REQUEST_TIMEOUTS: usize = 5; // 5 timeouts

/// The maximum number of blocks tolerated before the primary is considered behind its peers.
pub const MAX_BLOCKS_BEHIND: u32 = 1; // blocks

/// This is a dummy IP address that is used to represent the local node.
/// Note: This here does not need to be a real IP address, but it must be unique/distinct from all other connections.
const DUMMY_SELF_IP: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum BlockSyncMode {
    Router,
    Gateway,
}

impl BlockSyncMode {
    /// Returns `true` if the node is in router mode.
    pub const fn is_router(&self) -> bool {
        matches!(self, Self::Router)
    }

    /// Returns `true` if the node is in gateway mode.
    pub const fn is_gateway(&self) -> bool {
        matches!(self, Self::Gateway)
    }
}

/// A struct that keeps track of the current block sync state.
///
/// # State
/// - When a request is inserted, the `requests` map and `request_timestamps` map insert an entry for the request height.
/// - When a response is inserted, the `requests` map inserts the entry for the request height.
/// - When a request is completed, the `requests` map still has the entry, but its `sync_ips` is empty;
///   the `request_timestamps` map remains unchanged.
/// - When a response is removed/completed, the `requests` map and `request_timestamps` map also remove the entry for the request height.
/// - When a request is timed out, the `requests`, `request_timestamps`, and `responses` map remove the entry for the request height;
#[derive(Clone, Debug)]
pub struct BlockSync<N: Network> {
    /// The block sync mode.
    mode: BlockSyncMode,
    /// The canonical map of block height to block hash.
    /// This map is a linearly-increasing map of block heights to block hashes,
    /// updated solely from the ledger and candidate blocks (not from peers' block locators, to ensure there are no forks).
    canon: Arc<dyn LedgerService<N>>,
    /// The map of peer IP to their block locators.
    /// The block locators are consistent with the canonical map and every other peer's block locators.
    locators: Arc<RwLock<IndexMap<SocketAddr, BlockLocators<N>>>>,
    /// The map of peer-to-peer to their common ancestor.
    /// This map is used to determine which peers to request blocks from.
    common_ancestors: Arc<RwLock<IndexMap<PeerPair, u32>>>,
    /// The map of block height to the expected block hash and peer IPs.
    /// Each entry is removed when its corresponding entry in the responses map is removed.
    requests: Arc<RwLock<BTreeMap<u32, SyncRequest<N>>>>,
    /// The map of block height to the received blocks.
    /// Removing an entry from this map must remove the corresponding entry from the requests map.
    responses: Arc<RwLock<BTreeMap<u32, Block<N>>>>,
    /// The map of block height to the timestamp of the last time the block was requested.
    /// This map is used to determine which requests to remove if they have been pending for too long.
    request_timestamps: Arc<RwLock<BTreeMap<u32, Instant>>>,
    /// The map of (timed out) peer IPs to their request timestamps.
    /// This map is used to determine which peers to remove if they have timed out too many times.
    request_timeouts: Arc<RwLock<IndexMap<SocketAddr, Vec<Instant>>>>,
    /// The boolean indicator of whether the node is synced up to the latest block (within the given tolerance).
    is_block_synced: Arc<AtomicBool>,
    /// The lock to guarantee advance_with_sync_blocks() is called only once at a time.
    advance_with_sync_blocks_lock: Arc<Mutex<()>>,
}

impl<N: Network> BlockSync<N> {
    /// Initializes a new block sync module.
    pub fn new(mode: BlockSyncMode, ledger: Arc<dyn LedgerService<N>>) -> Self {
        Self {
            mode,
            canon: ledger,
            locators: Default::default(),
            common_ancestors: Default::default(),
            requests: Default::default(),
            responses: Default::default(),
            request_timestamps: Default::default(),
            request_timeouts: Default::default(),
            is_block_synced: Default::default(),
            advance_with_sync_blocks_lock: Default::default(),
        }
    }

    /// Returns the block sync mode.
    #[inline]
    pub const fn mode(&self) -> BlockSyncMode {
        self.mode
    }

    /// Returns `true` if the node is synced up to the latest block (within the given tolerance).
    #[inline]
    pub fn is_block_synced(&self) -> bool {
        self.is_block_synced.load(Ordering::SeqCst)
    }
}

#[allow(dead_code)]
impl<N: Network> BlockSync<N> {
    /// Returns the latest block height of the given peer IP.
    fn get_peer_height(&self, peer_ip: &SocketAddr) -> Option<u32> {
        self.locators.read().get(peer_ip).map(|locators| locators.latest_locator_height())
    }

    // /// Returns a map of peer height to peer IPs.
    // /// e.g. `{{ 127 => \[peer1, peer2\], 128 => \[peer3\], 135 => \[peer4, peer5\] }}`
    // fn get_peer_heights(&self) -> BTreeMap<u32, Vec<SocketAddr>> {
    //     self.locators.read().iter().map(|(peer_ip, locators)| (locators.latest_locator_height(), *peer_ip)).fold(
    //         Default::default(),
    //         |mut map, (height, peer_ip)| {
    //             map.entry(height).or_default().push(peer_ip);
    //             map
    //         },
    //     )
    // }

    // /// Returns the list of peers with their heights, sorted by height (descending).
    // fn get_peers_by_height(&self) -> Vec<(SocketAddr, u32)> {
    //     self.locators
    //         .read()
    //         .iter()
    //         .map(|(peer_ip, locators)| (*peer_ip, locators.latest_locator_height()))
    //         .sorted_by(|(_, a), (_, b)| b.cmp(a))
    //         .collect()
    // }

    /// Returns the common ancestor for the given peer pair, if it exists.
    fn get_common_ancestor(&self, peer_a: SocketAddr, peer_b: SocketAddr) -> Option<u32> {
        self.common_ancestors.read().get(&PeerPair(peer_a, peer_b)).copied()
    }

    /// Returns the block request for the given height, if it exists.
    fn get_block_request(&self, height: u32) -> Option<SyncRequest<N>> {
        self.requests.read().get(&height).cloned()
    }

    /// Returns the timestamp of the last time the block was requested, if it exists.
    fn get_block_request_timestamp(&self, height: u32) -> Option<Instant> {
        self.request_timestamps.read().get(&height).copied()
    }
}

impl<N: Network> BlockSync<N> {
    /// Returns the block locators.
    #[inline]
    pub fn get_block_locators(&self) -> Result<BlockLocators<N>> {
        // Retrieve the latest block height.
        let latest_height = self.canon.latest_block_height();

        // Initialize the recents map.
        let mut recents = IndexMap::with_capacity(NUM_RECENT_BLOCKS);
        // Retrieve the recent block hashes.
        for height in latest_height.saturating_sub((NUM_RECENT_BLOCKS - 1) as u32)..=latest_height {
            recents.insert(height, self.canon.get_block_hash(height)?);
        }

        // Initialize the checkpoints map.
        let mut checkpoints = IndexMap::with_capacity((latest_height / CHECKPOINT_INTERVAL + 1).try_into()?);
        // Retrieve the checkpoint block hashes.
        for height in (0..=latest_height).step_by(CHECKPOINT_INTERVAL as usize) {
            checkpoints.insert(height, self.canon.get_block_hash(height)?);
        }

        // Construct the block locators.
        BlockLocators::new(recents, checkpoints)
    }

    /// Performs one iteration of the block sync.
    #[inline]
    pub async fn try_block_sync<C: CommunicationService>(&self, communication: &C) {
        // Prepare the block requests, if any.
        // In the process, we update the state of `is_block_synced` for the sync module.
        let block_requests = self.prepare_block_requests();
        trace!("Prepared {} block requests", block_requests.len());

        // If there are no block requests, but there are pending block responses in the sync pool,
        // then try to advance the ledger using these pending block responses.
        // Note: This condition is guarded by `mode.is_router()` because validators sync blocks
        // using another code path that updates both `storage` and `ledger` when advancing blocks.
        if block_requests.is_empty() && !self.responses.read().is_empty() && self.mode.is_router() {
            // Retrieve the latest block height.
            let current_height = self.canon.latest_block_height();
            // Try to advance the ledger with the sync pool.
            trace!("No block requests to send - try advancing with block responses (at block {current_height})");
            self.try_advancing_with_block_responses(current_height);
            // Return early.
            return;
        }

        // Process the block requests.
        'outer: for requests in block_requests.chunks(DataBlocks::<N>::MAXIMUM_NUMBER_OF_BLOCKS as usize) {
            // Get the start height and the sync ips for the first block request.
            // This unwrap is safe because we know that the chunk is non-empty.
            let (start_height, (_, _, sync_ips)) = match requests.first() {
                Some(request) => request.clone(),
                None => {
                    warn!("Block sync failed - no block requests");
                    // Break out of the loop.
                    break;
                }
            };

            // Calculate the end height.
            let end_height = start_height.saturating_add(requests.len() as u32);

            for (height, (hash, previous_hash, _)) in requests.iter() {
                // TODO (raychu86): Fix this. We are using the sync peers selected for the first block request and disregarding the ones generated for the other block requests.
                // Insert the block request into the sync pool.
                if let Err(error) = self.insert_block_request(*height, (*hash, *previous_hash, sync_ips.clone())) {
                    warn!("Block sync failed - {error}");
                    // Break out of the loop.
                    break 'outer;
                }
            }

            /* Send the block request to the peers */

            // Construct the message.
            let message = C::prepare_block_request(start_height, end_height);

            // TODO (raychu86): These sync IPs are not correct. Because we have randomly selected sync IPS for each block request. This needs to be fixed on another level.
            // Send the message to the peers.
            for sync_ip in sync_ips {
                let sender = communication.send(sync_ip, message.clone()).await;
                // If the send fails for any peer, remove the block requests from the sync pool.
                if sender.is_none() {
                    warn!("Failed to send block request to peer '{sync_ip}'");
                    // Remove the entire block request from the sync pool.
                    for height in start_height..end_height {
                        self.remove_block_request(height);
                    }
                    // Break out of the loop.
                    break 'outer;
                }
            }

            // Sleep for 10 milliseconds to avoid triggering spam detection.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// Processes the block response from the given peer IP.
    #[inline]
    pub fn process_block_response(&self, peer_ip: SocketAddr, blocks: Vec<Block<N>>) -> Result<()> {
        // Insert the candidate blocks into the sync pool.
        for block in blocks {
            if let Err(error) = self.insert_block_response(peer_ip, block) {
                bail!("{error}");
            }
        }
        Ok(())
    }

    /// Returns the next block to process, if one is ready.
    #[inline]
    pub fn process_next_block(&self, next_height: u32) -> Option<Block<N>> {
        // Try to advance the ledger with a block from the sync pool.
        self.remove_block_response(next_height)
    }

    /// Attempts to advance with blocks from the sync pool.
    #[inline]
    pub fn advance_with_sync_blocks(&self, peer_ip: SocketAddr, blocks: Vec<Block<N>>) -> Result<()> {
        // Process the block response from the given peer IP.
        self.process_block_response(peer_ip, blocks)?;

        // Acquire the lock to ensure this function is called only once at a time.
        // If the lock is already acquired, return early.
        let Some(_lock) = self.advance_with_sync_blocks_lock.try_lock() else {
            trace!("Skipping a call to advance_with_sync_blocks() as it is already in progress");
            return Ok(());
        };

        // Retrieve the latest block height.
        let current_height = self.canon.latest_block_height();
        // Try to advance the ledger with the sync pool.
        self.try_advancing_with_block_responses(current_height);
        Ok(())
    }

    /// Handles the block responses from the sync pool.
    fn try_advancing_with_block_responses(&self, mut current_height: u32) {
        while let Some(block) = self.remove_block_response(current_height + 1) {
            // Ensure the block height matches.
            if block.height() != current_height + 1 {
                warn!("Block height mismatch: expected {}, found {}", current_height + 1, block.height());
                break;
            }
            // Check the next block.
            if let Err(error) = self.canon.check_next_block(&block) {
                warn!("The next block ({}) is invalid - {error}", block.height());
                break;
            }
            // Attempt to advance to the next block.
            if let Err(error) = self.canon.advance_to_next_block(&block) {
                warn!("{error}");
                break;
            }
            // Update the latest height.
            current_height = self.canon.latest_block_height();
        }
    }
}

impl<N: Network> BlockSync<N> {
    /// Returns the sync peers with their latest heights, and their minimum common ancestor, if the node can sync.
    /// This function returns peers that are consistent with each other, and have a block height
    /// that is greater than the canon height of this node.
    pub fn find_sync_peers(&self) -> Option<(IndexMap<SocketAddr, u32>, u32)> {
        if let Some((sync_peers, min_common_ancestor)) = self.find_sync_peers_inner() {
            // Map the locators into the latest height.
            let sync_peers =
                sync_peers.into_iter().map(|(ip, locators)| (ip, locators.latest_locator_height())).collect();
            // Return the sync peers and their minimum common ancestor.
            Some((sync_peers, min_common_ancestor))
        } else {
            None
        }
    }

    /// Updates the block locators and common ancestors for the given peer IP.
    /// This function checks that the given block locators are well-formed, however it does **not** check
    /// that the block locators are consistent the peer's previous block locators or other peers' block locators.
    pub fn update_peer_locators(&self, peer_ip: SocketAddr, locators: BlockLocators<N>) -> Result<()> {
        // If the locators match the existing locators for the peer, return early.
        if self.locators.read().get(&peer_ip) == Some(&locators) {
            return Ok(());
        }

        // Ensure the given block locators are well-formed.
        locators.ensure_is_valid()?;
        // Update the locators entry for the given peer IP.
        self.locators.write().insert(peer_ip, locators.clone());

        // Compute the common ancestor with this node.
        let mut ancestor = 0;
        for (height, hash) in locators.clone().into_iter() {
            if let Ok(canon_hash) = self.canon.get_block_hash(height) {
                match canon_hash == hash {
                    true => ancestor = height,
                    false => break, // fork
                }
            }
        }
        // Update the common ancestor entry for this node.
        self.common_ancestors.write().insert(PeerPair(DUMMY_SELF_IP, peer_ip), ancestor);

        // Compute the common ancestor with every other peer.
        let mut common_ancestors = self.common_ancestors.write();
        for (other_ip, other_locators) in self.locators.read().iter() {
            // Skip if the other peer is the given peer.
            if other_ip == &peer_ip {
                continue;
            }
            // Compute the common ancestor with the other peer.
            let mut ancestor = 0;
            for (height, hash) in other_locators.clone().into_iter() {
                if let Some(expected_hash) = locators.get_hash(height) {
                    match expected_hash == hash {
                        true => ancestor = height,
                        false => break, // fork
                    }
                }
            }
            common_ancestors.insert(PeerPair(peer_ip, *other_ip), ancestor);
        }

        Ok(())
    }

    /// TODO (howardwu): Remove the `common_ancestor` entry. But check that this is safe
    ///  (that we don't rely upon it for safety when we re-connect with the same peer).
    /// Removes the peer from the sync pool, if they exist.
    pub fn remove_peer(&self, peer_ip: &SocketAddr) {
        // Remove the locators entry for the given peer IP.
        self.locators.write().swap_remove(peer_ip);
        // Remove all block requests to the peer.
        self.remove_block_requests_to_peer(peer_ip);
        // Remove the timeouts for the peer.
        self.request_timeouts.write().swap_remove(peer_ip);
    }
}

impl<N: Network> BlockSync<N> {
    /// Returns a list of block requests, if the node needs to sync.
    fn prepare_block_requests(&self) -> Vec<(u32, SyncRequest<N>)> {
        // Remove timed out block requests.
        self.remove_timed_out_block_requests();
        // Prepare the block requests.
        if let Some((sync_peers, min_common_ancestor)) = self.find_sync_peers_inner() {
            // Retrieve the highest block height.
            let greatest_peer_height = sync_peers.values().map(|l| l.latest_locator_height()).max().unwrap_or(0);
            // Update the state of `is_block_synced` for the sync module.
            self.update_is_block_synced(greatest_peer_height, MAX_BLOCKS_BEHIND);
            // Return the list of block requests.
            self.construct_requests(sync_peers, min_common_ancestor, &mut rand::thread_rng())
        } else {
            // Update the state of `is_block_synced` for the sync module.
            self.update_is_block_synced(0, MAX_BLOCKS_BEHIND);
            // Return an empty list of block requests.
            Vec::new()
        }
    }

    /// Updates the state of `is_block_synced` for the sync module.
    fn update_is_block_synced(&self, greatest_peer_height: u32, max_blocks_behind: u32) {
        // Retrieve the latest block height.
        let canon_height = self.canon.latest_block_height();
        trace!(
            "Updating is_block_synced: greatest_peer_height = {greatest_peer_height}, canon_height = {canon_height}"
        );
        // Compute the number of blocks that we are behind by.
        let num_blocks_behind = greatest_peer_height.saturating_sub(canon_height);
        // Determine if the primary is synced.
        let is_synced = num_blocks_behind <= max_blocks_behind;
        // Update the sync status.
        self.is_block_synced.store(is_synced, Ordering::SeqCst);
    }

    /// Inserts a block request for the given height.
    fn insert_block_request(&self, height: u32, (hash, previous_hash, sync_ips): SyncRequest<N>) -> Result<()> {
        // Ensure the block request does not already exist.
        self.check_block_request(height)?;
        // Ensure the sync IPs are not empty.
        ensure!(!sync_ips.is_empty(), "Cannot insert a block request with no sync IPs");
        // Insert the block request.
        self.requests.write().insert(height, (hash, previous_hash, sync_ips));
        // Insert the request timestamp.
        self.request_timestamps.write().insert(height, Instant::now());
        Ok(())
    }

    /// Inserts the given block response, after checking that the request exists and the response is well-formed.
    /// On success, this function removes the peer IP from the requests map.
    /// On failure, this function removes all block requests from the given peer IP.
    fn insert_block_response(&self, peer_ip: SocketAddr, block: Block<N>) -> Result<()> {
        // Retrieve the block height.
        let height = block.height();

        // Ensure the block (response) from the peer is well-formed. On failure, remove all block requests to the peer.
        if let Err(error) = self.check_block_response(&peer_ip, &block) {
            // Remove all block requests to the peer.
            self.remove_block_requests_to_peer(&peer_ip);
            return Err(error);
        }

        // Remove the peer IP from the request entry.
        if let Some((_, _, sync_ips)) = self.requests.write().get_mut(&height) {
            sync_ips.swap_remove(&peer_ip);
        }

        // Acquire the write lock on the responses map.
        let mut responses = self.responses.write();
        // Insert the candidate block into the responses map.
        if let Some(existing_block) = responses.insert(height, block.clone()) {
            // If the candidate block was already present, ensure it is the same block.
            if block != existing_block {
                // Remove the candidate block.
                responses.remove(&height);
                // Drop the write lock on the responses map.
                drop(responses);
                // Remove all block requests to the peer.
                self.remove_block_requests_to_peer(&peer_ip);
                bail!("Candidate block {height} from '{peer_ip}' is malformed");
            }
        }

        Ok(())
    }

    /// Checks that a block request for the given height does not already exist.
    fn check_block_request(&self, height: u32) -> Result<()> {
        // Ensure the block height is not already canon.
        if self.canon.contains_block_height(height) {
            bail!("Failed to add block request, as block {height} exists in the canonical ledger");
        }
        // Ensure the block height is not already requested.
        if self.requests.read().contains_key(&height) {
            bail!("Failed to add block request, as block {height} exists in the requests map");
        }
        // Ensure the block height is not already responded.
        if self.responses.read().contains_key(&height) {
            bail!("Failed to add block request, as block {height} exists in the responses map");
        }
        // Ensure the block height is not already requested.
        if self.request_timestamps.read().contains_key(&height) {
            bail!("Failed to add block request, as block {height} exists in the timestamps map");
        }
        Ok(())
    }

    /// Checks the given block (response) from a peer against the expected block hash and previous block hash.
    fn check_block_response(&self, peer_ip: &SocketAddr, block: &Block<N>) -> Result<()> {
        // Retrieve the block height.
        let height = block.height();

        // Retrieve the request entry for the candidate block.
        if let Some((expected_hash, expected_previous_hash, sync_ips)) = self.requests.read().get(&height) {
            // Ensure the candidate block hash matches the expected hash.
            if let Some(expected_hash) = expected_hash {
                if block.hash() != *expected_hash {
                    bail!("The block hash for candidate block {height} from '{peer_ip}' is incorrect")
                }
            }
            // Ensure the previous block hash matches if it exists.
            if let Some(expected_previous_hash) = expected_previous_hash {
                if block.previous_hash() != *expected_previous_hash {
                    bail!("The previous block hash in candidate block {height} from '{peer_ip}' is incorrect")
                }
            }
            // Ensure the sync pool requested this block from the given peer.
            if !sync_ips.contains(peer_ip) {
                bail!("The sync pool did not request block {height} from '{peer_ip}'")
            }
            Ok(())
        } else {
            bail!("The sync pool did not request block {height}")
        }
    }

    /// Removes the entire block request for the given height, if it exists.
    fn remove_block_request(&self, height: u32) {
        // Remove the request entry for the given height.
        self.requests.write().remove(&height);
        // Remove the response entry for the given height.
        self.responses.write().remove(&height);
        // Remove the request timestamp entry for the given height.
        self.request_timestamps.write().remove(&height);
    }

    /// Removes and returns the block response for the given height, if the request is complete.
    fn remove_block_response(&self, height: u32) -> Option<Block<N>> {
        // Acquire the requests write lock.
        // Note: This lock must be held across the entire scope, due to asynchronous block responses
        // from multiple peers that may be received concurrently.
        let mut requests = self.requests.write();

        // Determine if the request is complete.
        let is_request_complete = requests.get(&height).map(|(_, _, peer_ips)| peer_ips.is_empty()).unwrap_or(false);

        // If the request is not complete, return early.
        if !is_request_complete {
            return None;
        }
        // Remove the request entry for the given height.
        requests.remove(&height);
        // Remove the request timestamp entry for the given height.
        self.request_timestamps.write().remove(&height);
        // Remove the response entry for the given height.
        self.responses.write().remove(&height)
    }

    /// Removes the block request for the given peer IP, if it exists.
    #[allow(dead_code)]
    fn remove_block_request_to_peer(&self, peer_ip: &SocketAddr, height: u32) {
        let mut can_revoke = self.responses.read().get(&height).is_none();

        // Remove the peer IP from the request entry. If the request entry is now empty,
        // and the response entry for this height is also empty, then remove the request entry altogether.
        if let Some((_, _, sync_ips)) = self.requests.write().get_mut(&height) {
            sync_ips.swap_remove(peer_ip);
            can_revoke &= sync_ips.is_empty();
        }

        if can_revoke {
            self.requests.write().remove(&height);
            self.request_timestamps.write().remove(&height);
        }
    }

    /// Removes all block requests for the given peer IP.
    fn remove_block_requests_to_peer(&self, peer_ip: &SocketAddr) {
        trace!("Block sync is removing all block requests to peer {peer_ip}...");
        // Acquire the write lock on the requests map.
        let mut requests = self.requests.write();
        // Acquire the read lock on the responses map.
        let responses = self.responses.read();

        // Remove the peer IP from the requests map. If any request entry is now empty,
        // and its corresponding response entry is also empty, then remove that request entry altogether.
        requests.retain(|height, (_, _, peer_ips)| {
            peer_ips.swap_remove(peer_ip);

            let retain = !peer_ips.is_empty() || responses.get(height).is_some();
            if !retain {
                self.request_timestamps.write().remove(height);
            }
            retain
        });
    }

    /// Removes block requests that have timed out. This also removes the corresponding block responses,
    /// and adds the timed out sync IPs to a map for tracking. Returns the number of timed out block requests.
    fn remove_timed_out_block_requests(&self) -> usize {
        // Acquire the write lock on the requests map.
        let mut requests = self.requests.write();
        // Acquire the write lock on the responses map.
        let mut responses = self.responses.write();
        // Acquire the write lock on the request timestamps map.
        let mut request_timestamps = self.request_timestamps.write();

        // Retrieve the current time.
        let now = Instant::now();

        // Track each unique peer IP that has timed out.
        let mut timeout_ips = IndexSet::new();
        // Track the number of timed out block requests.
        let mut num_timed_out_block_requests = 0;

        // Remove timed out block requests.
        request_timestamps.retain(|height, timestamp| {
            // Determine if the duration since the request timestamp has exceeded the request timeout.
            let is_time_passed = now.duration_since(*timestamp).as_secs() > BLOCK_REQUEST_TIMEOUT_IN_SECS;
            // Determine if the request is incomplete.
            let is_request_incomplete =
                !requests.get(height).map(|(_, _, peer_ips)| peer_ips.is_empty()).unwrap_or(false);
            // Determine if the request has timed out.
            let is_timeout = is_time_passed && is_request_incomplete;

            // If the request has timed out, then remove it.
            if is_timeout {
                // Remove the request entry for the given height.
                if let Some((_, _, sync_ips)) = requests.remove(height) {
                    // Add each sync IP to the timeout IPs.
                    timeout_ips.extend(sync_ips);
                }
                // Remove the response entry for the given height.
                responses.remove(height);
                // Increment the number of timed out block requests.
                num_timed_out_block_requests += 1;
            }
            // Retain if this is not a timeout.
            !is_timeout
        });

        // If there are timeout IPs, then add them to the request timeouts map.
        if !timeout_ips.is_empty() {
            // Acquire the write lock on the request timeouts map.
            let mut request_timeouts = self.request_timeouts.write();
            // Add each timeout IP to the request timeouts map.
            for timeout_ip in timeout_ips {
                request_timeouts.entry(timeout_ip).or_default().push(now);
            }
        }

        num_timed_out_block_requests
    }

    /// Returns the sync peers and their minimum common ancestor, if the node needs to sync.
    fn find_sync_peers_inner(&self) -> Option<(IndexMap<SocketAddr, BlockLocators<N>>, u32)> {
        // Retrieve the latest canon height.
        let latest_canon_height = self.canon.latest_block_height();

        // Compute the timeout frequency of each peer.
        let timeouts = self
            .request_timeouts
            .read()
            .iter()
            .map(|(peer_ip, timestamps)| (*peer_ip, timestamps.len()))
            .collect::<IndexMap<_, _>>();

        // Pick a set of peers above the latest canon height, and include their locators.
        let candidate_locators: IndexMap<_, _> = self
            .locators
            .read()
            .iter()
            .filter(|(_, locators)| locators.latest_locator_height() > latest_canon_height)
            .filter(|(ip, _)| timeouts.get(*ip).map(|count| *count < MAX_BLOCK_REQUEST_TIMEOUTS).unwrap_or(true))
            .sorted_by(|(_, a), (_, b)| b.latest_locator_height().cmp(&a.latest_locator_height()))
            .take(NUM_SYNC_CANDIDATE_PEERS)
            .map(|(peer_ip, locators)| (*peer_ip, locators.clone()))
            .collect();

        // Case 0: If there are no candidate peers, return `None`.
        if candidate_locators.is_empty() {
            return None;
        }

        // TODO (howardwu): Change this to the highest cumulative weight for Phase 3.
        // Case 1: If all of the candidate peers share a common ancestor below the latest canon height,
        // then pick the peer with the highest height, and find peers (up to extra redundancy) with
        // a common ancestor above the block request range. Set the end height to their common ancestor.

        // Determine the threshold number of peers to sync from.
        let threshold_to_request = core::cmp::min(candidate_locators.len(), REDUNDANCY_FACTOR);

        let mut min_common_ancestor = 0;
        let mut sync_peers = IndexMap::new();

        // Breaks the loop when the first threshold number of peers are found, biasing for the peer with the highest height
        // and a cohort of peers who share a common ancestor above this node's latest canon height.
        for (i, (peer_ip, peer_locators)) in candidate_locators.iter().enumerate() {
            // As the previous iteration did not `break`, reset the sync peers.
            sync_peers.clear();

            // Set the minimum common ancestor.
            min_common_ancestor = peer_locators.latest_locator_height();
            // Add the peer to the sync peers.
            sync_peers.insert(*peer_ip, peer_locators.clone());

            for (other_ip, other_locators) in candidate_locators.iter().skip(i + 1) {
                // Check if these two peers have a common ancestor above the latest canon height.
                if let Some(common_ancestor) = self.common_ancestors.read().get(&PeerPair(*peer_ip, *other_ip)) {
                    if *common_ancestor > latest_canon_height {
                        // If so, then check that their block locators are consistent.
                        if peer_locators.is_consistent_with(other_locators) {
                            // If their common ancestor is less than the minimum common ancestor, then update it.
                            if *common_ancestor < min_common_ancestor {
                                min_common_ancestor = *common_ancestor;
                            }
                            // Add the other peer to the list of sync peers.
                            sync_peers.insert(*other_ip, other_locators.clone());
                        }
                    }
                }
            }

            // If we have enough sync peers above the latest canon height, then break the loop.
            if min_common_ancestor > latest_canon_height && sync_peers.len() >= threshold_to_request {
                break;
            }
        }

        // If there is not enough peers with a minimum common ancestor above the latest canon height, then return early.
        if min_common_ancestor <= latest_canon_height || sync_peers.len() < threshold_to_request {
            return None;
        }

        Some((sync_peers, min_common_ancestor))
    }

    /// Given the sync peers and their minimum common ancestor, return a list of block requests.
    fn construct_requests<R: Rng + CryptoRng>(
        &self,
        sync_peers: IndexMap<SocketAddr, BlockLocators<N>>,
        min_common_ancestor: u32,
        rng: &mut R,
    ) -> Vec<(u32, SyncRequest<N>)> {
        // Retrieve the latest canon height.
        let latest_canon_height = self.canon.latest_block_height();

        // If the minimum common ancestor is at or below the latest canon height, then return early.
        if min_common_ancestor <= latest_canon_height {
            return vec![];
        }

        // Compute the start height for the block request.
        let start_height = latest_canon_height + 1;
        // Compute the end height for the block request.
        let end_height = (min_common_ancestor + 1).min(start_height + MAX_BLOCK_REQUESTS as u32);

        let mut requests = Vec::with_capacity((start_height..end_height).len());

        for height in start_height..end_height {
            // Ensure the current height is not canonized or already requested.
            if self.check_block_request(height).is_err() {
                continue;
            }

            // Construct the block request.
            let (hash, previous_hash, num_sync_ips, is_honest) = construct_request(height, &sync_peers);

            // Handle the dishonest case.
            if !is_honest {
                // TODO (howardwu): Consider performing an integrity check on peers (to disconnect).
                warn!("Detected dishonest peer(s) when preparing block request");
                // If there are not enough peers in the dishonest case, then return early.
                if sync_peers.len() < num_sync_ips {
                    break;
                }
            }

            // Pick the sync peers.
            let sync_ips = sync_peers.keys().copied().choose_multiple(rng, num_sync_ips);

            // Append the request.
            requests.push((height, (hash, previous_hash, sync_ips.into_iter().collect())));
        }

        requests
    }
}

/// If any peer is detected to be dishonest in this function, it will not set the hash or previous hash,
/// in order to allow the caller to determine what to do.
fn construct_request<N: Network>(
    height: u32,
    sync_peers: &IndexMap<SocketAddr, BlockLocators<N>>,
) -> (Option<N::BlockHash>, Option<N::BlockHash>, usize, bool) {
    let mut hash = None;
    let mut hash_redundancy: usize = 0;
    let mut previous_hash = None;
    let mut is_honest = true;

    for peer_locators in sync_peers.values() {
        if let Some(candidate_hash) = peer_locators.get_hash(height) {
            match hash {
                // Increment the redundancy count if the hash matches.
                Some(hash) if hash == candidate_hash => hash_redundancy += 1,
                // Some peer is dishonest.
                Some(_) => {
                    hash = None;
                    hash_redundancy = 0;
                    previous_hash = None;
                    is_honest = false;
                    break;
                }
                // Set the hash if it is not set.
                None => {
                    hash = Some(candidate_hash);
                    hash_redundancy = 1;
                }
            }
        }
        if let Some(candidate_previous_hash) = peer_locators.get_hash(height.saturating_sub(1)) {
            match previous_hash {
                // Increment the redundancy count if the previous hash matches.
                Some(previous_hash) if previous_hash == candidate_previous_hash => (),
                // Some peer is dishonest.
                Some(_) => {
                    hash = None;
                    hash_redundancy = 0;
                    previous_hash = None;
                    is_honest = false;
                    break;
                }
                // Set the previous hash if it is not set.
                None => previous_hash = Some(candidate_previous_hash),
            }
        }
    }

    // Note that we intentionally do not just pick the peers that have the hash we have chosen,
    // to give stronger confidence that we are syncing during times when the network is consistent/stable.
    let num_sync_ips = {
        // Extra redundant peers - as the block hash was dishonest.
        if !is_honest {
            // Choose up to the extra redundancy factor in sync peers.
            EXTRA_REDUNDANCY_FACTOR
        }
        // No redundant peers - as we have redundancy on the block hash.
        else if hash.is_some() && hash_redundancy >= REDUNDANCY_FACTOR {
            // Choose one sync peer.
            1
        }
        // Redundant peers - as we do not have redundancy on the block hash.
        else {
            // Choose up to the redundancy factor in sync peers.
            REDUNDANCY_FACTOR
        }
    };

    (hash, previous_hash, num_sync_ips, is_honest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::locators::{
        test_helpers::{sample_block_locators, sample_block_locators_with_fork},
        CHECKPOINT_INTERVAL,
        NUM_RECENT_BLOCKS,
    };
    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkvm::prelude::{Field, TestRng};

    use indexmap::indexset;
    use snarkvm::ledger::committee::Committee;
    use std::net::{IpAddr, Ipv4Addr};

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    /// Returns the peer IP for the sync pool.
    fn sample_peer_ip(id: u16) -> SocketAddr {
        assert_ne!(id, 0, "The peer ID must not be 0 (reserved for local IP in testing)");
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), id)
    }

    /// Returns a sample committee.
    fn sample_committee() -> Committee<CurrentNetwork> {
        let rng = &mut TestRng::default();
        snarkvm::ledger::committee::test_helpers::sample_committee(rng)
    }

    /// Returns the ledger service, initialized to the given height.
    fn sample_ledger_service(height: u32) -> MockLedgerService<CurrentNetwork> {
        MockLedgerService::new_at_height(sample_committee(), height)
    }

    /// Returns the sync pool, with the canonical ledger initialized to the given height.
    fn sample_sync_at_height(height: u32) -> BlockSync<CurrentNetwork> {
        BlockSync::<CurrentNetwork>::new(BlockSyncMode::Router, Arc::new(sample_ledger_service(height)))
    }

    /// Checks that the sync pool (starting at genesis) returns the correct requests.
    fn check_prepare_block_requests(
        sync: BlockSync<CurrentNetwork>,
        min_common_ancestor: u32,
        peers: IndexSet<SocketAddr>,
    ) {
        // Check test assumptions are met.
        assert_eq!(sync.canon.latest_block_height(), 0, "This test assumes the sync pool is at genesis");

        // Determine the number of peers within range of this sync pool.
        let num_peers_within_recent_range_of_canon = {
            // If no peers are within range, then set to 0.
            if min_common_ancestor >= NUM_RECENT_BLOCKS as u32 {
                0
            }
            // Otherwise, manually check the number of peers within range.
            else {
                peers.iter().filter(|peer_ip| sync.get_peer_height(peer_ip).unwrap() < NUM_RECENT_BLOCKS as u32).count()
            }
        };

        // Prepare the block requests.
        let requests = sync.prepare_block_requests();

        // If there are no peers, then there should be no requests.
        if peers.is_empty() {
            assert!(requests.is_empty());
            return;
        }

        // Otherwise, there should be requests.
        let expected_num_requests = core::cmp::min(min_common_ancestor as usize, MAX_BLOCK_REQUESTS);
        assert_eq!(requests.len(), expected_num_requests);

        for (idx, (height, (hash, previous_hash, sync_ips))) in requests.into_iter().enumerate() {
            assert_eq!(height, 1 + idx as u32);
            assert_eq!(hash, Some((Field::<CurrentNetwork>::from_u32(height)).into()));
            assert_eq!(previous_hash, Some((Field::<CurrentNetwork>::from_u32(height - 1)).into()));

            if num_peers_within_recent_range_of_canon >= REDUNDANCY_FACTOR {
                assert_eq!(sync_ips.len(), 1);
            } else {
                assert_eq!(sync_ips.len(), num_peers_within_recent_range_of_canon);
                assert_eq!(sync_ips, peers);
            }
        }
    }

    #[test]
    fn test_latest_block_height() {
        for height in 0..100_002u32 {
            let sync = sample_sync_at_height(height);
            assert_eq!(sync.canon.latest_block_height(), height);
        }
    }

    #[test]
    fn test_get_block_height() {
        for height in 0..100_002u32 {
            let sync = sample_sync_at_height(height);
            assert_eq!(sync.canon.get_block_height(&(Field::<CurrentNetwork>::from_u32(0)).into()).unwrap(), 0);
            assert_eq!(
                sync.canon.get_block_height(&(Field::<CurrentNetwork>::from_u32(height)).into()).unwrap(),
                height
            );
        }
    }

    #[test]
    fn test_get_block_hash() {
        for height in 0..100_002u32 {
            let sync = sample_sync_at_height(height);
            assert_eq!(sync.canon.get_block_hash(0).unwrap(), (Field::<CurrentNetwork>::from_u32(0)).into());
            assert_eq!(sync.canon.get_block_hash(height).unwrap(), (Field::<CurrentNetwork>::from_u32(height)).into());
        }
    }

    #[test]
    fn test_prepare_block_requests() {
        for num_peers in 0..111 {
            println!("Testing with {num_peers} peers");

            let sync = sample_sync_at_height(0);

            let mut peers = indexset![];

            for peer_id in 1..=num_peers {
                // Add a peer.
                sync.update_peer_locators(sample_peer_ip(peer_id), sample_block_locators(10)).unwrap();
                // Add the peer to the set of peers.
                peers.insert(sample_peer_ip(peer_id));
            }

            // If all peers are ahead, then requests should be prepared.
            check_prepare_block_requests(sync, 10, peers);
        }
    }

    #[test]
    fn test_prepare_block_requests_with_leading_fork_at_11() {
        let sync = sample_sync_at_height(0);

        // Intuitively, peer 1's fork is above peer 2 and peer 3's height.
        // So from peer 2 and peer 3's perspective, they don't even realize that peer 1 is on a fork.
        // Thus, you can sync up to block 10 from any of the 3 peers.

        // When there are NUM_REDUNDANCY peers ahead, and 1 peer is on a leading fork at 11,
        // then the sync pool should request blocks 1..=10 from the NUM_REDUNDANCY peers.
        // This is safe because the leading fork is at 11, and the sync pool is at 0,
        // so all candidate peers are at least 10 blocks ahead of the sync pool.

        // Add a peer (fork).
        let peer_1 = sample_peer_ip(1);
        sync.update_peer_locators(peer_1, sample_block_locators_with_fork(20, 11)).unwrap();

        // Add a peer.
        let peer_2 = sample_peer_ip(2);
        sync.update_peer_locators(peer_2, sample_block_locators(10)).unwrap();

        // Add a peer.
        let peer_3 = sample_peer_ip(3);
        sync.update_peer_locators(peer_3, sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests();
        assert_eq!(requests.len(), 10);

        // Check the requests.
        for (idx, (height, (hash, previous_hash, sync_ips))) in requests.into_iter().enumerate() {
            assert_eq!(height, 1 + idx as u32);
            assert_eq!(hash, Some((Field::<CurrentNetwork>::from_u32(height)).into()));
            assert_eq!(previous_hash, Some((Field::<CurrentNetwork>::from_u32(height - 1)).into()));
            assert_eq!(sync_ips.len(), 1); // Only 1 needed since we have redundancy factor on this (recent locator) hash.
        }
    }

    #[test]
    fn test_prepare_block_requests_with_leading_fork_at_10() {
        let sync = sample_sync_at_height(0);

        // Intuitively, peer 1's fork is at peer 2 and peer 3's height.
        // So from peer 2 and peer 3's perspective, they recognize that peer 1 has forked.
        // Thus, you don't have NUM_REDUNDANCY peers to sync to block 10.
        //
        // Now, while you could in theory sync up to block 9 from any of the 3 peers,
        // we choose not to do this as either side is likely to disconnect from us,
        // and we would rather wait for enough redundant peers before syncing.

        // When there are NUM_REDUNDANCY peers ahead, and 1 peer is on a leading fork at 10,
        // then the sync pool should not request blocks as 1 peer conflicts with the other NUM_REDUNDANCY-1 peers.
        // We choose to sync with a cohort of peers that are *consistent* with each other,
        // and prioritize from descending heights (so the highest peer gets priority).

        // Add a peer (fork).
        let peer_1 = sample_peer_ip(1);
        sync.update_peer_locators(peer_1, sample_block_locators_with_fork(20, 10)).unwrap();

        // Add a peer.
        let peer_2 = sample_peer_ip(2);
        sync.update_peer_locators(peer_2, sample_block_locators(10)).unwrap();

        // Add a peer.
        let peer_3 = sample_peer_ip(3);
        sync.update_peer_locators(peer_3, sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests();
        assert_eq!(requests.len(), 0);

        // When there are NUM_REDUNDANCY+1 peers ahead, and 1 is on a fork, then there should be block requests.

        // Add a peer.
        let peer_4 = sample_peer_ip(4);
        sync.update_peer_locators(peer_4, sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests();
        assert_eq!(requests.len(), 10);

        // Check the requests.
        for (idx, (height, (hash, previous_hash, sync_ips))) in requests.into_iter().enumerate() {
            assert_eq!(height, 1 + idx as u32);
            assert_eq!(hash, Some((Field::<CurrentNetwork>::from_u32(height)).into()));
            assert_eq!(previous_hash, Some((Field::<CurrentNetwork>::from_u32(height - 1)).into()));
            assert_eq!(sync_ips.len(), 1); // Only 1 needed since we have redundancy factor on this (recent locator) hash.
            assert_ne!(sync_ips[0], peer_1); // It should never be the forked peer.
        }
    }

    #[test]
    fn test_prepare_block_requests_with_trailing_fork_at_9() {
        let sync = sample_sync_at_height(0);

        // Peer 1 and 2 diverge from peer 3 at block 10. We only sync when there are NUM_REDUNDANCY peers
        // who are *consistent* with each other. So if you add a 4th peer that is consistent with peer 1 and 2,
        // then you should be able to sync up to block 10, thereby biasing away from peer 3.

        // Add a peer (fork).
        let peer_1 = sample_peer_ip(1);
        sync.update_peer_locators(peer_1, sample_block_locators(10)).unwrap();

        // Add a peer.
        let peer_2 = sample_peer_ip(2);
        sync.update_peer_locators(peer_2, sample_block_locators(10)).unwrap();

        // Add a peer.
        let peer_3 = sample_peer_ip(3);
        sync.update_peer_locators(peer_3, sample_block_locators_with_fork(20, 10)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests();
        assert_eq!(requests.len(), 0);

        // When there are NUM_REDUNDANCY+1 peers ahead, and peer 3 is on a fork, then there should be block requests.

        // Add a peer.
        let peer_4 = sample_peer_ip(4);
        sync.update_peer_locators(peer_4, sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests();
        assert_eq!(requests.len(), 10);

        // Check the requests.
        for (idx, (height, (hash, previous_hash, sync_ips))) in requests.into_iter().enumerate() {
            assert_eq!(height, 1 + idx as u32);
            assert_eq!(hash, Some((Field::<CurrentNetwork>::from_u32(height)).into()));
            assert_eq!(previous_hash, Some((Field::<CurrentNetwork>::from_u32(height - 1)).into()));
            assert_eq!(sync_ips.len(), 1); // Only 1 needed since we have redundancy factor on this (recent locator) hash.
            assert_ne!(sync_ips[0], peer_3); // It should never be the forked peer.
        }
    }

    #[test]
    fn test_insert_block_requests() {
        let sync = sample_sync_at_height(0);

        // Add a peer.
        sync.update_peer_locators(sample_peer_ip(1), sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests();
        assert_eq!(requests.len(), 10);

        for (height, (hash, previous_hash, sync_ips)) in requests.clone() {
            // Insert the block request.
            sync.insert_block_request(height, (hash, previous_hash, sync_ips.clone())).unwrap();
            // Check that the block requests were inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }

        for (height, (hash, previous_hash, sync_ips)) in requests.clone() {
            // Check that the block requests are still inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }

        for (height, (hash, previous_hash, sync_ips)) in requests {
            // Ensure that the block requests cannot be inserted twice.
            sync.insert_block_request(height, (hash, previous_hash, sync_ips.clone())).unwrap_err();
            // Check that the block requests are still inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }
    }

    #[test]
    fn test_insert_block_requests_fails() {
        let sync = sample_sync_at_height(9);

        // Add a peer.
        sync.update_peer_locators(sample_peer_ip(1), sample_block_locators(10)).unwrap();

        // Inserting a block height that is already canonized should fail.
        sync.insert_block_request(9, (None, None, indexset![sample_peer_ip(1)])).unwrap_err();
        // Inserting a block height that is not canonized should succeed.
        sync.insert_block_request(10, (None, None, indexset![sample_peer_ip(1)])).unwrap();
    }

    #[test]
    fn test_update_peer_locators() {
        let sync = sample_sync_at_height(0);

        // Test 2 peers.
        let peer1_ip = sample_peer_ip(1);
        for peer1_height in 0..500u32 {
            sync.update_peer_locators(peer1_ip, sample_block_locators(peer1_height)).unwrap();
            assert_eq!(sync.get_peer_height(&peer1_ip), Some(peer1_height));

            let peer2_ip = sample_peer_ip(2);
            for peer2_height in 0..500u32 {
                println!("Testing peer 1 height at {peer1_height} and peer 2 height at {peer2_height}");

                sync.update_peer_locators(peer2_ip, sample_block_locators(peer2_height)).unwrap();
                assert_eq!(sync.get_peer_height(&peer2_ip), Some(peer2_height));

                // Compute the distance between the peers.
                let distance =
                    if peer1_height > peer2_height { peer1_height - peer2_height } else { peer2_height - peer1_height };

                // Check the common ancestor.
                if distance < NUM_RECENT_BLOCKS as u32 {
                    let expected_ancestor = core::cmp::min(peer1_height, peer2_height);
                    assert_eq!(sync.get_common_ancestor(peer1_ip, peer2_ip), Some(expected_ancestor));
                    assert_eq!(sync.get_common_ancestor(peer2_ip, peer1_ip), Some(expected_ancestor));
                } else {
                    let min_checkpoints =
                        core::cmp::min(peer1_height / CHECKPOINT_INTERVAL, peer2_height / CHECKPOINT_INTERVAL);
                    let expected_ancestor = min_checkpoints * CHECKPOINT_INTERVAL;
                    assert_eq!(sync.get_common_ancestor(peer1_ip, peer2_ip), Some(expected_ancestor));
                    assert_eq!(sync.get_common_ancestor(peer2_ip, peer1_ip), Some(expected_ancestor));
                }
            }
        }
    }

    #[test]
    fn test_remove_peer() {
        let sync = sample_sync_at_height(0);

        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, sample_block_locators(100)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(100));

        sync.remove_peer(&peer_ip);
        assert_eq!(sync.get_peer_height(&peer_ip), None);

        sync.update_peer_locators(peer_ip, sample_block_locators(200)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(200));

        sync.remove_peer(&peer_ip);
        assert_eq!(sync.get_peer_height(&peer_ip), None);
    }

    #[test]
    fn test_locators_insert_remove_insert() {
        let sync = sample_sync_at_height(0);

        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, sample_block_locators(100)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(100));

        sync.remove_peer(&peer_ip);
        assert_eq!(sync.get_peer_height(&peer_ip), None);

        sync.update_peer_locators(peer_ip, sample_block_locators(200)).unwrap();
        assert_eq!(sync.get_peer_height(&peer_ip), Some(200));
    }

    #[test]
    fn test_requests_insert_remove_insert() {
        let sync = sample_sync_at_height(0);

        // Add a peer.
        let peer_ip = sample_peer_ip(1);
        sync.update_peer_locators(peer_ip, sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests();
        assert_eq!(requests.len(), 10);

        for (height, (hash, previous_hash, sync_ips)) in requests.clone() {
            // Insert the block request.
            sync.insert_block_request(height, (hash, previous_hash, sync_ips.clone())).unwrap();
            // Check that the block requests were inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }

        // Remove the peer.
        sync.remove_peer(&peer_ip);

        for (height, _) in requests {
            // Check that the block requests were removed.
            assert_eq!(sync.get_block_request(height), None);
            assert!(sync.get_block_request_timestamp(height).is_none());
        }

        // As there is no peer, it should not be possible to prepare block requests.
        let requests = sync.prepare_block_requests();
        assert_eq!(requests.len(), 0);

        // Add the peer again.
        sync.update_peer_locators(peer_ip, sample_block_locators(10)).unwrap();

        // Prepare the block requests.
        let requests = sync.prepare_block_requests();
        assert_eq!(requests.len(), 10);

        for (height, (hash, previous_hash, sync_ips)) in requests {
            // Insert the block request.
            sync.insert_block_request(height, (hash, previous_hash, sync_ips.clone())).unwrap();
            // Check that the block requests were inserted.
            assert_eq!(sync.get_block_request(height), Some((hash, previous_hash, sync_ips)));
            assert!(sync.get_block_request_timestamp(height).is_some());
        }
    }

    // TODO: duplicate responses, ensure fails.
}
