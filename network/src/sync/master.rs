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

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::Duration,
};

use crate::{NetworkError, Node, Payload, Peer};
use futures::{pin_mut, select, FutureExt};
use rand::prelude::SliceRandom;
use snarkos_storage::Storage;
use snarkvm_algorithms::crh::double_sha256;
use snarkvm_objects::{BlockHeader, BlockHeaderHash};
use tokio::{sync::mpsc, time::Instant};

pub enum SyncInbound {
    BlockHashes(SocketAddr, Vec<BlockHeaderHash>),
    Block(SocketAddr, Vec<u8>),
}

pub struct SyncMaster<S: Storage> {
    node: Node<S>,
    incoming: mpsc::Receiver<SyncInbound>,
}

struct SyncBlock {
    address: SocketAddr,
    block: Vec<u8>,
}

impl<S: Storage> SyncMaster<S> {
    pub fn new(node: Node<S>) -> (Self, mpsc::Sender<SyncInbound>) {
        let (sender, receiver) = mpsc::channel(256);
        let new = Self {
            node,
            incoming: receiver,
        };
        (new, sender)
    }

    async fn find_sync_nodes(&mut self) -> Vec<Peer> {
        let our_block_height = self.node.expect_sync().current_block_height();
        let mut interesting_peers = vec![];
        for mut node in self.node.peer_book.connected_peers_snapshot().await {
            let judge = node.judge();
            if !judge && node.quality.block_height > our_block_height + 1 {
                interesting_peers.push(node);
            }
        }
        interesting_peers.sort_by(|x, y| y.quality.block_height.cmp(&x.quality.block_height));

        // trim nodes close to us if any are > 10 blocks ahead
        if let Some(i) = interesting_peers
            .iter()
            .position(|x| x.quality.block_height <= our_block_height + 10)
        {
            interesting_peers.truncate(i + 1);
        }

        info!("found {} interesting peers for sync", interesting_peers.len());
        debug!("sync interesting peers = {:?}", interesting_peers);

        interesting_peers
    }

    async fn block_locator_hashes(&mut self) -> Vec<BlockHeaderHash> {
        match self.node.expect_sync().storage().get_block_locator_hashes().await {
            Ok(block_locator_hashes) => block_locator_hashes,
            _ => {
                error!("Unable to get block locator hashes from storage");
                vec![]
            }
        }
    }

    async fn send_sync_messages(&mut self) -> usize {
        let sync_nodes = self.find_sync_nodes().await;

        info!("requested block information from {} peers", sync_nodes.len());
        let block_locator_hashes = self.block_locator_hashes().await;
        let mut sent = 0usize;
        let mut future_set = vec![];
        for peer in sync_nodes.iter() {
            if let Some(handle) = self.node.peer_book.get_peer_handle(peer.address) {
                let block_locator_hashes = block_locator_hashes.clone();
                sent += 1;
                future_set.push(async move {
                    handle.send_payload(Payload::GetSync(block_locator_hashes)).await;
                });
            }
        }
        futures::future::join_all(future_set).await;
        sent
    }

    async fn receive_messages<F: FnMut(SyncInbound) -> bool>(
        &mut self,
        timeout_sec: u64,
        moving_timeout_sec: u64,
        mut handler: F,
    ) {
        let end = Instant::now() + Duration::from_secs(timeout_sec);
        let mut moving_end = Instant::now() + Duration::from_secs(moving_timeout_sec);
        loop {
            let timeout = tokio::time::sleep_until(end.min(moving_end)).fuse();
            pin_mut!(timeout);
            select! {
                msg = self.incoming.recv().fuse() => {
                    if msg.is_none() {
                        break;
                    }
                    if handler(msg.unwrap()) {
                        break;
                    }
                    moving_end = Instant::now() + Duration::from_secs(moving_timeout_sec);
                },
                _ = timeout => {
                    break;
                }
            }
        }
    }

    async fn receive_sync_hashes(&mut self, max_message_count: usize) -> HashMap<SocketAddr, Vec<BlockHeaderHash>> {
        const TIMEOUT: u64 = 5;
        let mut received_block_hashes = HashMap::new();

        self.receive_messages(TIMEOUT, TIMEOUT, |msg| {
            match msg {
                SyncInbound::BlockHashes(addr, hashes) => {
                    received_block_hashes.insert(addr, hashes);
                }
                SyncInbound::Block(_, _) => {
                    warn!("received sync block prematurely");
                }
            }
            //todo: fail if peer sends > 1 block hash packet
            received_block_hashes.len() >= max_message_count
        })
        .await;

        info!(
            "received {} hashes from {} peers in {} seconds",
            received_block_hashes.values().map(|x| x.len()).sum::<usize>(),
            received_block_hashes.len(),
            TIMEOUT
        );

        received_block_hashes
    }

    async fn receive_sync_blocks(&mut self, block_count: usize) -> Vec<SyncBlock> {
        const TIMEOUT: u64 = 30;
        let mut blocks = vec![];

        self.receive_messages(TIMEOUT, 4, |msg| {
            match msg {
                SyncInbound::BlockHashes(_, _) => {
                    // late, ignored
                }
                SyncInbound::Block(address, block) => {
                    blocks.push(SyncBlock { address, block });
                }
            }
            blocks.len() >= block_count
        })
        .await;

        info!("received {} blocks in {} seconds", blocks.len(), TIMEOUT);

        blocks
    }

    fn order_block_hashes(input: &[(SocketAddr, Vec<BlockHeaderHash>)]) -> Vec<BlockHeaderHash> {
        let mut block_order = vec![];
        let mut seen = HashSet::<&BlockHeaderHash>::new();
        let mut block_index = 0;
        loop {
            let mut found_row = false;
            for (_, hashes) in input {
                if let Some(hash) = hashes.get(block_index) {
                    found_row = true;
                    if seen.contains(&hash) {
                        continue;
                    }
                    seen.insert(hash);
                    block_order.push(hash.clone());
                }
            }
            block_index += 1;
            if !found_row {
                break;
            }
        }
        block_order
    }

    fn block_peer_map(blocks: &[(SocketAddr, Vec<BlockHeaderHash>)]) -> HashMap<BlockHeaderHash, Vec<SocketAddr>> {
        let mut block_peer_map = HashMap::new();
        for (addr, hashes) in blocks {
            for hash in hashes {
                block_peer_map.entry(hash.clone()).or_insert_with(|| vec![]).push(*addr);
            }
        }
        block_peer_map
    }

    fn get_peer_blocks(
        &mut self,
        blocks: &[BlockHeaderHash],
        block_peer_map: &HashMap<BlockHeaderHash, Vec<SocketAddr>>,
    ) -> (
        Vec<SocketAddr>,
        HashMap<BlockHeaderHash, SocketAddr>,
        HashMap<SocketAddr, Vec<BlockHeaderHash>>,
    ) {
        let mut peer_block_requests: HashMap<SocketAddr, Vec<BlockHeaderHash>> = HashMap::new();
        let mut block_peers = HashMap::new();
        for block in blocks {
            let peers = block_peer_map.get(&block);
            if peers.is_none() {
                continue;
            }
            let random_peer = peers.unwrap().choose(&mut rand::thread_rng());
            if random_peer.is_none() {
                continue;
            }
            block_peers.insert(block.clone(), *random_peer.unwrap());
            peer_block_requests
                .entry(random_peer.unwrap().clone())
                .or_insert_with(|| vec![])
                .push(block.clone());
        }
        let addresses: Vec<SocketAddr> = peer_block_requests.keys().copied().collect();
        (addresses, block_peers, peer_block_requests)
    }

    async fn request_blocks(&mut self, peer_block_requests: HashMap<SocketAddr, Vec<BlockHeaderHash>>) -> usize {
        let mut sent = 0usize;

        let mut future_set = vec![];
        for (addr, request) in peer_block_requests {
            if let Some(peer) = self.node.peer_book.get_peer_handle(addr) {
                sent += request.len();
                future_set.push(async move {
                    peer.expecting_sync_blocks(request.len() as u32).await;
                    peer.send_payload(Payload::GetBlocks(request)).await;
                });
            }
        }
        futures::future::join_all(future_set).await;
        sent
    }

    async fn cancel_outstanding_syncs(&mut self, addresses: &[SocketAddr]) {
        let mut future_set = vec![];
        for addr in addresses {
            if let Some(peer) = self.node.peer_book.get_peer_handle(*addr) {
                future_set.push(async move {
                    peer.cancel_sync().await;
                });
            }
        }
        futures::future::join_all(future_set).await;
    }

    pub async fn run(mut self) -> Result<(), NetworkError> {
        let hash_requests_sent = self.send_sync_messages().await;

        if hash_requests_sent == 0 {
            return Ok(());
        }

        let received_block_hashes = self.receive_sync_hashes(hash_requests_sent).await;

        if received_block_hashes.is_empty() {
            return Ok(());
        }

        let blocks = received_block_hashes.into_iter().collect::<Vec<_>>();

        let early_blocks = Self::order_block_hashes(&blocks[..]);
        let early_blocks_count = early_blocks.len();

        let ledger = &self.node.expect_sync().consensus.ledger;
        let block_order: Vec<_> = early_blocks.iter().map(|x| ledger.block_hash_exists(x)).collect();
        let block_order = futures::future::join_all(block_order).await;

        let block_order: Vec<BlockHeaderHash> = early_blocks
            .into_iter()
            .zip(block_order)
            .filter(|(_, present)| !*present)
            .map(|x| x.0)
            .collect();

        info!(
            "requesting {} blocks for sync, received headers for {} known blocks",
            block_order.len(),
            early_blocks_count - block_order.len()
        );
        if block_order.is_empty() {
            return Ok(());
        }

        let block_peer_map = Self::block_peer_map(&blocks[..]);

        let (peer_addresses, block_peers, peer_block_requests) =
            self.get_peer_blocks(&block_order[..], &block_peer_map);

        let sent_block_requests = self.request_blocks(peer_block_requests).await;

        let received_blocks = self.receive_sync_blocks(sent_block_requests).await;

        info!(
            "received {}/{} blocks for sync",
            received_blocks.len(),
            sent_block_requests
        );

        self.cancel_outstanding_syncs(&peer_addresses[..]).await;

        let mut blocks_by_hash = HashMap::new();

        for block in received_blocks {
            let block_header = &block.block[..BlockHeader::size()];
            let hash = BlockHeaderHash(double_sha256(block_header));
            blocks_by_hash.insert(hash, block);
        }

        for (i, hash) in block_order.iter().enumerate() {
            if let Some(block) = blocks_by_hash.remove(hash) {
                self.node
                    .process_received_block(block.address, block.block, false)
                    .await?;
            } else {
                warn!(
                    "did not receive block {}/{} '{}' by deadline for sync from {}",
                    i,
                    block_order.len(),
                    hash,
                    block_peers.get(hash).map(|x| x.to_string()).unwrap_or_default(),
                );
            }
        }

        self.node.finished_syncing_blocks();
        Ok(())
    }
}
