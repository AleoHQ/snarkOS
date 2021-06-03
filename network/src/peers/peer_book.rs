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

use crate::{stats, NetworkError, Node, Payload, Peer, PeerEvent, PeerEventData, PeerHandle, PeerStatus};
use futures::Future;
use mpmc_map::MpmcMap;
use rand::prelude::IteratorRandom;
use snarkos_storage::{BlockHeight, Storage};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};
use tokio::{net::TcpStream, sync::mpsc};

///
/// A data structure for storing the history of all peers with this node server.
///
#[derive(Debug)]
pub struct PeerBook {
    disconnected_peers: MpmcMap<SocketAddr, Peer>,
    connected_peers: MpmcMap<SocketAddr, PeerHandle>,
    pending_connections: Arc<AtomicU32>,
    peer_events: mpsc::Sender<PeerEvent>,
}

// to avoid circular reference to peer_events
struct PeerBookRef {
    disconnected_peers: MpmcMap<SocketAddr, Peer>,
    connected_peers: MpmcMap<SocketAddr, PeerHandle>,
    pending_connections: Arc<AtomicU32>,
}

impl PeerBookRef {
    // gets terminated when sender is dropped from PeerBook
    async fn handle_peer_events(self, mut receiver: mpsc::Receiver<PeerEvent>) {
        while let Some(event) = receiver.recv().await {
            match event.data {
                PeerEventData::Connected(handle) => {
                    self.pending_connections.fetch_sub(1, Ordering::SeqCst);
                    if let Some(old_peer) = self.connected_peers.insert(event.address, handle).await {
                        old_peer.disconnect().await;
                    }
                }
                PeerEventData::Disconnect(peer, status) => {
                    self.connected_peers.remove(peer.address).await;
                    self.disconnected_peers.insert(peer.address, peer).await;
                    if status == PeerStatus::Connecting {
                        self.pending_connections.fetch_sub(1, Ordering::SeqCst);
                    }
                    metrics::increment_gauge!(stats::CONNECTIONS_DISCONNECTED, 1.0);
                }
                PeerEventData::FailHandshake => {
                    self.pending_connections.fetch_sub(1, Ordering::SeqCst);
                    metrics::increment_gauge!(stats::CONNECTIONS_DISCONNECTED, 1.0);
                }
            }
        }
    }
}

impl PeerBook {
    pub fn spawn() -> Self {
        let (sender, receiver) = mpsc::channel(256);
        let peers = PeerBook {
            disconnected_peers: Default::default(),
            connected_peers: Default::default(),
            pending_connections: Arc::new(AtomicU32::default()),
            peer_events: sender,
        };
        tokio::spawn(
            PeerBookRef {
                disconnected_peers: peers.disconnected_peers.clone(),
                connected_peers: peers.connected_peers.clone(),
                pending_connections: peers.pending_connections.clone(),
            }
            .handle_peer_events(receiver),
        );

        peers
    }

    pub fn is_connected(&self, address: SocketAddr) -> bool {
        self.connected_peers.contains_key(&address)
    }

    pub fn is_disconnected(&self, address: SocketAddr) -> bool {
        self.disconnected_peers.contains_key(&address)
    }

    pub fn connected_peers(&self) -> Vec<SocketAddr> {
        self.connected_peers.inner().keys().copied().collect()
    }

    pub fn get_active_peer_count(&self) -> u32 {
        self.connected_peers.len() as u32 + self.pending_connections()
    }

    pub fn get_disconnected_peer_count(&self) -> u32 {
        self.disconnected_peers.len() as u32
    }

    pub fn get_peer_handle(&self, address: SocketAddr) -> Option<PeerHandle> {
        self.connected_peers.get(&address)
    }

    pub async fn get_active_peer(&self, address: SocketAddr) -> Option<Peer> {
        self.get_peer_handle(address)?.load().await
    }

    pub fn get_disconnected_peer(&self, address: SocketAddr) -> Option<Peer> {
        self.disconnected_peers.get(&address)
    }

    pub fn disconnected_peers(&self) -> Vec<SocketAddr> {
        self.disconnected_peers.inner().keys().copied().collect()
    }

    async fn take_disconnected_peer(&self, address: SocketAddr) -> Option<Peer> {
        metrics::decrement_gauge!(stats::CONNECTIONS_DISCONNECTED, 1.0);
        self.disconnected_peers.remove(address).await
    }

    pub fn pending_connections(&self) -> u32 {
        self.pending_connections.load(Ordering::SeqCst)
    }

    pub async fn receive_connection<S: Storage>(
        &self,
        node: Node<S>,
        address: SocketAddr,
        stream: TcpStream,
    ) -> Result<(), NetworkError> {
        self.pending_connections.fetch_add(1, Ordering::SeqCst);
        Peer::receive(address, node, stream, self.peer_events.clone());
        Ok(())
    }

    pub async fn get_or_connect<S: Storage>(
        &self,
        node: Node<S>,
        address: SocketAddr,
    ) -> Result<Option<PeerHandle>, NetworkError> {
        if let Some(active_handler) = self.connected_peers.get(&address) {
            Ok(Some(active_handler))
        } else {
            if let Some(mut peer) = self.get_disconnected_peer(address) {
                if peer.judge_offline() {
                    // dont reconnect to bad peers
                    return Ok(None);
                }
            }
            let peer = if let Some(peer) = self.take_disconnected_peer(address).await {
                peer
            } else {
                Peer::new(address, node.config.bootnodes().contains(&address))
            };
            self.pending_connections.fetch_add(1, Ordering::SeqCst);
            peer.connect(node, self.peer_events.clone());
            Ok(None)
        }
    }

    /// concurrently iterates over peers
    async fn for_each_peer<F: Future<Output = ()>, FN: Fn(PeerHandle) -> F>(&self, func: FN) {
        let mut futures = vec![];
        for (_, peer) in self.connected_peers.inner().iter() {
            futures.push(func(peer.clone()));
        }
        futures::future::join_all(futures).await;
    }

    /// concurrently iterates over peers
    async fn map_each_peer<O: Send + Sync, F: Future<Output = Option<O>>, FN: Fn(PeerHandle) -> F>(
        &self,
        func: FN,
    ) -> Vec<O> {
        let mut futures = Vec::with_capacity(self.connected_peers.len());
        for (_, peer) in self.connected_peers.inner().iter() {
            futures.push(func(peer.clone()));
        }
        futures::future::join_all(futures).await.into_iter().flatten().collect()
    }

    pub async fn judgement(&self) {
        self.for_each_peer(move |peer| async move {
            peer.judge().await;
        })
        .await;
    }

    pub async fn broadcast(&self, payload: Payload) {
        self.for_each_peer(move |peer| {
            let payload = payload.clone();
            async move {
                peer.send_payload(payload).await;
            }
        })
        .await;
    }

    pub async fn send_to(&self, address: SocketAddr, payload: Payload) -> Option<()> {
        self.connected_peers.get(&address)?.send_payload(payload).await;
        Some(())
    }

    pub async fn connected_peers_snapshot(&self) -> Vec<Peer> {
        self.map_each_peer(|peer| async move { peer.load().await }).await
    }

    ///
    /// Adds the given address to the disconnected peers in this `PeerBook`.
    ///
    pub async fn add_peer(&self, address: SocketAddr, is_bootnode: bool) {
        if self.connected_peers.contains_key(&address) || self.disconnected_peers.contains_key(&address) {
            return;
        }

        // Add the given address to the map of disconnected peers.
        self.disconnected_peers
            .insert(address, Peer::new(address, is_bootnode))
            .await;

        metrics::increment_gauge!(stats::CONNECTIONS_DISCONNECTED, 1.0);

        debug!("Added {} to the peer book", address);
    }

    ///
    /// Returns the `SocketAddr` of the last seen peer to be used as a sync node, or `None`.
    ///
    pub async fn last_seen(&self) -> Option<SocketAddr> {
        self.connected_peers_snapshot()
            .await
            .into_iter()
            .max_by(|a, b| a.quality.last_seen.cmp(&b.quality.last_seen))
            .map(|x| x.address)
    }

    /// returns (peer, count_total_higher)
    pub async fn random_higher_peer(&self, block_height: BlockHeight) -> Option<(Peer, usize)> {
        let peers = self
            .connected_peers_snapshot()
            .await
            .into_iter()
            .filter(|x| x.quality.block_height > block_height)
            .collect::<Vec<Peer>>();
        let count_total_higher = peers.len();

        Some((peers.into_iter().choose(&mut rand::thread_rng())?, count_total_higher))
    }

    /// Cancels any expected sync block counts from all peers.
    pub async fn cancel_any_unfinished_syncing(&self) {
        self.for_each_peer(move |peer| async move {
            peer.cancel_sync().await;
        })
        .await;
    }
}
