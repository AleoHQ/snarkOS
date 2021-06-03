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

use crate::{message::*, stats, NetworkError, Node};
use snarkos_storage::Storage;

use std::{cmp, net::SocketAddr, time::Duration};

use rand::seq::IteratorRandom;
use tokio::task;

impl<S: Storage> Node<S> {
    /// Obtain a list of addresses of connected peers for this node.
    pub(crate) fn connected_peers(&self) -> Vec<SocketAddr> {
        self.peer_book.connected_peers()
    }
}

impl<S: Storage> Node<S> {
    ///
    /// Broadcasts updates with connected peers and maintains a permitted number of connected peers.
    ///
    pub(crate) async fn update_peers(&self) {
        // Fetch the number of connected and connecting peers.
        let active_peer_count = self.peer_book.get_active_peer_count() as usize;

        info!(
            "Connected to {} peer{}",
            active_peer_count,
            if active_peer_count == 1 { "" } else { "s" }
        );

        // Drop peers whose RTT is too high or have too many failures.
        self.peer_book.judgement().await;
        // give us 100ms to close some negatively judged connections (probably less needed, but we have time)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Attempt to connect to the default bootnodes of the network.
        self.connect_to_bootnodes().await;

        // Attempt to connect to each disconnected peer saved in the peer book.
        if !self.config.is_bootnode() {
            self.connect_to_disconnected_peers().await;
        }

        // Broadcast a `GetPeers` message to request for more peers.
        self.broadcast_getpeers_requests().await;

        let new_active_peer_count = self.peer_book.get_active_peer_count() as usize;
        // Check if this node server is above the permitted number of connected peers.
        let max_peers = self.config.maximum_number_of_connected_peers() as usize;
        if new_active_peer_count > max_peers {
            let number_to_disconnect = new_active_peer_count - max_peers;
            trace!(
                "Disconnecting from {} peers to maintain their permitted number",
                number_to_disconnect
            );

            let mut current_peers = self.peer_book.connected_peers_snapshot().await;

            // Bootnodes will disconnect from random peers...
            if !self.config.is_bootnode() {
                // ...while regular peers from the most recently connected.
                current_peers.sort_unstable_by_key(|peer| peer.quality.last_connected);
            }

            for _ in 0..number_to_disconnect {
                if let Some(peer) = current_peers.pop() {
                    self.disconnect_from_peer(peer.address).await;
                }
            }
        }

        if new_active_peer_count != 0 {
            // Send a `Ping` to every connected peer.
            self.broadcast_pings().await;
        }
    }

    async fn initiate_connection(&self, remote_address: SocketAddr) -> Result<(), NetworkError> {
        debug!("Connecting to {}...", remote_address);

        // Local address must be known by now.
        let own_address = self.local_address().unwrap();

        // Don't connect if maximum number of connections has been reached.
        if !self.can_connect() {
            return Err(NetworkError::TooManyConnections);
        }

        if remote_address == own_address
            || ((remote_address.ip().is_unspecified() || remote_address.ip().is_loopback())
                && remote_address.port() == own_address.port())
        {
            return Err(NetworkError::SelfConnectAttempt);
        }
        if self.peer_book.is_connected(remote_address) {
            return Err(NetworkError::PeerAlreadyConnected);
        }

        metrics::increment_counter!(stats::CONNECTIONS_ALL_INITIATED);

        self.peer_book.get_or_connect(self.clone(), remote_address).await?;

        Ok(())
    }

    ///
    /// Broadcasts a connection request to all default bootnodes of the network.
    ///
    /// This function attempts to reconnect this node server with any bootnode peer
    /// that this node may have failed to connect to.
    ///
    /// This function filters out any bootnode peers the node server is
    /// either connnecting to or already connected to.
    ///
    async fn connect_to_bootnodes(&self) {
        // Local address must be known by now.
        let own_address = self.local_address().unwrap();

        // Iterate through each bootnode address and attempt a connection request.
        for bootnode_address in self
            .config
            .bootnodes()
            .iter()
            .filter(|peer| **peer != own_address)
            .copied()
        {
            let node = self.clone();
            if node.peer_book.is_connected(bootnode_address) {
                return;
            }
            task::spawn(async move {
                match node.initiate_connection(bootnode_address).await {
                    Err(NetworkError::PeerAlreadyConnecting) | Err(NetworkError::PeerAlreadyConnected) => {
                        // no issue here, already connecting
                    }
                    Err(e @ NetworkError::TooManyConnections) => {
                        warn!("Couldn't connect to bootnode {}: {}", bootnode_address, e);
                        // the connection hasn't been established, no need to disconnect
                    }
                    Err(e) => {
                        warn!("Couldn't connect to bootnode {}: {}", bootnode_address, e);
                        node.disconnect_from_peer(bootnode_address).await;
                    }
                    Ok(_) => {}
                }
            });
        }
    }

    ///
    /// Broadcasts a connection request to all disconnected peers.
    ///
    async fn connect_to_disconnected_peers(&self) {
        // Local address must be known by now.
        let own_address = self.local_address().unwrap();

        // If this node is a bootnode, attempt to connect to all disconnected peers.
        // If this node is not a bootnode, attempt to satisfy the minimum number of peer connections.
        let random_peers = {
            // Fetch the number of connected and connecting peers.
            let number_of_peers = self.peer_book.get_active_peer_count() as usize;

            // Check if this node server is below the permitted number of connected peers.
            let min_peers = self.config.minimum_number_of_connected_peers() as usize;
            if number_of_peers >= min_peers {
                return;
            }

            // Set the number of peers to attempt a connection to.
            let count = min_peers - number_of_peers;

            if count == 0 {
                return;
            }

            let disconnected_peers = self.peer_book.disconnected_peers();

            trace!(
                "Connecting to {} disconnected peers",
                cmp::min(count, disconnected_peers.len())
            );

            let bootnodes = self.config.bootnodes();

            // Iterate through a selection of random peers and attempt to connect.
            disconnected_peers
                .iter()
                .filter(|peer| **peer != own_address && !bootnodes.contains(peer))
                .copied()
                .choose_multiple(&mut rand::thread_rng(), count)
        };

        for remote_address in random_peers {
            let node = self.clone();
            task::spawn(async move {
                match node.initiate_connection(remote_address).await {
                    Err(NetworkError::PeerAlreadyConnecting) | Err(NetworkError::PeerAlreadyConnected) => {
                        // no issue here, already connecting
                    }
                    Err(e @ NetworkError::TooManyConnections) | Err(e @ NetworkError::SelfConnectAttempt) => {
                        warn!("Couldn't connect to peer {}: {}", remote_address, e);
                        // the connection hasn't been established, no need to disconnect
                    }
                    Err(e) => {
                        warn!("Couldn't connect to peer {}: {}", remote_address, e);
                        node.disconnect_from_peer(remote_address).await;
                    }
                    Ok(_) => {}
                }
            });
        }
    }

    /// Broadcasts a `GetPeers` message to all connected peers to request for more peers.
    async fn broadcast_getpeers_requests(&self) {
        // Check that this node is not a bootnode.
        if !self.config.is_bootnode() {
            // Fetch the number of connected and connecting peers.
            let number_of_peers = self.peer_book.get_active_peer_count() as usize;

            // Check if this node server is below the permitted number of connected peers.
            let min_peers = self.config.minimum_number_of_connected_peers() as usize;
            if number_of_peers >= min_peers {
                return;
            }
        }

        trace!("Sending `GetPeers` requests to connected peers");

        self.peer_book.broadcast(Payload::GetPeers).await;
    }

    /// Broadcasts a `Ping` message to all connected peers.
    async fn broadcast_pings(&self) {
        trace!("Broadcasting `Ping` messages");

        // Consider peering tests that don't use the sync layer.
        let current_block_height = if let Some(ref sync) = self.sync() {
            sync.current_block_height()
        } else {
            0
        };

        self.peer_book.broadcast(Payload::Ping(current_block_height)).await;
    }

    ///
    /// Removes the given remote address channel and sets the peer in the peer book
    /// as disconnected from this node server.
    ///
    #[inline]
    pub async fn disconnect_from_peer(&self, remote_address: SocketAddr) {
        if let Some(handle) = self.peer_book.get_peer_handle(remote_address) {
            if handle.disconnect().await {
                trace!("Disconnected from {}", remote_address);
            }
        }
    }

    pub(crate) async fn send_peers(&self, remote_address: SocketAddr) {
        // Broadcast the sanitized list of connected peers back to the requesting peer.
        let peers = self
            .peer_book
            .connected_peers()
            .into_iter()
            .filter(|&addr| addr != remote_address)
            .choose_multiple(&mut rand::thread_rng(), crate::SHARED_PEER_COUNT);

        self.peer_book.send_to(remote_address, Payload::Peers(peers)).await;
    }

    /// A node has sent their list of peer addresses.
    /// Add all new/updated addresses to our disconnected.
    /// The connection handler will be responsible for sending out handshake requests to them.
    pub(crate) async fn process_inbound_peers(&self, peers: Vec<SocketAddr>) {
        let local_address = self.local_address().unwrap(); // the address must be known by now

        for peer_address in peers.into_iter().filter(|&peer_addr| peer_addr != local_address) {
            // Inform the peer book that we found a peer.
            // The peer book will determine if we have seen the peer before,
            // and include the peer if it is new.
            self.peer_book
                .add_peer(peer_address, self.config.bootnodes().contains(&peer_address))
                .await;
        }
    }

    pub fn can_connect(&self) -> bool {
        let num_connected = self.peer_book.get_active_peer_count() as usize;

        let max_peers = self.config.maximum_number_of_connected_peers() as usize;

        if num_connected > max_peers {
            warn!(
                "Max number of connections ({} connected; max: {}) reached",
                num_connected, max_peers
            );
            false
        } else {
            true
        }
    }
}
