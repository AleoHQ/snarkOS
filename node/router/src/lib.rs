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

#![forbid(unsafe_code)]

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate tracing;

pub use snarkos_node_router_messages as messages;

mod handshake;

mod heartbeat;
pub use heartbeat::*;

mod helpers;
pub use helpers::*;

mod inbound;
pub use inbound::*;

mod outbound;
pub use outbound::*;

mod routing;
pub use routing::*;

use crate::messages::NodeType;
use snarkos_account::Account;
use snarkos_node_tcp::{is_bogon_ip, is_unspecified_or_broadcast_ip, Config, Tcp};
use snarkvm::prelude::{Address, Network, PrivateKey, ViewKey};

use anyhow::{bail, Result};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::SocketAddr,
    ops::Deref,
    str::FromStr,
    sync::Arc,
    time::Instant,
};
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct Router<N: Network>(Arc<InnerRouter<N>>);

impl<N: Network> Deref for Router<N> {
    type Target = Arc<InnerRouter<N>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct InnerRouter<N: Network> {
    /// The TCP stack.
    tcp: Tcp,
    /// The node type.
    node_type: NodeType,
    /// The account of the node.
    account: Account<N>,
    /// The cache.
    cache: Cache<N>,
    /// The resolver.
    resolver: Resolver,
    /// The set of trusted peers.
    trusted_peers: HashSet<SocketAddr>,
    /// The map of connected peer IPs to their peer handlers.
    connected_peers: RwLock<HashMap<SocketAddr, Peer<N>>>,
    /// The set of handshaking peers. While `Tcp` already recognizes the connecting IP addresses
    /// and prevents duplicate outbound connection attempts to the same IP address, it is unable to
    /// prevent simultaneous "two-way" connections between two peers (i.e. both nodes simultaneously
    /// attempt to connect to each other). This set is used to prevent this from happening.
    connecting_peers: Mutex<HashSet<SocketAddr>>,
    /// The set of candidate peer IPs.
    candidate_peers: RwLock<HashSet<SocketAddr>>,
    /// The set of restricted peer IPs.
    restricted_peers: RwLock<HashMap<SocketAddr, Instant>>,
    /// The spawned handles.
    handles: Mutex<Vec<JoinHandle<()>>>,
    /// The boolean flag for the development mode.
    is_dev: bool,
}

impl<N: Network> Router<N> {
    /// The maximum number of candidate peers permitted to be stored in the node.
    const MAXIMUM_CANDIDATE_PEERS: usize = 10_000;
    /// The maximum number of connection failures permitted by an inbound connecting peer.
    const MAXIMUM_CONNECTION_FAILURES: usize = 5;
    /// The duration in seconds after which a connected peer is considered inactive or
    /// disconnected if no message has been received in the meantime.
    const RADIO_SILENCE_IN_SECS: u64 = 150; // 2.5 minutes
}

impl<N: Network> Router<N> {
    /// Initializes a new `Router` instance.
    pub async fn new(
        node_ip: SocketAddr,
        node_type: NodeType,
        account: Account<N>,
        trusted_peers: &[SocketAddr],
        max_peers: u16,
        is_dev: bool,
    ) -> Result<Self> {
        // Initialize the TCP stack.
        let tcp = Tcp::new(Config::new(node_ip, max_peers));
        // Initialize the router.
        Ok(Self(Arc::new(InnerRouter {
            tcp,
            node_type,
            account,
            cache: Default::default(),
            resolver: Default::default(),
            trusted_peers: trusted_peers.iter().copied().collect(),
            connected_peers: Default::default(),
            connecting_peers: Default::default(),
            candidate_peers: Default::default(),
            restricted_peers: Default::default(),
            handles: Default::default(),
            is_dev,
        })))
    }
}

impl<N: Network> Router<N> {
    /// Attempts to connect to the given peer IP.
    pub fn connect(&self, peer_ip: SocketAddr) -> Option<JoinHandle<bool>> {
        // Return early if the attempt is against the protocol rules.
        if let Err(forbidden_message) = self.check_connection_attempt(peer_ip) {
            warn!("{forbidden_message}");
            return None;
        }

        let router = self.clone();
        Some(tokio::spawn(async move {
            // Attempt to connect to the candidate peer.
            match router.tcp.connect(peer_ip).await {
                // Remove the peer from the candidate peers.
                Ok(()) => {
                    router.remove_candidate_peer(peer_ip);
                    true
                }
                // If the connection was not allowed, log the error.
                Err(error) => {
                    router.connecting_peers.lock().remove(&peer_ip);
                    warn!("Unable to connect to '{peer_ip}' - {error}");
                    false
                }
            }
        }))
    }

    /// Ensure we are allowed to connect to the given peer.
    fn check_connection_attempt(&self, peer_ip: SocketAddr) -> Result<()> {
        // Ensure the peer IP is not this node.
        if self.is_local_ip(&peer_ip) {
            bail!("Dropping connection attempt to '{peer_ip}' (attempted to self-connect)")
        }
        // Ensure the node does not surpass the maximum number of peer connections.
        if self.number_of_connected_peers() >= self.max_connected_peers() {
            bail!("Dropping connection attempt to '{peer_ip}' (maximum peers reached)")
        }
        // Ensure the node is not already connected to this peer.
        if self.is_connected(&peer_ip) {
            bail!("Dropping connection attempt to '{peer_ip}' (already connected)")
        }
        // Ensure the peer is not restricted.
        if self.is_restricted(&peer_ip) {
            bail!("Dropping connection attempt to '{peer_ip}' (restricted)")
        }
        // Ensure the node is not already connecting to this peer.
        if !self.connecting_peers.lock().insert(peer_ip) {
            bail!("Dropping connection attempt to '{peer_ip}' (already shaking hands as the initiator)")
        }
        Ok(())
    }

    /// Disconnects from the given peer IP, if the peer is connected.
    pub fn disconnect(&self, peer_ip: SocketAddr) -> JoinHandle<bool> {
        let router = self.clone();
        tokio::spawn(async move {
            if let Some(peer_addr) = router.resolve_to_ambiguous(&peer_ip) {
                // Disconnect from this peer.
                let disconnected = router.tcp.disconnect(peer_addr).await;
                // FIXME (ljedrz): this shouldn't be necessary; it's a double-check
                //  that the higher-level collection is cleaned up after the lower-level disconnect.
                if router.is_connected(&peer_ip) && !router.tcp.is_connected(peer_addr) {
                    warn!("Disconnecting with fallback safety (report this to @ljedrz)");
                    router.remove_connected_peer(peer_ip);
                }
                disconnected
            } else {
                false
            }
        })
    }

    /// Returns the IP address of this node.
    pub fn local_ip(&self) -> SocketAddr {
        self.tcp.listening_addr().expect("The TCP listener is not enabled")
    }

    /// Returns `true` if the given IP is this node.
    pub fn is_local_ip(&self, ip: &SocketAddr) -> bool {
        *ip == self.local_ip()
            || (ip.ip().is_unspecified() || ip.ip().is_loopback()) && ip.port() == self.local_ip().port()
    }

    /// Returns `true` if the given IP is not this node, is not a bogon address, and is not unspecified.
    pub fn is_valid_peer_ip(&self, ip: &SocketAddr) -> bool {
        !self.is_local_ip(ip) && !is_bogon_ip(ip.ip()) && !is_unspecified_or_broadcast_ip(ip.ip())
    }

    /// Returns the node type.
    pub fn node_type(&self) -> NodeType {
        self.node_type
    }

    /// Returns the account private key of the node.
    pub fn private_key(&self) -> &PrivateKey<N> {
        self.account.private_key()
    }

    /// Returns the account view key of the node.
    pub fn view_key(&self) -> &ViewKey<N> {
        self.account.view_key()
    }

    /// Returns the account address of the node.
    pub fn address(&self) -> Address<N> {
        self.account.address()
    }

    /// Returns `true` if the node is in development mode.
    pub fn is_dev(&self) -> bool {
        self.is_dev
    }

    /// Returns the listener IP address from the (ambiguous) peer address.
    pub fn resolve_to_listener(&self, peer_addr: &SocketAddr) -> Option<SocketAddr> {
        self.resolver.get_listener(peer_addr)
    }

    /// Returns the (ambiguous) peer address from the listener IP address.
    pub fn resolve_to_ambiguous(&self, peer_ip: &SocketAddr) -> Option<SocketAddr> {
        self.resolver.get_ambiguous(peer_ip)
    }

    /// Returns `true` if the node is connected to the given peer IP.
    pub fn is_connected(&self, ip: &SocketAddr) -> bool {
        self.connected_peers.read().contains_key(ip)
    }

    /// Returns `true` if the given peer IP is a connected validator.
    pub fn is_connected_validator(&self, peer_ip: &SocketAddr) -> bool {
        self.connected_peers.read().get(peer_ip).map_or(false, |peer| peer.is_validator())
    }

    /// Returns `true` if the given peer IP is a connected prover.
    pub fn is_connected_prover(&self, peer_ip: &SocketAddr) -> bool {
        self.connected_peers.read().get(peer_ip).map_or(false, |peer| peer.is_prover())
    }

    /// Returns `true` if the given peer IP is a connected client.
    pub fn is_connected_client(&self, peer_ip: &SocketAddr) -> bool {
        self.connected_peers.read().get(peer_ip).map_or(false, |peer| peer.is_client())
    }

    /// Returns `true` if the node is currently connecting to the given peer IP.
    pub fn is_connecting(&self, ip: &SocketAddr) -> bool {
        self.connecting_peers.lock().contains(ip)
    }

    /// Returns `true` if the given IP is restricted.
    pub fn is_restricted(&self, ip: &SocketAddr) -> bool {
        self.restricted_peers
            .read()
            .get(ip)
            .map(|time| time.elapsed().as_secs() < Self::RADIO_SILENCE_IN_SECS)
            .unwrap_or(false)
    }

    /// Returns the maximum number of connected peers.
    pub fn max_connected_peers(&self) -> usize {
        self.tcp.config().max_connections as usize
    }

    /// Returns the number of connected peers.
    pub fn number_of_connected_peers(&self) -> usize {
        self.connected_peers.read().len()
    }

    /// Returns the number of connecting peers.
    pub fn number_of_connecting_peers(&self) -> usize {
        self.connecting_peers.lock().len()
    }

    /// Returns the number of connected validators.
    pub fn number_of_connected_validators(&self) -> usize {
        self.connected_peers.read().values().filter(|peer| peer.is_validator()).count()
    }

    /// Returns the number of connected provers.
    pub fn number_of_connected_provers(&self) -> usize {
        self.connected_peers.read().values().filter(|peer| peer.is_prover()).count()
    }

    /// Returns the number of connected clients.
    pub fn number_of_connected_clients(&self) -> usize {
        self.connected_peers.read().values().filter(|peer| peer.is_client()).count()
    }

    /// Returns the number of candidate peers.
    pub fn number_of_candidate_peers(&self) -> usize {
        self.candidate_peers.read().len()
    }

    /// Returns the number of restricted peers.
    pub fn number_of_restricted_peers(&self) -> usize {
        self.restricted_peers.read().len()
    }

    /// Returns the connected peer given the peer IP, if it exists.
    pub fn get_connected_peer(&self, ip: &SocketAddr) -> Option<Peer<N>> {
        self.connected_peers.read().get(ip).cloned()
    }

    /// Returns the connected peers.
    pub fn get_connected_peers(&self) -> Vec<Peer<N>> {
        self.connected_peers.read().values().cloned().collect()
    }

    /// Returns the list of connected peers.
    pub fn connected_peers(&self) -> Vec<SocketAddr> {
        self.connected_peers.read().keys().copied().collect()
    }

    /// Returns the list of connected validators.
    pub fn connected_validators(&self) -> Vec<SocketAddr> {
        self.connected_peers.read().iter().filter(|(_, peer)| peer.is_validator()).map(|(ip, _)| *ip).collect()
    }

    /// Returns the list of connected provers.
    pub fn connected_provers(&self) -> Vec<SocketAddr> {
        self.connected_peers.read().iter().filter(|(_, peer)| peer.is_prover()).map(|(ip, _)| *ip).collect()
    }

    /// Returns the list of connected clients.
    pub fn connected_clients(&self) -> Vec<SocketAddr> {
        self.connected_peers.read().iter().filter(|(_, peer)| peer.is_client()).map(|(ip, _)| *ip).collect()
    }

    /// Returns the list of candidate peers.
    pub fn candidate_peers(&self) -> HashSet<SocketAddr> {
        self.candidate_peers.read().clone()
    }

    /// Returns the list of restricted peers.
    pub fn restricted_peers(&self) -> Vec<SocketAddr> {
        self.restricted_peers.read().keys().copied().collect()
    }

    /// Returns the list of trusted peers.
    pub fn trusted_peers(&self) -> &HashSet<SocketAddr> {
        &self.trusted_peers
    }

    /// Returns the list of bootstrap peers.
    pub fn bootstrap_peers(&self) -> Vec<SocketAddr> {
        if cfg!(feature = "test") || self.is_dev {
            vec![]
        } else {
            vec![
                SocketAddr::from_str("35.224.50.150:4133").unwrap(),
                SocketAddr::from_str("35.227.159.141:4133").unwrap(),
                SocketAddr::from_str("34.139.203.87:4133").unwrap(),
                SocketAddr::from_str("34.150.221.166:4133").unwrap(),
            ]
        }
    }

    /// Returns the list of metrics for the connected peers.
    pub fn connected_metrics(&self) -> Vec<(SocketAddr, NodeType)> {
        self.connected_peers.read().iter().map(|(ip, peer)| (*ip, peer.node_type())).collect()
    }

    #[cfg(feature = "metrics")]
    fn update_metrics(&self) {
        metrics::gauge(metrics::router::CONNECTED, self.connected_peers.read().len() as f64);
        metrics::gauge(metrics::router::CANDIDATE, self.candidate_peers.read().len() as f64);
        metrics::gauge(metrics::router::RESTRICTED, self.restricted_peers.read().len() as f64);
    }

    /// Inserts the given peer into the connected peers.
    pub fn insert_connected_peer(&self, peer: Peer<N>, peer_addr: SocketAddr) {
        let peer_ip = peer.ip();
        // Adds a bidirectional map between the listener address and (ambiguous) peer address.
        self.resolver.insert_peer(peer_ip, peer_addr);
        // Add an entry for this `Peer` in the connected peers.
        self.connected_peers.write().insert(peer_ip, peer);
        // Remove this peer from the candidate peers, if it exists.
        self.candidate_peers.write().remove(&peer_ip);
        // Remove this peer from the restricted peers, if it exists.
        self.restricted_peers.write().remove(&peer_ip);
        #[cfg(feature = "metrics")]
        self.update_metrics();
    }

    /// Inserts the given peer IPs to the set of candidate peers.
    ///
    /// This method skips adding any given peers if the combined size exceeds the threshold,
    /// as the peer providing this list could be subverting the protocol.
    pub fn insert_candidate_peers(&self, peers: &[SocketAddr]) {
        // Compute the maximum number of candidate peers.
        let max_candidate_peers = Self::MAXIMUM_CANDIDATE_PEERS.saturating_sub(self.number_of_candidate_peers());
        // Ensure the combined number of peers does not surpass the threshold.
        let eligible_peers = peers
            .iter()
            .filter(|peer_ip| {
                // Ensure the peer is not itself, is not already connected, and is not restricted.
                !self.is_local_ip(peer_ip) && !self.is_connected(peer_ip) && !self.is_restricted(peer_ip)
            })
            .take(max_candidate_peers);

        // Proceed to insert the eligible candidate peer IPs.
        self.candidate_peers.write().extend(eligible_peers);
        #[cfg(feature = "metrics")]
        self.update_metrics();
    }

    /// Inserts the given peer into the restricted peers.
    pub fn insert_restricted_peer(&self, peer_ip: SocketAddr) {
        // Remove this peer from the candidate peers, if it exists.
        self.candidate_peers.write().remove(&peer_ip);
        // Add the peer to the restricted peers.
        self.restricted_peers.write().insert(peer_ip, Instant::now());
        #[cfg(feature = "metrics")]
        self.update_metrics();
    }

    /// Updates the connected peer with the given function.
    pub fn update_connected_peer<Fn: FnMut(&mut Peer<N>)>(
        &self,
        peer_ip: SocketAddr,
        node_type: NodeType,
        mut write_fn: Fn,
    ) -> Result<()> {
        // Retrieve the peer.
        if let Some(peer) = self.connected_peers.write().get_mut(&peer_ip) {
            // Ensure the node type has not changed.
            if peer.node_type() != node_type {
                bail!("Peer '{peer_ip}' has changed node types from {} to {node_type}", peer.node_type())
            }
            // Lastly, update the peer with the given function.
            write_fn(peer);
        }
        Ok(())
    }

    /// Removes the connected peer and adds them to the candidate peers.
    pub fn remove_connected_peer(&self, peer_ip: SocketAddr) {
        // Removes the bidirectional map between the listener address and (ambiguous) peer address.
        self.resolver.remove_peer(&peer_ip);
        // Remove this peer from the connected peers, if it exists.
        self.connected_peers.write().remove(&peer_ip);
        // Add the peer to the candidate peers.
        self.candidate_peers.write().insert(peer_ip);
        #[cfg(feature = "metrics")]
        self.update_metrics();
    }

    /// Removes the connecting peer.
    pub fn remove_connecting_peer(&self, peer_ip: SocketAddr) {
        self.connecting_peers.lock().remove(&peer_ip);
    }

    #[cfg(feature = "test")]
    pub fn clear_candidate_peers(&self) {
        self.candidate_peers.write().clear();
        #[cfg(feature = "metrics")]
        self.update_metrics();
    }

    /// Removes the given address from the candidate peers, if it exists.
    pub fn remove_candidate_peer(&self, peer_ip: SocketAddr) {
        self.candidate_peers.write().remove(&peer_ip);
        #[cfg(feature = "metrics")]
        self.update_metrics();
    }

    /// Spawns a task with the given future; it should only be used for long-running tasks.
    pub fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        self.handles.lock().push(tokio::spawn(future));
    }

    /// Shuts down the router.
    pub async fn shut_down(&self) {
        info!("Shutting down the router...");
        // Abort the tasks.
        self.handles.lock().iter().for_each(|handle| handle.abort());
        // Close the listener.
        self.tcp.shut_down().await;
    }
}
