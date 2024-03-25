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
    events::{EventCodec, PrimaryPing},
    helpers::{assign_to_worker, PeerCache, PrimarySender, Resolver, SyncSender, WorkerSender},
    spawn_blocking,
    CONTEXT,
    MAX_BATCH_DELAY_IN_MS,
    MAX_GC_ROUNDS,
    MAX_TRANSMISSIONS_PER_BATCH,
    MAX_TRANSMISSIONS_PER_WORKER_PING,
    MEMORY_POOL_PORT,
};
use snarkos_account::Account;
use snarkos_node_bft_events::{
    BlockRequest,
    BlockResponse,
    CertificateRequest,
    CertificateResponse,
    ChallengeRequest,
    ChallengeResponse,
    DataBlocks,
    DisconnectReason,
    Event,
    EventTrait,
    TransmissionRequest,
    TransmissionResponse,
    ValidatorsRequest,
    ValidatorsResponse,
};
use snarkos_node_bft_ledger_service::LedgerService;
use snarkos_node_sync::communication_service::CommunicationService;
use snarkos_node_tcp::{
    is_bogon_ip,
    is_unspecified_or_broadcast_ip,
    protocols::{Disconnect, Handshake, OnConnect, Reading, Writing},
    Config,
    Connection,
    ConnectionSide,
    Tcp,
    P2P,
};
use snarkvm::{
    console::prelude::*,
    ledger::{committee::Committee, narwhal::Data},
    prelude::Address,
};

use colored::Colorize;
use futures::SinkExt;
use indexmap::{IndexMap, IndexSet};
use parking_lot::{Mutex, RwLock};
use rand::seq::{IteratorRandom, SliceRandom};
use std::{collections::HashSet, future::Future, io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    sync::{oneshot, OnceCell},
    task::{self, JoinHandle},
};
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

/// The maximum interval of events to cache.
const CACHE_EVENTS_INTERVAL: i64 = (MAX_BATCH_DELAY_IN_MS / 1000) as i64; // seconds
/// The maximum interval of requests to cache.
const CACHE_REQUESTS_INTERVAL: i64 = (MAX_BATCH_DELAY_IN_MS / 1000) as i64; // seconds

/// The maximum number of connection attempts in an interval.
const MAX_CONNECTION_ATTEMPTS: usize = 10;
/// The maximum interval to restrict a peer.
const RESTRICTED_INTERVAL: i64 = (MAX_CONNECTION_ATTEMPTS as u64 * MAX_BATCH_DELAY_IN_MS / 1000) as i64; // seconds

/// The minimum number of validators to maintain a connection to.
const MIN_CONNECTED_VALIDATORS: usize = 175;
/// The maximum number of validators to send in a validators response event.
const MAX_VALIDATORS_TO_SEND: usize = 200;

/// Part of the Gateway API that deals with networking.
/// This is a separate trait to allow for easier testing/mocking.
#[async_trait]
pub trait Transport<N: Network>: Send + Sync {
    async fn send(&self, peer_ip: SocketAddr, event: Event<N>) -> Option<oneshot::Receiver<io::Result<()>>>;
    fn broadcast(&self, event: Event<N>);
}

#[derive(Clone)]
pub struct Gateway<N: Network> {
    /// The account of the node.
    account: Account<N>,
    /// The ledger service.
    ledger: Arc<dyn LedgerService<N>>,
    /// The TCP stack.
    tcp: Tcp,
    /// The cache.
    cache: Arc<PeerCache<N>>,
    /// The resolver.
    resolver: Arc<Resolver<N>>,
    /// The set of trusted validators.
    trusted_validators: IndexSet<SocketAddr>,
    /// The map of connected peer IPs to their peer handlers.
    connected_peers: Arc<RwLock<IndexSet<SocketAddr>>>,
    /// The set of handshaking peers. While `Tcp` already recognizes the connecting IP addresses
    /// and prevents duplicate outbound connection attempts to the same IP address, it is unable to
    /// prevent simultaneous "two-way" connections between two peers (i.e. both nodes simultaneously
    /// attempt to connect to each other). This set is used to prevent this from happening.
    connecting_peers: Arc<Mutex<IndexSet<SocketAddr>>>,
    /// The primary sender.
    primary_sender: Arc<OnceCell<PrimarySender<N>>>,
    /// The worker senders.
    worker_senders: Arc<OnceCell<IndexMap<u8, WorkerSender<N>>>>,
    /// The sync sender.
    sync_sender: Arc<OnceCell<SyncSender<N>>>,
    /// The spawned handles.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// The development mode.
    dev: Option<u16>,
}

impl<N: Network> Gateway<N> {
    /// Initializes a new gateway.
    pub fn new(
        account: Account<N>,
        ledger: Arc<dyn LedgerService<N>>,
        ip: Option<SocketAddr>,
        trusted_validators: &[SocketAddr],
        dev: Option<u16>,
    ) -> Result<Self> {
        // Initialize the gateway IP.
        let ip = match (ip, dev) {
            (None, Some(dev)) => SocketAddr::from_str(&format!("127.0.0.1:{}", MEMORY_POOL_PORT + dev))?,
            (None, None) => SocketAddr::from_str(&format!("0.0.0.0:{}", MEMORY_POOL_PORT))?,
            (Some(ip), _) => ip,
        };
        // Initialize the TCP stack.
        let tcp = Tcp::new(Config::new(ip, Committee::<N>::MAX_COMMITTEE_SIZE));
        // Return the gateway.
        Ok(Self {
            account,
            ledger,
            tcp,
            cache: Default::default(),
            resolver: Default::default(),
            trusted_validators: trusted_validators.iter().copied().collect(),
            connected_peers: Default::default(),
            connecting_peers: Default::default(),
            primary_sender: Default::default(),
            worker_senders: Default::default(),
            sync_sender: Default::default(),
            handles: Default::default(),
            dev,
        })
    }

    /// Run the gateway.
    pub async fn run(
        &self,
        primary_sender: PrimarySender<N>,
        worker_senders: IndexMap<u8, WorkerSender<N>>,
        sync_sender: Option<SyncSender<N>>,
    ) {
        debug!("Starting the gateway for the memory pool...");

        // Set the primary sender.
        self.primary_sender.set(primary_sender).expect("Primary sender already set in gateway");

        // Set the worker senders.
        self.worker_senders.set(worker_senders).expect("The worker senders are already set");

        // If the sync sender was provided, set the sync sender.
        if let Some(sync_sender) = sync_sender {
            self.sync_sender.set(sync_sender).expect("Sync sender already set in gateway");
        }

        // Enable the TCP protocols.
        self.enable_handshake().await;
        self.enable_reading().await;
        self.enable_writing().await;
        self.enable_disconnect().await;
        self.enable_on_connect().await;
        // Enable the TCP listener. Note: This must be called after the above protocols.
        let _listening_addr = self.tcp.enable_listener().await.expect("Failed to enable the TCP listener");

        // Initialize the heartbeat.
        self.initialize_heartbeat();

        info!("Started the gateway for the memory pool at '{}'", self.local_ip());
    }
}

// Dynamic rate limiting.
impl<N: Network> Gateway<N> {
    /// The current maximum committee size.
    fn max_committee_size(&self) -> usize {
        self.ledger
            .current_committee()
            .map_or_else(|_e| Committee::<N>::MAX_COMMITTEE_SIZE as usize, |committee| committee.num_members())
    }

    /// The maximum number of events to cache.
    fn max_cache_events(&self) -> usize {
        self.max_cache_transmissions()
    }

    /// The maximum number of certificate requests to cache.
    fn max_cache_certificates(&self) -> usize {
        2 * MAX_GC_ROUNDS as usize * self.max_committee_size()
    }

    /// The maximum number of transmission requests to cache.
    fn max_cache_transmissions(&self) -> usize {
        self.max_cache_certificates() * MAX_TRANSMISSIONS_PER_BATCH
    }

    /// The maximum number of duplicates for any particular request.
    fn max_cache_duplicates(&self) -> usize {
        self.max_committee_size().pow(2)
    }
}

#[async_trait]
impl<N: Network> CommunicationService for Gateway<N> {
    /// The message type.
    type Message = Event<N>;

    /// Prepares a block request to be sent.
    fn prepare_block_request(start_height: u32, end_height: u32) -> Self::Message {
        debug_assert!(start_height < end_height, "Invalid block request format");
        Event::BlockRequest(BlockRequest { start_height, end_height })
    }

    /// Sends the given message to specified peer.
    ///
    /// This function returns as soon as the message is queued to be sent,
    /// without waiting for the actual delivery; instead, the caller is provided with a [`oneshot::Receiver`]
    /// which can be used to determine when and whether the message has been delivered.
    async fn send(&self, peer_ip: SocketAddr, message: Self::Message) -> Option<oneshot::Receiver<io::Result<()>>> {
        Transport::send(self, peer_ip, message).await
    }
}

impl<N: Network> Gateway<N> {
    /// Returns the account of the node.
    pub const fn account(&self) -> &Account<N> {
        &self.account
    }

    /// Returns the IP address of this node.
    pub fn local_ip(&self) -> SocketAddr {
        self.tcp.listening_addr().expect("The TCP listener is not enabled")
    }

    /// Returns `true` if the given IP is this node.
    pub fn is_local_ip(&self, ip: SocketAddr) -> bool {
        ip == self.local_ip()
            || (ip.ip().is_unspecified() || ip.ip().is_loopback()) && ip.port() == self.local_ip().port()
    }

    /// Returns `true` if the given IP is not this node, is not a bogon address, and is not unspecified.
    pub fn is_valid_peer_ip(&self, ip: SocketAddr) -> bool {
        !self.is_local_ip(ip) && !is_bogon_ip(ip.ip()) && !is_unspecified_or_broadcast_ip(ip.ip())
    }

    /// Returns the resolver.
    pub fn resolver(&self) -> &Resolver<N> {
        &self.resolver
    }

    /// Returns the primary sender.
    pub fn primary_sender(&self) -> &PrimarySender<N> {
        self.primary_sender.get().expect("Primary sender not set in gateway")
    }

    /// Returns the number of workers.
    pub fn num_workers(&self) -> u8 {
        u8::try_from(self.worker_senders.get().expect("Missing worker senders in gateway").len())
            .expect("Too many workers")
    }

    /// Returns the worker sender for the given worker ID.
    pub fn get_worker_sender(&self, worker_id: u8) -> Option<&WorkerSender<N>> {
        self.worker_senders.get().and_then(|senders| senders.get(&worker_id))
    }

    /// Returns `true` if the node is connected to the given Aleo address.
    pub fn is_connected_address(&self, address: Address<N>) -> bool {
        // Retrieve the peer IP of the given address.
        match self.resolver.get_peer_ip_for_address(address) {
            // Determine if the peer IP is connected.
            Some(peer_ip) => self.is_connected_ip(peer_ip),
            None => false,
        }
    }

    /// Returns `true` if the node is connected to the given peer IP.
    pub fn is_connected_ip(&self, ip: SocketAddr) -> bool {
        self.connected_peers.read().contains(&ip)
    }

    /// Returns `true` if the node is connecting to the given peer IP.
    pub fn is_connecting_ip(&self, ip: SocketAddr) -> bool {
        self.connecting_peers.lock().contains(&ip)
    }

    /// Returns `true` if the given peer IP is an authorized validator.
    pub fn is_authorized_validator_ip(&self, ip: SocketAddr) -> bool {
        // If the peer IP is in the trusted validators, return early.
        if self.trusted_validators.contains(&ip) {
            return true;
        }
        // Retrieve the Aleo address of the peer IP.
        match self.resolver.get_address(ip) {
            // Determine if the peer IP is an authorized validator.
            Some(address) => self.is_authorized_validator_address(address),
            None => false,
        }
    }

    /// Returns `true` if the given address is an authorized validator.
    pub fn is_authorized_validator_address(&self, validator_address: Address<N>) -> bool {
        // Determine if the validator address is a member of the committee lookback or the current committee.
        // We allow leniency in this validation check in order to accommodate these two scenarios:
        //  1. New validators should be able to connect immediately once bonded as a committee member.
        //  2. Existing validators must remain connected until they are no longer bonded as a committee member.
        //     (i.e. meaning they must stay online until the next block has been produced)
        self.ledger
            .get_committee_lookback_for_round(self.ledger.latest_round())
            .map_or(false, |committee| committee.is_committee_member(validator_address))
            || self
                .ledger
                .current_committee()
                .map_or(false, |committee| committee.is_committee_member(validator_address))
    }

    /// Returns the maximum number of connected peers.
    pub fn max_connected_peers(&self) -> usize {
        self.tcp.config().max_connections as usize
    }

    /// Returns the number of connected peers.
    pub fn number_of_connected_peers(&self) -> usize {
        self.connected_peers.read().len()
    }

    /// Returns the list of connected addresses.
    pub fn connected_addresses(&self) -> HashSet<Address<N>> {
        self.connected_peers.read().iter().filter_map(|peer_ip| self.resolver.get_address(*peer_ip)).collect()
    }

    /// Returns the list of connected peers.
    pub fn connected_peers(&self) -> &RwLock<IndexSet<SocketAddr>> {
        &self.connected_peers
    }

    /// Attempts to connect to the given peer IP.
    pub fn connect(&self, peer_ip: SocketAddr) -> Option<JoinHandle<()>> {
        // Return early if the attempt is against the protocol rules.
        if let Err(forbidden_error) = self.check_connection_attempt(peer_ip) {
            warn!("{forbidden_error}");
            return None;
        }

        let self_ = self.clone();
        Some(tokio::spawn(async move {
            debug!("Connecting to validator {peer_ip}...");
            // Attempt to connect to the peer.
            if let Err(error) = self_.tcp.connect(peer_ip).await {
                self_.connecting_peers.lock().shift_remove(&peer_ip);
                warn!("Unable to connect to '{peer_ip}' - {error}");
            }
        }))
    }

    /// Ensure we are allowed to connect to the given peer.
    fn check_connection_attempt(&self, peer_ip: SocketAddr) -> Result<()> {
        // Ensure the peer IP is not this node.
        if self.is_local_ip(peer_ip) {
            bail!("{CONTEXT} Dropping connection attempt to '{peer_ip}' (attempted to self-connect)")
        }
        // Ensure the node does not surpass the maximum number of peer connections.
        if self.number_of_connected_peers() >= self.max_connected_peers() {
            bail!("{CONTEXT} Dropping connection attempt to '{peer_ip}' (maximum peers reached)")
        }
        // Ensure the node is not already connected to this peer.
        if self.is_connected_ip(peer_ip) {
            bail!("{CONTEXT} Dropping connection attempt to '{peer_ip}' (already connected)")
        }
        // Ensure the node is not already connecting to this peer.
        if self.is_connecting_ip(peer_ip) {
            bail!("{CONTEXT} Dropping connection attempt to '{peer_ip}' (already connecting)")
        }
        Ok(())
    }

    /// Ensure the peer is allowed to connect.
    fn ensure_peer_is_allowed(&self, peer_ip: SocketAddr) -> Result<()> {
        // Ensure the peer IP is not this node.
        if self.is_local_ip(peer_ip) {
            bail!("{CONTEXT} Dropping connection request from '{peer_ip}' (attempted to self-connect)")
        }
        // Ensure the node is not already connecting to this peer.
        if !self.connecting_peers.lock().insert(peer_ip) {
            bail!("{CONTEXT} Dropping connection request from '{peer_ip}' (already shaking hands as the initiator)")
        }
        // Ensure the node is not already connected to this peer.
        if self.is_connected_ip(peer_ip) {
            bail!("{CONTEXT} Dropping connection request from '{peer_ip}' (already connected)")
        }
        // Ensure the peer is not spamming connection attempts.
        if !peer_ip.ip().is_loopback() {
            // Add this connection attempt and retrieve the number of attempts.
            let num_attempts = self.cache.insert_inbound_connection(peer_ip.ip(), RESTRICTED_INTERVAL);
            // Ensure the connecting peer has not surpassed the connection attempt limit.
            if num_attempts > MAX_CONNECTION_ATTEMPTS {
                bail!("Dropping connection request from '{peer_ip}' (tried {num_attempts} times)")
            }
        }
        Ok(())
    }

    #[cfg(feature = "metrics")]
    fn update_metrics(&self) {
        metrics::gauge(metrics::bft::CONNECTED, self.connected_peers.read().len() as f64);
        metrics::gauge(metrics::bft::CONNECTING, self.connecting_peers.lock().len() as f64);
    }

    /// Inserts the given peer into the connected peers.
    #[cfg(not(test))]
    fn insert_connected_peer(&self, peer_ip: SocketAddr, peer_addr: SocketAddr, address: Address<N>) {
        // Adds a bidirectional map between the listener address and (ambiguous) peer address.
        self.resolver.insert_peer(peer_ip, peer_addr, address);
        // Add a transmission for this peer in the connected peers.
        self.connected_peers.write().insert(peer_ip);
        #[cfg(feature = "metrics")]
        self.update_metrics();
    }

    /// Inserts the given peer into the connected peers.
    #[cfg(test)]
    // For unit tests, we need to make this public so we can inject peers.
    pub fn insert_connected_peer(&self, peer_ip: SocketAddr, peer_addr: SocketAddr, address: Address<N>) {
        // Adds a bidirectional map between the listener address and (ambiguous) peer address.
        self.resolver.insert_peer(peer_ip, peer_addr, address);
        // Add a transmission for this peer in the connected peers.
        self.connected_peers.write().insert(peer_ip);
    }

    /// Removes the connected peer and adds them to the candidate peers.
    fn remove_connected_peer(&self, peer_ip: SocketAddr) {
        // If a sync sender was provided, remove the peer from the sync module.
        if let Some(sync_sender) = self.sync_sender.get() {
            let tx_block_sync_remove_peer_ = sync_sender.tx_block_sync_remove_peer.clone();
            tokio::spawn(async move {
                if let Err(e) = tx_block_sync_remove_peer_.send(peer_ip).await {
                    warn!("Unable to remove '{peer_ip}' from the sync module - {e}");
                }
            });
        }
        // Removes the bidirectional map between the listener address and (ambiguous) peer address.
        self.resolver.remove_peer(peer_ip);
        // Remove this peer from the connected peers, if it exists.
        self.connected_peers.write().shift_remove(&peer_ip);
        #[cfg(feature = "metrics")]
        self.update_metrics();
    }

    /// Sends the given event to specified peer.
    ///
    /// This function returns as soon as the event is queued to be sent,
    /// without waiting for the actual delivery; instead, the caller is provided with a [`oneshot::Receiver`]
    /// which can be used to determine when and whether the event has been delivered.
    fn send_inner(&self, peer_ip: SocketAddr, event: Event<N>) -> Option<oneshot::Receiver<io::Result<()>>> {
        // Resolve the listener IP to the (ambiguous) peer address.
        let Some(peer_addr) = self.resolver.get_ambiguous(peer_ip) else {
            warn!("Unable to resolve the listener IP address '{peer_ip}'");
            return None;
        };
        // Retrieve the event name.
        let name = event.name();
        // Send the event to the peer.
        trace!("{CONTEXT} Sending '{name}' to '{peer_ip}'");
        let result = self.unicast(peer_addr, event);
        // If the event was unable to be sent, disconnect.
        if let Err(e) = &result {
            warn!("{CONTEXT} Failed to send '{name}' to '{peer_ip}': {e}");
            debug!("{CONTEXT} Disconnecting from '{peer_ip}' (unable to send)");
            self.disconnect(peer_ip);
        }
        result.ok()
    }

    /// Handles the inbound event from the peer.
    async fn inbound(&self, peer_addr: SocketAddr, event: Event<N>) -> Result<()> {
        // Retrieve the listener IP for the peer.
        let Some(peer_ip) = self.resolver.get_listener(peer_addr) else {
            bail!("{CONTEXT} Unable to resolve the (ambiguous) peer address '{peer_addr}'")
        };
        // Ensure that the peer is an authorized committee member.
        if !self.is_authorized_validator_ip(peer_ip) {
            bail!("{CONTEXT} Dropping '{}' from '{peer_ip}' (not authorized)", event.name())
        }
        // Drop the peer, if they have exceeded the rate limit (i.e. they are requesting too much from us).
        let num_events = self.cache.insert_inbound_event(peer_ip, CACHE_EVENTS_INTERVAL);
        if num_events >= self.max_cache_events() {
            bail!("Dropping '{peer_ip}' for spamming events (num_events = {num_events})")
        }
        // Rate limit for duplicate requests.
        if matches!(&event, &Event::CertificateRequest(_) | &Event::CertificateResponse(_)) {
            // Retrieve the certificate ID.
            let certificate_id = match &event {
                Event::CertificateRequest(CertificateRequest { certificate_id }) => *certificate_id,
                Event::CertificateResponse(CertificateResponse { certificate }) => certificate.id(),
                _ => unreachable!(),
            };
            // Skip processing this certificate if the rate limit was exceed (i.e. someone is spamming a specific certificate).
            let num_events = self.cache.insert_inbound_certificate(certificate_id, CACHE_REQUESTS_INTERVAL);
            if num_events >= self.max_cache_duplicates() {
                return Ok(());
            }
        } else if matches!(&event, &Event::TransmissionRequest(_) | Event::TransmissionResponse(_)) {
            // Retrieve the transmission ID.
            let transmission_id = match &event {
                Event::TransmissionRequest(TransmissionRequest { transmission_id }) => *transmission_id,
                Event::TransmissionResponse(TransmissionResponse { transmission_id, .. }) => *transmission_id,
                _ => unreachable!(),
            };
            // Skip processing this certificate if the rate limit was exceeded (i.e. someone is spamming a specific certificate).
            let num_events = self.cache.insert_inbound_transmission(transmission_id, CACHE_REQUESTS_INTERVAL);
            if num_events >= self.max_cache_duplicates() {
                return Ok(());
            }
        }
        trace!("{CONTEXT} Received '{}' from '{peer_ip}'", event.name());

        // This match statement handles the inbound event by deserializing the event,
        // checking the event is valid, and then calling the appropriate (trait) handler.
        match event {
            Event::BatchPropose(batch_propose) => {
                // Send the batch propose to the primary.
                let _ = self.primary_sender().tx_batch_propose.send((peer_ip, batch_propose)).await;
                Ok(())
            }
            Event::BatchSignature(batch_signature) => {
                // Send the batch signature to the primary.
                let _ = self.primary_sender().tx_batch_signature.send((peer_ip, batch_signature)).await;
                Ok(())
            }
            Event::BatchCertified(batch_certified) => {
                // Send the batch certificate to the primary.
                let _ = self.primary_sender().tx_batch_certified.send((peer_ip, batch_certified.certificate)).await;
                Ok(())
            }
            Event::BlockRequest(block_request) => {
                let BlockRequest { start_height, end_height } = block_request;

                // Ensure the block request is well-formed.
                if start_height >= end_height {
                    bail!("Block request from '{peer_ip}' has an invalid range ({start_height}..{end_height})")
                }
                // Ensure that the block request is within the allowed bounds.
                if end_height - start_height > DataBlocks::<N>::MAXIMUM_NUMBER_OF_BLOCKS as u32 {
                    bail!("Block request from '{peer_ip}' has an excessive range ({start_height}..{end_height})")
                }

                let self_ = self.clone();
                let blocks = match task::spawn_blocking(move || {
                    // Retrieve the blocks within the requested range.
                    match self_.ledger.get_blocks(start_height..end_height) {
                        Ok(blocks) => Ok(Data::Object(DataBlocks(blocks))),
                        Err(error) => bail!("Missing blocks {start_height} to {end_height} from ledger - {error}"),
                    }
                })
                .await
                {
                    Ok(Ok(blocks)) => blocks,
                    Ok(Err(error)) => return Err(error),
                    Err(error) => return Err(anyhow!("[BlockRequest] {error}")),
                };

                let self_ = self.clone();
                tokio::spawn(async move {
                    // Send the `BlockResponse` message to the peer.
                    let event = Event::BlockResponse(BlockResponse { request: block_request, blocks });
                    Transport::send(&self_, peer_ip, event).await;
                });
                Ok(())
            }
            Event::BlockResponse(block_response) => {
                // If a sync sender was provided, then process the block response.
                if let Some(sync_sender) = self.sync_sender.get() {
                    // Retrieve the block response.
                    let BlockResponse { request, blocks } = block_response;
                    // Perform the deferred non-blocking deserialization of the blocks.
                    let blocks = blocks.deserialize().await.map_err(|error| anyhow!("[BlockResponse] {error}"))?;
                    // Ensure the block response is well-formed.
                    blocks.ensure_response_is_well_formed(peer_ip, request.start_height, request.end_height)?;
                    // Send the blocks to the sync module.
                    return sync_sender.advance_with_sync_blocks(peer_ip, blocks.0).await;
                }
                Ok(())
            }
            Event::CertificateRequest(certificate_request) => {
                // If a sync sender was provided, send the certificate request to the sync module.
                if let Some(sync_sender) = self.sync_sender.get() {
                    // Send the certificate request to the sync module.
                    let _ = sync_sender.tx_certificate_request.send((peer_ip, certificate_request)).await;
                }
                Ok(())
            }
            Event::CertificateResponse(certificate_response) => {
                // If a sync sender was provided, send the certificate response to the sync module.
                if let Some(sync_sender) = self.sync_sender.get() {
                    // Send the certificate response to the sync module.
                    let _ = sync_sender.tx_certificate_response.send((peer_ip, certificate_response)).await;
                }
                Ok(())
            }
            Event::ChallengeRequest(..) | Event::ChallengeResponse(..) => {
                // Disconnect as the peer is not following the protocol.
                bail!("{CONTEXT} Peer '{peer_ip}' is not following the protocol")
            }
            Event::Disconnect(disconnect) => {
                bail!("{CONTEXT} {:?}", disconnect.reason)
            }
            Event::PrimaryPing(ping) => {
                let PrimaryPing { version, block_locators, primary_certificate, batch_certificates } = ping;

                // Ensure the event version is not outdated.
                if version < Event::<N>::VERSION {
                    bail!("Dropping '{peer_ip}' on event version {version} (outdated)");
                }

                // If a sync sender was provided, update the peer locators.
                if let Some(sync_sender) = self.sync_sender.get() {
                    // Check the block locators are valid, and update the validators in the sync module.
                    if let Err(error) = sync_sender.update_peer_locators(peer_ip, block_locators).await {
                        bail!("Validator '{peer_ip}' sent invalid block locators - {error}");
                    }
                }

                // Send the batch certificates to the primary.
                let _ = self
                    .primary_sender()
                    .tx_primary_ping
                    .send((peer_ip, primary_certificate, batch_certificates))
                    .await;
                Ok(())
            }
            Event::TransmissionRequest(request) => {
                // TODO (howardwu): Add rate limiting checks on this event, on a per-peer basis.
                // Determine the worker ID.
                let Ok(worker_id) = assign_to_worker(request.transmission_id, self.num_workers()) else {
                    warn!("{CONTEXT} Unable to assign transmission ID '{}' to a worker", request.transmission_id);
                    return Ok(());
                };
                // Send the transmission request to the worker.
                if let Some(sender) = self.get_worker_sender(worker_id) {
                    // Send the transmission request to the worker.
                    let _ = sender.tx_transmission_request.send((peer_ip, request)).await;
                }
                Ok(())
            }
            Event::TransmissionResponse(response) => {
                // Determine the worker ID.
                let Ok(worker_id) = assign_to_worker(response.transmission_id, self.num_workers()) else {
                    warn!("{CONTEXT} Unable to assign transmission ID '{}' to a worker", response.transmission_id);
                    return Ok(());
                };
                // Send the transmission response to the worker.
                if let Some(sender) = self.get_worker_sender(worker_id) {
                    // Send the transmission response to the worker.
                    let _ = sender.tx_transmission_response.send((peer_ip, response)).await;
                }
                Ok(())
            }
            Event::ValidatorsRequest(_) => {
                // Retrieve the connected peers.
                let mut connected_peers: Vec<_> = match self.dev.is_some() {
                    // In development mode, relax the validity requirements to make operating devnets more flexible.
                    true => self.connected_peers.read().iter().copied().collect(),
                    // In production mode, ensure the peer IPs are valid.
                    false => {
                        self.connected_peers.read().iter().copied().filter(|ip| self.is_valid_peer_ip(*ip)).collect()
                    }
                };
                // Shuffle the connected peers.
                connected_peers.shuffle(&mut rand::thread_rng());

                let self_ = self.clone();
                tokio::spawn(async move {
                    // Initialize the validators.
                    let mut validators = IndexMap::with_capacity(MAX_VALIDATORS_TO_SEND);
                    // Iterate over the validators.
                    for validator_ip in connected_peers.into_iter().take(MAX_VALIDATORS_TO_SEND) {
                        // Retrieve the validator address.
                        if let Some(validator_address) = self_.resolver.get_address(validator_ip) {
                            // Add the validator to the list of validators.
                            validators.insert(validator_ip, validator_address);
                        }
                    }
                    // Send the validators response to the peer.
                    let event = Event::ValidatorsResponse(ValidatorsResponse { validators });
                    Transport::send(&self_, peer_ip, event).await;
                });
                Ok(())
            }
            Event::ValidatorsResponse(response) => {
                let ValidatorsResponse { validators } = response;
                // Ensure the number of validators is not too large.
                ensure!(validators.len() <= MAX_VALIDATORS_TO_SEND, "{CONTEXT} Received too many validators");
                // Ensure the cache contains a validators request for this peer.
                if !self.cache.contains_outbound_validators_request(peer_ip) {
                    bail!("{CONTEXT} Received validators response from '{peer_ip}' without a validators request")
                }
                // Decrement the number of validators requests for this peer.
                self.cache.decrement_outbound_validators_requests(peer_ip);

                // If the number of connected validators is less than the minimum, connect to more validators.
                if self.number_of_connected_peers() < MIN_CONNECTED_VALIDATORS {
                    // Attempt to connect to any validators that are not already connected.
                    let self_ = self.clone();
                    tokio::spawn(async move {
                        for (validator_ip, validator_address) in validators {
                            if self_.dev.is_some() {
                                // Ensure the validator IP is not this node.
                                if self_.is_local_ip(validator_ip) {
                                    continue;
                                }
                            } else {
                                // Ensure the validator IP is not this node and is well-formed.
                                if !self_.is_valid_peer_ip(validator_ip) {
                                    continue;
                                }
                            }

                            // Ensure the validator address is not this node.
                            if self_.account.address() == validator_address {
                                continue;
                            }
                            // Ensure the validator IP is not already connected or connecting.
                            if self_.is_connected_ip(validator_ip) || self_.is_connecting_ip(validator_ip) {
                                continue;
                            }
                            // Ensure the validator address is not already connected.
                            if self_.is_connected_address(validator_address) {
                                continue;
                            }
                            // Ensure the validator address is an authorized validator.
                            if !self_.is_authorized_validator_address(validator_address) {
                                continue;
                            }
                            // Attempt to connect to the validator.
                            self_.connect(validator_ip);
                        }
                    });
                }
                Ok(())
            }
            Event::WorkerPing(ping) => {
                // Ensure the number of transmissions is not too large.
                ensure!(
                    ping.transmission_ids.len() <= MAX_TRANSMISSIONS_PER_WORKER_PING,
                    "{CONTEXT} Received too many transmissions"
                );
                // Retrieve the number of workers.
                let num_workers = self.num_workers();
                // Iterate over the transmission IDs.
                for transmission_id in ping.transmission_ids.into_iter() {
                    // Determine the worker ID.
                    let Ok(worker_id) = assign_to_worker(transmission_id, num_workers) else {
                        warn!("{CONTEXT} Unable to assign transmission ID '{transmission_id}' to a worker");
                        continue;
                    };
                    // Send the transmission ID to the worker.
                    if let Some(sender) = self.get_worker_sender(worker_id) {
                        // Send the transmission ID to the worker.
                        let _ = sender.tx_worker_ping.send((peer_ip, transmission_id)).await;
                    }
                }
                Ok(())
            }
        }
    }

    /// Disconnects from the given peer IP, if the peer is connected.
    pub fn disconnect(&self, peer_ip: SocketAddr) -> JoinHandle<()> {
        let gateway = self.clone();
        tokio::spawn(async move {
            if let Some(peer_addr) = gateway.resolver.get_ambiguous(peer_ip) {
                // Disconnect from this peer.
                let _disconnected = gateway.tcp.disconnect(peer_addr).await;
                debug_assert!(_disconnected);
            }
        })
    }

    /// Initialize a new instance of the heartbeat.
    fn initialize_heartbeat(&self) {
        let self_clone = self.clone();
        self.spawn(async move {
            // Sleep briefly to ensure the other nodes are ready to connect.
            tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
            info!("Starting the heartbeat of the gateway...");
            loop {
                // Process a heartbeat in the router.
                self_clone.heartbeat();
                // Sleep for the heartbeat interval.
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });
    }

    /// Spawns a task with the given future; it should only be used for long-running tasks.
    #[allow(dead_code)]
    fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        self.handles.lock().push(tokio::spawn(future));
    }

    /// Shuts down the gateway.
    pub async fn shut_down(&self) {
        info!("Shutting down the gateway...");
        // Abort the tasks.
        self.handles.lock().iter().for_each(|handle| handle.abort());
        // Close the listener.
        self.tcp.shut_down().await;
    }
}

impl<N: Network> Gateway<N> {
    /// Handles the heartbeat request.
    fn heartbeat(&self) {
        self.log_connected_validators();
        // Keep the trusted validators connected.
        self.handle_trusted_validators();
        // Removes any validators that not in the current committee.
        self.handle_unauthorized_validators();
        // If the number of connected validators is less than the minimum, send a `ValidatorsRequest`.
        self.handle_min_connected_validators();
    }

    /// Logs the connected validators.
    fn log_connected_validators(&self) {
        // Log the connected validators.
        let validators = self.connected_peers().read().clone();
        // Resolve the total number of connectable validators.
        let validators_total = self.ledger.current_committee().map_or(0, |c| c.num_members().saturating_sub(1));
        // Format the total validators message.
        let total_validators = format!("(of {validators_total} bonded validators)").dimmed();
        // Construct the connections message.
        let connections_msg = match validators.len() {
            0 => "No connected validators".to_string(),
            num_connected => format!("Connected to {num_connected} validators {total_validators}"),
        };
        // Log the connected validators.
        info!("{connections_msg}");
        for peer_ip in validators {
            let address = self.resolver.get_address(peer_ip).map_or("Unknown".to_string(), |a| a.to_string());
            debug!("{}", format!("  {peer_ip} - {address}").dimmed());
        }
    }

    /// This function attempts to connect to any disconnected trusted validators.
    fn handle_trusted_validators(&self) {
        // Ensure that the trusted nodes are connected.
        for validator_ip in &self.trusted_validators {
            // If the trusted_validator is not connected, attempt to connect to it.
            if !self.is_local_ip(*validator_ip)
                && !self.is_connecting_ip(*validator_ip)
                && !self.is_connected_ip(*validator_ip)
            {
                // Attempt to connect to the trusted validator.
                self.connect(*validator_ip);
            }
        }
    }

    /// This function attempts to disconnect any validators that are not in the current committee.
    fn handle_unauthorized_validators(&self) {
        let self_ = self.clone();
        tokio::spawn(async move {
            // Retrieve the connected validators.
            let validators = self_.connected_peers().read().clone();
            // Iterate over the validator IPs.
            for peer_ip in validators {
                // Disconnect any validator that is not in the current committee.
                if !self_.is_authorized_validator_ip(peer_ip) {
                    warn!("{CONTEXT} Disconnecting from '{peer_ip}' - Validator is not in the current committee");
                    Transport::send(&self_, peer_ip, DisconnectReason::ProtocolViolation.into()).await;
                    // Disconnect from this peer.
                    self_.disconnect(peer_ip);
                }
            }
        });
    }

    /// This function sends a `ValidatorsRequest` to a random validator,
    /// if the number of connected validators is less than the minimum.
    fn handle_min_connected_validators(&self) {
        // If the number of connected validators is less than the minimum, send a `ValidatorsRequest`.
        if self.number_of_connected_peers() < MIN_CONNECTED_VALIDATORS {
            // Retrieve the connected validators.
            let validators = self.connected_peers().read().clone();
            // If there are no validator IPs to connect to, return early.
            if validators.is_empty() {
                return;
            }
            // Select a random validator IP.
            if let Some(validator_ip) = validators.into_iter().choose(&mut rand::thread_rng()) {
                let self_ = self.clone();
                tokio::spawn(async move {
                    // Increment the number of outbound validators requests for this validator.
                    self_.cache.increment_outbound_validators_requests(validator_ip);
                    // Send a `ValidatorsRequest` to the validator.
                    let _ = Transport::send(&self_, validator_ip, Event::ValidatorsRequest(ValidatorsRequest)).await;
                });
            }
        }
    }
}

#[async_trait]
impl<N: Network> Transport<N> for Gateway<N> {
    /// Sends the given event to specified peer.
    ///
    /// This method is rate limited to prevent spamming the peer.
    ///
    /// This function returns as soon as the event is queued to be sent,
    /// without waiting for the actual delivery; instead, the caller is provided with a [`oneshot::Receiver`]
    /// which can be used to determine when and whether the event has been delivered.
    async fn send(&self, peer_ip: SocketAddr, event: Event<N>) -> Option<oneshot::Receiver<io::Result<()>>> {
        macro_rules! send {
            ($self:ident, $cache_map:ident, $interval:expr, $freq:ident) => {{
                // Rate limit the number of certificate requests sent to the peer.
                while $self.cache.$cache_map(peer_ip, $interval) > $self.$freq() {
                    // Sleep for a short period of time to allow the cache to clear.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                // Send the event to the peer.
                $self.send_inner(peer_ip, event)
            }};
        }

        // If the event type is a certificate request, increment the cache.
        if matches!(event, Event::CertificateRequest(_)) | matches!(event, Event::CertificateResponse(_)) {
            // Update the outbound event cache. This is necessary to ensure we don't under count the outbound events.
            self.cache.insert_outbound_event(peer_ip, CACHE_EVENTS_INTERVAL);
            // Send the event to the peer.
            send!(self, insert_outbound_certificate, CACHE_REQUESTS_INTERVAL, max_cache_certificates)
        }
        // If the event type is a transmission request, increment the cache.
        else if matches!(event, Event::TransmissionRequest(_)) | matches!(event, Event::TransmissionResponse(_)) {
            // Update the outbound event cache. This is necessary to ensure we don't under count the outbound events.
            self.cache.insert_outbound_event(peer_ip, CACHE_EVENTS_INTERVAL);
            // Send the event to the peer.
            send!(self, insert_outbound_transmission, CACHE_REQUESTS_INTERVAL, max_cache_transmissions)
        }
        // Otherwise, employ a general rate limit.
        else {
            // Send the event to the peer.
            send!(self, insert_outbound_event, CACHE_EVENTS_INTERVAL, max_cache_events)
        }
    }

    /// Broadcasts the given event to all connected peers.
    // TODO(ljedrz): the event should be checked for the presence of Data::Object, and
    // serialized in advance if it's there.
    fn broadcast(&self, event: Event<N>) {
        // Ensure there are connected peers.
        if self.number_of_connected_peers() > 0 {
            let self_ = self.clone();
            let connected_peers = self.connected_peers.read().clone();
            tokio::spawn(async move {
                // Iterate through all connected peers.
                for peer_ip in connected_peers {
                    // Send the event to the peer.
                    let _ = Transport::send(&self_, peer_ip, event.clone()).await;
                }
            });
        }
    }
}

impl<N: Network> P2P for Gateway<N> {
    /// Returns a reference to the TCP instance.
    fn tcp(&self) -> &Tcp {
        &self.tcp
    }
}

#[async_trait]
impl<N: Network> Reading for Gateway<N> {
    type Codec = EventCodec<N>;
    type Message = Event<N>;

    /// The maximum queue depth of incoming messages for a single peer.
    const MESSAGE_QUEUE_DEPTH: usize =
        2 * MAX_GC_ROUNDS as usize * Committee::<N>::MAX_COMMITTEE_SIZE as usize * MAX_TRANSMISSIONS_PER_BATCH;

    /// Creates a [`Decoder`] used to interpret messages from the network.
    /// The `side` param indicates the connection side **from the node's perspective**.
    fn codec(&self, _peer_addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }

    /// Processes a message received from the network.
    async fn process_message(&self, peer_addr: SocketAddr, message: Self::Message) -> io::Result<()> {
        // Process the message. Disconnect if the peer violated the protocol.
        if let Err(error) = self.inbound(peer_addr, message).await {
            if let Some(peer_ip) = self.resolver.get_listener(peer_addr) {
                warn!("{CONTEXT} Disconnecting from '{peer_ip}' - {error}");
                let self_ = self.clone();
                tokio::spawn(async move {
                    Transport::send(&self_, peer_ip, DisconnectReason::ProtocolViolation.into()).await;
                    // Disconnect from this peer.
                    self_.disconnect(peer_ip);
                });
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<N: Network> Writing for Gateway<N> {
    type Codec = EventCodec<N>;
    type Message = Event<N>;

    /// The maximum queue depth of outgoing messages for a single peer.
    const MESSAGE_QUEUE_DEPTH: usize =
        2 * MAX_GC_ROUNDS as usize * Committee::<N>::MAX_COMMITTEE_SIZE as usize * MAX_TRANSMISSIONS_PER_BATCH;

    /// Creates an [`Encoder`] used to write the outbound messages to the target stream.
    /// The `side` parameter indicates the connection side **from the node's perspective**.
    fn codec(&self, _peer_addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }
}

#[async_trait]
impl<N: Network> Disconnect for Gateway<N> {
    /// Any extra operations to be performed during a disconnect.
    async fn handle_disconnect(&self, peer_addr: SocketAddr) {
        if let Some(peer_ip) = self.resolver.get_listener(peer_addr) {
            self.remove_connected_peer(peer_ip);
        }
    }
}

#[async_trait]
impl<N: Network> OnConnect for Gateway<N> {
    async fn on_connect(&self, _peer_addr: SocketAddr) {
        return;
    }
}

#[async_trait]
impl<N: Network> Handshake for Gateway<N> {
    /// Performs the handshake protocol.
    async fn perform_handshake(&self, mut connection: Connection) -> io::Result<Connection> {
        // Perform the handshake.
        let peer_addr = connection.addr();
        let peer_side = connection.side();
        let stream = self.borrow_stream(&mut connection);

        // If this is an inbound connection, we log it, but don't know the listening address yet.
        // Otherwise, we can immediately register the listening address.
        let mut peer_ip = if peer_side == ConnectionSide::Initiator {
            debug!("{CONTEXT} Gateway received a connection request from '{peer_addr}'");
            None
        } else {
            debug!("{CONTEXT} Gateway is connecting to {peer_addr}...");
            Some(peer_addr)
        };

        // Perform the handshake; we pass on a mutable reference to peer_ip in case the process is broken at any point in time.
        let handshake_result = if peer_side == ConnectionSide::Responder {
            self.handshake_inner_initiator(peer_addr, peer_ip, stream).await
        } else {
            self.handshake_inner_responder(peer_addr, &mut peer_ip, stream).await
        };

        // Remove the address from the collection of connecting peers (if the handshake got to the point where it's known).
        if let Some(ip) = peer_ip {
            self.connecting_peers.lock().shift_remove(&ip);
        }
        let (ref peer_ip, _) = handshake_result?;
        info!("{CONTEXT} Gateway is connected to '{peer_ip}'");

        Ok(connection)
    }
}

/// A macro unwrapping the expected handshake event or returning an error for unexpected events.
macro_rules! expect_event {
    ($event_ty:path, $framed:expr, $peer_addr:expr) => {
        match $framed.try_next().await? {
            // Received the expected event, proceed.
            Some($event_ty(data)) => {
                trace!("{CONTEXT} Gateway received '{}' from '{}'", data.name(), $peer_addr);
                data
            }
            // Received a disconnect event, abort.
            Some(Event::Disconnect(reason)) => {
                return Err(error(format!("{CONTEXT} '{}' disconnected: {reason:?}", $peer_addr)));
            }
            // Received an unexpected event, abort.
            Some(ty) => {
                return Err(error(format!(
                    "{CONTEXT} '{}' did not follow the handshake protocol: received {:?} instead of {}",
                    $peer_addr,
                    ty.name(),
                    stringify!($event_ty),
                )))
            }
            // Received nothing.
            None => {
                return Err(error(format!(
                    "{CONTEXT} '{}' disconnected before sending {:?}",
                    $peer_addr,
                    stringify!($event_ty)
                )))
            }
        }
    };
}

/// Send the given message to the peer.
async fn send_event<N: Network>(
    framed: &mut Framed<&mut TcpStream, EventCodec<N>>,
    peer_addr: SocketAddr,
    event: Event<N>,
) -> io::Result<()> {
    trace!("{CONTEXT} Gateway is sending '{}' to '{peer_addr}'", event.name());
    framed.send(event).await
}

impl<N: Network> Gateway<N> {
    /// The connection initiator side of the handshake.
    async fn handshake_inner_initiator<'a>(
        &'a self,
        peer_addr: SocketAddr,
        peer_ip: Option<SocketAddr>,
        stream: &'a mut TcpStream,
    ) -> io::Result<(SocketAddr, Framed<&mut TcpStream, EventCodec<N>>)> {
        // This value is immediately guaranteed to be present, so it can be unwrapped.
        let peer_ip = peer_ip.unwrap();

        // Construct the stream.
        let mut framed = Framed::new(stream, EventCodec::<N>::handshake());

        // Initialize an RNG.
        let rng = &mut rand::rngs::OsRng;

        /* Step 1: Send the challenge request. */

        // Sample a random nonce.
        let our_nonce = rng.gen();
        // Send a challenge request to the peer.
        let our_request = ChallengeRequest::new(self.local_ip().port(), self.account.address(), our_nonce);
        send_event(&mut framed, peer_addr, Event::ChallengeRequest(our_request)).await?;

        /* Step 2: Receive the peer's challenge response followed by the challenge request. */

        // Listen for the challenge response message.
        let peer_response = expect_event!(Event::ChallengeResponse, framed, peer_addr);
        // Listen for the challenge request message.
        let peer_request = expect_event!(Event::ChallengeRequest, framed, peer_addr);

        // Verify the challenge response. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) =
            self.verify_challenge_response(peer_addr, peer_request.address, peer_response, our_nonce).await
        {
            send_event(&mut framed, peer_addr, reason.into()).await?;
            return Err(error(format!("Dropped '{peer_addr}' for reason: {reason:?}")));
        }
        // Verify the challenge request. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) = self.verify_challenge_request(peer_addr, &peer_request) {
            send_event(&mut framed, peer_addr, reason.into()).await?;
            return Err(error(format!("Dropped '{peer_addr}' for reason: {reason:?}")));
        }

        /* Step 3: Send the challenge response. */

        // Sign the counterparty nonce.
        let response_nonce: u64 = rng.gen();
        let data = [peer_request.nonce.to_le_bytes(), response_nonce.to_le_bytes()].concat();
        let Ok(our_signature) = self.account.sign_bytes(&data, rng) else {
            return Err(error(format!("Failed to sign the challenge request nonce from '{peer_addr}'")));
        };
        // Send the challenge response.
        let our_response = ChallengeResponse { signature: Data::Object(our_signature), nonce: response_nonce };
        send_event(&mut framed, peer_addr, Event::ChallengeResponse(our_response)).await?;

        // Add the peer to the gateway.
        self.insert_connected_peer(peer_ip, peer_addr, peer_request.address);

        Ok((peer_ip, framed))
    }

    /// The connection responder side of the handshake.
    async fn handshake_inner_responder<'a>(
        &'a self,
        peer_addr: SocketAddr,
        peer_ip: &mut Option<SocketAddr>,
        stream: &'a mut TcpStream,
    ) -> io::Result<(SocketAddr, Framed<&mut TcpStream, EventCodec<N>>)> {
        // Construct the stream.
        let mut framed = Framed::new(stream, EventCodec::<N>::handshake());

        /* Step 1: Receive the challenge request. */

        // Listen for the challenge request message.
        let peer_request = expect_event!(Event::ChallengeRequest, framed, peer_addr);

        // Ensure the address is not the same as this node.
        if self.account.address() == peer_request.address {
            return Err(error("Skipping request to connect to self".to_string()));
        }

        // Obtain the peer's listening address.
        *peer_ip = Some(SocketAddr::new(peer_addr.ip(), peer_request.listener_port));
        let peer_ip = peer_ip.unwrap();

        // Knowing the peer's listening address, ensure it is allowed to connect.
        if let Err(forbidden_message) = self.ensure_peer_is_allowed(peer_ip) {
            return Err(error(format!("{forbidden_message}")));
        }
        // Verify the challenge request. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) = self.verify_challenge_request(peer_addr, &peer_request) {
            send_event(&mut framed, peer_addr, reason.into()).await?;
            return Err(error(format!("Dropped '{peer_addr}' for reason: {reason:?}")));
        }

        /* Step 2: Send the challenge response followed by own challenge request. */

        // Initialize an RNG.
        let rng = &mut rand::rngs::OsRng;

        // Sign the counterparty nonce.
        let response_nonce: u64 = rng.gen();
        let data = [peer_request.nonce.to_le_bytes(), response_nonce.to_le_bytes()].concat();
        let Ok(our_signature) = self.account.sign_bytes(&data, rng) else {
            return Err(error(format!("Failed to sign the challenge request nonce from '{peer_addr}'")));
        };
        // Send the challenge response.
        let our_response = ChallengeResponse { signature: Data::Object(our_signature), nonce: response_nonce };
        send_event(&mut framed, peer_addr, Event::ChallengeResponse(our_response)).await?;

        // Sample a random nonce.
        let our_nonce = rng.gen();
        // Send the challenge request.
        let our_request = ChallengeRequest::new(self.local_ip().port(), self.account.address(), our_nonce);
        send_event(&mut framed, peer_addr, Event::ChallengeRequest(our_request)).await?;

        /* Step 3: Receive the challenge response. */

        // Listen for the challenge response message.
        let peer_response = expect_event!(Event::ChallengeResponse, framed, peer_addr);
        // Verify the challenge response. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) =
            self.verify_challenge_response(peer_addr, peer_request.address, peer_response, our_nonce).await
        {
            send_event(&mut framed, peer_addr, reason.into()).await?;
            return Err(error(format!("Dropped '{peer_addr}' for reason: {reason:?}")));
        }
        // Add the peer to the gateway.
        self.insert_connected_peer(peer_ip, peer_addr, peer_request.address);

        Ok((peer_ip, framed))
    }

    /// Verifies the given challenge request. Returns a disconnect reason if the request is invalid.
    fn verify_challenge_request(&self, peer_addr: SocketAddr, event: &ChallengeRequest<N>) -> Option<DisconnectReason> {
        // Retrieve the components of the challenge request.
        let &ChallengeRequest { version, listener_port: _, address, nonce: _ } = event;
        // Ensure the event protocol version is not outdated.
        if version < Event::<N>::VERSION {
            warn!("{CONTEXT} Gateway is dropping '{peer_addr}' on version {version} (outdated)");
            return Some(DisconnectReason::OutdatedClientVersion);
        }
        // Ensure the address is a current committee member.
        if !self.is_authorized_validator_address(address) {
            warn!("{CONTEXT} Gateway is dropping '{peer_addr}' for being an unauthorized validator ({address})");
            return Some(DisconnectReason::ProtocolViolation);
        }
        // Ensure the address is not already connected.
        if self.is_connected_address(address) {
            warn!("{CONTEXT} Gateway is dropping '{peer_addr}' for being already connected ({address})");
            return Some(DisconnectReason::ProtocolViolation);
        }
        None
    }

    /// Verifies the given challenge response. Returns a disconnect reason if the response is invalid.
    async fn verify_challenge_response(
        &self,
        peer_addr: SocketAddr,
        peer_address: Address<N>,
        response: ChallengeResponse<N>,
        expected_nonce: u64,
    ) -> Option<DisconnectReason> {
        // Retrieve the components of the challenge response.
        let ChallengeResponse { signature, nonce } = response;
        // Perform the deferred non-blocking deserialization of the signature.
        let Ok(signature) = spawn_blocking!(signature.deserialize_blocking()) else {
            warn!("{CONTEXT} Gateway handshake with '{peer_addr}' failed (cannot deserialize the signature)");
            return Some(DisconnectReason::InvalidChallengeResponse);
        };
        // Verify the signature.
        if !signature.verify_bytes(&peer_address, &[expected_nonce.to_le_bytes(), nonce.to_le_bytes()].concat()) {
            warn!("{CONTEXT} Gateway handshake with '{peer_addr}' failed (invalid signature)");
            return Some(DisconnectReason::InvalidChallengeResponse);
        }
        None
    }
}

#[cfg(test)]
mod prop_tests {
    use crate::{
        gateway::prop_tests::GatewayAddress::{Dev, Prod},
        helpers::{init_primary_channels, init_worker_channels, Storage},
        Gateway,
        Worker,
        MAX_WORKERS,
        MEMORY_POOL_PORT,
    };
    use snarkos_account::Account;
    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkos_node_tcp::P2P;
    use snarkvm::{
        ledger::committee::{
            prop_tests::{CommitteeContext, ValidatorSet},
            Committee,
        },
        prelude::{PrivateKey, Testnet3},
    };

    use indexmap::IndexMap;
    use proptest::{
        prelude::{any, any_with, Arbitrary, BoxedStrategy, Just, Strategy},
        sample::Selector,
    };
    use std::{
        fmt::{Debug, Formatter},
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    };
    use test_strategy::proptest;

    type CurrentNetwork = Testnet3;

    impl Debug for Gateway<CurrentNetwork> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            // TODO implement Debug properly and move it over to production code
            f.debug_tuple("Gateway").field(&self.account.address()).field(&self.tcp.config()).finish()
        }
    }

    #[derive(Debug, test_strategy::Arbitrary)]
    enum GatewayAddress {
        Dev(u8),
        Prod(Option<SocketAddr>),
    }

    impl GatewayAddress {
        fn ip(&self) -> Option<SocketAddr> {
            if let GatewayAddress::Prod(ip) = self {
                return *ip;
            }
            None
        }

        fn port(&self) -> Option<u16> {
            if let GatewayAddress::Dev(port) = self {
                return Some(*port as u16);
            }
            None
        }
    }

    impl Arbitrary for Gateway<CurrentNetwork> {
        type Parameters = ();
        type Strategy = BoxedStrategy<Gateway<CurrentNetwork>>;

        fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
            any_valid_dev_gateway()
                .prop_map(|(storage, _, private_key, address)| {
                    Gateway::new(
                        Account::try_from(private_key).unwrap(),
                        storage.ledger().clone(),
                        address.ip(),
                        &[],
                        address.port(),
                    )
                    .unwrap()
                })
                .boxed()
        }
    }

    type GatewayInput = (Storage<CurrentNetwork>, CommitteeContext, PrivateKey<CurrentNetwork>, GatewayAddress);

    fn any_valid_dev_gateway() -> BoxedStrategy<GatewayInput> {
        (any::<CommitteeContext>(), any::<Selector>())
            .prop_flat_map(|(context, account_selector)| {
                let CommitteeContext(_, ValidatorSet(validators)) = context.clone();
                (
                    any_with::<Storage<CurrentNetwork>>(context.clone()),
                    Just(context),
                    Just(account_selector.select(validators)),
                    0u8..,
                )
                    .prop_map(|(a, b, c, d)| (a, b, c.private_key, Dev(d)))
            })
            .boxed()
    }

    fn any_valid_prod_gateway() -> BoxedStrategy<GatewayInput> {
        (any::<CommitteeContext>(), any::<Selector>())
            .prop_flat_map(|(context, account_selector)| {
                let CommitteeContext(_, ValidatorSet(validators)) = context.clone();
                (
                    any_with::<Storage<CurrentNetwork>>(context.clone()),
                    Just(context),
                    Just(account_selector.select(validators)),
                    any::<Option<SocketAddr>>(),
                )
                    .prop_map(|(a, b, c, d)| (a, b, c.private_key, Prod(d)))
            })
            .boxed()
    }

    #[proptest]
    fn gateway_dev_initialization(#[strategy(any_valid_dev_gateway())] input: GatewayInput) {
        let (storage, _, private_key, dev) = input;
        let account = Account::try_from(private_key).unwrap();

        let gateway = Gateway::new(account.clone(), storage.ledger().clone(), dev.ip(), &[], dev.port()).unwrap();
        let tcp_config = gateway.tcp().config();
        assert_eq!(tcp_config.listener_ip, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert_eq!(tcp_config.desired_listening_port, Some(MEMORY_POOL_PORT + dev.port().unwrap()));

        let tcp_config = gateway.tcp().config();
        assert_eq!(tcp_config.max_connections, Committee::<CurrentNetwork>::MAX_COMMITTEE_SIZE);
        assert_eq!(gateway.account().address(), account.address());
    }

    #[proptest]
    fn gateway_prod_initialization(#[strategy(any_valid_prod_gateway())] input: GatewayInput) {
        let (storage, _, private_key, dev) = input;
        let account = Account::try_from(private_key).unwrap();

        let gateway = Gateway::new(account.clone(), storage.ledger().clone(), dev.ip(), &[], dev.port()).unwrap();
        let tcp_config = gateway.tcp().config();
        if let Some(socket_addr) = dev.ip() {
            assert_eq!(tcp_config.listener_ip, Some(socket_addr.ip()));
            assert_eq!(tcp_config.desired_listening_port, Some(socket_addr.port()));
        } else {
            assert_eq!(tcp_config.listener_ip, Some(IpAddr::V4(Ipv4Addr::UNSPECIFIED)));
            assert_eq!(tcp_config.desired_listening_port, Some(MEMORY_POOL_PORT));
        }

        let tcp_config = gateway.tcp().config();
        assert_eq!(tcp_config.max_connections, Committee::<CurrentNetwork>::MAX_COMMITTEE_SIZE);
        assert_eq!(gateway.account().address(), account.address());
    }

    #[proptest(async = "tokio")]
    async fn gateway_start(
        #[strategy(any_valid_dev_gateway())] input: GatewayInput,
        #[strategy(0..MAX_WORKERS)] workers_count: u8,
    ) {
        let (storage, committee, private_key, dev) = input;
        let committee = committee.0;
        let worker_storage = storage.clone();
        let account = Account::try_from(private_key).unwrap();

        let gateway = Gateway::new(account, storage.ledger().clone(), dev.ip(), &[], dev.port()).unwrap();

        let (primary_sender, _) = init_primary_channels();

        let (workers, worker_senders) = {
            // Construct a map of the worker senders.
            let mut tx_workers = IndexMap::new();
            let mut workers = IndexMap::new();

            // Initialize the workers.
            for id in 0..workers_count {
                // Construct the worker channels.
                let (tx_worker, rx_worker) = init_worker_channels();
                // Construct the worker instance.
                let ledger = Arc::new(MockLedgerService::new(committee.clone()));
                let worker =
                    Worker::new(id, Arc::new(gateway.clone()), worker_storage.clone(), ledger, Default::default())
                        .unwrap();
                // Run the worker instance.
                worker.run(rx_worker);

                // Add the worker and the worker sender to maps
                workers.insert(id, worker);
                tx_workers.insert(id, tx_worker);
            }
            (workers, tx_workers)
        };

        gateway.run(primary_sender, worker_senders, None).await;
        assert_eq!(
            gateway.local_ip(),
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), MEMORY_POOL_PORT + dev.port().unwrap())
        );
        assert_eq!(gateway.num_workers(), workers.len() as u8);
    }
}
