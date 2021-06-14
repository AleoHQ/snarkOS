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

use crate::*;
use snarkvm_dpc::Storage;

use chrono::{DateTime, Utc};
use metrics::{register_counter, register_gauge};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use rand::{seq::SliceRandom, thread_rng, Rng};
use std::{
    net::SocketAddr,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Arc,
    },
    thread,
};
use tokio::{task, time::sleep};

#[cfg(feature = "prometheus")]
use metrics_exporter_prometheus::PrometheusBuilder;

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
#[repr(u8)]
pub enum State {
    Idle = 0,
    Mining,
    Syncing,
}

#[derive(Default)]
pub struct StateCode(AtomicU8);

/// The internal state of a node.
pub struct InnerNode<S: Storage> {
    /// The node's random numeric identifier.
    pub id: u64,
    /// The current state of the node.
    state: StateCode,
    /// The local address of this node.
    pub local_address: OnceCell<SocketAddr>,
    /// The pre-configured parameters of this node.
    pub config: Config,
    /// The inbound handler of this node.
    pub inbound: Inbound,
    /// The outbound handler of this node.
    pub outbound: Outbound,
    /// The list of connected and disconnected peers of this node.
    pub peer_book: PeerBook,
    /// The sync handler of this node.
    pub sync: OnceCell<Arc<Sync<S>>>,
    /// The node's start-up timestamp.
    pub launched: DateTime<Utc>,
    /// The tasks spawned by the node.
    tasks: Mutex<Vec<task::JoinHandle<()>>>,
    /// The threads spawned by the node.
    threads: Mutex<Vec<thread::JoinHandle<()>>>,
    /// An indicator of whether the node is shutting down.
    shutting_down: AtomicBool,
}

impl<S: Storage> Drop for InnerNode<S> {
    // this won't make a difference in regular scenarios, but will be practical for test
    // purposes, so that there are no lingering tasks
    fn drop(&mut self) {
        // since we're going out of scope, we don't care about holding the read lock here
        // also, the connections are going to be broken automatically, so we only need to
        // take care of the associated tasks here
        for peer_info in self.peer_book.connected_peers().values() {
            for (handle, _abortable) in peer_info.tasks.lock().drain(..).rev() {
                // We're already shutting down, so always abort.
                handle.abort();
            }
        }

        for handle in self.threads.lock().drain(..).rev() {
            let _ = handle.join().map_err(|e| error!("Can't join a thread: {:?}", e));
        }

        for handle in self.tasks.lock().drain(..).rev() {
            handle.abort();
        }
    }
}

/// A core data structure for operating the networking stack of this node.
#[derive(Derivative)]
#[derivative(Clone(bound = ""))]
pub struct Node<S: Storage>(Arc<InnerNode<S>>);

impl<S: Storage> Deref for Node<S> {
    type Target = Arc<InnerNode<S>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S: Storage> Node<S> {
    /// Returns the current state of the node.
    #[inline]
    pub fn state(&self) -> State {
        match self.state.0.load(Ordering::SeqCst) {
            0 => State::Idle,
            1 => State::Mining,
            2 => State::Syncing,
            _ => unreachable!(),
        }
    }

    /// Changes the current state of the node.
    #[inline]
    pub fn set_state(&self, new_state: State) {
        let code = new_state as u8;

        self.state.0.store(code, Ordering::SeqCst);
    }
}

impl<S: Storage + Send + core::marker::Sync + 'static> Node<S> {
    /// Creates a new instance of `Node`.
    pub async fn new(config: Config) -> Result<Self, NetworkError> {
        Ok(Self(Arc::new(InnerNode {
            id: thread_rng().gen(),
            state: Default::default(),
            local_address: Default::default(),
            config,
            inbound: Default::default(),
            outbound: Default::default(),
            peer_book: Default::default(),
            sync: Default::default(),
            launched: Utc::now(),
            tasks: Default::default(),
            threads: Default::default(),
            shutting_down: Default::default(),
        })))
    }

    pub fn set_sync(&mut self, sync: Sync<S>) {
        if self.sync.set(Arc::new(sync)).is_err() {
            panic!("sync was set more than once!");
        }
    }

    /// Returns a reference to the sync objects.
    #[inline]
    pub fn sync(&self) -> Option<&Arc<Sync<S>>> {
        self.sync.get()
    }

    /// Returns a reference to the sync objects, expecting them to be available.
    #[inline]
    pub fn expect_sync(&self) -> &Sync<S> {
        self.sync().expect("no sync!")
    }

    #[inline]
    #[doc(hidden)]
    pub fn has_sync(&self) -> bool {
        self.sync().is_some()
    }

    pub async fn start_services(&self) {
        let node_clone = self.clone();
        let mut receiver = self.inbound.take_receiver();
        let incoming_task = task::spawn(async move {
            let mut cache = Cache::default();

            loop {
                if let Err(e) = node_clone.process_incoming_messages(&mut receiver, &mut cache).await {
                    metrics::increment_counter!(stats::INBOUND_ALL_FAILURES);
                    error!("Node error: {}", e);
                } else {
                    metrics::increment_counter!(stats::INBOUND_ALL_SUCCESSES);
                }
            }
        });
        self.register_task(incoming_task);

        let node_clone = self.clone();
        let peer_sync_interval = self.config.peer_sync_interval();
        let peering_task = task::spawn(async move {
            loop {
                info!("Updating peers");

                node_clone.update_peers();

                sleep(peer_sync_interval).await;
            }
        });
        self.register_task(peering_task);

        let node_clone = self.clone();
        let state_tracking_task = task::spawn(async move {
            loop {
                sleep(std::time::Duration::from_secs(5)).await;

                // Report node's current state.
                trace!("Node state: {:?}", node_clone.state());
            }
        });
        self.register_task(state_tracking_task);

        if self.sync().is_some() {
            let bootnodes = self.config.bootnodes();

            let node_clone = self.clone();
            let mempool_sync_interval = node_clone.expect_sync().mempool_sync_interval();
            let sync_mempool_task = task::spawn(async move {
                loop {
                    if !node_clone.is_syncing_blocks() {
                        // TODO (howardwu): Add some random sync nodes beyond this approach
                        //  to ensure some diversity in mempool state that is fetched.
                        //  For now, this is acceptable because we propogate the mempool to
                        //  all of our connected peers anyways.

                        // The order of preference for the sync node is as follows:
                        //   1. Iterate (in declared order) through the bootnodes:
                        //      a. Check if this node is connected to the specified bootnode in the peer book.
                        //      b. Select the specified bootnode as the sync node if this node is connected to it.
                        //   2. If this node is not connected to any bootnode,
                        //      then select the last seen peer as the sync node.

                        // Step 1.
                        let mut sync_node = None;
                        for bootnode in bootnodes.iter() {
                            if node_clone.peer_book.is_connected(*bootnode) {
                                sync_node = Some(*bootnode);
                                break;
                            }
                        }

                        // Step 2.
                        if sync_node.is_none() {
                            // Select last seen node as block sync node.
                            sync_node = node_clone.peer_book.last_seen();
                        }

                        node_clone.update_memory_pool(sync_node);
                    }

                    sleep(mempool_sync_interval).await;
                }
            });
            self.register_task(sync_mempool_task);

            let node_clone = self.clone();
            let block_sync_interval = node_clone.expect_sync().block_sync_interval();
            let sync_block_task = task::spawn(async move {
                loop {
                    let is_syncing_blocks = node_clone.is_syncing_blocks();
                    let is_sync_expired = node_clone.expect_sync().has_block_sync_expired();

                    // if the node is not currently syncing blocks or an earlier sync attempt has expired,
                    // consider syncing blocks with a peer who has a longer chain
                    if !is_syncing_blocks || is_sync_expired {
                        // if the node's state is `Syncing`, change it to `Idle`, as it means the
                        // previous attempt has expired - the peer has disconnected or was too slow
                        // to deliver the batch of sync blocks
                        if is_syncing_blocks {
                            debug!("An unfinished block sync has expired.");
                            node_clone.set_state(State::Idle);
                        }

                        let mut prospect_sync_nodes = Vec::new();
                        let my_height = node_clone.expect_sync().current_block_height();

                        // Pick a random peer of all the connected ones that claim
                        // to have a longer chain.
                        for (peer, info) in node_clone.peer_book.connected_peers().iter() {
                            // Fetch the current block height of this connected peer.
                            let peer_block_height = info.block_height();

                            if peer_block_height > my_height + 1 {
                                prospect_sync_nodes.push((*peer, peer_block_height));
                            }
                        }

                        let random_sync_peer = prospect_sync_nodes.choose(&mut rand::thread_rng());
                        if let Some((sync_node, peer_height)) = random_sync_peer {
                            // Log the sync job as a trace.
                            trace!(
                                "Preparing to sync from {} with a block height of {} (mine: {}, {} peers with a greater height)",
                                sync_node,
                                peer_height,
                                my_height,
                                prospect_sync_nodes.len()
                            );

                            // Cancel any possibly ongoing sync attempts.
                            node_clone.peer_book.cancel_any_unfinished_syncing();

                            // Begin a new sync attempt.
                            node_clone.register_block_sync_attempt();
                            node_clone.update_blocks(*sync_node);
                        }
                    }

                    sleep(block_sync_interval).await;
                }
            });
            self.register_task(sync_block_task);
        }
    }

    pub fn shut_down(&self) {
        debug!("Shutting down");

        for addr in self.connected_peers() {
            self.disconnect_from_peer(addr);
        }

        for handle in self.threads.lock().drain(..).rev() {
            let _ = handle.join().map_err(|e| error!("Can't join a thread: {:?}", e));
        }

        for handle in self.tasks.lock().drain(..).rev() {
            handle.abort();
        }

        info!("The node can be shut down now");
    }

    pub fn register_task(&self, handle: task::JoinHandle<()>) {
        self.tasks.lock().push(handle);
    }

    pub fn register_thread(&self, handle: thread::JoinHandle<()>) {
        self.threads.lock().push(handle);
    }

    #[inline]
    pub fn local_address(&self) -> Option<SocketAddr> {
        self.local_address.get().copied()
    }

    #[inline]
    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Relaxed)
    }

    /// Sets the local address of the node to the given value.
    #[inline]
    pub fn set_local_address(&self, addr: SocketAddr) {
        self.local_address
            .set(addr)
            .expect("local address was set more than once!");
    }

    #[cfg(feature = "prometheus")]
    pub fn initialize_metrics(&self) {
        debug!("Initializing prometheus metrics");

        let prometheus_builder = PrometheusBuilder::new();

        let (recorder, exporter) = prometheus_builder
            .build_with_exporter()
            .expect("can't build the prometheus exporter");
        metrics::set_boxed_recorder(Box::new(recorder)).expect("can't set the prometheus exporter");

        let metrics_exporter_task = task::spawn(async move {
            exporter.await.expect("can't await the prometheus exporter");
        });
        self.register_task(metrics_exporter_task);
    }

    #[cfg(not(feature = "prometheus"))]
    pub fn initialize_metrics(&self) {
        debug!("Initializing RPC metrics");

        metrics::set_recorder(&NODE_STATS).expect("couldn't initialize the metrics recorder!");
    }

    pub fn register_metrics(&self) {
        register_counter!(crate::INBOUND_ALL_SUCCESSES);
        register_counter!(crate::INBOUND_ALL_FAILURES);
        register_counter!(crate::INBOUND_BLOCKS);
        register_counter!(crate::INBOUND_GETBLOCKS);
        register_counter!(crate::INBOUND_GETMEMORYPOOL);
        register_counter!(crate::INBOUND_GETPEERS);
        register_counter!(crate::INBOUND_GETSYNC);
        register_counter!(crate::INBOUND_MEMORYPOOL);
        register_counter!(crate::INBOUND_PEERS);
        register_counter!(crate::INBOUND_PINGS);
        register_counter!(crate::INBOUND_PONGS);
        register_counter!(crate::INBOUND_SYNCS);
        register_counter!(crate::INBOUND_SYNCBLOCKS);
        register_counter!(crate::INBOUND_TRANSACTIONS);
        register_counter!(crate::INBOUND_UNKNOWN);

        register_counter!(crate::OUTBOUND_ALL_SUCCESSES);
        register_counter!(crate::OUTBOUND_ALL_FAILURES);

        register_counter!(crate::CONNECTIONS_ALL_ACCEPTED);
        register_counter!(crate::CONNECTIONS_ALL_INITIATED);
        register_counter!(crate::CONNECTIONS_ALL_REJECTED);
        register_gauge!(crate::CONNECTIONS_CONNECTING);
        register_gauge!(crate::CONNECTIONS_CONNECTED);
        register_gauge!(crate::CONNECTIONS_DISCONNECTED);

        register_counter!(crate::HANDSHAKES_FAILURES_INIT);
        register_counter!(crate::HANDSHAKES_FAILURES_RESP);
        register_counter!(crate::HANDSHAKES_SUCCESSES_INIT);
        register_counter!(crate::HANDSHAKES_SUCCESSES_RESP);
        register_counter!(crate::HANDSHAKES_TIMEOUTS_INIT);
        register_counter!(crate::HANDSHAKES_TIMEOUTS_RESP);

        register_gauge!(crate::QUEUES_INBOUND);
        register_gauge!(crate::QUEUES_OUTBOUND);

        register_counter!(crate::MISC_BLOCK_HEIGHT);
        register_counter!(crate::MISC_BLOCKS_MINED);
        register_counter!(crate::MISC_DUPLICATE_BLOCKS);
        register_counter!(crate::MISC_DUPLICATE_SYNC_BLOCKS);
        register_counter!(crate::MISC_RPC_REQUESTS);

        // The node can already be at some non-zero height.
        if let Some(sync) = self.sync() {
            metrics::counter!(crate::MISC_BLOCK_HEIGHT, sync.current_block_height() as u64);
        }
    }
}
