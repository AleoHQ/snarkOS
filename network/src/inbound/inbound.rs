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

use crate::{errors::NetworkError, message::*, stats, Cache, Node, Receiver, Sender, State};

use std::time::Duration;

use snarkos_storage::Storage;
use tokio::{
    net::TcpListener,
    sync::{mpsc::error::TrySendError, Mutex},
    task,
};

/// A stateless component for handling inbound network traffic.
#[derive(Debug)]
pub struct Inbound {
    /// The producer for sending inbound messages to the server.
    pub(crate) sender: Sender,
    /// The consumer for receiving inbound messages to the server.
    receiver: Mutex<Option<Receiver>>,
}

impl Default for Inbound {
    fn default() -> Self {
        // Initialize the sender and receiver.
        let (sender, receiver) = tokio::sync::mpsc::channel(crate::INBOUND_CHANNEL_DEPTH);

        Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
        }
    }
}

impl Inbound {
    #[inline]
    pub(crate) async fn take_receiver(&self) -> Receiver {
        self.receiver
            .lock()
            .await
            .take()
            .expect("The Inbound Receiver had already been taken!")
    }
}

impl<S: Storage> Node<S> {
    /// This method handles new inbound connection requests.
    pub async fn listen(&self) -> Result<(), NetworkError> {
        let listener = TcpListener::bind(&self.config.desired_address).await?;
        let own_listener_address = listener.local_addr()?;

        self.set_local_address(own_listener_address);
        info!("Initializing listener for node ({:x})", self.id);

        let node_clone = self.clone();
        let listener_handle = task::spawn(async move {
            info!("Listening for nodes at {}", own_listener_address);

            loop {
                match listener.accept().await {
                    Ok((stream, remote_address)) => {
                        if !node_clone.can_connect() {
                            continue;
                        }
                        let node_clone = node_clone.clone();
                        tokio::spawn(async move {
                            match node_clone
                                .peer_book
                                .receive_connection(node_clone.clone(), remote_address, stream)
                                .await
                            {
                                Ok(_) => (),
                                Err(e) => {
                                    error!("Failed to receive a connection: {}", e);
                                }
                            }
                        });

                        // add a tiny delay to avoid connecting above the limit
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(e) => error!("Failed to accept a connection: {}", e),
                }
                metrics::increment_counter!(stats::CONNECTIONS_ALL_ACCEPTED);
            }
        });

        self.register_task(listener_handle);

        Ok(())
    }

    pub async fn process_incoming_messages(
        &self,
        receiver: &mut Receiver,
        cache: &mut Cache,
    ) -> Result<(), NetworkError> {
        let Message { direction, payload } = receiver.recv().await.ok_or(NetworkError::ReceiverFailedToParse)?;

        metrics::decrement_gauge!(stats::QUEUES_INBOUND, 1.0);

        let source = if let Direction::Inbound(addr) = direction {
            addr
        } else {
            unreachable!("All messages processed sent to the inbound receiver are Inbound");
        };

        // Check if the message hasn't already been processed recently if it's a `Block`.
        // The node should also reject them while syncing, as it is bound to receive them later.
        if matches!(payload, Payload::Block(..)) && (self.state() == State::Syncing || cache.contains(&payload)) {
            return Ok(());
        }

        match payload {
            Payload::Transaction(transaction) => {
                metrics::increment_counter!(stats::INBOUND_TRANSACTIONS);

                if self.sync().is_some() {
                    self.received_memory_pool_transaction(source, transaction).await?;
                }
            }
            Payload::Block(block) => {
                metrics::increment_counter!(stats::INBOUND_BLOCKS);

                if self.sync().is_some() {
                    self.received_block(source, block, true).await?;
                }
            }
            Payload::SyncBlock(block) => {
                metrics::increment_counter!(stats::INBOUND_SYNCBLOCKS);

                if self.sync().is_some() {
                    self.received_block(source, block, false).await?;

                    // Update the peer and possibly finish the sync process.
                    if let Some(peer) = self.peer_book.get_peer_handle(source) {
                        peer.got_sync_block().await;
                    }
                }
            }
            Payload::GetBlocks(hashes) => {
                metrics::increment_counter!(stats::INBOUND_GETBLOCKS);

                if self.sync().is_some() {
                    self.received_get_blocks(source, hashes).await?;
                }
            }
            Payload::GetMemoryPool => {
                metrics::increment_counter!(stats::INBOUND_GETMEMORYPOOL);

                if self.sync().is_some() {
                    self.received_get_memory_pool(source).await;
                }
            }
            Payload::MemoryPool(mempool) => {
                metrics::increment_counter!(stats::INBOUND_MEMORYPOOL);

                if self.sync().is_some() {
                    self.received_memory_pool(mempool).await?;
                }
            }
            Payload::GetSync(getsync) => {
                metrics::increment_counter!(stats::INBOUND_GETSYNC);

                if self.sync().is_some() {
                    self.received_get_sync(source, getsync).await?;
                }
            }
            Payload::Sync(sync) => {
                metrics::increment_counter!(stats::INBOUND_SYNCS);

                if self.sync().is_some() {
                    if sync.is_empty() {
                        // An empty `Sync` is unexpected, as `GetSync` requests are only
                        // sent to peers that declare a greater block height.
                        warn!("{} doesn't have sync blocks to share", source);
                        if let Some(peer) = self.peer_book.get_peer_handle(source) {
                            peer.soft_fail().await;
                        }
                    } else {
                        trace!("Received {} sync block hashes from {}", sync.len(), source);
                        self.received_sync(source, sync).await;
                    }
                }
            }
            Payload::GetPeers => {
                metrics::increment_counter!(stats::INBOUND_GETPEERS);

                self.send_peers(source).await;
            }
            Payload::Peers(peers) => {
                metrics::increment_counter!(stats::INBOUND_PEERS);

                self.process_inbound_peers(peers).await;
            }
            Payload::Ping(_) | Payload::Pong => {
                // Skip as this case is already handled with priority in inbound_handler
                unreachable!()
            }
            Payload::Unknown => {
                metrics::increment_counter!(stats::INBOUND_UNKNOWN);
                warn!("Unknown payload received; this could indicate that the client you're using is out-of-date");
            }
        }

        Ok(())
    }

    #[inline]
    pub(crate) fn route(&self, response: Message) {
        match self.inbound.sender.try_send(response) {
            Err(TrySendError::Full(msg)) => {
                metrics::increment_counter!(stats::INBOUND_ALL_FAILURES);
                error!("Failed to route a {}: the inbound channel is full", msg);
            }
            Err(TrySendError::Closed(msg)) => {
                // TODO: this shouldn't happen, but is critical if it does
                error!("Failed to route a {}: the inbound channel is closed", msg);
            }
            Ok(_) => {
                metrics::increment_gauge!(stats::QUEUES_INBOUND, 1.0);
            }
        }
    }
}
