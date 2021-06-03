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

use crate::{message::*, NetworkError, Node};
use snarkos_consensus::memory_pool::Entry;
use snarkos_storage::Storage;
use snarkvm_dpc::base_dpc::instantiated::Tx;
use snarkvm_utilities::{
    bytes::{FromBytes, ToBytes},
    to_bytes,
};

use std::net::SocketAddr;

impl<S: Storage> Node<S> {
    ///
    /// Triggers the memory pool sync with a selected peer.
    ///
    pub async fn update_memory_pool(&self, sync_node: Option<SocketAddr>) {
        if let Some(sync_node) = sync_node {
            info!("Updating memory pool from {}", sync_node);

            self.peer_book.send_to(sync_node, Payload::GetMemoryPool).await;
        } else {
            debug!("No sync node is registered, memory pool could not be synced");
        }
    }

    ///
    /// Broadcast memory pool transaction to connected peers.
    ///
    pub(crate) async fn propagate_memory_pool_transaction(
        &self,
        transaction_bytes: Vec<u8>,
        transaction_sender: SocketAddr,
    ) {
        debug!("Propagating a memory pool transaction to connected peers");

        let local_address = self.local_address().unwrap();

        for remote_address in self.connected_peers() {
            if remote_address != transaction_sender && remote_address != local_address {
                // Send a `Transaction` message to the connected peer.
                self.peer_book
                    .send_to(remote_address, Payload::Transaction(transaction_bytes.clone()))
                    .await;
            }
        }
    }

    ///
    /// Verifies a received memory pool transaction, adds it to the memory pool,
    /// and propagates it to peers.
    ///
    pub(crate) async fn received_memory_pool_transaction(
        &self,
        source: SocketAddr,
        transaction: Vec<u8>,
    ) -> Result<(), NetworkError> {
        if let Ok(tx) = Tx::read(&*transaction) {
            let insertion = {
                let storage = self.expect_sync().storage();

                if !self.expect_sync().consensus.verify_transaction(&tx).await? {
                    error!("Received a transaction that was invalid");
                    return Ok(());
                }

                if tx.value_balance.is_negative() {
                    error!("Received a transaction that was a coinbase transaction");
                    return Ok(());
                }

                let entry = Entry::<Tx> {
                    size_in_bytes: transaction.len(),
                    transaction: tx,
                };

                self.expect_sync().memory_pool().insert(storage, entry).await
            };

            if let Ok(inserted) = insertion {
                if inserted.is_some() {
                    info!("Transaction added to memory pool.");
                    self.propagate_memory_pool_transaction(transaction, source).await;
                }
            }
        }

        Ok(())
    }

    /// A peer has requested our memory pool transactions.
    pub(crate) async fn received_get_memory_pool(&self, remote_address: SocketAddr) {
        // TODO (howardwu): This should have been written with Rayon - it is easily parallelizable.
        let transactions = {
            let mut txs = vec![];

            for entry in self.expect_sync().memory_pool().transactions.inner().values() {
                if let Ok(transaction_bytes) = to_bytes![entry.transaction] {
                    txs.push(transaction_bytes);
                }
            }

            txs
        };

        if !transactions.is_empty() {
            // Send a `MemoryPool` message to the connected peer.
            self.peer_book
                .send_to(remote_address, Payload::MemoryPool(transactions))
                .await;
        }
    }

    /// A peer has sent us their memory pool transactions.
    pub(crate) async fn received_memory_pool(&self, transactions: Vec<Vec<u8>>) -> Result<(), NetworkError> {
        let memory_pool = self.expect_sync().memory_pool();
        let storage = self.expect_sync().storage();

        for transaction_bytes in transactions {
            let transaction: Tx = Tx::read(&transaction_bytes[..])?;
            let entry = Entry::<Tx> {
                size_in_bytes: transaction_bytes.len(),
                transaction,
            };

            if let Ok(Some(txid)) = memory_pool.insert(&storage, entry).await {
                debug!(
                    "Transaction added to memory pool with txid: {:?}",
                    hex::encode(txid.clone())
                );
            }
        }

        // Cleanse and store transactions once batch has been received.
        debug!("Cleansing memory pool transactions in database");
        memory_pool
            .cleanse(&storage)
            .await
            .unwrap_or_else(|error| debug!("Failed to cleanse memory pool transactions in database {}", error));
        debug!("Storing memory pool transactions in database");
        memory_pool
            .store(&storage)
            .await
            .unwrap_or_else(|error| debug!("Failed to store memory pool transaction in database {}", error));

        Ok(())
    }
}
