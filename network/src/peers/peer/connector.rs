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

use std::{io::ErrorKind, time::Duration};

use futures::{select, FutureExt};
use snarkos_storage::Storage;
use tokio::{net::TcpStream, sync::mpsc};

use super::PeerAction;
use crate::{stats, NetworkError, Node, Peer, PeerEvent, PeerEventData, PeerHandle, Version};

use super::network::PeerNetwork;

const CONNECTION_TIMEOUT_SECS: u64 = 3;

impl Peer {
    async fn inner_connect(&mut self, our_version: Version) -> Result<PeerNetwork, NetworkError> {
        metrics::increment_gauge!(stats::CONNECTIONS_CONNECTING, 1.0);
        let _x = defer::defer(|| metrics::decrement_gauge!(stats::CONNECTIONS_CONNECTING, 1.0));

        let tcp_stream;
        select! {
            stream = TcpStream::connect(self.address).fuse() => {
                tcp_stream = stream?;
            },
            _ = tokio::time::sleep(Duration::from_secs(CONNECTION_TIMEOUT_SECS)).fuse() => {
                return Err(NetworkError::Io(std::io::Error::new(ErrorKind::TimedOut, "connection timed out")));
            },
        }
        self.inner_handshake_initiator(tcp_stream, our_version).await
    }

    pub fn connect<S: Storage>(mut self, node: Node<S>, event_target: mpsc::Sender<PeerEvent>) {
        let (sender, receiver) = mpsc::channel::<PeerAction>(64);
        tokio::spawn(async move {
            self.set_connecting();
            match self.inner_connect(node.version()).await {
                Err(e) => {
                    self.fail();
                    if !e.is_trivial() {
                        error!(
                            "failed to send outgoing connection to peer '{}': '{:?}'",
                            self.address, e
                        );
                    } else {
                        warn!(
                            "failed to send outgoing connection to peer '{}': '{:?}'",
                            self.address, e
                        );
                    }
                }
                Ok(network) => {
                    self.set_connected();
                    metrics::increment_gauge!(stats::CONNECTIONS_CONNECTED, 1.0);
                    event_target
                        .send(PeerEvent {
                            address: self.address,
                            data: PeerEventData::Connected(PeerHandle { sender: sender.clone() }),
                        })
                        .await
                        .ok();
                    if let Err(e) = self.run(node, network, receiver).await {
                        self.fail();
                        if !e.is_trivial() {
                            error!(
                                "unrecoverable failure communicating to outbound peer '{}': '{:?}'",
                                self.address, e
                            );
                        } else {
                            warn!(
                                "unrecoverable failure communicating to outbound peer '{}': '{:?}'",
                                self.address, e
                            );
                        }
                    }
                    metrics::decrement_gauge!(stats::CONNECTIONS_CONNECTED, 1.0);
                }
            }
            let state = self.status;
            self.set_disconnected();
            event_target
                .send(PeerEvent {
                    address: self.address,
                    data: PeerEventData::Disconnect(self, state),
                })
                .await
                .ok();
        });
    }
}
