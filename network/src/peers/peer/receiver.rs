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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use snarkos_storage::Storage;
use tokio::{net::TcpStream, sync::mpsc};

use super::PeerAction;
use crate::{stats, NetworkError, Node, Peer, PeerEvent, PeerEventData, PeerHandle, PeerStatus, Version};

use super::network::PeerNetwork;

impl Peer {
    async fn inner_receive(
        remote_address: SocketAddr,
        stream: TcpStream,
        our_version: Version,
    ) -> Result<(Peer, PeerNetwork), NetworkError> {
        metrics::increment_gauge!(stats::CONNECTIONS_CONNECTING, 1.0);
        let _x = defer::defer(|| metrics::decrement_gauge!(stats::CONNECTIONS_CONNECTING, 1.0));

        Peer::inner_handshake_responder(remote_address, stream, our_version).await
    }

    pub fn receive<S: Storage>(
        remote_address: SocketAddr,
        node: Node<S>,
        stream: TcpStream,
        event_target: mpsc::Sender<PeerEvent>,
    ) {
        let (sender, receiver) = mpsc::channel::<PeerAction>(64);
        tokio::spawn(async move {
            let (mut peer, network) = match Peer::inner_receive(remote_address, stream, node.version()).await {
                Err(e) => {
                    error!(
                        "failed to receive incoming connection from peer '{}': '{:?}'",
                        remote_address, e
                    );
                    event_target
                        .send(PeerEvent {
                            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0),
                            data: PeerEventData::FailHandshake,
                        })
                        .await
                        .ok();
                    return;
                }
                Ok(x) => x,
            };

            peer.set_connected();
            metrics::increment_gauge!(stats::CONNECTIONS_CONNECTED, 1.0);
            event_target
                .send(PeerEvent {
                    address: peer.address,
                    data: PeerEventData::Connected(PeerHandle { sender: sender.clone() }),
                })
                .await
                .ok();
            if let Err(e) = peer.run(node, network, receiver).await {
                peer.fail();
                if !e.is_trivial() {
                    error!(
                        "unrecoverable failure communicating to inbound peer '{}': '{:?}'",
                        peer.address, e
                    );
                } else {
                    warn!(
                        "unrecoverable failure communicating to inbound peer '{}': '{:?}'",
                        peer.address, e
                    );
                }
            }
            metrics::decrement_gauge!(stats::CONNECTIONS_CONNECTED, 1.0);
            peer.set_disconnected();
            event_target
                .send(PeerEvent {
                    address: peer.address,
                    data: PeerEventData::Disconnect(peer, PeerStatus::Connected),
                })
                .await
                .ok();
        });
    }
}
