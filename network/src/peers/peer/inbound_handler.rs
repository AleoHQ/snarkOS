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

use snarkos_storage::Storage;

use crate::{stats, Direction, Message, NetworkError, Node, Payload, Peer};

use super::network::PeerNetwork;

impl Peer {
    pub(super) async fn inner_dispatch_payload<S: Storage>(
        &mut self,
        node: &Node<S>,
        network: &mut PeerNetwork,
        payload: Result<Payload, NetworkError>,
    ) -> Result<(), NetworkError> {
        let payload = payload?;
        self.quality.see();
        self.quality.num_messages_received += 1;

        // If message is a `SyncBlock` message, log it as a trace.
        match payload {
            Payload::SyncBlock(_) => trace!("Received a '{}' message from {}", payload, self.address),
            _ => debug!("Received a '{}' message from {}", payload, self.address),
        }

        match payload {
            Payload::Pong => {
                if self.quality.expecting_pong {
                    let rtt = self
                        .quality
                        .last_ping_sent
                        .map(|x| x.elapsed().as_millis() as u64)
                        .unwrap_or(u64::MAX);
                    trace!("RTT for {} is {}ms", self.address, rtt);
                    self.quality.expecting_pong = false;
                    self.quality.rtt_ms = rtt;
                } else {
                    self.fail();
                }
                metrics::increment_counter!(stats::INBOUND_PONGS);
            }
            Payload::Ping(block_height) => {
                network.write_payload(&Payload::Pong).await?;
                self.quality.block_height = block_height;
                metrics::increment_counter!(stats::INBOUND_PINGS);
            }
            payload => {
                node.route(Message {
                    direction: Direction::Inbound(self.address),
                    payload,
                });
            }
        }

        Ok(())
    }

    pub(super) async fn dispatch_payload<S: Storage>(
        &mut self,
        node: &Node<S>,
        network: &mut PeerNetwork,
        payload: Result<Payload, NetworkError>,
    ) -> Result<(), NetworkError> {
        match self.inner_dispatch_payload(node, network, payload).await {
            Ok(()) => (),
            Err(e) => {
                if e.is_trivial() {
                    trace!("Unable to read message from {}: {}", self.address, e);
                } else {
                    warn!("Unable to read message from {}: {}", self.address, e);
                }
                return Err(e);
            }
        }
        Ok(())
    }

    pub(super) fn deserialize_payload(&self, payload: Result<&[u8], NetworkError>) -> Result<Payload, NetworkError> {
        let payload = payload?;
        let payload = Payload::deserialize(payload)?;
        Ok(payload)
    }
}
