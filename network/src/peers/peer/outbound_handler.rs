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

use std::time::Instant;

use tokio::sync::{mpsc, oneshot};

use crate::{stats, NetworkError, Payload, Peer};

use super::network::PeerNetwork;

pub type PeerJudge = dyn Fn(&Peer) -> bool + Send + Sync + 'static;

pub(super) enum PeerAction {
    Disconnect,
    Send(Payload),
    Get(oneshot::Sender<Peer>),
    QualityJudgement,
    CancelSync,
    GotSyncBlock,
    ExpectingSyncBlocks(u32),
    SoftFail,
}

#[derive(Clone, Debug)]
pub struct PeerHandle {
    pub(super) sender: mpsc::Sender<PeerAction>,
}

impl PeerHandle {
    pub async fn load(&self) -> Option<Peer> {
        metrics::increment_gauge!(stats::QUEUES_OUTBOUND, 1.0);
        let (sender, receiver) = oneshot::channel();
        self.sender.send(PeerAction::Get(sender)).await.ok()?;
        receiver.await.ok()
    }

    pub async fn judge(&self) {
        metrics::increment_gauge!(stats::QUEUES_OUTBOUND, 1.0);
        self.sender.send(PeerAction::QualityJudgement).await.ok();
    }

    /// returns true if disconnected, false if not connected anymore
    pub async fn disconnect(&self) -> bool {
        metrics::increment_gauge!(stats::QUEUES_OUTBOUND, 1.0);
        self.sender.send(PeerAction::Disconnect).await.is_ok()
    }

    pub async fn send_payload(&self, payload: Payload) {
        metrics::increment_gauge!(stats::QUEUES_OUTBOUND, 1.0);
        self.sender.send(PeerAction::Send(payload)).await.ok();
    }

    pub async fn cancel_sync(&self) {
        metrics::increment_gauge!(stats::QUEUES_OUTBOUND, 1.0);
        self.sender.send(PeerAction::CancelSync).await.ok();
    }

    pub async fn got_sync_block(&self) {
        metrics::increment_gauge!(stats::QUEUES_OUTBOUND, 1.0);
        self.sender.send(PeerAction::GotSyncBlock).await.ok();
    }

    pub async fn expecting_sync_blocks(&self, amount: u32) {
        metrics::increment_gauge!(stats::QUEUES_OUTBOUND, 1.0);
        self.sender.send(PeerAction::ExpectingSyncBlocks(amount)).await.ok();
    }

    pub async fn soft_fail(&self) {
        metrics::increment_gauge!(stats::QUEUES_OUTBOUND, 1.0);
        self.sender.send(PeerAction::SoftFail).await.ok();
    }
}

pub(super) enum PeerResponse {
    Disconnect,
    None,
}

impl Peer {
    pub(super) async fn process_message(
        &mut self,
        network: &mut PeerNetwork,
        message: PeerAction,
    ) -> Result<PeerResponse, NetworkError> {
        metrics::decrement_gauge!(stats::QUEUES_OUTBOUND, 1.0);
        match message {
            PeerAction::Disconnect => Ok(PeerResponse::Disconnect),
            PeerAction::Send(message) => {
                if matches!(message, Payload::Ping(_)) {
                    self.quality.expecting_pong = true;
                    self.quality.last_ping_sent = Some(Instant::now());
                }
                network.write_payload(&message).await?;
                match &message {
                    Payload::SyncBlock(_) => trace!("Sent a '{}' message to {}", &message, self.address),
                    _ => debug!("Sent a '{}' message to {}", &message, self.address),
                }
                Ok(PeerResponse::None)
            }
            PeerAction::Get(sender) => {
                sender.send(self.clone()).ok();
                Ok(PeerResponse::None)
            }
            PeerAction::QualityJudgement => {
                if self.judge() {
                    let f = self.failures();
                    debug!(
                        "judgement results for {}: {} {} {}",
                        self.address,
                        self.quality.rtt_ms,
                        f,
                        self.quality.is_inactive(chrono::Utc::now())
                    );
                    warn!("Peer {} has a low quality score; disconnecting.", self.address);
                    Ok(PeerResponse::Disconnect)
                } else {
                    Ok(PeerResponse::None)
                }
            }
            PeerAction::CancelSync => {
                if self.quality.remaining_sync_blocks > self.quality.total_sync_blocks / 2 {
                    warn!(
                        "Was expecting {} more sync blocks from {}",
                        self.quality.remaining_sync_blocks, self.address,
                    );
                    self.quality.remaining_sync_blocks = 0;
                    self.quality.total_sync_blocks = 0;
                    self.fail();
                } else if self.quality.remaining_sync_blocks > 0 {
                    trace!(
                        "Was expecting {} more sync blocks from {}",
                        self.quality.remaining_sync_blocks,
                        self.address,
                    );
                    self.quality.remaining_sync_blocks = 0;
                    self.quality.total_sync_blocks = 0;
                }
                Ok(PeerResponse::None)
                //todo: should we notify the peer we are no longer expecting anything from them?
            }
            PeerAction::GotSyncBlock => {
                if self.quality.remaining_sync_blocks > 0 {
                    self.quality.remaining_sync_blocks -= 1;
                } else {
                    warn!("received unexpected or late sync block from {}", self.address);
                }
                Ok(PeerResponse::None)
            }
            PeerAction::ExpectingSyncBlocks(amount) => {
                self.quality.remaining_sync_blocks = amount;
                self.quality.total_sync_blocks = amount;
                Ok(PeerResponse::None)
            }
            PeerAction::SoftFail => {
                self.fail();
                Ok(PeerResponse::None)
            }
        }
    }
}
