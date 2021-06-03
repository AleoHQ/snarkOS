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

// Compilation
#![allow(clippy::module_inception)]
#![warn(unused_extern_crates)]
#![forbid(unsafe_code)]
// Documentation
#![cfg_attr(nightly, feature(doc_cfg, external_doc))]
#![cfg_attr(nightly, doc(include = "../documentation/concepts/network_server.md"))]

#[macro_use]
extern crate derivative;
#[macro_use]
extern crate tracing;

pub mod config;
pub use config::*;

pub mod errors;
pub use errors::*;

pub mod inbound;
pub use inbound::*;

pub mod message;
pub use message::*;

pub mod node;
pub use node::*;

pub mod peers;
pub use peers::*;

pub mod stats;
pub use stats::*;

pub mod sync;
pub use sync::*;

mod drop_join;
pub use drop_join::*;

/// The maximum number of block hashes that can be requested or provided in a single batch.
pub const MAX_BLOCK_SYNC_COUNT: u32 = 64;
/// The maximum amount of time allowed to process a single batch of sync blocks. It should be aligned
/// with `MAX_BLOCK_SYNC_COUNT`.
pub const BLOCK_SYNC_EXPIRATION_SECS: u8 = 30;

/// The noise handshake pattern.
pub const HANDSHAKE_PATTERN: &str = "Noise_XXpsk3_25519_ChaChaPoly_SHA256";
/// The pre-shared key for the noise handshake.
pub const HANDSHAKE_PSK: &[u8] = b"b765e427e836e0029a1e2a22ba60c52a"; // the PSK must be 32B
/// The spec-compliant size of the noise buffer.
pub const NOISE_BUF_LEN: usize = 65535;
/// The spec-compliant size of the noise tag field.
pub const NOISE_TAG_LEN: usize = 16;

/// The maximum amount of time in which a handshake with a bootnode can conclude before dropping the
/// connection; it should be no greater than the `peer_sync_interval`.
pub const HANDSHAKE_BOOTNODE_TIMEOUT_SECS: u8 = 10;
/// The maximum amount of time in which a handshake with a regular node can conclude before dropping the
/// connection; it should be no greater than the `peer_sync_interval`.
pub const HANDSHAKE_PEER_TIMEOUT_SECS: u8 = 5;
/// The amount of time after which a peer will be considered inactive an disconnected from if they have
/// not sent any messages in the meantime.
pub const MAX_PEER_INACTIVITY_SECS: u8 = 30;

/// The maximum size of a message that can be transmitted in the network.
pub const MAX_MESSAGE_SIZE: usize = 8 * 1024 * 1024; // 8MiB
/// The maximum number of peers shared at once in response to a `GetPeers` message.
pub const SHARED_PEER_COUNT: usize = 25;

/// The depth of the common inbound channel.
pub const INBOUND_CHANNEL_DEPTH: usize = 1024;
/// The depth of the per-connection outbound channels.
pub const OUTBOUND_CHANNEL_DEPTH: usize = 1024;

/// The version of the network protocol; it can be incremented in order to force users to update.
/// FIXME: probably doesn't need to be a u64, could also be more informative than just a number
// TODO (raychu86): Establish a formal node version.
pub const PROTOCOL_VERSION: u64 = 2;

pub(crate) type Sender = tokio::sync::mpsc::Sender<Message>;

pub(crate) type Receiver = tokio::sync::mpsc::Receiver<Message>;
