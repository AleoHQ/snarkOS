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
#![allow(clippy::blocks_in_conditions)]
#![allow(clippy::type_complexity)]

#[macro_use]
extern crate async_trait;
#[macro_use]
extern crate tracing;

pub use snarkos_node_bft_events as events;
pub use snarkos_node_bft_ledger_service as ledger_service;
pub use snarkos_node_bft_storage_service as storage_service;

pub mod helpers;

mod bft;
pub use bft::*;

mod gateway;
pub use gateway::*;

mod primary;
pub use primary::*;

mod sync;
pub use sync::*;

mod worker;
pub use worker::*;

pub const CONTEXT: &str = "[MemoryPool]";

/// The port on which the memory pool listens for incoming connections.
pub const MEMORY_POOL_PORT: u16 = 5000; // port

/// The maximum number of milliseconds to wait before proposing a batch.
pub const MAX_BATCH_DELAY_IN_MS: u64 = 2500; // ms
/// The minimum number of seconds to wait before proposing a batch.
pub const MIN_BATCH_DELAY_IN_SECS: u64 = 1; // seconds
/// The maximum number of milliseconds to wait before timing out on a fetch.
pub const MAX_FETCH_TIMEOUT_IN_MS: u64 = 3 * MAX_BATCH_DELAY_IN_MS; // ms
/// The maximum number of seconds allowed for the leader to send their certificate.
pub const MAX_LEADER_CERTIFICATE_DELAY_IN_SECS: i64 = 2 * MAX_BATCH_DELAY_IN_MS as i64 / 1000; // seconds
/// The maximum number of seconds before the timestamp is considered expired.
pub const MAX_TIMESTAMP_DELTA_IN_SECS: i64 = 10; // seconds
/// The maximum number of workers that can be spawned.
pub const MAX_WORKERS: u8 = 1; // worker(s)

/// The frequency at which each primary broadcasts a ping to every other node.
/// Note: If this is updated, be sure to update `MAX_BLOCKS_BEHIND` to correspond properly.
pub const PRIMARY_PING_IN_MS: u64 = 2 * MAX_BATCH_DELAY_IN_MS; // ms
/// The frequency at which each worker broadcasts a ping to every other node.
pub const WORKER_PING_IN_MS: u64 = 4 * MAX_BATCH_DELAY_IN_MS; // ms

/// A helper macro to spawn a blocking task.
#[macro_export]
macro_rules! spawn_blocking {
    ($expr:expr) => {
        match tokio::task::spawn_blocking(move || $expr).await {
            Ok(value) => value,
            Err(error) => Err(anyhow::anyhow!("[tokio::spawn_blocking] {error}")),
        }
    };
}
