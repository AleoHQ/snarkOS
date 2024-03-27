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

pub(super) const COUNTER_NAMES: [&str; 1] = [bft::LEADERS_ELECTED];

pub(super) const GAUGE_NAMES: [&str; 21] = [
    bft::CONNECTED,
    bft::CONNECTING,
    bft::LAST_STORED_ROUND,
    bft::PROPOSAL_ROUND,
    bft::CERTIFIED_BATCHES,
    blocks::HEIGHT,
    blocks::SOLUTIONS,
    blocks::TRANSACTIONS,
    blocks::TRANSMISSIONS,
    blocks::PROOF_TARGET,
    blocks::COINBASE_TARGET,
    blocks::CUMULATIVE_PROOF_TARGET,
    consensus::COMMITTED_CERTIFICATES,
    consensus::LAST_COMMITTED_ROUND,
    consensus::UNCONFIRMED_SOLUTIONS,
    consensus::UNCONFIRMED_TRANSACTIONS,
    consensus::UNCONFIRMED_TRANSMISSIONS,
    router::CONNECTED,
    router::CANDIDATE,
    router::RESTRICTED,
    tcp::TCP_TASKS,
];

pub(super) const HISTOGRAM_NAMES: [&str; 7] = [
    bft::COMMIT_ROUNDS_LATENCY,
    consensus::CERTIFICATE_COMMIT_LATENCY,
    consensus::BLOCK_LATENCY,
    tcp::NOISE_CODEC_ENCRYPTION_TIME,
    tcp::NOISE_CODEC_DECRYPTION_TIME,
    tcp::NOISE_CODEC_ENCRYPTION_SIZE,
    tcp::NOISE_CODEC_DECRYPTION_SIZE,
];

pub mod bft {
    pub const COMMIT_ROUNDS_LATENCY: &str = "snarkos_bft_commit_rounds_latency_secs"; // <-- This one doesn't even make sense.
    pub const CONNECTED: &str = "snarkos_bft_connected_total";
    pub const CONNECTING: &str = "snarkos_bft_connecting_total";
    pub const LAST_STORED_ROUND: &str = "snarkos_bft_last_stored_round";
    pub const LEADERS_ELECTED: &str = "snarkos_bft_leaders_elected_total";
    pub const PROPOSAL_ROUND: &str = "snarkos_bft_primary_proposal_round";
    pub const CERTIFIED_BATCHES: &str = "snarkos_bft_primary_certified_batches";
}

pub mod blocks {
    pub const HEIGHT: &str = "snarkos_blocks_height_total";
    pub const TRANSACTIONS: &str = "snarkos_blocks_transactions_total";
    pub const TRANSMISSIONS: &str = "snarkos_blocks_transmissions_total";
    pub const SOLUTIONS: &str = "snarkos_blocks_solutions_total";
    pub const PROOF_TARGET: &str = "snarkos_blocks_proof_target";
    pub const COINBASE_TARGET: &str = "snarkos_blocks_coinbase_target";
    pub const CUMULATIVE_PROOF_TARGET: &str = "snarkos_blocks_cumulative_proof_target";
}

pub mod consensus {
    pub const CERTIFICATE_COMMIT_LATENCY: &str = "snarkos_consensus_certificate_commit_latency_secs";
    pub const COMMITTED_CERTIFICATES: &str = "snarkos_consensus_committed_certificates_total";
    pub const LAST_COMMITTED_ROUND: &str = "snarkos_consensus_last_committed_round";
    pub const BLOCK_LATENCY: &str = "snarkos_consensus_block_latency_secs";
    pub const UNCONFIRMED_TRANSACTIONS: &str = "snarkos_consensus_unconfirmed_transactions_total";
    pub const UNCONFIRMED_TRANSMISSIONS: &str = "snarkos_consensus_unconfirmed_transmissions_total";
    pub const UNCONFIRMED_SOLUTIONS: &str = "snarkos_consensus_unconfirmed_solutions_total";
}

pub mod router {
    pub const CONNECTED: &str = "snarkos_router_connected_total";
    pub const CANDIDATE: &str = "snarkos_router_candidate_total";
    pub const RESTRICTED: &str = "snarkos_router_restricted_total";
}

pub mod tcp {
    pub const NOISE_CODEC_ENCRYPTION_TIME: &str = "snarkos_tcp_noise_codec_encryption_micros";
    pub const NOISE_CODEC_DECRYPTION_TIME: &str = "snarkos_tcp_noise_codec_decryption_micros";
    pub const NOISE_CODEC_ENCRYPTION_SIZE: &str = "snarkos_tcp_noise_codec_encryption_size";
    pub const NOISE_CODEC_DECRYPTION_SIZE: &str = "snarkos_tcp_noise_codec_decryption_size";
    pub const TCP_TASKS: &str = "snarkos_tcp_tasks_total";
}
