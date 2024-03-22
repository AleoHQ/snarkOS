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

use crate::{
    common::CurrentNetwork,
    components::{sample_ledger, sample_worker},
};
use snarkos_node_bft::helpers::max_redundant_requests;
use snarkvm::{
    ledger::{committee::Committee, narwhal::TransmissionID},
    prelude::{Network, TestRng},
};

use std::net::SocketAddr;

#[tokio::test]
#[rustfmt::skip]
async fn test_resend_transmission_request() {
    const NUM_NODES: u16 = Committee::<CurrentNetwork>::MAX_COMMITTEE_SIZE;

    // Initialize the RNG.
    let rng = &mut TestRng::default();
    // Sample a ledger.
    let ledger = sample_ledger(NUM_NODES, rng);
    // Sample a worker.
    let worker = sample_worker(0, ledger.clone());

    // Determine the maximum number of redundant requests.
    let max_redundancy = max_redundant_requests(ledger.clone(), 0);
    assert_eq!(max_redundancy, 34, "Update me if the formula changes");

    // Prepare a dummy transmission ID.
    let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
    let transmission_id = TransmissionID::Transaction(<CurrentNetwork as Network>::TransactionID::default());

    // Ensure the worker does not have the dummy transmission ID.
    assert!(!worker.contains_transmission(transmission_id), "Transmission should not exist");

    // Send a request to fetch the dummy transmission.
    let worker_ = worker.clone();
    tokio::spawn(async move { worker_.get_or_fetch_transmission(peer_ip, transmission_id).await });

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let pending = worker.pending();
    // Ensure the transmission ID exists in the pending queue.
    assert!(pending.contains(transmission_id), "Missing a transmission in the pending queue");
    // Ensure the peer IP is in the pending queue for the transmission ID.
    assert!(pending.contains_peer(transmission_id, peer_ip), "Missing a peer IP for transmission in the pending queue");
    assert_eq!(pending.get_peers(transmission_id), Some([peer_ip].into_iter().collect()), "Missing a peer IP for transmission in the pending queue");
    // Ensure the number of callbacks is correct.
    assert_eq!(pending.num_callbacks(transmission_id), 1, "Incorrect number of callbacks for transmission");
    // Ensure the number of sent requests is correct.
    assert_eq!(pending.num_sent_requests(transmission_id), 1, "Incorrect number of sent requests for transmission");

    // Rebroadcast the same request to fetch the dummy transmission.
    for i in 1..=10 {
        let worker_ = worker.clone();
        tokio::spawn(async move { worker_.get_or_fetch_transmission(peer_ip, transmission_id).await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Ensure the transmission ID exists in the pending queue.
        assert!(pending.contains(transmission_id), "Missing a transmission in the pending queue");
        // Ensure the peer IP is in the pending queue for the transmission ID.
        assert!(pending.contains_peer(transmission_id, peer_ip), "Missing a peer IP for transmission in the pending queue");
        assert_eq!(pending.get_peers(transmission_id), Some([peer_ip].into_iter().collect()), "Missing a peer IP for transmission in the pending queue");
        // Ensure the number of callbacks is correct.
        assert_eq!(pending.num_callbacks(transmission_id), 1 + i, "Incorrect number of callbacks for transmission");
        // Ensure the number of sent requests is correct.
        assert_eq!(pending.num_sent_requests(transmission_id), (1 + i).min(max_redundancy), "Incorrect number of sent requests for transmission");
    }
}

#[tokio::test]
#[rustfmt::skip]
async fn test_flood_transmission_requests() {
    const NUM_NODES: u16 = Committee::<CurrentNetwork>::MAX_COMMITTEE_SIZE;

    // Initialize the RNG.
    let rng = &mut TestRng::default();
    // Sample a ledger.
    let ledger = sample_ledger(NUM_NODES, rng);
    // Sample a worker.
    let worker = sample_worker(0, ledger.clone());

    // Determine the maximum number of redundant requests.
    let max_redundancy = max_redundant_requests(ledger.clone(), 0);
    assert_eq!(max_redundancy, 34, "Update me if the formula changes");

    // Prepare a dummy transmission ID.
    let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
    let transmission_id = TransmissionID::Transaction(<CurrentNetwork as Network>::TransactionID::default());

    // Ensure the worker does not have the dummy transmission ID.
    assert!(!worker.contains_transmission(transmission_id), "Transmission should not exist");

    // Send the maximum number of redundant requests to fetch the dummy transmission.
    for _ in 0..max_redundancy {
        let worker_ = worker.clone();
        tokio::spawn(async move { worker_.get_or_fetch_transmission(peer_ip, transmission_id).await });
    }

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let pending = worker.pending();
    // Ensure the transmission ID exists in the pending queue.
    assert!(pending.contains(transmission_id), "Missing a transmission in the pending queue");
    // Ensure the peer IP is in the pending queue for the transmission ID.
    assert!(pending.contains_peer(transmission_id, peer_ip), "Missing a peer IP for transmission in the pending queue");
    assert_eq!(pending.get_peers(transmission_id), Some([peer_ip].into_iter().collect()), "Missing a peer IP for transmission in the pending queue");
    // Ensure the number of callbacks is correct.
    assert_eq!(pending.num_callbacks(transmission_id), max_redundancy, "Incorrect number of callbacks for transmission");
    // Ensure the number of sent requests is correct.
    assert_eq!(pending.num_sent_requests(transmission_id), max_redundancy, "Incorrect number of sent requests for transmission");

    // Ensure any further redundant requests are not sent.
    for i in 1..=20 {
        let worker_ = worker.clone();
        tokio::spawn(async move { worker_.get_or_fetch_transmission(peer_ip, transmission_id).await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Ensure the transmission ID exists in the pending queue.
        assert!(pending.contains(transmission_id), "Missing a transmission in the pending queue");
        // Ensure the peer IP is in the pending queue for the transmission ID.
        assert!(pending.contains_peer(transmission_id, peer_ip), "Missing a peer IP for transmission in the pending queue");
        assert_eq!(pending.get_peers(transmission_id), Some([peer_ip].into_iter().collect()), "Missing a peer IP for transmission in the pending queue");
        // Ensure the number of callbacks is correct.
        assert_eq!(pending.num_callbacks(transmission_id), max_redundancy + i, "Incorrect number of callbacks for transmission");
        // Ensure the number of sent requests is correct.
        assert_eq!(pending.num_sent_requests(transmission_id), max_redundancy, "Incorrect number of sent requests for transmission");
    }
}
