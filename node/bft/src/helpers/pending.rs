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

use crate::MAX_FETCH_TIMEOUT_IN_MS;
use snarkos_node_bft_ledger_service::LedgerService;
use snarkvm::{console::network::Network, ledger::committee::Committee};

use parking_lot::RwLock;
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    net::SocketAddr,
    sync::Arc,
};
use time::OffsetDateTime;
use tokio::sync::oneshot;

/// The maximum number of seconds to wait before expiring a callback.
/// We ensure that we don't truncate `MAX_FETCH_TIMEOUT_IN_MS` when converting to seconds.
const CALLBACK_EXPIRATION_IN_SECS: i64 = (MAX_FETCH_TIMEOUT_IN_MS as i64 + (1000 - 1)) / 1000;

/// Returns the maximum number of redundant requests for the number of validators in the specified round.
pub fn max_redundant_requests<N: Network>(ledger: Arc<dyn LedgerService<N>>, round: u64) -> usize {
    // Determine the number of validators in the committee lookback for the given round.
    let num_validators = ledger
        .get_committee_lookback_for_round(round)
        .map(|committee| committee.num_members())
        .ok()
        .unwrap_or(Committee::<N>::MAX_COMMITTEE_SIZE as usize);

    // Note: It is adequate to set this value to the availability threshold,
    // as with high probability one will respond honestly (in the best and worst case
    // with stake spread across the validators evenly and unevenly, respectively).
    1 + num_validators.saturating_div(3)
}

#[derive(Debug)]
pub struct Pending<T: PartialEq + Eq + Hash, V: Clone> {
    /// The map of pending `items` to a map of `peer IPs` and their optional `callback` queue.
    /// Each callback has a timeout and a flag indicating if it is associated with a sent request.
    pending: RwLock<HashMap<T, HashMap<SocketAddr, Vec<(oneshot::Sender<V>, i64, bool)>>>>,
}

impl<T: Copy + Clone + PartialEq + Eq + Hash, V: Clone> Default for Pending<T, V> {
    /// Initializes a new instance of the pending queue.
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Copy + Clone + PartialEq + Eq + Hash, V: Clone> Pending<T, V> {
    /// Initializes a new instance of the pending queue.
    pub fn new() -> Self {
        Self { pending: Default::default() }
    }

    /// Returns `true` if the pending queue is empty.
    pub fn is_empty(&self) -> bool {
        self.pending.read().is_empty()
    }

    /// Returns the number of pending in the pending queue.
    pub fn len(&self) -> usize {
        self.pending.read().len()
    }

    /// Returns `true` if the pending queue contains the specified `item`.
    pub fn contains(&self, item: impl Into<T>) -> bool {
        self.pending.read().contains_key(&item.into())
    }

    /// Returns `true` if the pending queue contains the specified `item` for the specified `peer IP`.
    pub fn contains_peer(&self, item: impl Into<T>, peer_ip: SocketAddr) -> bool {
        self.pending.read().get(&item.into()).map_or(false, |peer_ips| peer_ips.contains_key(&peer_ip))
    }

    /// Returns the peer IPs for the specified `item`.
    pub fn get_peers(&self, item: impl Into<T>) -> Option<HashSet<SocketAddr>> {
        self.pending.read().get(&item.into()).map(|map| map.keys().cloned().collect())
    }

    /// Returns the number of pending callbacks for the specified `item`.
    pub fn num_callbacks(&self, item: impl Into<T>) -> usize {
        let item = item.into();
        // Clear the callbacks that have expired.
        self.clear_expired_callbacks_for_item(item);
        // Return the number of live callbacks.
        self.pending.read().get(&item).map_or(0, |peers| peers.values().flatten().count())
    }

    /// Returns the number of pending sent requests for the specified `item`.
    pub fn num_sent_requests(&self, item: impl Into<T>) -> usize {
        let item = item.into();
        // Clear the callbacks that have expired.
        self.clear_expired_callbacks_for_item(item);
        // Return the number of live callbacks.
        self.pending
            .write()
            .get(&item)
            .map_or(0, |peers| peers.values().flatten().filter(|(_, _, request_sent)| *request_sent).count())
    }

    /// Inserts the specified `item` and `peer IP` to the pending queue,
    /// returning `true` if the `peer IP` was newly-inserted into the entry for the `item`.
    ///
    /// In addition, an optional `callback` may be provided, that is triggered upon removal.
    /// Note: The callback, if provided, is **always** inserted into the callback queue.
    pub fn insert(
        &self,
        item: impl Into<T>,
        peer_ip: SocketAddr,
        callback: Option<(oneshot::Sender<V>, bool)>,
    ) -> bool {
        let item = item.into();
        // Insert the peer IP and optional callback into the pending queue.
        let result = {
            // Acquire the pending lock.
            let mut pending = self.pending.write();

            // Insert a peer into the pending queue.
            let entry = pending.entry(item).or_default();

            // Check if the peer IP is already present in the entry.
            let is_new_peer = entry.contains_key(&peer_ip);

            // Get the entry for the peer IP.
            let peer_entry = entry.entry(peer_ip).or_default();

            // If a callback is provided, insert it into the callback queue.
            if let Some((callback, request_sent)) = callback {
                peer_entry.push((callback, OffsetDateTime::now_utc().unix_timestamp(), request_sent));
            }

            is_new_peer
        };

        // Clear the callbacks that have expired.
        self.clear_expired_callbacks_for_item(item);

        // Return the result.
        result
    }

    /// Removes the specified `item` from the pending queue.
    /// If the `item` exists and is removed, the peer IPs are returned.
    /// If the `item` does not exist, `None` is returned.
    pub fn remove(&self, item: impl Into<T>, callback_value: Option<V>) -> Option<HashSet<SocketAddr>> {
        let item = item.into();
        // Remove the item from the pending queue and process any remaining callbacks.
        match self.pending.write().remove(&item) {
            Some(callbacks) => {
                // Get the peer IPs.
                let peer_ips = callbacks.keys().cloned().collect();
                // Process the callbacks.
                if let Some(callback_value) = callback_value {
                    // Send a notification to the callback.
                    for (callback, _, _) in callbacks.into_values().flat_map(|callbacks| callbacks.into_iter()) {
                        callback.send(callback_value.clone()).ok();
                    }
                }
                // Return the peer IPs.
                Some(peer_ips)
            }
            None => None,
        }
    }

    /// Removes the callbacks for the specified `item` that have expired.
    pub fn clear_expired_callbacks_for_item(&self, item: impl Into<T>) {
        let item = item.into();
        let now = OffsetDateTime::now_utc().unix_timestamp();

        // Acquire the pending lock.
        let mut pending = self.pending.write();

        // Clear the callbacks that have expired.
        if let Some(peer_map) = pending.get_mut(&item) {
            // Iterate over each peer IP for the item and filter out expired callbacks.
            for (_, callbacks) in peer_map.iter_mut() {
                callbacks.retain(|(_, timestamp, _)| now - *timestamp <= CALLBACK_EXPIRATION_IN_SECS);
            }

            // Remove peer IPs that no longer have any callbacks.
            peer_map.retain(|_, callbacks| !callbacks.is_empty());

            // If there are no more remaining callbacks for the item across all peer IPs, remove the item from pending.
            if peer_map.is_empty() {
                pending.remove(&item);
            }
        }
    }

    /// Removes the callbacks for all items have that expired.
    pub fn clear_expired_callbacks(&self) {
        let items = self.pending.read().keys().copied().collect::<Vec<T>>();
        for item in items.into_iter() {
            self.clear_expired_callbacks_for_item(item);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::{
        ledger::{coinbase::PuzzleCommitment, narwhal::TransmissionID},
        prelude::{Rng, TestRng},
    };

    use std::{thread, time::Duration};

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    const ITERATIONS: usize = 100;

    #[test]
    fn test_pending() {
        let rng = &mut TestRng::default();

        // Initialize the ready queue.
        let pending = Pending::<TransmissionID<CurrentNetwork>, ()>::new();

        // Check initially empty.
        assert!(pending.is_empty());
        assert_eq!(pending.len(), 0);

        // Initialize the commitments.
        let commitment_1 = TransmissionID::Solution(PuzzleCommitment::from_g1_affine(rng.gen()));
        let commitment_2 = TransmissionID::Solution(PuzzleCommitment::from_g1_affine(rng.gen()));
        let commitment_3 = TransmissionID::Solution(PuzzleCommitment::from_g1_affine(rng.gen()));

        // Initialize the SocketAddrs.
        let addr_1 = SocketAddr::from(([127, 0, 0, 1], 1234));
        let addr_2 = SocketAddr::from(([127, 0, 0, 1], 2345));
        let addr_3 = SocketAddr::from(([127, 0, 0, 1], 3456));

        // Insert the commitments.
        assert!(pending.insert(commitment_1, addr_1, None));
        assert!(pending.insert(commitment_2, addr_2, None));
        assert!(pending.insert(commitment_3, addr_3, None));

        // Check the number of SocketAddrs.
        assert_eq!(pending.len(), 3);
        assert!(!pending.is_empty());

        // Check the items.
        let ids = [commitment_1, commitment_2, commitment_3];
        let peers = [addr_1, addr_2, addr_3];

        for i in 0..3 {
            let id = ids[i];
            assert!(pending.contains(id));
            assert!(pending.contains_peer(id, peers[i]));
        }
        let unknown_id = TransmissionID::Solution(PuzzleCommitment::from_g1_affine(rng.gen()));
        assert!(!pending.contains(unknown_id));

        // Check get.
        assert_eq!(pending.get_peers(commitment_1), Some(HashSet::from([addr_1])));
        assert_eq!(pending.get_peers(commitment_2), Some(HashSet::from([addr_2])));
        assert_eq!(pending.get_peers(commitment_3), Some(HashSet::from([addr_3])));
        assert_eq!(pending.get_peers(unknown_id), None);

        // Check remove.
        assert!(pending.remove(commitment_1, None).is_some());
        assert!(pending.remove(commitment_2, None).is_some());
        assert!(pending.remove(commitment_3, None).is_some());
        assert!(pending.remove(unknown_id, None).is_none());

        // Check empty again.
        assert!(pending.is_empty());
    }

    #[test]
    fn test_expired_callbacks() {
        let rng = &mut TestRng::default();

        // Initialize the ready queue.
        let pending = Pending::<TransmissionID<CurrentNetwork>, ()>::new();

        // Check initially empty.
        assert!(pending.is_empty());
        assert_eq!(pending.len(), 0);

        // Initialize the commitments.
        let commitment_1 = TransmissionID::Solution(PuzzleCommitment::from_g1_affine(rng.gen()));

        // Initialize the SocketAddrs.
        let addr_1 = SocketAddr::from(([127, 0, 0, 1], 1234));
        let addr_2 = SocketAddr::from(([127, 0, 0, 1], 2345));
        let addr_3 = SocketAddr::from(([127, 0, 0, 1], 3456));

        // Initialize the callbacks.
        let (callback_sender_1, _) = oneshot::channel();
        let (callback_sender_2, _) = oneshot::channel();
        let (callback_sender_3, _) = oneshot::channel();

        // Insert the commitments.
        assert!(pending.insert(commitment_1, addr_1, Some((callback_sender_1, true))));
        assert!(pending.insert(commitment_1, addr_2, Some((callback_sender_2, true))));

        // Sleep for a few seconds.
        thread::sleep(Duration::from_secs(CALLBACK_EXPIRATION_IN_SECS as u64 - 1));

        assert!(pending.insert(commitment_1, addr_3, Some((callback_sender_3, true))));

        // Check that the number of callbacks has not changed.
        assert_eq!(pending.num_callbacks(commitment_1), 3);

        // Wait for 2 seconds.
        thread::sleep(Duration::from_secs(2));

        // Ensure that the expired callbacks have been removed.
        assert_eq!(pending.num_callbacks(commitment_1), 1);

        // Wait for ` CALLBACK_EXPIRATION_IN_SECS` seconds.
        thread::sleep(Duration::from_secs(CALLBACK_EXPIRATION_IN_SECS as u64));

        // Ensure that the expired callbacks have been removed.
        assert_eq!(pending.num_callbacks(commitment_1), 0);
    }

    #[test]
    fn test_num_sent_requests() {
        let rng = &mut TestRng::default();

        // Initialize the ready queue.
        let pending = Pending::<TransmissionID<CurrentNetwork>, ()>::new();

        for _ in 0..ITERATIONS {
            // Generate a commitment.
            let commitment = TransmissionID::Solution(PuzzleCommitment::from_g1_affine(rng.gen()));
            // Check if the number of sent requests is correct.
            let mut expected_num_sent_requests = 0;
            for i in 0..ITERATIONS {
                // Generate a peer address.
                let addr = SocketAddr::from(([127, 0, 0, 1], i as u16));
                // Initialize a callback.
                let (callback_sender, _) = oneshot::channel();
                // Randomly determine if the callback is associated with a sent request.
                let is_sent_request = rng.gen();
                // Increment the expected number of sent requests.
                if is_sent_request {
                    expected_num_sent_requests += 1;
                }
                // Insert the commitment.
                assert!(pending.insert(commitment, addr, Some((callback_sender, is_sent_request))));
            }
            // Ensure that the number of sent requests is correct.
            assert_eq!(pending.num_sent_requests(commitment), expected_num_sent_requests);
        }
    }

    #[test]
    fn test_expired_items() {
        let rng = &mut TestRng::default();

        // Initialize the ready queue.
        let pending = Pending::<TransmissionID<CurrentNetwork>, ()>::new();

        // Check initially empty.
        assert!(pending.is_empty());
        assert_eq!(pending.len(), 0);

        // Initialize the commitments.
        let commitment_1 = TransmissionID::Solution(PuzzleCommitment::from_g1_affine(rng.gen()));
        let commitment_2 = TransmissionID::Solution(PuzzleCommitment::from_g1_affine(rng.gen()));

        // Initialize the SocketAddrs.
        let addr_1 = SocketAddr::from(([127, 0, 0, 1], 1234));
        let addr_2 = SocketAddr::from(([127, 0, 0, 1], 2345));
        let addr_3 = SocketAddr::from(([127, 0, 0, 1], 3456));

        // Initialize the callbacks.
        let (callback_sender_1, _) = oneshot::channel();
        let (callback_sender_2, _) = oneshot::channel();
        let (callback_sender_3, _) = oneshot::channel();

        // Insert the commitments.
        assert!(pending.insert(commitment_1, addr_1, Some((callback_sender_1, true))));
        assert!(pending.insert(commitment_1, addr_2, Some((callback_sender_2, true))));
        assert!(pending.insert(commitment_2, addr_3, Some((callback_sender_3, true))));

        // Ensure that the items have not been expired yet.
        assert_eq!(pending.num_callbacks(commitment_1), 2);
        assert_eq!(pending.num_callbacks(commitment_2), 1);
        assert_eq!(pending.len(), 2);

        // Wait for ` CALLBACK_EXPIRATION_IN_SECS + 1` seconds.
        thread::sleep(Duration::from_secs(CALLBACK_EXPIRATION_IN_SECS as u64 + 1));

        // Expire the pending callbacks.
        pending.clear_expired_callbacks();

        // Ensure that the items have been expired.
        assert_eq!(pending.num_callbacks(commitment_1), 0);
        assert_eq!(pending.num_callbacks(commitment_2), 0);
        assert_eq!(pending.len(), 0);
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;

    use test_strategy::{proptest, Arbitrary};

    #[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
    pub struct Item {
        pub id: usize,
    }

    #[derive(Arbitrary, Clone, Debug)]
    pub struct PendingInput {
        #[strategy(1..5_000usize)]
        pub count: usize,
    }

    impl PendingInput {
        pub fn to_pending(&self) -> Pending<Item, ()> {
            let pending = Pending::<Item, ()>::new();
            for i in 0..self.count {
                pending.insert(Item { id: i }, SocketAddr::from(([127, 0, 0, 1], i as u16)), None);
            }
            pending
        }
    }

    #[proptest]
    fn test_pending_proptest(input: PendingInput) {
        let pending = input.to_pending();
        assert_eq!(pending.len(), input.count);
        assert!(!pending.is_empty());
        assert!(!pending.contains(Item { id: input.count + 1 }));
        assert_eq!(pending.get_peers(Item { id: input.count + 1 }), None);
        assert!(pending.remove(Item { id: input.count + 1 }, None).is_none());
        for i in 0..input.count {
            assert!(pending.contains(Item { id: i }));
            let peer_ip = SocketAddr::from(([127, 0, 0, 1], i as u16));
            assert!(pending.contains_peer(Item { id: i }, peer_ip));
            assert_eq!(pending.get_peers(Item { id: i }), Some(HashSet::from([peer_ip])));
            assert!(pending.remove(Item { id: i }, None).is_some());
        }
        assert!(pending.is_empty());
    }
}
