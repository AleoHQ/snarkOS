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

use crate::helpers::Committee;
use snarkvm::{
    ledger::narwhal::{BatchCertificate, Transmission, TransmissionID},
    prelude::{Address, Field, Network},
};

use anyhow::{bail, Result};
use indexmap::{IndexMap, IndexSet};
use parking_lot::RwLock;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// The storage for the memory pool.
///
/// The storage is used to store the following:
/// - `round` to `committee` entries.
/// - `round` to `(certificate ID, batch ID, author)` entries.
/// - `certificate ID` to `certificate` entries.
/// - `batch ID` to `round` entries.
/// - `transmission ID` to `certificate IDs` entries.
/// - `transmission ID` to `transmission` entries.
///
/// The chain of events is as follows:
/// 1. A `transmission` is received.
/// 2. The `transmission` is inserted into the `transmissions` map.
/// 3. After a `batch` is ready to be stored:
///   - The `certificate` is inserted, triggering updates to the
///     `rounds`, `certificates`, `batch_ids`, and `transmission_ids` maps.
/// 4. After a `round` reaches quorum threshold:
///  - The `committee` for the next round is inserted into the `committees` map.
#[derive(Clone, Debug)]
pub struct Storage<N: Network> {
    /* Once per round */
    /// The map of `round` to `committee`.
    committees: Arc<RwLock<IndexMap<u64, Committee<N>>>>,
    /// The `round` for which garbage collection has occurred **up to** (inclusive).
    gc_round: Arc<AtomicU64>,
    /// The maximum number of rounds to keep in storage.
    max_gc_rounds: u64,
    /* Once per batch */
    /// The map of `round` to a list of `(certificate ID, batch ID, author)` entries.
    rounds: Arc<RwLock<IndexMap<u64, IndexSet<(Field<N>, Field<N>, Address<N>)>>>>,
    /// The map of `certificate ID` to `certificate`.
    certificates: Arc<RwLock<IndexMap<Field<N>, BatchCertificate<N>>>>,
    /// The map of `batch ID` to `round`.
    batch_ids: Arc<RwLock<IndexMap<Field<N>, u64>>>,
    /// The map of `transmission ID` to `certificate IDs`.
    transmission_ids: Arc<RwLock<IndexMap<TransmissionID<N>, IndexSet<Field<N>>>>>,
    /* Once per transmission */
    /// The map of `transmission ID` to `transmission`.
    transmissions: Arc<RwLock<IndexMap<TransmissionID<N>, Transmission<N>>>>,
}

impl<N: Network> Storage<N> {
    /// Initializes a new instance of storage.
    pub fn new(max_gc_rounds: u64) -> Self {
        Self {
            committees: Default::default(),
            gc_round: Arc::new(AtomicU64::new(0)),
            max_gc_rounds,
            rounds: Default::default(),
            certificates: Default::default(),
            batch_ids: Default::default(),
            transmission_ids: Default::default(),
            transmissions: Default::default(),
        }
    }
}

impl<N: Network> Storage<N> {
    /// Returns an iterator over the `(round, committee)` entries.
    pub fn committees_iter(&self) -> impl Iterator<Item = (u64, Committee<N>)> {
        self.committees.read().clone().into_iter()
    }

    /// Returns an iterator over the `(round, (certificate ID, batch ID, author))` entries.
    pub fn rounds_iter(&self) -> impl Iterator<Item = (u64, IndexSet<(Field<N>, Field<N>, Address<N>)>)> {
        self.rounds.read().clone().into_iter()
    }

    /// Returns an iterator over the `(certificate ID, certificate)` entries.
    pub fn certificates_iter(&self) -> impl Iterator<Item = (Field<N>, BatchCertificate<N>)> {
        self.certificates.read().clone().into_iter()
    }

    /// Returns an iterator over the `(batch ID, round)` entries.
    pub fn batch_ids_iter(&self) -> impl Iterator<Item = (Field<N>, u64)> {
        self.batch_ids.read().clone().into_iter()
    }

    /// Returns an iterator over the `(transmission ID, certificate IDs)` entries.
    pub fn transmission_ids_iter(&self) -> impl Iterator<Item = (TransmissionID<N>, IndexSet<Field<N>>)> {
        self.transmission_ids.read().clone().into_iter()
    }

    /// Returns an iterator over the `(transmission ID, transmission)` entries.
    pub fn transmissions_iter(&self) -> impl Iterator<Item = (TransmissionID<N>, Transmission<N>)> {
        self.transmissions.read().clone().into_iter()
    }
}

impl<N: Network> Storage<N> {
    /// Returns the `round` that garbage collection has occurred **up to** (inclusive).
    pub fn gc_round(&self) -> u64 {
        // Get the GC round.
        self.gc_round.load(Ordering::Relaxed)
    }

    /// Returns the maximum number of rounds to keep in storage.
    pub fn max_gc_rounds(&self) -> u64 {
        self.max_gc_rounds
    }

    /// Returns the `committee` for the given `round`.
    /// If the round does not exist in storage, `None` is returned.
    pub fn get_committee_for_round(&self, round: u64) -> Option<Committee<N>> {
        // Get the committee from storage.
        self.committees.read().get(&round).cloned()
    }

    /// Insert the given `committee` into storage.
    /// Note: This method is only called once per round, upon certification of the primary's batch.
    pub fn insert_committee(&self, committee: Committee<N>) {
        // Retrieve the round.
        let round = committee.round();
        // Insert the committee into storage.
        self.committees.write().insert(round, committee);

        // Fetch the current GC round.
        let current_gc_round = self.gc_round();
        // Compute the next GC round.
        let next_gc_round = round.saturating_sub(self.max_gc_rounds);
        // Check if storage needs to be garbage collected.
        if next_gc_round > current_gc_round {
            // Remove the GC round(s) from storage.
            for gc_round in current_gc_round..next_gc_round {
                // TODO (howardwu): Handle removal of transmissions.
                // Iterate over the certificates for the GC round.
                for certificate in self.get_certificates_for_round(gc_round).iter() {
                    // Remove the certificate from storage.
                    self.remove_certificate(certificate.certificate_id());
                }
                // Remove the GC round from the committee.
                self.remove_committee(gc_round);
            }
            // Update the GC round.
            self.gc_round.store(next_gc_round, Ordering::Relaxed);
        }
    }

    /// Removes the committee for the given `round` from storage.
    /// Note: This method should only be called by garbage collection.
    fn remove_committee(&self, round: u64) {
        // Remove the committee from storage.
        self.committees.write().remove(&round);
    }
}

impl<N: Network> Storage<N> {
    /// Returns `true` if the storage contains the specified `round`.
    pub fn contains_round(&self, round: u64) -> bool {
        // Check if the round exists in storage.
        self.rounds.read().contains_key(&round)
    }

    /// Returns `true` if the storage contains the specified `certificate ID`.
    pub fn contains_certificate(&self, certificate_id: Field<N>) -> bool {
        // Check if the certificate ID exists in storage.
        self.certificates.read().contains_key(&certificate_id)
    }

    /// Returns `true` if the storage contains the specified `batch ID`.
    pub fn contains_batch(&self, batch_id: Field<N>) -> bool {
        // Check if the batch ID exists in storage.
        self.batch_ids.read().contains_key(&batch_id)
    }

    /// Returns the round for the given `certificate ID`.
    /// If the certificate ID does not exist in storage, `None` is returned.
    pub fn get_round_for_certificate(&self, certificate_id: Field<N>) -> Option<u64> {
        // Get the round.
        self.certificates.read().get(&certificate_id).map(|certificate| certificate.round())
    }

    /// Returns the round for the given `batch ID`.
    /// If the batch ID does not exist in storage, `None` is returned.
    pub fn get_round_for_batch(&self, batch_id: Field<N>) -> Option<u64> {
        // Get the round.
        self.batch_ids.read().get(&batch_id).cloned()
    }

    /// Returns the certificate for the given `certificate ID`.
    /// If the certificate ID does not exist in storage, `None` is returned.
    pub fn get_certificate(&self, certificate_id: Field<N>) -> Option<BatchCertificate<N>> {
        // Get the batch certificate.
        self.certificates.read().get(&certificate_id).cloned()
    }

    /// Returns the certificates for the given `round`.
    /// If the round does not exist in storage, `None` is returned.
    pub fn get_certificates_for_round(&self, round: u64) -> IndexSet<BatchCertificate<N>> {
        // The genesis round does not have batch certificates.
        if round == 0 {
            return Default::default();
        }
        // Retrieve the certificates.
        if let Some(entries) = self.rounds.read().get(&round) {
            let certificates = self.certificates.read();
            entries.iter().flat_map(|(certificate_id, _, _)| certificates.get(certificate_id).cloned()).collect()
        } else {
            Default::default()
        }
    }

    /// Inserts the given `certificate` into storage.
    /// This method triggers updates to the `rounds`, `certificates`, and `batch_ids` maps.
    ///
    /// This method ensures the following invariants:
    /// - The certificate ID does not already exist in storage.
    /// - The batch ID does not already exist in storage.
    /// - All transmissions declared in the certificate exist in storage (up to GC).
    /// - All previous certificates declared in the certificate exist in storage (up to GC).
    /// - All previous certificates are for the previous round (i.e. round - 1).
    /// - The previous certificates reached the quorum threshold (2f+1).
    pub fn insert_certificate(&self, certificate: BatchCertificate<N>) -> Result<()> {
        // Retrieve the round.
        let round = certificate.round();
        // Retrieve the certificate ID.
        let certificate_id = certificate.certificate_id();
        // Retrieve the batch ID.
        let batch_id = certificate.batch_id();
        // Retrieve the author of the batch.
        let author = certificate.author();

        // Ensure the certificate ID does not already exist in storage.
        if self.contains_certificate(certificate_id) {
            bail!("Certificate for round {round} already exists in storage");
        }
        // Ensure the batch ID does not already exist in storage.
        if self.contains_batch(batch_id) {
            bail!("Batch for round {round} already exists in storage");
        }

        // TODO (howardwu): Ensure the certificate is well-formed. If not, do not store.
        // TODO (howardwu): Ensure the address is in the committee of the specified round. If not, do not store.
        // TODO (howardwu): Ensure the previous certificates have reached 2f+1. If not, do not store.

        // Retrieve the GC round.
        let gc_round = self.gc_round();
        // Compute the previous round.
        let previous_round = round.saturating_sub(1);

        // Check if the previous round is within range of the GC round.
        if previous_round > gc_round {
            // Ensure the previous round exists in storage.
            if !self.contains_round(previous_round) {
                bail!("Missing state for the previous round {previous_round} in storage (gc={gc_round})");
            }
        }

        // If the certificate's round is greater than the GC round, ensure the transmissions exists.
        if round > gc_round {
            // Ensure storage contains all declared transmissions (up to GC).
            for transmission_id in certificate.transmission_ids() {
                // Ensure storage contains the declared transmission ID.
                if !self.contains_transmission(*transmission_id) {
                    bail!("Missing transmission {transmission_id} for certificate in round {round} (gc={gc_round})");
                }
            }
        }

        // If the certificate's *previous* round is greater than the GC round, ensure the previous certificates exists.
        if previous_round > gc_round {
            // Retrieve the committee for the previous round.
            if self.get_committee_for_round(previous_round).is_none() {
                bail!("Missing committee for the previous round {previous_round} in storage (gc={gc_round})");
            };
            // Ensure storage contains all declared previous certificates (up to GC).
            for previous_certificate_id in certificate.previous_certificate_ids() {
                // Retrieve the previous certificate.
                let Some(previous_certificate) = self.get_certificate(*previous_certificate_id) else {
                    bail!("Missing previous certificate for certificate in round {round} (gc={gc_round})");
                };
                // Ensure the previous certificate is for the previous round.
                if previous_certificate.round() != previous_round {
                    bail!(
                        "Previous certificate for round {previous_round} found in certificate for round {round} (gc={gc_round})"
                    );
                }
            }
        }

        /* Proceed to store the certificate. */

        // Insert the round to certificate ID entry.
        self.rounds.write().entry(round).or_default().insert((certificate_id, batch_id, author));
        // Insert the certificate.
        self.certificates.write().insert(certificate_id, certificate.clone());
        // Insert the batch ID.
        self.batch_ids.write().insert(batch_id, round);
        // Scope and acquire the write lock.
        {
            let mut transmission_ids = self.transmission_ids.write();
            // Insert the transmission IDs.
            for transmission_id in certificate.transmission_ids() {
                transmission_ids.entry(*transmission_id).or_default().insert(certificate_id);
            }
        }
        Ok(())
    }

    /// Removes the given `certificate ID` from storage.
    /// This method triggers updates to the `rounds`, `certificates`, and `batch_ids` maps.
    ///
    /// If the certificate was successfully removed, `true` is returned.
    /// If the certificate did not exist in storage, `false` is returned.
    pub fn remove_certificate(&self, certificate_id: Field<N>) -> bool {
        // Retrieve the certificate.
        let Some(certificate) = self.get_certificate(certificate_id) else {
            warn!("Certificate {certificate_id} does not exist in storage");
            return false;
        };
        // Retrieve the round.
        let round = certificate.round();
        // Retrieve the batch ID.
        let batch_id = certificate.batch_id();
        // Compute the author of the batch.
        let author = certificate.author();

        // Scope and acquire the write lock.
        {
            let mut rounds = self.rounds.write();
            // Remove the round to certificate ID entry.
            rounds.entry(round).or_default().remove(&(certificate_id, batch_id, author));
            // If the round is empty, remove it.
            if rounds.get(&round).map_or(false, |entries| entries.is_empty()) {
                rounds.remove(&round);
            }
        }
        // Remove the certificate.
        self.certificates.write().remove(&certificate_id);
        // Remove the batch ID.
        self.batch_ids.write().remove(&batch_id);

        // Scope and acquire the write lock.
        {
            let mut transmission_ids = self.transmission_ids.write();
            // Iterate over the transmission IDs.
            for transmission_id in certificate.transmission_ids() {
                // Remove the certificate ID for the transmission ID.
                transmission_ids.entry(*transmission_id).or_default().remove(&certificate_id);
                // If this is the last certificate ID for the transmission ID, remove the transmission.
                if transmission_ids.get(transmission_id).map_or(true, |certificate_ids| certificate_ids.is_empty()) {
                    // Remove the entry for the transmission ID.
                    transmission_ids.remove(transmission_id);
                    // Remove the transmission.
                    self.remove_transmission(*transmission_id);
                }
            }
        }
        // Return successfully.
        true
    }
}

impl<N: Network> Storage<N> {
    /// Returns `true` if the storage contains the specified `transmission ID`.
    pub fn contains_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> bool {
        // Check if the transmission ID exists in storage.
        self.transmissions.read().contains_key(&transmission_id.into())
    }

    /// Returns the transmission for the given `transmission ID`.
    /// If the transmission ID does not exist in storage, `None` is returned.
    pub fn get_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> Option<Transmission<N>> {
        // Get the transmission.
        self.transmissions.read().get(&transmission_id.into()).cloned()
    }

    /// Inserts the given (`transmission ID`, `transmission`) into storage.
    /// If the transmission ID already exists in storage, the existing transmission is returned.
    pub fn insert_transmission(
        &self,
        transmission_id: impl Into<TransmissionID<N>>,
        transmission: Transmission<N>,
    ) -> Option<Transmission<N>> {
        // Insert the transmission.
        self.transmissions.write().insert(transmission_id.into(), transmission)
    }

    /// Removes the given `transmission ID` from storage.
    fn remove_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) {
        // Remove the transmission.
        self.transmissions.write().remove(&transmission_id.into());
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use snarkvm::{
        ledger::narwhal::Data,
        prelude::{Rng, TestRng},
    };

    use ::bytes::Bytes;
    use indexmap::indexset;

    type CurrentNetwork = snarkvm::prelude::Testnet3;

    /// Returns `true` if the storage is empty.
    fn is_empty<N: Network>(storage: &Storage<N>) -> bool {
        storage.committees.read().is_empty()
            && storage.rounds.read().is_empty()
            && storage.certificates.read().is_empty()
            && storage.batch_ids.read().is_empty()
            && storage.transmissions.read().is_empty()
    }

    /// Asserts that the storage matches the expected layout.
    fn assert_storage<N: Network>(
        storage: &Storage<N>,
        committees: Vec<(u64, Committee<N>)>,
        rounds: Vec<(u64, IndexSet<(Field<N>, Field<N>, Address<N>)>)>,
        certificates: Vec<(Field<N>, BatchCertificate<N>)>,
        batch_ids: Vec<(Field<N>, u64)>,
        transmissions: Vec<(TransmissionID<N>, Transmission<N>)>,
    ) {
        // Ensure the committees are well-formed.
        assert_eq!(storage.committees_iter().collect::<Vec<_>>(), committees);
        // Ensure the rounds are well-formed.
        assert_eq!(storage.rounds_iter().collect::<Vec<_>>(), rounds);
        // Ensure the certificates are well-formed.
        assert_eq!(storage.certificates_iter().collect::<Vec<_>>(), certificates);
        // Ensure the batch IDs are well-formed.
        assert_eq!(storage.batch_ids_iter().collect::<Vec<_>>(), batch_ids);
        // Ensure the transmissions are well-formed.
        assert_eq!(storage.transmissions_iter().collect::<Vec<_>>(), transmissions);
    }

    /// Samples a random transmission.
    fn sample_transmission(rng: &mut TestRng) -> Transmission<CurrentNetwork> {
        // Sample random fake solution bytes.
        let s = |rng: &mut TestRng| Data::Buffer(Bytes::from((0..512).map(|_| rng.gen::<u8>()).collect::<Vec<_>>()));
        // Sample random fake transaction bytes.
        let t = |rng: &mut TestRng| Data::Buffer(Bytes::from((0..2048).map(|_| rng.gen::<u8>()).collect::<Vec<_>>()));
        // Sample a random transmission.
        match rng.gen::<bool>() {
            true => Transmission::Solution(s(rng)),
            false => Transmission::Transaction(t(rng)),
        }
    }

    // TODO (howardwu): Testing with 'max_gc_rounds' set to '0' should ensure everything is cleared after insertion.

    #[test]
    fn test_certificate_duplicate() {
        let rng = &mut TestRng::default();

        // Create a new storage.
        let storage = Storage::<CurrentNetwork>::new(1);
        // Ensure the storage is empty.
        assert!(is_empty(&storage));

        // Create a new certificate.
        let certificate = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate(rng);

        // Construct the sample 'transmissions' and insert them into storage.
        let mut transmissions = vec![];
        for transmission_id in certificate.transmission_ids() {
            // Initialize the transmission.
            let transmission = sample_transmission(rng);
            // Insert the transmission.
            transmissions.push((*transmission_id, transmission.clone()));
            storage.insert_transmission(*transmission_id, transmission);
        }

        // Insert the certificate.
        storage.insert_certificate(certificate.clone()).unwrap();
        // Ensure the certificate exists in storage.
        assert!(storage.contains_certificate(certificate.certificate_id()));

        // Insert the certificate again.
        assert!(storage.insert_certificate(certificate).is_err());
    }

    #[test]
    fn test_certificate_insert_remove() {
        let rng = &mut TestRng::default();

        // Create a new storage.
        let storage = Storage::<CurrentNetwork>::new(1);
        // Ensure the storage is empty.
        assert!(is_empty(&storage));

        // Create a new certificate.
        let certificate = snarkvm::ledger::narwhal::batch_certificate::test_helpers::sample_batch_certificate(rng);
        // Retrieve the certificate ID.
        let certificate_id = certificate.certificate_id();
        // Retrieve the round.
        let round = certificate.round();
        // Retrieve the batch ID.
        let batch_id = certificate.batch_id();
        // Retrieve the author of the batch.
        let author = certificate.author();

        // Construct the sample 'transmissions' and insert them into storage.
        let mut transmissions = vec![];
        for transmission_id in certificate.transmission_ids() {
            // Initialize the transmission.
            let transmission = sample_transmission(rng);
            // Insert the transmission.
            transmissions.push((*transmission_id, transmission.clone()));
            storage.insert_transmission(*transmission_id, transmission);
        }

        // Insert the certificate.
        storage.insert_certificate(certificate.clone()).unwrap();
        // Ensure the storage is not empty.
        assert!(!is_empty(&storage));
        // Ensure the certificate exists in storage.
        assert!(storage.contains_certificate(certificate_id));
        // Ensure the certificate is stored in the correct round.
        assert_eq!(storage.get_certificates_for_round(round), indexset! { certificate.clone() });

        // Check that the underlying storage representation is correct.
        {
            // Construct the expected layout for 'rounds'.
            let rounds = vec![(round, indexset! { (certificate_id, batch_id, author) })];
            // Construct the expected layout for 'certificates'.
            let certificates = vec![(certificate_id, certificate.clone())];
            // Construct the expected layout for 'batch_ids'.
            let batch_ids = vec![(batch_id, round)];
            // Assert the storage is well-formed.
            assert_storage(&storage, vec![], rounds, certificates, batch_ids, transmissions);
        }

        // Retrieve the certificate.
        let candidate_certificate = storage.get_certificate(certificate_id).unwrap();
        // Ensure the retrieved certificate is the same as the inserted certificate.
        assert_eq!(certificate, candidate_certificate);

        // Remove the certificate.
        assert!(storage.remove_certificate(certificate_id));
        // Ensure the storage is empty.
        assert!(is_empty(&storage));
        // Ensure the certificate does not exist in storage.
        assert!(!storage.contains_certificate(certificate_id));
        // Ensure the certificate is no longer stored in the round.
        assert!(storage.get_certificates_for_round(round).is_empty());
    }
}

#[cfg(test)]
pub mod prop_tests {
    use super::*;

    use test_strategy::Arbitrary;

    type CurrentNetwork = snarkvm::prelude::Testnet3;

    #[derive(Arbitrary, Debug, Clone)]
    pub struct StorageInput {
        pub gc_rounds: u64,
    }

    impl StorageInput {
        pub fn to_storage(&self) -> Storage<CurrentNetwork> {
            Storage::new(self.gc_rounds)
        }
    }
}
