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

use crate::StorageService;
use snarkvm::{
    ledger::narwhal::{Transmission, TransmissionID},
    prelude::{bail, Network, Result},
};

use indexmap::{map::Entry, IndexMap, IndexSet};
use parking_lot::RwLock;
use snarkvm::prelude::Field;
use std::collections::HashMap;

/// A BFT in-memory storage service.
#[derive(Debug)]
pub struct BFTMemoryService<N: Network> {
    /// The map of `transmission ID` to `(transmission, certificate IDs)` entries.
    transmissions: RwLock<IndexMap<TransmissionID<N>, (Transmission<N>, IndexSet<Field<N>>)>>,
}

impl<N: Network> Default for BFTMemoryService<N> {
    /// Initializes a new BFT in-memory storage service.
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Network> BFTMemoryService<N> {
    /// Initializes a new BFT in-memory storage service.
    pub fn new() -> Self {
        Self { transmissions: Default::default() }
    }
}

impl<N: Network> IntoIterator for BFTMemoryService<N> {
    type IntoIter = core::iter::Map<
        indexmap::map::IntoIter<TransmissionID<N>, (Transmission<N>, IndexSet<Field<N>>)>,
        fn(
            (TransmissionID<N>, (Transmission<N>, IndexSet<Field<N>>)),
        ) -> (TransmissionID<N>, Transmission<N>, IndexSet<Field<N>>),
    >;
    type Item = (TransmissionID<N>, Transmission<N>, IndexSet<Field<N>>);

    /// Returns an iterator over the `(transmission ID, transmission, certificate IDs)` entries.
    fn into_iter(self) -> Self::IntoIter {
        self.transmissions
            .read()
            .clone()
            .into_iter()
            .map(|(id, (transmission, certificate_ids))| (id, transmission, certificate_ids))
    }
}

impl<N: Network> StorageService<N> for BFTMemoryService<N> {
    /// Returns `true` if the storage contains the specified `transmission ID`.
    fn contains_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> bool {
        // Check if the transmission ID exists in storage.
        self.transmissions.read().contains_key(&transmission_id.into())
    }

    /// Returns the transmission for the given `transmission ID`.
    /// If the transmission ID does not exist in storage, `None` is returned.
    fn get_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> Option<Transmission<N>> {
        // Get the transmission.
        self.transmissions.read().get(&transmission_id.into()).map(|(transmission, _)| transmission).cloned()
    }

    /// Given a list of transmission IDs, identify and return the transmissions that are missing from storage.
    fn find_missing_transmissions(
        &self,
        transmission_ids: &IndexSet<TransmissionID<N>>,
        mut transmissions: HashMap<TransmissionID<N>, Transmission<N>>,
    ) -> Result<HashMap<TransmissionID<N>, Transmission<N>>> {
        // Initialize a list for the missing transmissions from storage.
        let mut missing_transmissions = HashMap::new();
        // Lock the existing transmissions.
        let known_transmissions = self.transmissions.read();
        // Ensure the declared transmission IDs are all present in storage or the given transmissions map.
        for transmission_id in transmission_ids {
            // If the transmission ID does not exist, ensure it was provided by the caller.
            if !known_transmissions.contains_key(transmission_id) {
                // Retrieve the transmission.
                let Some(transmission) = transmissions.remove(transmission_id) else {
                    bail!("Failed to provide transmission '{transmission_id}' to storage");
                };
                // Append the transmission.
                missing_transmissions.insert(*transmission_id, transmission);
            }
        }
        Ok(missing_transmissions)
    }

    /// Inserts the transmissions from the given list of transmission IDs,
    /// using the provided map of missing transmissions.
    fn insert_transmissions(
        &self,
        round: u64,
        certificate_id: Field<N>,
        transmission_ids: IndexSet<TransmissionID<N>>,
        mut missing_transmissions: HashMap<TransmissionID<N>, Transmission<N>>,
    ) -> Result<()> {
        // Acquire the transmissions write lock.
        let mut transmissions = self.transmissions.write();
        // Inserts the following:
        //   - Inserts **only the missing** transmissions from storage.
        //   - Inserts the certificate ID into the corresponding set for **all** transmissions.
        for transmission_id in transmission_ids {
            // Retrieve the transmission entry.
            transmissions.entry(transmission_id)
                // Insert **only the missing** transmissions from storage.
                .or_insert_with( || {
                    // Retrieve the missing transmission.
                    let transmission = missing_transmissions.remove(&transmission_id).expect("Missing transmission not found");
                    // Return the transmission and an empty set of certificate IDs.
                    (transmission, Default::default())
                })
                // Insert the certificate ID into the corresponding set for **all** transmissions.
                .1.insert(certificate_id);
        }
        Ok(())
    }

    /// Removes the transmissions for the given round and certificate ID, from the given list of transmission IDs from storage.
    fn remove_transmissions(
        &self,
        round: u64,
        certificate_id: Field<N>,
        transmission_ids: &IndexSet<TransmissionID<N>>,
    ) -> Result<()> {
        // Acquire the transmissions write lock.
        let mut transmissions = self.transmissions.write();
        // If this is the last certificate ID for the transmission ID, remove the transmission.
        for transmission_id in transmission_ids {
            // Remove the certificate ID for the transmission ID, and determine if there are any more certificate IDs.
            match transmissions.entry(*transmission_id) {
                Entry::Occupied(mut occupied_entry) => {
                    let (_, certificate_ids) = occupied_entry.get_mut();
                    // Remove the certificate ID for the transmission ID.
                    certificate_ids.remove(&certificate_id);
                    // If there are no more certificate IDs for the transmission ID, remove the transmission.
                    if certificate_ids.is_empty() {
                        // Remove the entry for the transmission ID.
                        occupied_entry.remove();
                    }
                }
                Entry::Vacant(_) => {}
            }
        }
        Ok(())
    }
}
