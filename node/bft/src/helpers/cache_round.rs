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

use anyhow::{ensure, Result};
use snarkvm::{
    console::types::{Address, Field},
    ledger::committee::Committee,
    prelude::Network,
};
use std::collections::HashSet;

#[derive(Copy, Clone, Debug)]
struct AddressWithCoordinate<N: Network> {
    address: Address<N>,
    x: Field<N>,
}

impl<N: Network> From<Address<N>> for AddressWithCoordinate<N> {
    fn from(address: Address<N>) -> Self {
        Self { address, x: address.to_group().to_x_coordinate() }
    }
}

#[derive(Debug)]
pub struct RoundCache<N: Network> {
    /// The current highest round which has (stake-weighted) quorum
    last_highest_round_with_quorum: u64,
    /// A sorted list of (round, Vec<AddressWithCoordinate<N>>), indicating the last seen highest round for each address
    highest_rounds: Vec<(u64, Vec<AddressWithCoordinate<N>>)>,
    /// A list of (AddressWithCoordinate<N>, round) to quickly find an Address' round by their x coordinate
    address_rounds: Vec<(AddressWithCoordinate<N>, u64)>,
}

impl<N: Network> Default for RoundCache<N> {
    /// Initializes a new instance of the cache.
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Network> RoundCache<N> {
    /// Initializes a new instance of the cache.
    pub fn new() -> Self {
        Self {
            last_highest_round_with_quorum: Default::default(),
            highest_rounds: Default::default(),
            address_rounds: Default::default(),
        }
    }

    /// Insert a round seen for a validator.
    fn insert_round_for_validator(&mut self, round: u64, validator: AddressWithCoordinate<N>) {
        match self.highest_rounds.binary_search_by_key(&round, |(r, _)| *r) {
            // Add the validator to the existing round.
            Ok(new_address_index) => self.highest_rounds[new_address_index].1.push(validator),
            // Initialize a new round.
            Err(new_address_index) => self.highest_rounds.insert(new_address_index, (round, vec![validator])),
        }
    }

    /// Find and prune a validator from the list of highest rounds.
    fn prune_validator_from_highest_rounds(&mut self, round: u64, validator: Field<N>) -> Result<()> {
        // Find the index of the round.
        let round_index = self.highest_rounds.binary_search_by_key(&round, |(r, _)| *r).map_err(anyhow::Error::msg)?;
        // Find the index of the address.
        let address_index =
            self.highest_rounds[round_index].1.binary_search_by_key(&validator, |a| a.x).map_err(anyhow::Error::msg)?;
        // Remove the address from the round.
        self.highest_rounds[round_index].1.remove(address_index);
        // Remove the round if it's empty.
        if self.highest_rounds[round_index].1.is_empty() {
            self.highest_rounds.remove(round_index);
        }
        Ok(())
    }

    /// Find and prune validators which are no longer in the committee
    fn prune_stale_validators(&mut self, committee: &Committee<N>) -> Result<()> {
        // Determine which addresses are no longer in the committee.
        let addresses_to_prune = self
            .address_rounds
            .iter()
            .filter_map(|(a, _)| (!committee.members().contains_key(&a.address)).then_some(a.x))
            .collect::<Vec<_>>();
        // Prune the stale addresses.
        for address_x in addresses_to_prune {
            // Find the index of the address.
            let address_index =
                self.address_rounds.binary_search_by_key(&address_x, |&(a, _)| a.x).map_err(anyhow::Error::msg)?;
            // Get the old round for the address.
            let old_round = self.address_rounds[address_index].1;
            // Remove the address.
            self.address_rounds.remove(address_index);
            // Prune the address from the highest rounds.
            self.prune_validator_from_highest_rounds(old_round, address_x)?;
        }
        Ok(())
    }

    /// Update the cache based on a new (round, address) pair. This does two things:
    /// - If the round is higher than a previous one from this address, set it in `highest_rounds`
    /// - Keep incrementing `last_highest_round_with_quorum` as long as it passes a stake-weighted quorum
    /// We ignore the case where tomorrow's stake-weighted quorum round is *lower* than the current one
    pub fn update(&mut self, round: u64, validator_address: Address<N>, committee: &Committee<N>) -> Result<u64> {
        ensure!(committee.members().contains_key(&validator_address), "Address is not a member of the committee");
        let validator = AddressWithCoordinate::from(validator_address);

        // Determine if validator was inserted into the cache.
        let mut inserted = false;
        // Only consider updating the cache if we see a high round.
        if round > self.last_highest_round_with_quorum {
            match self.address_rounds.binary_search_by_key(&validator.x, |&(a, _)| a.x) {
                // Update the existing validator.
                Ok(address_index) => {
                    let (_, old_round) = self.address_rounds[address_index];
                    // Should we update the validator's highest seen round?
                    if old_round < round {
                        inserted = true;
                        self.address_rounds[address_index].1 = round;
                        self.prune_validator_from_highest_rounds(old_round, validator.x)?;
                        self.insert_round_for_validator(round, validator);
                    }
                }
                // Insert the new validator.
                Err(address_index) => {
                    inserted = true;
                    self.address_rounds.insert(address_index, (validator, round));
                    self.insert_round_for_validator(round, validator);
                }
            }
            // Prune validators if the cache exceeds the current committee size.
            if self.address_rounds.len() > committee.num_members() {
                self.prune_stale_validators(committee)?;
            }
            // Ensure the cache does not contain more validators than the current committee size.
            ensure!(self.address_rounds.len() <= committee.num_members());
            ensure!(self.highest_rounds.iter().map(|(_, a)| a.len()).sum::<usize>() <= committee.num_members());
        }

        // Check if we reached quorum on a new round.
        if inserted {
            while committee.is_quorum_threshold_reached(&self.validators_in_support(committee)?) {
                self.last_highest_round_with_quorum += 1;
            }
        }
        Ok(self.last_highest_round_with_quorum)
    }

    /// Update the cache based on a new round with quorum.
    pub fn update_quorum_round(&mut self, round: u64) {
        self.last_highest_round_with_quorum = round;
    }

    /// Count the total stake backing an increase of last_highest_round_with_quorum
    fn validators_in_support(&self, committee: &Committee<N>) -> Result<HashSet<Address<N>>> {
        let mut validators_in_support = HashSet::with_capacity(committee.num_members());
        // Get the index for the next round.
        let quorum_index =
            match self.highest_rounds.binary_search_by_key(&(self.last_highest_round_with_quorum + 1), |(r, _)| *r) {
                Ok(quorum_index) => quorum_index,
                Err(quorum_index) => quorum_index,
            };
        // Find the validators in support of the next round.
        if let Some(highest_rounds) = self.highest_rounds.get(quorum_index..) {
            for (_, addresses) in highest_rounds {
                validators_in_support.extend(addresses.iter().map(|a| a.address));
            }
        };
        Ok(validators_in_support)
    }

    /// Return `self.last_highest_round_with_quorum`
    pub fn last_highest_round(&self) -> u64 {
        self.last_highest_round_with_quorum
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use snarkvm::{
        prelude::{Testnet3, Uniform},
        utilities::TestRng,
    };

    type CurrentNetwork = Testnet3;

    #[test]
    fn test_round_cache() {
        let mut rng = TestRng::default();

        let num_validators = 200;
        let mut addresses = Vec::new();
        for _ in 0..num_validators {
            addresses.push(Address::<CurrentNetwork>::rand(&mut rng));
        }

        let minimum_stake = 1000000000000;
        let accepts_delegators = true;
        let committee_members =
            addresses.iter().map(|&a| (a, (minimum_stake, accepts_delegators))).collect::<IndexMap<_, _>>();
        let committee = Committee::<CurrentNetwork>::new(0, committee_members).unwrap();

        // Test case 1: when we always observe increasing round numbers
        let mut cache = RoundCache::<CurrentNetwork>::default();
        // Check that the cache is empty
        assert_eq!(cache.last_highest_round(), 0);
        for round in 1..1000 {
            cache.update(round as u64, addresses[round % num_validators], &committee).unwrap();
        }
        // Check that the cache is correctly updated
        assert_eq!(cache.last_highest_round(), 866);

        // Test case 2: when we always observe the same round number
        let mut cache = RoundCache::<CurrentNetwork>::default();
        for round in 1..1000 {
            cache.update(round as u64, addresses[0], &committee).unwrap();
        }
        // Check that the cache is correctly updated
        assert_eq!(cache.last_highest_round(), 0);

        // Test case 3: when we observe non-consecutive round numbers
        let mut cache = RoundCache::<CurrentNetwork>::default();
        for round in 0..50 {
            cache.update(0, addresses[round % num_validators], &committee).unwrap();
            cache.update(10, addresses[round + 50 % num_validators], &committee).unwrap();
            cache.update(15, addresses[round + 100 % num_validators], &committee).unwrap();
            cache.update(20, addresses[round + 150 % num_validators], &committee).unwrap();
        }
        // Check that the cache is correctly updated
        assert_eq!(cache.last_highest_round(), 10);

        // Test case 4: remove and add validators from the committee
        let mut cache = RoundCache::<CurrentNetwork>::default();
        for round in 1..1000 {
            cache.update(round as u64, addresses[round % num_validators], &committee).unwrap();
        }

        // Remove a member from the committee
        let mut committee_members = committee.members().clone();
        committee_members.remove(&addresses[0]);
        let committee = Committee::<CurrentNetwork>::new(0, committee_members).unwrap();
        // Updating with address which is not in the committee should fail
        assert!(cache.update(1001, addresses[0], &committee).is_err());
        // Updating with a smaller commitee should prune the removed addresses from the cache
        cache.update(1001, addresses[1], &committee).unwrap();

        // Add a member back to the committee
        let mut committee_members = committee.members().clone();
        let new_address = Address::<CurrentNetwork>::rand(&mut rng);
        committee_members.insert(new_address, (minimum_stake, accepts_delegators));
        let committee = Committee::<CurrentNetwork>::new(0, committee_members).unwrap();
        cache.update(1000, new_address, &committee).unwrap();
    }
}
