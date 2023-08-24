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

use crate::common::{
    utils::{fire_unconfirmed_solutions, fire_unconfirmed_transactions, initialize_logger},
    CurrentNetwork,
    MockLedgerService,
};
use snarkos_account::Account;
use snarkos_node_narwhal::{
    helpers::{init_primary_channels, PrimarySender, Storage},
    Primary,
    BFT,
    MAX_BATCH_DELAY,
    MAX_GC_ROUNDS,
};
use snarkos_node_narwhal_committee::{Committee, MIN_STAKE};
use snarkvm::prelude::TestRng;

use std::{collections::HashMap, net::SocketAddr, ops::RangeBounds, sync::Arc, time::Duration};

use indexmap::IndexMap;
use itertools::Itertools;
use parking_lot::Mutex;
use tokio::{task::JoinHandle, time::sleep};
use tracing::*;

/// The configuration for the test network.
#[derive(Clone, Copy, Debug)]
pub struct TestNetworkConfig {
    /// The number of nodes to spin up.
    pub num_nodes: u16,
    /// If this is set to `true`, the BFT protocol is started on top of Narwhal.
    pub bft: bool,
    /// If this is set to `true`, all nodes are connected to each other (when they're first
    /// started).
    pub connect_all: bool,
    /// If `Some(i)` is set, the cannons will fire every `i` milliseconds.
    pub fire_transmissions: Option<u64>,
    /// The log level to use for the test.
    pub log_level: Option<u8>,
    /// If this is set to `true`, the number of connections is logged every 5 seconds.
    pub log_connections: bool,
}

/// A test network.
#[derive(Clone)]
pub struct TestNetwork {
    /// The configuration for the test network.
    pub config: TestNetworkConfig,
    /// A map of node IDs to validators in the network.
    pub validators: HashMap<u16, TestValidator>,
}

/// A test validator.
#[derive(Clone)]
pub struct TestValidator {
    /// The ID of the validator.
    pub id: u16,
    /// The primary instance. When the BFT is enabled this is a clone of the BFT primary.
    pub primary: Primary<CurrentNetwork>,
    /// The channel sender of the primary.
    pub primary_sender: Option<PrimarySender<CurrentNetwork>>,
    /// The BFT instance. This is only set if the BFT is enabled.
    pub bft: Option<BFT<CurrentNetwork>>,
    /// The tokio handles of all long-running tasks associated with the validator (incl. cannons).
    pub handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl TestValidator {
    pub fn fire_transmissions(&mut self, interval_ms: u64) {
        let solution_handle = fire_unconfirmed_solutions(self.primary_sender.as_mut().unwrap(), self.id, interval_ms);
        let transaction_handle =
            fire_unconfirmed_transactions(self.primary_sender.as_mut().unwrap(), self.id, interval_ms);

        self.handles.lock().push(solution_handle);
        self.handles.lock().push(transaction_handle);
    }

    pub fn log_connections(&mut self) {
        let self_clone = self.clone();
        self.handles.lock().push(tokio::task::spawn(async move {
            loop {
                let connections = self_clone.primary.gateway().connected_peers().read().clone();
                info!("{} connections", connections.len());
                for connection in connections {
                    debug!("  {}", connection);
                }
                sleep(Duration::from_secs(5)).await;
            }
        }));
    }
}

impl TestNetwork {
    // Creates a new test network with the given configuration.
    pub fn new(config: TestNetworkConfig) -> Self {
        if let Some(log_level) = config.log_level {
            initialize_logger(log_level);
        }

        let (accounts, committee) = new_test_committee(config.num_nodes);

        let mut validators = HashMap::with_capacity(config.num_nodes as usize);
        for (id, account) in accounts.into_iter().enumerate() {
            let storage = Storage::new(committee.clone(), MAX_GC_ROUNDS);
            let ledger = Arc::new(MockLedgerService::new());

            let bft_ip: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let (primary, bft) = if config.bft {
                let bft = BFT::<CurrentNetwork>::new(account, storage, ledger, Some(bft_ip), None).unwrap();
                (bft.primary().clone(), Some(bft))
            } else {
                let primary = Primary::<CurrentNetwork>::new(account, storage, ledger, Some(bft_ip), None).unwrap();
                (primary, None)
            };

            let test_validator =
                TestValidator { id: id as u16, primary, primary_sender: None, bft, handles: Default::default() };
            validators.insert(id as u16, test_validator);
        }

        Self { config, validators }
    }

    // Starts each node in the network.
    pub async fn start(&mut self) {
        for validator in self.validators.values_mut() {
            let (primary_sender, primary_receiver) = init_primary_channels();
            validator.primary_sender = Some(primary_sender.clone());
            if let Some(bft) = &mut validator.bft {
                // Setup the channels and start the bft.
                bft.run(primary_sender, primary_receiver, None).await.unwrap();
            } else {
                // Setup the channels and start the primary.
                validator.primary.run(primary_sender, primary_receiver, None).await.unwrap();
            }

            if let Some(interval_ms) = self.config.fire_transmissions {
                validator.fire_transmissions(interval_ms);
            }

            if self.config.log_connections {
                validator.log_connections();
            }
        }

        if self.config.connect_all {
            self.connect_all().await;
        }
    }

    // Starts the solution and trasnaction cannons for node.
    pub fn fire_transmissions_at(&mut self, id: u16, interval_ms: u64) {
        self.validators.get_mut(&id).unwrap().fire_transmissions(interval_ms);
    }

    // Connects a node to another node.
    pub async fn connect_validators(&self, first_id: u16, second_id: u16) {
        let first_validator = self.validators.get(&first_id).unwrap();
        let second_validator_ip = self.validators.get(&second_id).unwrap().primary.gateway().local_ip();
        first_validator.primary.gateway().connect(second_validator_ip);
        // Give the connection time to be established.
        sleep(Duration::from_millis(100)).await;
    }

    // Connects all nodes to each other.
    pub async fn connect_all(&self) {
        for (validator, other_validator) in self.validators.values().tuple_combinations() {
            // Connect to the node.
            let ip = other_validator.primary.gateway().local_ip();
            validator.primary.gateway().connect(ip);
            // Give the connection time to be established.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    // Disconnects N nodes from all other nodes.
    pub async fn disconnect(&self, num_nodes: u16) {
        for validator in self.validators.values().take(num_nodes as usize) {
            for peer_ip in validator.primary.gateway().connected_peers().read().iter() {
                validator.primary.gateway().disconnect(*peer_ip);
            }
        }

        // Give the connections time to be closed.
        sleep(Duration::from_millis(100)).await;
    }

    // Checks if at least 2f + 1 nodes have reached the given round.
    pub fn is_round_reached(&self, round: u64) -> bool {
        let quorum_threshold = self.validators.len() / 2 + 1;
        self.validators.values().filter(|v| v.primary.current_round() >= round).count() >= quorum_threshold
    }

    // Checks if all the nodes have stopped progressing.
    pub async fn is_halted(&self) -> bool {
        let halt_round = self.validators.values().map(|v| v.primary.current_round()).max().unwrap();
        sleep(Duration::from_millis(MAX_BATCH_DELAY * 2)).await;
        self.validators.values().all(|v| v.primary.current_round() <= halt_round)
    }

    // Checks if the committee is coherent in storage for all nodes (not quorum) over a range of
    // rounds.
    pub fn is_committee_coherent<T>(&self, rounds_range: T) -> bool
    where
        T: RangeBounds<u64> + IntoIterator<Item = u64>,
    {
        rounds_range.into_iter().all(|round| {
            self.validators.values().map(|v| v.primary.storage().get_committee(round).unwrap()).dedup().count() == 1
        })
    }

    // Checks if the certificates are coherent in storage for all nodes (not quorum) over a range
    // of rounds.
    pub fn is_certificate_round_coherent<T>(&self, rounds_range: T) -> bool
    where
        T: RangeBounds<u64> + IntoIterator<Item = u64>,
    {
        rounds_range.into_iter().all(|round| {
            self.validators.values().map(|v| v.primary.storage().get_certificates_for_round(round)).dedup().count() == 1
        })
    }
}

// Initializes a new test committee.
fn new_test_committee(n: u16) -> (Vec<Account<CurrentNetwork>>, Committee<CurrentNetwork>) {
    const INITIAL_STAKE: u64 = MIN_STAKE;

    let mut accounts = Vec::with_capacity(n as usize);
    let mut members = IndexMap::with_capacity(n as usize);
    for i in 0..n {
        // Sample the account.
        let account = Account::new(&mut TestRng::fixed(i as u64)).unwrap();

        info!("Validator {}: {}", i, account.address());

        members.insert(account.address(), INITIAL_STAKE);
        accounts.push(account);
    }
    // Initialize the committee.
    let committee = Committee::<CurrentNetwork>::new(1u64, members).unwrap();

    (accounts, committee)
}
