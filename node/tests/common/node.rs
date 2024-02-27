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

use crate::common::test_peer::sample_genesis_block;
use snarkos_account::Account;
use snarkos_node::{Client, Prover, Validator};
use snarkvm::prelude::{store::helpers::memory::ConsensusMemory, Testnet3 as CurrentNetwork};

use aleo_std::StorageMode;
use std::str::FromStr;

pub async fn client() -> Client<CurrentNetwork, ConsensusMemory<CurrentNetwork>> {
    Client::new(
        "127.0.0.1:0".parse().unwrap(),
        None,
        10,
        Account::<CurrentNetwork>::from_str("APrivateKey1zkp2oVPTci9kKcUprnbzMwq95Di1MQERpYBhEeqvkrDirK1").unwrap(),
        &[],
        sample_genesis_block(),
        None, // No CDN.
        StorageMode::Production,
    )
    .await
    .expect("couldn't create client instance")
}

pub async fn prover() -> Prover<CurrentNetwork, ConsensusMemory<CurrentNetwork>> {
    Prover::new(
        "127.0.0.1:0".parse().unwrap(),
        Account::<CurrentNetwork>::from_str("APrivateKey1zkp2oVPTci9kKcUprnbzMwq95Di1MQERpYBhEeqvkrDirK1").unwrap(),
        &[],
        sample_genesis_block(),
        StorageMode::Production,
    )
    .await
    .expect("couldn't create prover instance")
}

pub async fn validator() -> Validator<CurrentNetwork, ConsensusMemory<CurrentNetwork>> {
    Validator::new(
        "127.0.0.1:0".parse().unwrap(),
        None,
        None,
        10,
        Account::<CurrentNetwork>::from_str("APrivateKey1zkp2oVPTci9kKcUprnbzMwq95Di1MQERpYBhEeqvkrDirK1").unwrap(),
        &[],
        &[],
        sample_genesis_block(), // Should load the current network's genesis block.
        None,                   // No CDN.
        StorageMode::Production,
        None,
    )
    .await
    .expect("couldn't create validator instance")
}
