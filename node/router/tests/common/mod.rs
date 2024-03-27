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

#[allow(dead_code)]
pub mod router;
pub use router::*;

use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
};

use snarkos_account::Account;
use snarkos_node_router::{messages::NodeType, Router};
use snarkvm::prelude::{block::Block, FromBytes, MainnetV0 as CurrentNetwork, Network};

/// A helper macro to print the TCP listening address, along with the connected and connecting peers.
#[macro_export]
macro_rules! print_tcp {
    ($node:expr) => {
        println!(
            "{}: Active - {:?}, Pending - {:?}",
            $node.local_ip(),
            $node.tcp().connected_addrs(),
            $node.tcp().connecting_addrs()
        );
    };
}

/// Returns a fixed account.
pub fn sample_account() -> Account<CurrentNetwork> {
    Account::<CurrentNetwork>::from_str("APrivateKey1zkp2oVPTci9kKcUprnbzMwq95Di1MQERpYBhEeqvkrDirK1").unwrap()
}

/// Loads the current network's genesis block.
pub fn sample_genesis_block<N: Network>() -> Block<N> {
    Block::<N>::from_bytes_le(N::genesis_bytes()).unwrap()
}

/// Enables logging in tests.
#[allow(dead_code)]
pub fn initialize_logger(level: u8) {
    match level {
        0 => env::set_var("RUST_LOG", "info"),
        1 => env::set_var("RUST_LOG", "debug"),
        2 | 3 => env::set_var("RUST_LOG", "trace"),
        _ => env::set_var("RUST_LOG", "info"),
    };

    // Filter out undesirable logs.
    let filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("tokio_util=off".parse().unwrap())
        .add_directive("mio=off".parse().unwrap());

    // Initialize tracing.
    let _ = tracing_subscriber::fmt().with_env_filter(filter).with_target(level == 3).try_init();
}

/// Initializes a client router. Setting the `listening_port = 0` will result in a random port being assigned.
#[allow(dead_code)]
pub async fn client(listening_port: u16, max_peers: u16) -> TestRouter<CurrentNetwork> {
    Router::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), listening_port),
        NodeType::Client,
        sample_account(),
        &[],
        max_peers,
        true,
        true,
    )
    .await
    .expect("couldn't create client router")
    .into()
}

/// Initializes a prover router. Setting the `listening_port = 0` will result in a random port being assigned.
#[allow(dead_code)]
pub async fn prover(listening_port: u16, max_peers: u16) -> TestRouter<CurrentNetwork> {
    Router::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), listening_port),
        NodeType::Prover,
        sample_account(),
        &[],
        max_peers,
        true,
        true,
    )
    .await
    .expect("couldn't create prover router")
    .into()
}

/// Initializes a validator router. Setting the `listening_port = 0` will result in a random port being assigned.
#[allow(dead_code)]
pub async fn validator(listening_port: u16, max_peers: u16) -> TestRouter<CurrentNetwork> {
    Router::new(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), listening_port),
        NodeType::Validator,
        sample_account(),
        &[],
        max_peers,
        true, // Router tests require validators to connect to peers.
        true,
    )
    .await
    .expect("couldn't create validator router")
    .into()
}
