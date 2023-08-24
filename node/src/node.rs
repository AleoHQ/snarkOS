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

use crate::{traits::NodeInterface, Beacon, Client, Prover, Validator};
use snarkos_account::Account;
use snarkos_node_messages::NodeType;
use snarkvm::prelude::{
    block::Block,
    store::helpers::{memory::ConsensusMemory, rocksdb::ConsensusDB},
    Address,
    Network,
    PrivateKey,
    ViewKey,
};

use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};

pub enum Node<N: Network> {
    /// A beacon is a full node, capable of producing blocks.
    Beacon(Arc<Beacon<N, ConsensusDB<N>>>),
    /// A validator is a full node, capable of validating blocks.
    Validator(Arc<Validator<N, ConsensusDB<N>>>),
    /// A prover is a full node, capable of producing proofs for consensus.
    Prover(Arc<Prover<N, ConsensusMemory<N>>>),
    /// A client node is a full node, capable of querying with the network.
    Client(Arc<Client<N, ConsensusMemory<N>>>),
}

impl<N: Network> Node<N> {
    /// Initializes a new beacon node.
    pub async fn new_beacon(
        node_ip: SocketAddr,
        rest_ip: Option<SocketAddr>,
        account: Account<N>,
        trusted_peers: &[SocketAddr],
        genesis: Block<N>,
        cdn: Option<String>,
        dev: Option<u16>,
    ) -> Result<Self> {
        Ok(Self::Beacon(Arc::new(
            Beacon::new(node_ip, rest_ip, None, account, trusted_peers, genesis, cdn, dev).await?,
        )))
    }

    /// Initializes a new validator node.
    pub async fn new_validator(
        node_ip: SocketAddr,
        rest_ip: Option<SocketAddr>,
        account: Account<N>,
        trusted_peers: &[SocketAddr],
        genesis: Block<N>,
        cdn: Option<String>,
        dev: Option<u16>,
    ) -> Result<Self> {
        Ok(Self::Validator(Arc::new(
            Validator::new(node_ip, rest_ip, None, account, trusted_peers, genesis, cdn, dev).await?,
        )))
    }

    /// Initializes a new prover node.
    pub async fn new_prover(
        node_ip: SocketAddr,
        account: Account<N>,
        trusted_peers: &[SocketAddr],
        genesis: Block<N>,
        dev: Option<u16>,
    ) -> Result<Self> {
        Ok(Self::Prover(Arc::new(Prover::new(node_ip, account, trusted_peers, genesis, dev).await?)))
    }

    /// Initializes a new client node.
    pub async fn new_client(
        node_ip: SocketAddr,
        account: Account<N>,
        trusted_peers: &[SocketAddr],
        genesis: Block<N>,
        dev: Option<u16>,
    ) -> Result<Self> {
        Ok(Self::Client(Arc::new(Client::new(node_ip, account, trusted_peers, genesis, dev).await?)))
    }

    /// Returns the node type.
    pub fn node_type(&self) -> NodeType {
        match self {
            Self::Beacon(beacon) => beacon.node_type(),
            Self::Validator(validator) => validator.node_type(),
            Self::Prover(prover) => prover.node_type(),
            Self::Client(client) => client.node_type(),
        }
    }

    /// Returns the account private key of the node.
    pub fn private_key(&self) -> &PrivateKey<N> {
        match self {
            Self::Beacon(node) => node.private_key(),
            Self::Validator(node) => node.private_key(),
            Self::Prover(node) => node.private_key(),
            Self::Client(node) => node.private_key(),
        }
    }

    /// Returns the account view key of the node.
    pub fn view_key(&self) -> &ViewKey<N> {
        match self {
            Self::Beacon(node) => node.view_key(),
            Self::Validator(node) => node.view_key(),
            Self::Prover(node) => node.view_key(),
            Self::Client(node) => node.view_key(),
        }
    }

    /// Returns the account address of the node.
    pub fn address(&self) -> Address<N> {
        match self {
            Self::Beacon(node) => node.address(),
            Self::Validator(node) => node.address(),
            Self::Prover(node) => node.address(),
            Self::Client(node) => node.address(),
        }
    }

    /// Returns `true` if the node is in development mode.
    pub fn is_dev(&self) -> bool {
        match self {
            Self::Beacon(node) => node.is_dev(),
            Self::Validator(node) => node.is_dev(),
            Self::Prover(node) => node.is_dev(),
            Self::Client(node) => node.is_dev(),
        }
    }
}
