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

use snarkos_account::Account;
use snarkos_node_router::{
    expect_message,
    messages::{ChallengeRequest, ChallengeResponse, Message, MessageCodec, MessageTrait, NodeType},
};
use snarkvm::{
    ledger::narwhal::Data,
    prelude::{block::Block, error, Address, FromBytes, MainnetV0 as CurrentNetwork, Network, TestRng},
};

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
};

use futures_util::{sink::SinkExt, TryStreamExt};
use pea2pea::{
    protocols::{Handshake, OnDisconnect, Reading, Writing},
    Config,
    Connection,
    ConnectionSide,
    Node,
    Pea2Pea,
};
use rand::Rng;
use tokio_util::codec::Framed;
use tracing::*;

const ALEO_MAXIMUM_FORK_DEPTH: u32 = 4096;

/// Returns a fixed account.
pub fn sample_account() -> Account<CurrentNetwork> {
    Account::<CurrentNetwork>::from_str("APrivateKey1zkp2oVPTci9kKcUprnbzMwq95Di1MQERpYBhEeqvkrDirK1").unwrap()
}

/// Loads the current network's genesis block.
pub fn sample_genesis_block() -> Block<CurrentNetwork> {
    Block::<CurrentNetwork>::from_bytes_le(CurrentNetwork::genesis_bytes()).unwrap()
}

#[derive(Clone)]
pub struct TestPeer {
    node: Node,
    node_type: NodeType,
    account: Account<CurrentNetwork>,
}

impl Pea2Pea for TestPeer {
    fn node(&self) -> &Node {
        &self.node
    }
}

impl TestPeer {
    pub async fn client() -> Self {
        Self::new(NodeType::Client, sample_account()).await
    }

    pub async fn prover() -> Self {
        Self::new(NodeType::Prover, sample_account()).await
    }

    pub async fn validator() -> Self {
        Self::new(NodeType::Validator, sample_account()).await
    }

    pub async fn new(node_type: NodeType, account: Account<CurrentNetwork>) -> Self {
        let peer = Self {
            node: Node::new(Config {
                max_connections: 200,
                listener_addr: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
                ..Default::default()
            }),
            node_type,
            account,
        };

        peer.enable_handshake().await;
        peer.enable_reading().await;
        peer.enable_writing().await;
        peer.enable_disconnect().await;

        peer.node().start_listening().await.unwrap();

        peer
    }

    pub fn node_type(&self) -> NodeType {
        self.node_type
    }

    pub fn account(&self) -> &Account<CurrentNetwork> {
        &self.account
    }

    pub fn address(&self) -> Address<CurrentNetwork> {
        self.account.address()
    }
}

impl Handshake for TestPeer {
    async fn perform_handshake(&self, mut conn: Connection) -> io::Result<Connection> {
        let rng = &mut TestRng::default();

        let local_ip = self.node().listening_addr().expect("listening address should be present");

        let peer_addr = conn.addr();
        let node_side = !conn.side();
        let stream = self.borrow_stream(&mut conn);
        let mut framed = Framed::new(stream, MessageCodec::<CurrentNetwork>::default());

        // Retrieve the genesis block header.
        let genesis_header = *sample_genesis_block().header();

        // TODO(nkls): add assertions on the contents of messages.
        match node_side {
            ConnectionSide::Initiator => {
                // Send a challenge request to the peer.
                let our_request = ChallengeRequest::new(local_ip.port(), self.node_type(), self.address(), rng.gen());
                framed.send(Message::ChallengeRequest(our_request)).await?;

                // Receive the peer's challenge bundle.
                let _peer_response = expect_message!(Message::ChallengeResponse, framed, peer_addr);
                let peer_request = expect_message!(Message::ChallengeRequest, framed, peer_addr);

                // Sign the nonce.
                let response_nonce: u64 = rng.gen();
                let data = [peer_request.nonce.to_le_bytes(), response_nonce.to_le_bytes()].concat();
                let signature = self.account().sign_bytes(&data, rng).unwrap();

                // Send the challenge response.
                let our_response =
                    ChallengeResponse { genesis_header, signature: Data::Object(signature), nonce: response_nonce };
                framed.send(Message::ChallengeResponse(our_response)).await?;
            }
            ConnectionSide::Responder => {
                // Listen for the challenge request.
                let peer_request = expect_message!(Message::ChallengeRequest, framed, peer_addr);

                // Sign the nonce.
                let response_nonce: u64 = rng.gen();
                let data = [peer_request.nonce.to_le_bytes(), response_nonce.to_le_bytes()].concat();
                let signature = self.account().sign_bytes(&data, rng).unwrap();

                // Send our challenge bundle.
                let our_response =
                    ChallengeResponse { genesis_header, signature: Data::Object(signature), nonce: response_nonce };
                framed.send(Message::ChallengeResponse(our_response)).await?;
                let our_request = ChallengeRequest::new(local_ip.port(), self.node_type(), self.address(), rng.gen());
                framed.send(Message::ChallengeRequest(our_request)).await?;

                // Listen for the challenge response.
                let _peer_response = expect_message!(Message::ChallengeResponse, framed, peer_addr);
            }
        }

        Ok(conn)
    }
}

impl Writing for TestPeer {
    type Codec = MessageCodec<CurrentNetwork>;
    type Message = Message<CurrentNetwork>;

    fn codec(&self, _addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }
}

impl Reading for TestPeer {
    type Codec = MessageCodec<CurrentNetwork>;
    type Message = Message<CurrentNetwork>;

    fn codec(&self, _peer_addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }

    async fn process_message(&self, _peer_ip: SocketAddr, _message: Self::Message) -> io::Result<()> {
        Ok(())
    }
}

impl OnDisconnect for TestPeer {
    async fn on_disconnect(&self, _peer_addr: SocketAddr) {}
}
