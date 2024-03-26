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

use crate::common::CurrentNetwork;
use snarkos_node_bft_events::{Event, EventCodec};

use std::{
    io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use pea2pea::{
    protocols::{Handshake, OnDisconnect, Reading, Writing},
    Config,
    Connection,
    ConnectionSide,
    Node,
    Pea2Pea,
};

use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::timeout,
};

pub struct TestPeer {
    inner_node: InnerNode,
    inbound_rx: Receiver<(SocketAddr, Event<CurrentNetwork>)>,
}

#[derive(Clone)]
struct InnerNode {
    // The pea2pea node instance.
    node: Node,
    // The inbound channel sender, used to consolidate inbound messages into a single queue so they
    // can be read in order in tests.
    inbound_tx: Sender<(SocketAddr, Event<CurrentNetwork>)>,
}

impl TestPeer {
    pub async fn new() -> Self {
        let (tx, rx) = mpsc::channel(100);
        let inner_node = InnerNode {
            node: Node::new(Config {
                max_connections: 200,
                listener_addr: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
                ..Default::default()
            }),
            inbound_tx: tx,
        };

        inner_node.enable_handshake().await;
        inner_node.enable_reading().await;
        inner_node.enable_writing().await;
        inner_node.enable_disconnect().await;
        inner_node.node().start_listening().await.unwrap();

        Self { inner_node, inbound_rx: rx }
    }

    pub fn listening_addr(&self) -> SocketAddr {
        self.inner_node.node().listening_addr().expect("addr should be present")
    }

    pub async fn connect(&self, target: SocketAddr) -> io::Result<()> {
        self.inner_node.node().connect(target).await?;
        Ok(())
    }

    // Note: the codec doesn't actually support sending bytes post-handshake, perhaps this should
    // be relaxed by making a test-only codec in future.
    pub fn unicast(&self, target: SocketAddr, message: Event<CurrentNetwork>) -> io::Result<()> {
        self.inner_node.unicast(target, message)?;
        Ok(())
    }

    pub async fn recv(&mut self) -> (SocketAddr, Event<CurrentNetwork>) {
        match self.inbound_rx.recv().await {
            Some(message) => message,
            None => panic!("all senders dropped!"),
        }
    }

    pub async fn recv_timeout(&mut self, duration: Duration) -> (SocketAddr, Event<CurrentNetwork>) {
        match timeout(duration, self.recv()).await {
            Ok(message) => message,
            _ => panic!("timed out waiting for message"),
        }
    }
}

impl Pea2Pea for InnerNode {
    fn node(&self) -> &Node {
        &self.node
    }
}

impl Handshake for InnerNode {
    // Set the timeout on the test peer to be longer than the gateway's timeout.
    const TIMEOUT_MS: u64 = 10_000;

    async fn perform_handshake(&self, connection: Connection) -> io::Result<Connection> {
        // Don't perform the Aleo handshake so we can test the edge cases fully.
        Ok(connection)
    }
}

impl Writing for InnerNode {
    type Codec = EventCodec<CurrentNetwork>;
    type Message = Event<CurrentNetwork>;

    fn codec(&self, _peer_addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }
}

impl Reading for InnerNode {
    type Codec = EventCodec<CurrentNetwork>;
    type Message = Event<CurrentNetwork>;

    fn codec(&self, _peer_addr: SocketAddr, _side: ConnectionSide) -> Self::Codec {
        Default::default()
    }

    async fn process_message(&self, peer_addr: SocketAddr, message: Self::Message) -> io::Result<()> {
        self.inbound_tx.send((peer_addr, message)).await.map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "failed to send message to test peer, all receivers have been dropped")
        })
    }
}

impl OnDisconnect for InnerNode {
    async fn on_disconnect(&self, _peer_addr: SocketAddr) {}
}
