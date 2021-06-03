// Copyright (C) 2019-2021 Aleo Systems Inc.
// This file is part of the snarkOS library.

// The snarkOS library is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// The snarkOS library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with the snarkOS library. If not, see <https://www.gnu.org/licenses/>.

pub mod blocks;
pub use blocks::*;

#[cfg(test)]
pub mod encryption;

#[cfg(test)]
pub mod sync;

pub mod topology;

use crate::sync::FIXTURE;

use snarkos_network::{errors::*, *};
use snarkos_storage::LedgerStorage;

use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{tcp::OwnedReadHalf, TcpListener, TcpStream},
    runtime,
};
use tracing::*;

/// Returns a random tcp socket address and binds it to a listener
pub async fn random_bound_address() -> (SocketAddr, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (addr, listener)
}

/// Waits until an expression is true or times out.
///
/// Uses polling to cut down on time otherwise used by calling `sleep` in tests.
#[macro_export]
macro_rules! wait_until {
    ($limit_secs: expr, $condition: expr $(, $sleep_millis: expr)?) => {
        let now = std::time::Instant::now();
        loop {
            if $condition {
                break;
            }

            // Default timout.
            let sleep_millis = 10;
            // Set if present in args.
            $(let sleep_millis = $sleep_millis;)?
            tokio::time::sleep(std::time::Duration::from_millis(sleep_millis)).await;
            if now.elapsed() > std::time::Duration::from_secs($limit_secs) {
                panic!("timed out!");
            }
        }
    };
}

#[derive(Clone)]
pub struct ConsensusSetup {
    pub is_miner: bool,
    pub block_sync_interval: u64,
    pub tx_sync_interval: u64,
}

impl ConsensusSetup {
    pub fn new(is_miner: bool, block_sync_interval: u64, tx_sync_interval: u64) -> Self {
        Self {
            is_miner,
            block_sync_interval,
            tx_sync_interval,
        }
    }
}

impl Default for ConsensusSetup {
    fn default() -> Self {
        Self {
            is_miner: false,
            block_sync_interval: 600,
            tx_sync_interval: 600,
        }
    }
}

#[derive(Clone)]
pub struct TestSetup {
    pub node_id: u64,
    pub socket_address: SocketAddr,
    pub consensus_setup: Option<ConsensusSetup>,
    pub peer_sync_interval: u64,
    pub min_peers: u16,
    pub max_peers: u16,
    pub is_bootnode: bool,
    pub bootnodes: Vec<String>,
    pub tokio_handle: Option<runtime::Handle>,
}

impl TestSetup {
    pub fn new(
        node_id: u64,
        socket_address: SocketAddr,
        consensus_setup: Option<ConsensusSetup>,
        peer_sync_interval: u64,
        min_peers: u16,
        max_peers: u16,
        is_bootnode: bool,
        bootnodes: Vec<String>,
        tokio_handle: Option<runtime::Handle>,
    ) -> Self {
        Self {
            node_id,
            socket_address,
            consensus_setup,
            peer_sync_interval,
            min_peers,
            max_peers,
            is_bootnode,
            bootnodes,
            tokio_handle,
        }
    }
}

impl Default for TestSetup {
    fn default() -> Self {
        Self {
            node_id: u64::MAX,
            socket_address: "127.0.0.1:0".parse().unwrap(),
            consensus_setup: Some(Default::default()),
            peer_sync_interval: 600,
            min_peers: 1,
            max_peers: 100,
            is_bootnode: false,
            bootnodes: vec![],
            tokio_handle: None,
        }
    }
}

pub async fn test_consensus(setup: ConsensusSetup) -> Sync<LedgerStorage> {
    let consensus = Arc::new(crate::sync::create_test_consensus().await);

    Sync::new(
        consensus,
        setup.is_miner,
        Duration::from_secs(setup.block_sync_interval),
        Duration::from_secs(setup.tx_sync_interval),
    )
}

/// Returns a `Config` struct based on the given `TestSetup`.
pub fn test_config(setup: TestSetup) -> Config {
    Config::new(
        setup.socket_address,
        setup.min_peers,
        setup.max_peers,
        setup.bootnodes,
        setup.is_bootnode,
        Duration::from_secs(setup.peer_sync_interval),
    )
    .unwrap()
}

/// Starts a node with the specified bootnodes.
pub async fn test_node(setup: TestSetup) -> Node<LedgerStorage> {
    let is_miner = setup.consensus_setup.as_ref().map(|c| c.is_miner) == Some(true);
    let config = test_config(setup.clone());
    let mut node = Node::new(config).await.unwrap();

    if let Some(consensus_setup) = setup.consensus_setup {
        let consensus = test_consensus(consensus_setup).await;
        node.set_sync(consensus);
    }

    node.listen().await.unwrap();
    node.start_services().await;

    if is_miner {
        let miner_address = FIXTURE.test_accounts[0].address.clone();
        MinerInstance::new(miner_address, node.clone()).spawn();
    }

    node
}

pub struct FakeNode {
    network: PeerNetwork,
    reader: PeerReader<OwnedReadHalf>,
}

impl FakeNode {
    pub fn new(stream: TcpStream, _peer_addr: SocketAddr, noise: snow::TransportState) -> Self {
        let (reader, writer) = stream.into_split();

        let mut network = PeerNetwork {
            reader: Some(reader),
            writer,
            cipher: Cipher::new(
                noise,
                vec![0u8; MAX_MESSAGE_SIZE + 4096].into(),
                vec![0u8; NOISE_BUF_LEN].into(),
            ),
        };

        let reader = network.take_reader();

        Self { network, reader }
    }

    pub async fn read_payload(&mut self) -> Result<Payload, NetworkError> {
        let raw = self.reader.read_raw_payload().await?;
        let message = match self.network.read_payload(raw) {
            Ok(msg) => {
                let msg = Payload::deserialize(msg)?;
                debug!("read a {}", msg);
                msg
            }
            Err(e) => {
                error!("can't read a payload: {}", e);
                return Err(e);
            }
        };

        Ok(message)
    }

    pub async fn write_message(&mut self, payload: &Payload) {
        self.network.write_payload(payload).await.unwrap();
        debug!("wrote a message containing a {} to the stream", payload);
    }

    pub async fn write_bytes(&mut self, bytes: &[u8]) {
        self.network.writer.write_all(bytes).await.unwrap();
        debug!("wrote {}B to the stream", bytes.len());
    }
}

pub async fn spawn_2_fake_nodes() -> (FakeNode, FakeNode) {
    // set up listeners and establish addresses
    let node0_listener = TcpListener::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .unwrap();
    let node0_addr = node0_listener.local_addr().unwrap();
    let node0_listening_task = tokio::spawn(async move { node0_listener.accept().await.unwrap() });

    // set up streams
    let mut node1_stream = TcpStream::connect(&node0_addr).await.unwrap();
    let (mut node0_stream, node1_addr) = node0_listening_task.await.unwrap();

    // node0's noise - initiator
    let builder = snow::Builder::with_resolver(
        snarkos_network::HANDSHAKE_PATTERN.parse().unwrap(),
        Box::new(snow::resolvers::SodiumResolver),
    );
    let static_key = builder.generate_keypair().unwrap().private;
    let noise_builder = builder
        .local_private_key(&static_key)
        .psk(3, snarkos_network::HANDSHAKE_PSK);
    let mut node0_noise = noise_builder.build_initiator().unwrap();

    // node1's noise - responder
    let builder = snow::Builder::with_resolver(
        snarkos_network::HANDSHAKE_PATTERN.parse().unwrap(),
        Box::new(snow::resolvers::SodiumResolver),
    );
    let static_key = builder.generate_keypair().unwrap().private;
    let noise_builder = builder
        .local_private_key(&static_key)
        .psk(3, snarkos_network::HANDSHAKE_PSK);
    let mut node1_noise = noise_builder.build_responder().unwrap();

    // shared bits
    let mut buffer: Box<[u8]> = vec![0u8; snarkos_network::NOISE_BUF_LEN].into();
    let mut buf = [0u8; snarkos_network::NOISE_BUF_LEN];

    // -> e (node0)
    let len = node0_noise.write_message(&[], &mut buffer).unwrap();
    node0_stream.write_all(&[len as u8]).await.unwrap();
    node0_stream.write_all(&buffer[..len]).await.unwrap();

    // <- e (node1)
    node1_stream.read_exact(&mut buf[..1]).await.unwrap();
    let len = buf[0] as usize;
    let len = node1_stream.read_exact(&mut buf[..len]).await.unwrap();
    node1_noise.read_message(&buf[..len], &mut buffer).unwrap();

    // -> e, ee, s, es (node1)
    let version = Version::serialize(&Version::new(snarkos_network::PROTOCOL_VERSION, node1_addr.port(), 1)).unwrap();
    let len = node1_noise.write_message(&version, &mut buffer).unwrap();
    node1_stream.write_all(&[len as u8]).await.unwrap();
    node1_stream.write_all(&buffer[..len]).await.unwrap();

    // <- e, ee, s, es (node0)
    node0_stream.read_exact(&mut buf[..1]).await.unwrap();
    let len = buf[0] as usize;
    let len = node0_stream.read_exact(&mut buf[..len]).await.unwrap();
    let len = node0_noise.read_message(&buf[..len], &mut buffer).unwrap();
    let _version = Version::deserialize(&buffer[..len]).unwrap();

    // -> s, se, psk (node0)
    let peer_version =
        Version::serialize(&Version::new(snarkos_network::PROTOCOL_VERSION, node0_addr.port(), 0)).unwrap();
    let len = node0_noise.write_message(&peer_version, &mut buffer).unwrap();
    node0_stream.write_all(&[len as u8]).await.unwrap();
    node0_stream.write_all(&buffer[..len]).await.unwrap();

    // <- e, ee, s, es (node1)
    node1_stream.read_exact(&mut buf[..1]).await.unwrap();
    let len = buf[0] as usize;
    let len = node1_stream.read_exact(&mut buf[..len]).await.unwrap();
    let len = node1_noise.read_message(&buf[..len], &mut buffer).unwrap();
    let _version = Version::deserialize(&buffer[..len]).unwrap();

    let node0_noise = node0_noise.into_transport_mode().unwrap();
    let node1_noise = node1_noise.into_transport_mode().unwrap();

    let node0 = FakeNode::new(node0_stream, node0_addr, node0_noise);
    let node1 = FakeNode::new(node1_stream, node1_addr, node1_noise);

    (node0, node1)
}

pub async fn handshaken_peer(node_listener: SocketAddr) -> FakeNode {
    // set up a fake node (peer), which is basically just a socket
    let mut peer_stream = TcpStream::connect(&node_listener).await.unwrap();

    // register the addresses bound to the connection between the node and the peer
    let peer_addr = peer_stream.local_addr().unwrap();

    let builder = snow::Builder::with_resolver(
        snarkos_network::HANDSHAKE_PATTERN.parse().unwrap(),
        Box::new(snow::resolvers::SodiumResolver),
    );
    let static_key = builder.generate_keypair().unwrap().private;
    let noise_builder = builder
        .local_private_key(&static_key)
        .psk(3, snarkos_network::HANDSHAKE_PSK);
    let mut noise = noise_builder.build_initiator().unwrap();
    let mut buffer: Box<[u8]> = vec![0u8; snarkos_network::NOISE_BUF_LEN].into();
    let mut buf = [0u8; snarkos_network::NOISE_BUF_LEN]; // a temporary intermediate buffer to decrypt from

    // -> e
    let len = noise.write_message(&[], &mut buffer).unwrap();
    peer_stream.write_all(&[len as u8]).await.unwrap();
    peer_stream.write_all(&buffer[..len]).await.unwrap();

    // <- e, ee, s, es
    peer_stream.read_exact(&mut buf[..1]).await.unwrap();
    let len = buf[0] as usize;
    let len = peer_stream.read_exact(&mut buf[..len]).await.unwrap();
    let len = noise.read_message(&buf[..len], &mut buffer).unwrap();
    let _node_version = Version::deserialize(&buffer[..len]).unwrap();

    // -> s, se, psk
    let peer_version =
        Version::serialize(&Version::new(snarkos_network::PROTOCOL_VERSION, peer_addr.port(), 0)).unwrap();
    let len = noise.write_message(&peer_version, &mut buffer).unwrap();
    peer_stream.write_all(&[len as u8]).await.unwrap();
    peer_stream.write_all(&buffer[..len]).await.unwrap();

    let noise = noise.into_transport_mode().unwrap();

    FakeNode::new(peer_stream, peer_addr, noise)
}

pub async fn handshaken_node_and_peer(node_setup: TestSetup) -> (Node<LedgerStorage>, FakeNode) {
    // start a test node and listen for incoming connections
    let node = test_node(node_setup).await;
    let node_listener = node.local_address().unwrap();
    let fake_node = handshaken_peer(node_listener).await;

    (node, fake_node)
}

#[allow(clippy::needless_lifetimes)] // clippy bug: https://github.com/rust-lang/rust-clippy/issues/5787
pub async fn read_payload<'a, T: AsyncRead + Unpin>(
    stream: &mut T,
    buffer: &'a mut [u8],
) -> Result<&'a [u8], NetworkError> {
    stream.read_exact(buffer).await?;

    Ok(buffer)
}

/// Reads the message header into a `MessageHeader`.
pub async fn read_header<T: AsyncRead + Unpin>(stream: &mut T) -> Result<MessageHeader, NetworkError> {
    let mut header_arr = [0u8; 4];
    stream.read_exact(&mut header_arr).await?;
    let header = MessageHeader::from(header_arr);

    if header.len as usize > MAX_MESSAGE_SIZE {
        Err(NetworkError::MessageTooBig(header.len as usize))
    } else {
        Ok(header)
    }
}

/// Writes a payload into the supplied `TcpStream`.
pub async fn write_message_to_stream(payload: Payload, peer_stream: &mut TcpStream) {
    let payload = Payload::serialize(&payload).unwrap();
    let header = MessageHeader {
        len: payload.len() as u32,
    }
    .as_bytes();
    peer_stream.write_all(&header[..]).await.unwrap();
    peer_stream.write_all(&payload).await.unwrap();
    peer_stream.flush().await.unwrap();
}
