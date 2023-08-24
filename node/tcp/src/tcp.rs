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

use std::{
    collections::HashSet,
    fmt,
    io,
    net::{IpAddr, SocketAddr},
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering::*},
        Arc,
    },
    time::Duration,
};

use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tokio::{
    io::split,
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};
use tracing::*;

use crate::{
    connections::{Connection, ConnectionSide, Connections},
    protocols::{Protocol, Protocols},
    Config,
    KnownPeers,
    Stats,
};

// A sequential numeric identifier assigned to `Tcp`s that were not provided with a name.
static SEQUENTIAL_NODE_ID: AtomicUsize = AtomicUsize::new(0);

/// The central object responsible for handling connections.
#[derive(Clone)]
pub struct Tcp(Arc<InnerTcp>);

impl Deref for Tcp {
    type Target = Arc<InnerTcp>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[doc(hidden)]
pub struct InnerTcp {
    /// The tracing span.
    span: Span,
    /// The node's configuration.
    config: Config,
    /// The node's listening address.
    listening_addr: OnceCell<SocketAddr>,
    /// Contains objects used by the protocols implemented by the node.
    pub(crate) protocols: Protocols,
    /// A list of connections that have not been finalized yet.
    connecting: Mutex<HashSet<SocketAddr>>,
    /// Contains objects related to the node's active connections.
    connections: Connections,
    /// Collects statistics related to the node's peers.
    known_peers: KnownPeers,
    /// Collects statistics related to the node itself.
    stats: Stats,
    /// The node's tasks.
    pub(crate) tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl Tcp {
    /// Creates a new [`Tcp`] using the given [`Config`].
    pub fn new(mut config: Config) -> Self {
        // If there is no pre-configured name, assign a sequential numeric identifier.
        if config.name.is_none() {
            config.name = Some(SEQUENTIAL_NODE_ID.fetch_add(1, Relaxed).to_string());
        }

        // Create a tracing span containing the node's name.
        let span = crate::helpers::create_span(config.name.as_deref().unwrap());

        // Initialize the Tcp stack.
        let tcp = Tcp(Arc::new(InnerTcp {
            span,
            config,
            listening_addr: Default::default(),
            protocols: Default::default(),
            connecting: Default::default(),
            connections: Default::default(),
            known_peers: Default::default(),
            stats: Default::default(),
            tasks: Default::default(),
        }));

        debug!(parent: tcp.span(), "The node is ready");

        tcp
    }

    /// Returns the name assigned.
    #[inline]
    pub fn name(&self) -> &str {
        // safe; can be set as None in Config, but receives a default value on Tcp creation
        self.config.name.as_deref().unwrap()
    }

    /// Returns a reference to the configuration.
    #[inline]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns the listening address; returns an error if Tcp was not configured
    /// to listen for inbound connections.
    pub fn listening_addr(&self) -> io::Result<SocketAddr> {
        self.listening_addr.get().copied().ok_or_else(|| io::ErrorKind::AddrNotAvailable.into())
    }

    /// Checks whether the provided address is connected.
    pub fn is_connected(&self, addr: SocketAddr) -> bool {
        self.connections.is_connected(addr)
    }

    /// Checks if Tcp is currently setting up a connection with the provided address.
    pub fn is_connecting(&self, addr: SocketAddr) -> bool {
        self.connecting.lock().contains(&addr)
    }

    /// Returns the number of active connections.
    pub fn num_connected(&self) -> usize {
        self.connections.num_connected()
    }

    /// Returns the number of connections that are currently being set up.
    pub fn num_connecting(&self) -> usize {
        self.connecting.lock().len()
    }

    /// Returns a list containing addresses of active connections.
    pub fn connected_addrs(&self) -> Vec<SocketAddr> {
        self.connections.addrs()
    }

    /// Returns a list containing addresses of pending connections.
    pub fn connecting_addrs(&self) -> Vec<SocketAddr> {
        self.connecting.lock().iter().copied().collect()
    }

    /// Returns a reference to the collection of statistics of known peers.
    #[inline]
    pub fn known_peers(&self) -> &KnownPeers {
        &self.known_peers
    }

    /// Returns a reference to the statistics.
    #[inline]
    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Returns the tracing [`Span`] associated with Tcp.
    #[inline]
    pub fn span(&self) -> &Span {
        &self.span
    }

    /// Gracefully shuts down the stack.
    pub async fn shut_down(&self) {
        debug!(parent: self.span(), "Shutting down the TCP stack");

        // Retrieve all tasks.
        let mut tasks = std::mem::take(&mut *self.tasks.lock()).into_iter();

        // Abort the listening task first.
        if let Some(listening_task) = tasks.next() {
            listening_task.abort(); // abort the listening task first
        }
        // Disconnect from all connected peers.
        for addr in self.connected_addrs() {
            self.disconnect(addr).await;
        }
        // Abort all remaining tasks.
        for handle in tasks {
            handle.abort();
        }
    }
}

impl Tcp {
    /// Connects to the provided `SocketAddr`.
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        if let Ok(listening_addr) = self.listening_addr() {
            // TODO(nkls): maybe this first check can be dropped; though it might be best to keep just in case.
            if addr == listening_addr || self.is_self_connect(addr) {
                error!(parent: self.span(), "Attempted to self-connect ({addr})");
                return Err(io::ErrorKind::AddrInUse.into());
            }
        }

        if !self.can_add_connection() {
            error!(parent: self.span(), "Too many connections; refusing to connect to {addr}");
            return Err(io::ErrorKind::ConnectionRefused.into());
        }

        if self.is_connected(addr) {
            warn!(parent: self.span(), "Already connected to {addr}");
            return Err(io::ErrorKind::AlreadyExists.into());
        }

        if !self.connecting.lock().insert(addr) {
            warn!(parent: self.span(), "Already connecting to {addr}");
            return Err(io::ErrorKind::AlreadyExists.into());
        }

        let stream =
            match timeout(Duration::from_millis(self.config().connection_timeout_ms.into()), TcpStream::connect(addr))
                .await
            {
                Ok(Ok(stream)) => Ok(stream),
                Ok(err) => {
                    self.connecting.lock().remove(&addr);
                    err
                }
                Err(err) => {
                    self.connecting.lock().remove(&addr);
                    error!("connection timeout error: {}", err);
                    Err(io::ErrorKind::TimedOut.into())
                }
            }?;

        let ret = self.adapt_stream(stream, addr, ConnectionSide::Initiator).await;

        if let Err(ref e) = ret {
            self.connecting.lock().remove(&addr);
            self.known_peers().register_failure(addr);
            error!(parent: self.span(), "Unable to initiate a connection with {addr}: {e}");
        }

        ret
    }

    /// Disconnects from the provided `SocketAddr`.
    pub async fn disconnect(&self, addr: SocketAddr) -> bool {
        if let Some(handler) = self.protocols.disconnect.get() {
            if self.is_connected(addr) {
                let (sender, receiver) = oneshot::channel();
                handler.trigger((addr, sender));
                let _ = receiver.await; // can't really fail
            }
        }

        let conn = self.connections.remove(addr);

        if let Some(ref conn) = conn {
            debug!(parent: self.span(), "Disconnecting from {}", conn.addr());

            // Shut down the associated tasks of the peer.
            for task in conn.tasks.iter().rev() {
                task.abort();
            }

            // If the (owning) Tcp was not the initiator of the connection, it doesn't know the listening address
            // of the associated peer, so the related stats are unreliable; the next connection initiated by the
            // peer could be bound to an entirely different port number
            if conn.side() == ConnectionSide::Initiator {
                self.known_peers().remove(conn.addr());
            }

            debug!(parent: self.span(), "Disconnected from {}", conn.addr());
        } else {
            warn!(parent: self.span(), "Failed to disconnect, was not connected to {addr}");
        }

        conn.is_some()
    }
}

impl Tcp {
    /// Spawns a task that listens for incoming connections.
    pub async fn enable_listener(&self) -> io::Result<SocketAddr> {
        // Retrieve the listening IP address, which must be set.
        let listener_ip =
            self.config().listener_ip.expect("Tcp::enable_listener was called, but Config::listener_ip is not set");

        // Initialize the TCP listener.
        let listener = self.create_listener(listener_ip).await?;

        // Discover the port, if it was unspecified.
        let port = listener.local_addr()?.port();

        // Set the listening IP address.
        let listening_addr = (listener_ip, port).into();
        self.listening_addr.set(listening_addr).expect("The node's listener was started more than once");

        // Use a channel to know when the listening task is ready.
        let (tx, rx) = oneshot::channel();

        let tcp = self.clone();
        let listening_task = tokio::spawn(async move {
            trace!(parent: tcp.span(), "Spawned the listening task");
            tx.send(()).unwrap(); // safe; the channel was just opened

            loop {
                // Await for a new connection.
                match listener.accept().await {
                    Ok((stream, addr)) => tcp.handle_connection(stream, addr),
                    Err(e) => error!(parent: tcp.span(), "Failed to accept a connection: {e}"),
                }
            }
        });
        self.tasks.lock().push(listening_task);
        let _ = rx.await;
        debug!(parent: self.span(), "Listening on {listening_addr}");

        Ok(listening_addr)
    }

    /// Creates an instance of `TcpListener` based on the node's configuration.
    async fn create_listener(&self, listener_ip: IpAddr) -> io::Result<TcpListener> {
        debug!("Creating a TCP listener on {listener_ip}...");
        let listener = if let Some(port) = self.config().desired_listening_port {
            // Construct the desired listening IP address.
            let desired_listening_addr = SocketAddr::new(listener_ip, port);
            // If a desired listening port is set, try to bind to it.
            match TcpListener::bind(desired_listening_addr).await {
                Ok(listener) => listener,
                Err(e) => {
                    if self.config().allow_random_port {
                        warn!(
                            parent: self.span(),
                            "Trying any listening port, as the desired port is unavailable: {e}"
                        );
                        let random_available_addr = SocketAddr::new(listener_ip, 0);
                        TcpListener::bind(random_available_addr).await?
                    } else {
                        error!(parent: self.span(), "The desired listening port is unavailable: {e}");
                        return Err(e);
                    }
                }
            }
        } else if self.config().allow_random_port {
            let random_available_addr = SocketAddr::new(listener_ip, 0);
            TcpListener::bind(random_available_addr).await?
        } else {
            panic!("As 'listener_ip' is set, either 'desired_listening_port' or 'allow_random_port' must be set");
        };

        Ok(listener)
    }

    /// Handles a new inbound connection.
    fn handle_connection(&self, stream: TcpStream, addr: SocketAddr) {
        debug!(parent: self.span(), "Received a connection from {addr}");

        if !self.can_add_connection() || self.is_self_connect(addr) {
            debug!(parent: self.span(), "Rejecting the connection from {addr}");
            return;
        }

        self.connecting.lock().insert(addr);

        let tcp = self.clone();
        tokio::spawn(async move {
            if let Err(e) = tcp.adapt_stream(stream, addr, ConnectionSide::Responder).await {
                tcp.connecting.lock().remove(&addr);
                tcp.known_peers().register_failure(addr);
                error!(parent: tcp.span(), "Failed to connect with {addr}: {e}");
            }
        });
    }

    /// Checks if the given IP address is the same as the listening address of this `Tcp`.
    fn is_self_connect(&self, addr: SocketAddr) -> bool {
        // SAFETY: if we're opening connections, this should never fail.
        let listening_addr = self.listening_addr().unwrap();

        match listening_addr.ip().is_loopback() {
            // If localhost, check the ports, this only works on outbound connections, since we
            // don't know the ephemeral port a peer might be using if they initiate the connection.
            true => listening_addr.port() == addr.port(),
            // If it's not localhost, matching IPs indicate a self-connect in both directions.
            false => listening_addr.ip() == addr.ip(),
        }
    }

    /// Checks whether the `Tcp` can handle an additional connection.
    fn can_add_connection(&self) -> bool {
        // Retrieve the number of connected peers.
        let num_connected = self.num_connected();
        // Retrieve the maximum number of connected peers.
        let limit = self.config.max_connections as usize;

        if num_connected >= limit {
            warn!(parent: self.span(), "Maximum number of active connections ({limit}) reached");
            false
        } else if num_connected + self.num_connecting() >= limit {
            warn!(parent: self.span(), "Maximum number of active & pending connections ({limit}) reached");
            false
        } else {
            true
        }
    }

    /// Prepares the freshly acquired connection to handle the protocols the Tcp implements.
    async fn adapt_stream(&self, stream: TcpStream, peer_addr: SocketAddr, own_side: ConnectionSide) -> io::Result<()> {
        self.known_peers.add(peer_addr);

        // Register the port seen by the peer.
        if own_side == ConnectionSide::Initiator {
            if let Ok(addr) = stream.local_addr() {
                debug!(
                    parent: self.span(), "establishing connection with {}; the peer is connected on port {}",
                    peer_addr, addr.port()
                );
            } else {
                warn!(parent: self.span(), "couldn't determine the peer's port");
            }
        }

        let connection = Connection::new(peer_addr, stream, !own_side);

        // Enact the enabled protocols.
        let mut connection = self.enable_protocols(connection).await?;

        // if Reading is enabled, we'll notify the related task when the connection is fully ready.
        let conn_ready_tx = connection.readiness_notifier.take();

        self.connections.add(connection);
        self.connecting.lock().remove(&peer_addr);

        // Send the aforementioned notification so that reading from the socket can commence.
        if let Some(tx) = conn_ready_tx {
            let _ = tx.send(());
        }

        // If enabled, enact OnConnect.
        if let Some(handler) = self.protocols.on_connect.get() {
            let (sender, receiver) = oneshot::channel();
            handler.trigger((peer_addr, sender));
            let _ = receiver.await; // can't really fail
        }

        Ok(())
    }

    /// Enacts the enabled protocols on the provided connection.
    async fn enable_protocols(&self, conn: Connection) -> io::Result<Connection> {
        /// A helper macro to enable a protocol on a connection.
        macro_rules! enable_protocol {
            ($handler_type: ident, $node:expr, $conn: expr) => {
                if let Some(handler) = $node.protocols.$handler_type.get() {
                    let (conn_returner, conn_retriever) = oneshot::channel();

                    handler.trigger(($conn, conn_returner));

                    match conn_retriever.await {
                        Ok(Ok(conn)) => conn,
                        Err(_) => return Err(io::ErrorKind::BrokenPipe.into()),
                        Ok(e) => return e,
                    }
                } else {
                    $conn
                }
            };
        }

        let mut conn = enable_protocol!(handshake, self, conn);

        // Split the stream after the handshake (if not done before).
        if let Some(stream) = conn.stream.take() {
            let (reader, writer) = split(stream);
            conn.reader = Some(Box::new(reader));
            conn.writer = Some(Box::new(writer));
        }

        let conn = enable_protocol!(reading, self, conn);
        let conn = enable_protocol!(writing, self, conn);

        Ok(conn)
    }
}

impl fmt::Debug for Tcp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "The TCP stack config: {:?}", self.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_new() {
        let tcp = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 200,
            ..Default::default()
        });

        assert_eq!(tcp.config.max_connections, 200);
        assert_eq!(tcp.config.listener_ip, Some(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert_eq!(tcp.enable_listener().await.unwrap().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));

        assert_eq!(tcp.num_connected(), 0);
        assert_eq!(tcp.num_connecting(), 0);
    }

    #[tokio::test]
    async fn test_connect() {
        let tcp = Tcp::new(Config { desired_listening_port: Some(0), ..Default::default() });
        let node_ip = tcp.enable_listener().await.unwrap();

        // Ensure self-connecting is not possible.
        tcp.connect(node_ip).await.unwrap_err();
        assert_eq!(tcp.num_connected(), 0);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(!tcp.is_connected(node_ip));
        assert!(!tcp.is_connecting(node_ip));

        // Initialize the peer.
        let peer = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer_ip = peer.enable_listener().await.unwrap();

        // Connect to the peer.
        tcp.connect(peer_ip).await.unwrap();
        assert_eq!(tcp.num_connected(), 1);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(tcp.is_connected(peer_ip));
        assert!(!tcp.is_connecting(peer_ip));
    }

    #[tokio::test]
    async fn test_disconnect() {
        let tcp = Tcp::new(Config { desired_listening_port: Some(0), ..Default::default() });
        let _node_ip = tcp.enable_listener().await.unwrap();

        // Initialize the peer.
        let peer = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer_ip = peer.enable_listener().await.unwrap();

        // Connect to the peer.
        tcp.connect(peer_ip).await.unwrap();
        assert_eq!(tcp.num_connected(), 1);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(tcp.is_connected(peer_ip));
        assert!(!tcp.is_connecting(peer_ip));

        // Disconnect from the peer.
        tcp.disconnect(peer_ip).await;
        assert_eq!(tcp.num_connected(), 0);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(!tcp.is_connected(peer_ip));
        assert!(!tcp.is_connecting(peer_ip));

        // Ensure disconnecting from the peer a second time is okay.
        tcp.disconnect(peer_ip).await;
        assert_eq!(tcp.num_connected(), 0);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(!tcp.is_connected(peer_ip));
        assert!(!tcp.is_connecting(peer_ip));
    }

    #[tokio::test]
    async fn test_can_add_connection() {
        let tcp = Tcp::new(Config { max_connections: 1, ..Default::default() });

        // Initialize the peer.
        let peer = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer_ip = peer.enable_listener().await.unwrap();

        assert!(tcp.can_add_connection());

        // Simulate an active connection.
        let stream = TcpStream::connect(peer_ip).await.unwrap();
        tcp.connections.add(Connection::new(peer_ip, stream, ConnectionSide::Initiator));
        assert!(!tcp.can_add_connection());

        // Remove the active connection.
        tcp.connections.remove(peer_ip);
        assert!(tcp.can_add_connection());

        // Simulate a pending connection.
        tcp.connecting.lock().insert(peer_ip);
        assert!(!tcp.can_add_connection());

        // Remove the pending connection.
        tcp.connecting.lock().remove(&peer_ip);
        assert!(tcp.can_add_connection());

        // Simulate an active and a pending connection (this case should never occur).
        let stream = TcpStream::connect(peer_ip).await.unwrap();
        tcp.connections.add(Connection::new(peer_ip, stream, ConnectionSide::Responder));
        tcp.connecting.lock().insert(peer_ip);
        assert!(!tcp.can_add_connection());

        // Remove the active and pending connection.
        tcp.connections.remove(peer_ip);
        tcp.connecting.lock().remove(&peer_ip);
        assert!(tcp.can_add_connection());
    }

    #[tokio::test]
    async fn test_handle_connection() {
        let tcp = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            max_connections: 1,
            ..Default::default()
        });

        // Initialize peer 1.
        let peer1 = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer1_ip = peer1.enable_listener().await.unwrap();

        // Simulate an active connection.
        let stream = TcpStream::connect(peer1_ip).await.unwrap();
        tcp.connections.add(Connection::new(peer1_ip, stream, ConnectionSide::Responder));
        assert!(!tcp.can_add_connection());
        assert_eq!(tcp.num_connected(), 1);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(tcp.is_connected(peer1_ip));
        assert!(!tcp.is_connecting(peer1_ip));

        // Initialize peer 2.
        let peer2 = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer2_ip = peer2.enable_listener().await.unwrap();

        // Handle the connection.
        let stream = TcpStream::connect(peer2_ip).await.unwrap();
        tcp.handle_connection(stream, peer2_ip);
        assert!(!tcp.can_add_connection());
        assert_eq!(tcp.num_connected(), 1);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(tcp.is_connected(peer1_ip));
        assert!(!tcp.is_connected(peer2_ip));
        assert!(!tcp.is_connecting(peer1_ip));
        assert!(!tcp.is_connecting(peer2_ip));
    }

    #[tokio::test]
    async fn test_adapt_stream() {
        let tcp = Tcp::new(Config { max_connections: 1, ..Default::default() });

        // Initialize the peer.
        let peer = Tcp::new(Config {
            listener_ip: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
            desired_listening_port: Some(0),
            max_connections: 1,
            ..Default::default()
        });
        let peer_ip = peer.enable_listener().await.unwrap();

        // Simulate a pending connection.
        tcp.connecting.lock().insert(peer_ip);
        assert_eq!(tcp.num_connected(), 0);
        assert_eq!(tcp.num_connecting(), 1);
        assert!(!tcp.is_connected(peer_ip));
        assert!(tcp.is_connecting(peer_ip));

        // Simulate a new connection.
        let stream = TcpStream::connect(peer_ip).await.unwrap();
        tcp.adapt_stream(stream, peer_ip, ConnectionSide::Responder).await.unwrap();
        assert_eq!(tcp.num_connected(), 1);
        assert_eq!(tcp.num_connecting(), 0);
        assert!(tcp.is_connected(peer_ip));
        assert!(!tcp.is_connecting(peer_ip));
    }
}
