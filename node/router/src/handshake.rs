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

use crate::{
    messages::{ChallengeRequest, ChallengeResponse, DisconnectReason, Message, MessageCodec, MessageTrait},
    Peer,
    Router,
};
use snarkos_node_tcp::{ConnectionSide, Tcp, P2P};
use snarkvm::{
    ledger::narwhal::Data,
    prelude::{block::Header, error, Address, Network},
};

use anyhow::{bail, Result};
use futures::SinkExt;
use rand::{rngs::OsRng, Rng};
use std::{io, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

impl<N: Network> P2P for Router<N> {
    /// Returns a reference to the TCP instance.
    fn tcp(&self) -> &Tcp {
        &self.tcp
    }
}

/// A macro unwrapping the expected handshake message or returning an error for unexpected messages.
#[macro_export]
macro_rules! expect_message {
    ($msg_ty:path, $framed:expr, $peer_addr:expr) => {
        match $framed.try_next().await? {
            // Received the expected message, proceed.
            Some($msg_ty(data)) => {
                trace!("Received '{}' from '{}'", data.name(), $peer_addr);
                data
            }
            // Received a disconnect message, abort.
            Some(Message::Disconnect(reason)) => {
                return Err(error(format!("'{}' disconnected: {reason:?}", $peer_addr)))
            }
            // Received an unexpected message, abort.
            Some(ty) => {
                return Err(error(format!(
                    "'{}' did not follow the handshake protocol: received {:?} instead of {}",
                    $peer_addr,
                    ty.name(),
                    stringify!($msg_ty),
                )))
            }
            // Received nothing.
            None => {
                return Err(error(format!("'{}' disconnected before sending {:?}", $peer_addr, stringify!($msg_ty),)))
            }
        }
    };
}

/// Send the given message to the peer.
async fn send<N: Network>(
    framed: &mut Framed<&mut TcpStream, MessageCodec<N>>,
    peer_addr: SocketAddr,
    message: Message<N>,
) -> io::Result<()> {
    trace!("Sending '{}' to '{peer_addr}'", message.name());
    framed.send(message).await
}

/// This object is a self-destruct wrapper that removes a peer from the list of
/// connecting entities in case the handshake is broken due to a timeout; this is
/// only needed when the node responds to a peer's connection request.
struct ConnectingPeer<'a, N: Network> {
    ip: SocketAddr,
    router: &'a Router<N>,
    connected: bool,
}

impl<'a, N: Network> ConnectingPeer<'a, N> {
    fn new(ip: SocketAddr, router: &'a Router<N>) -> Self {
        Self { ip, router, connected: false }
    }
}

impl<'a, N: Network> Drop for ConnectingPeer<'a, N> {
    fn drop(&mut self) {
        if !self.connected {
            self.router.remove_connecting_peer(self.ip);
        }
    }
}

impl<N: Network> Router<N> {
    /// Executes the handshake protocol.
    pub async fn handshake<'a>(
        &'a self,
        peer_addr: SocketAddr,
        stream: &'a mut TcpStream,
        peer_side: ConnectionSide,
        genesis_header: Header<N>,
    ) -> io::Result<(SocketAddr, Framed<&mut TcpStream, MessageCodec<N>>)> {
        // If this is an inbound connection, we log it, but don't know the listening address yet.
        // Otherwise, we can immediately register the listening address.
        let mut peer_ip = if peer_side == ConnectionSide::Initiator {
            debug!("Received a connection request from '{peer_addr}'");
            None
        } else {
            debug!("Connecting to {peer_addr}...");
            Some(peer_addr)
        };

        // Perform the handshake; we pass on a mutable reference to peer_ip in case the process is broken at any point in time.
        let handshake_result = if peer_side == ConnectionSide::Responder {
            self.handshake_inner_initiator(peer_addr, &mut peer_ip, stream, genesis_header).await
        } else {
            self.handshake_inner_responder(peer_addr, &mut peer_ip, stream, genesis_header).await
        };

        // Remove the address from the collection of connecting peers (if the handshake got to the point where it's known).
        if let Some(ip) = peer_ip {
            self.connecting_peers.lock().remove(&ip);
        }

        // If the handshake succeeded, announce it.
        if let Ok((ref peer_ip, _)) = handshake_result {
            info!("Connected to '{peer_ip}'");
        }

        handshake_result
    }

    /// The connection initiator side of the handshake.
    async fn handshake_inner_initiator<'a>(
        &'a self,
        peer_addr: SocketAddr,
        peer_ip: &mut Option<SocketAddr>,
        stream: &'a mut TcpStream,
        genesis_header: Header<N>,
    ) -> io::Result<(SocketAddr, Framed<&mut TcpStream, MessageCodec<N>>)> {
        // This value is immediately guaranteed to be present, so it can be unwrapped.
        let peer_ip = peer_ip.unwrap();
        // Construct the stream.
        let mut framed = Framed::new(stream, MessageCodec::<N>::handshake());

        // Initialize an RNG.
        let rng = &mut OsRng;

        /* Step 1: Send the challenge request. */

        // Sample a random nonce.
        let our_nonce = rng.gen();
        // Send a challenge request to the peer.
        let our_request = ChallengeRequest::new(self.local_ip().port(), self.node_type, self.address(), our_nonce);
        send(&mut framed, peer_addr, Message::ChallengeRequest(our_request)).await?;

        /* Step 2: Receive the peer's challenge response followed by the challenge request. */

        // Listen for the challenge response message.
        let peer_response = expect_message!(Message::ChallengeResponse, framed, peer_addr);
        // Listen for the challenge request message.
        let peer_request = expect_message!(Message::ChallengeRequest, framed, peer_addr);

        // Verify the challenge response. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) = self
            .verify_challenge_response(peer_addr, peer_request.address, peer_response, genesis_header, our_nonce)
            .await
        {
            send(&mut framed, peer_addr, reason.into()).await?;
            return Err(error(format!("Dropped '{peer_addr}' for reason: {reason:?}")));
        }
        // Verify the challenge request. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) = self.verify_challenge_request(peer_addr, &peer_request) {
            send(&mut framed, peer_addr, reason.into()).await?;
            return Err(error(format!("Dropped '{peer_addr}' for reason: {reason:?}")));
        }
        /* Step 3: Send the challenge response. */

        let response_nonce: u64 = rng.gen();
        let data = [peer_request.nonce.to_le_bytes(), response_nonce.to_le_bytes()].concat();
        // Sign the counterparty nonce.
        let Ok(our_signature) = self.account.sign_bytes(&data, rng) else {
            return Err(error(format!("Failed to sign the challenge request nonce from '{peer_addr}'")));
        };
        // Send the challenge response.
        let our_response =
            ChallengeResponse { genesis_header, signature: Data::Object(our_signature), nonce: response_nonce };
        send(&mut framed, peer_addr, Message::ChallengeResponse(our_response)).await?;

        // Add the peer to the router.
        self.insert_connected_peer(Peer::new(peer_ip, &peer_request), peer_addr);

        Ok((peer_ip, framed))
    }

    /// The connection responder side of the handshake.
    async fn handshake_inner_responder<'a>(
        &'a self,
        peer_addr: SocketAddr,
        peer_ip: &mut Option<SocketAddr>,
        stream: &'a mut TcpStream,
        genesis_header: Header<N>,
    ) -> io::Result<(SocketAddr, Framed<&mut TcpStream, MessageCodec<N>>)> {
        // Construct the stream.
        let mut framed = Framed::new(stream, MessageCodec::<N>::handshake());

        /* Step 1: Receive the challenge request. */

        // Listen for the challenge request message.
        let peer_request = expect_message!(Message::ChallengeRequest, framed, peer_addr);

        // Obtain the peer's listening address.
        *peer_ip = Some(SocketAddr::new(peer_addr.ip(), peer_request.listener_port));
        let peer_ip = peer_ip.unwrap();

        // Knowing the peer's listening address, ensure it is allowed to connect.
        let mut connecting_peer = match self.ensure_peer_is_allowed(peer_ip) {
            Ok(peer) => peer,
            Err(forbidden_message) => return Err(error(format!("{forbidden_message}"))),
        };
        // Verify the challenge request. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) = self.verify_challenge_request(peer_addr, &peer_request) {
            send(&mut framed, peer_addr, reason.into()).await?;
            return Err(error(format!("Dropped '{peer_addr}' for reason: {reason:?}")));
        }
        /* Step 2: Send the challenge response followed by own challenge request. */

        // Initialize an RNG.
        let rng = &mut OsRng;

        // Sign the counterparty nonce.
        let response_nonce: u64 = rng.gen();
        let data = [peer_request.nonce.to_le_bytes(), response_nonce.to_le_bytes()].concat();
        let Ok(our_signature) = self.account.sign_bytes(&data, rng) else {
            return Err(error(format!("Failed to sign the challenge request nonce from '{peer_addr}'")));
        };
        // Send the challenge response.
        let our_response =
            ChallengeResponse { genesis_header, signature: Data::Object(our_signature), nonce: response_nonce };
        send(&mut framed, peer_addr, Message::ChallengeResponse(our_response)).await?;

        // Sample a random nonce.
        let our_nonce = rng.gen();
        // Send the challenge request.
        let our_request = ChallengeRequest::new(self.local_ip().port(), self.node_type, self.address(), our_nonce);
        send(&mut framed, peer_addr, Message::ChallengeRequest(our_request)).await?;

        /* Step 3: Receive the challenge response. */

        // Listen for the challenge response message.
        let peer_response = expect_message!(Message::ChallengeResponse, framed, peer_addr);
        // Verify the challenge response. If a disconnect reason was returned, send the disconnect message and abort.
        if let Some(reason) = self
            .verify_challenge_response(peer_addr, peer_request.address, peer_response, genesis_header, our_nonce)
            .await
        {
            send(&mut framed, peer_addr, reason.into()).await?;
            return Err(error(format!("Dropped '{peer_addr}' for reason: {reason:?}")));
        }
        // Add the peer to the router.
        self.insert_connected_peer(Peer::new(peer_ip, &peer_request), peer_addr);

        connecting_peer.connected = true;

        Ok((peer_ip, framed))
    }

    /// Ensure the peer is allowed to connect.
    fn ensure_peer_is_allowed(&self, peer_ip: SocketAddr) -> Result<ConnectingPeer<'_, N>> {
        // Ensure the peer IP is not this node.
        if self.is_local_ip(&peer_ip) {
            bail!("Dropping connection request from '{peer_ip}' (attempted to self-connect)")
        }
        // Ensure the node is not already connecting to this peer.
        let connecting_peer = if !self.connecting_peers.lock().insert(peer_ip) {
            bail!("Dropping connection request from '{peer_ip}' (already shaking hands as the initiator)")
        } else {
            ConnectingPeer::new(peer_ip, self)
        };
        // Ensure the node is not already connected to this peer.
        if self.is_connected(&peer_ip) {
            bail!("Dropping connection request from '{peer_ip}' (already connected)")
        }
        // Ensure the peer is not restricted.
        if self.is_restricted(&peer_ip) {
            bail!("Dropping connection request from '{peer_ip}' (restricted)")
        }
        // Ensure the peer is not spamming connection attempts.
        if !peer_ip.ip().is_loopback() {
            // Add this connection attempt and retrieve the number of attempts.
            let num_attempts = self.cache.insert_inbound_connection(peer_ip.ip(), Self::RADIO_SILENCE_IN_SECS as i64);
            // Ensure the connecting peer has not surpassed the connection attempt limit.
            if num_attempts > Self::MAXIMUM_CONNECTION_FAILURES {
                // Restrict the peer.
                self.insert_restricted_peer(peer_ip);
                bail!("Dropping connection request from '{peer_ip}' (tried {num_attempts} times)")
            }
        }
        Ok(connecting_peer)
    }

    /// Verifies the given challenge request. Returns a disconnect reason if the request is invalid.
    fn verify_challenge_request(
        &self,
        peer_addr: SocketAddr,
        message: &ChallengeRequest<N>,
    ) -> Option<DisconnectReason> {
        // Retrieve the components of the challenge request.
        let &ChallengeRequest { version, listener_port: _, node_type: _, address: _, nonce: _ } = message;

        // Ensure the message protocol version is not outdated.
        if version < Message::<N>::VERSION {
            warn!("Dropping '{peer_addr}' on version {version} (outdated)");
            return Some(DisconnectReason::OutdatedClientVersion);
        }
        None
    }

    /// Verifies the given challenge response. Returns a disconnect reason if the response is invalid.
    async fn verify_challenge_response(
        &self,
        peer_addr: SocketAddr,
        peer_address: Address<N>,
        response: ChallengeResponse<N>,
        expected_genesis_header: Header<N>,
        expected_nonce: u64,
    ) -> Option<DisconnectReason> {
        // Retrieve the components of the challenge response.
        let ChallengeResponse { genesis_header, signature, nonce } = response;

        // Verify the challenge response, by checking that the block header matches.
        if genesis_header != expected_genesis_header {
            warn!("Handshake with '{peer_addr}' failed (incorrect block header)");
            return Some(DisconnectReason::InvalidChallengeResponse);
        }
        // Perform the deferred non-blocking deserialization of the signature.
        let Ok(signature) = signature.deserialize().await else {
            warn!("Handshake with '{peer_addr}' failed (cannot deserialize the signature)");
            return Some(DisconnectReason::InvalidChallengeResponse);
        };
        // Verify the signature.
        if !signature.verify_bytes(&peer_address, &[expected_nonce.to_le_bytes(), nonce.to_le_bytes()].concat()) {
            warn!("Handshake with '{peer_addr}' failed (invalid signature)");
            return Some(DisconnectReason::InvalidChallengeResponse);
        }
        None
    }
}
