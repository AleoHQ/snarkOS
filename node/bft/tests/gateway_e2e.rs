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
mod common;

use crate::common::{
    primary::new_test_committee,
    test_peer::TestPeer,
    utils::{sample_gateway, sample_ledger, sample_storage},
    CurrentNetwork,
};
use snarkos_account::Account;
use snarkos_node_bft::{helpers::init_primary_channels, Gateway};
use snarkos_node_bft_events::{ChallengeRequest, ChallengeResponse, Disconnect, DisconnectReason, Event, WorkerPing};
use snarkos_node_tcp::P2P;
use snarkvm::{ledger::narwhal::Data, prelude::TestRng};

use std::time::Duration;

use deadline::deadline;
use rand::Rng;

async fn new_test_gateway(
    num_nodes: u16,
    rng: &mut TestRng,
) -> (Vec<Account<CurrentNetwork>>, Gateway<CurrentNetwork>) {
    let (accounts, committee) = new_test_committee(num_nodes, rng);
    let ledger = sample_ledger(&accounts, &committee, rng);
    let storage = sample_storage(ledger.clone());
    let gateway = sample_gateway(accounts[0].clone(), storage, ledger);

    // Set up primary channels, we discard the rx as we're testing the gateway sans BFT.
    let (primary_tx, _primary_rx) = init_primary_channels();

    gateway.run(primary_tx, [].into(), None).await;

    (accounts, gateway)
}

// The test peer connects to the gateway and completes the no-op handshake (so
// the connection is registered). The gateway's handshake should timeout.
#[tokio::test(flavor = "multi_thread")]
async fn handshake_responder_side_timeout() {
    const NUM_NODES: u16 = 4;

    let mut rng = TestRng::default();
    let (_accounts, gateway) = new_test_gateway(NUM_NODES, &mut rng).await;
    let test_peer = TestPeer::new().await;

    dbg!(test_peer.listening_addr());

    // Initiate a connection with the gateway, this will only return once the handshake protocol has
    // completed on the test peer's side, which is a no-op.
    assert!(test_peer.connect(gateway.local_ip()).await.is_ok());

    /* Don't send any further messages and wait for the gateway to timeout. */

    // Check the connection has been registered.
    let gateway_clone = gateway.clone();
    deadline!(Duration::from_secs(1), move || gateway_clone.tcp().num_connecting() == 1);

    // Check the tcp stack's connection counts, wait longer than the gateway's timeout to ensure
    // connecting peers are cleared.
    let gateway_clone = gateway.clone();
    deadline!(Duration::from_secs(5), move || gateway_clone.tcp().num_connecting() == 0);

    // Check the test peer hasn't been added to the gateway's connected peers.
    assert!(gateway.connected_peers().read().is_empty());
    assert_eq!(gateway.tcp().num_connected(), 0);
}

// The test peer connects to the gateway and sends an unexpected event.
// The gateway's handshake should be interrupted and the peer should be
// disconnected.
macro_rules! handshake_responder_side_unexpected_event {
    ($test_name:ident, $payload:expr) => {
        paste::paste! {
            #[tokio::test(flavor = "multi_thread")]
            async fn [<handshake_responder_side_unexpected_ $test_name>]() {
                const NUM_NODES: u16 = 4;

                let mut rng = TestRng::default();
                let (_accounts, gateway) = new_test_gateway(NUM_NODES, &mut rng).await;
                let test_peer = TestPeer::new().await;

                // Initiate a connection with the gateway, this will only return once the handshake protocol has
                // completed on the test peer's side, which is a no-op.
                assert!(test_peer.connect(gateway.local_ip()).await.is_ok());

                // Check the connection has been registered.
                let gateway_clone = gateway.clone();
                deadline!(Duration::from_secs(1), move || gateway_clone.tcp().num_connecting() == 1);

                // Send an unexpected event.
                let _ = test_peer.unicast(
                    gateway.local_ip(),
                    $payload
                );

                // Check the tcp stack's connection counts, make sure the disconnect interrupted handshaking,
                // wait a short time to ensure the gateway has time to process the disconnect (note: this is
                // shorter than the gateway's timeout, so we can ensure that's not the reason for the
                // disconnect).
                let gateway_clone = gateway.clone();
                deadline!(Duration::from_secs(1), move || gateway_clone.tcp().num_connecting() == 0);

                // Check the test peer hasn't been added to the gateway's connected peers.
                assert!(gateway.connected_peers().read().is_empty());
                assert_eq!(gateway.tcp().num_connected(), 0);
            }
        }
    };
}

/* Unexpected disconnects. */

macro_rules! handshake_responder_side_unexpected_disconnect {
     ($($reason:ident),*) => {
         $(
             paste::paste! {
                 handshake_responder_side_unexpected_event!(
                     [<disconnect_ $reason:snake>],
                     Event::Disconnect(Disconnect::from(DisconnectReason::$reason))
                 );
             }
         )*
     }
 }

handshake_responder_side_unexpected_disconnect!(
    ProtocolViolation,
    NoReasonGiven,
    InvalidChallengeResponse,
    OutdatedClientVersion
);

/* Other unexpected event types */

handshake_responder_side_unexpected_event!(worker_ping, Event::WorkerPing(WorkerPing::new([].into())));

// TODO(nkls): other event types, can be done as a follow up.

/* Invalid challenge request */

#[tokio::test(flavor = "multi_thread")]
async fn handshake_responder_side_invalid_challenge_request() {
    const NUM_NODES: u16 = 4;

    let mut rng = TestRng::default();
    let (accounts, gateway) = new_test_gateway(NUM_NODES, &mut rng).await;
    let test_peer = TestPeer::new().await;

    // Initiate a connection with the gateway, this will only return once the handshake protocol has
    // completed on the test peer's side, which is a no-op.
    assert!(test_peer.connect(gateway.local_ip()).await.is_ok());

    // Check the connection has been registered.
    let gateway_clone = gateway.clone();
    deadline!(Duration::from_secs(1), move || gateway_clone.tcp().num_connecting() == 1);

    // Use the address from the second peer in the list, the test peer will use the first.
    let listener_port = test_peer.listening_addr().port();
    let address = accounts.get(1).unwrap().address();
    let nonce = rng.gen();
    // Set the wrong version so the challenge request is invalid.
    let challenge_request = ChallengeRequest { version: 0, listener_port, address, nonce };

    // Send the message
    let _ = test_peer.unicast(gateway.local_ip(), Event::ChallengeRequest(challenge_request));

    // FIXME(nkls): currently we can't assert on the disconnect type, the message isn't always sent
    // before the disconnect.

    // Check the test peer has been removed from the gateway's connecting peers.
    let gateway_clone = gateway.clone();
    deadline!(Duration::from_secs(1), move || gateway_clone.tcp().num_connecting() == 0);
    // Check the test peer hasn't been added to the gateway's connected peers.
    assert!(gateway.connected_peers().read().is_empty());
    assert_eq!(gateway.tcp().num_connected(), 0);
}

/* Invalid challenge response */

#[tokio::test(flavor = "multi_thread")]
async fn handshake_responder_side_invalid_challenge_response() {
    const NUM_NODES: u16 = 4;

    let mut rng = TestRng::default();
    let (accounts, gateway) = new_test_gateway(NUM_NODES, &mut rng).await;
    let mut test_peer = TestPeer::new().await;

    // Initiate a connection with the gateway, this will only return once the handshake protocol has
    // completed on the test peer's side, which is a no-op for the moment.
    assert!(test_peer.connect(gateway.local_ip()).await.is_ok());

    // Check the connection has been registered.
    let gateway_clone = gateway.clone();
    deadline!(Duration::from_secs(1), move || gateway_clone.tcp().num_connecting() == 1);

    // Use the address from the second peer in the list, the test peer will use the first.
    let listener_port = test_peer.listening_addr().port();
    let address = accounts.get(1).unwrap().address();
    let our_nonce = rng.gen();
    let challenge_request = ChallengeRequest { version: 6, listener_port, address, nonce: our_nonce };

    // Send the challenge request.
    let _ = test_peer.unicast(gateway.local_ip(), Event::ChallengeRequest(challenge_request));

    // Receive the gateway's challenge response.
    let (peer_addr, Event::ChallengeResponse(ChallengeResponse { signature, nonce })) =
        test_peer.recv_timeout(Duration::from_secs(1)).await
    else {
        panic!("Expected challenge response")
    };

    // Check the sender is the gateway.
    assert_eq!(peer_addr, gateway.local_ip());
    // Check the nonce we sent is in the signature.
    assert!(
        signature.deserialize_blocking().unwrap().verify_bytes(
            &accounts.first().unwrap().address(),
            &[our_nonce.to_le_bytes(), nonce.to_le_bytes()].concat()
        )
    );

    // Receive the gateway's challenge request.
    let (peer_addr, Event::ChallengeRequest(challenge_request)) = test_peer.recv_timeout(Duration::from_secs(1)).await
    else {
        panic!("Expected challenge request")
    };
    // Check the version, listener port and address are correct.
    assert_eq!(peer_addr, gateway.local_ip());
    assert_eq!(challenge_request.version, 6);
    assert_eq!(challenge_request.listener_port, gateway.local_ip().port());
    assert_eq!(challenge_request.address, accounts.first().unwrap().address());

    // Send the challenge response with an invalid signature.
    let response_nonce = rng.gen();
    let _ = test_peer.unicast(
        gateway.local_ip(),
        Event::ChallengeResponse(ChallengeResponse {
            signature: Data::Object(
                accounts.get(2).unwrap().sign_bytes(&challenge_request.nonce.to_le_bytes(), &mut rng).unwrap(),
            ),
            nonce: response_nonce,
        }),
    );

    // FIXME(nkls): currently we can't assert on the disconnect type, the message isn't always sent
    // before the disconnect.

    // Check the test peer has been removed from the gateway's connecting peers.
    let gateway_clone = gateway.clone();
    deadline!(Duration::from_secs(1), move || gateway_clone.tcp().num_connecting() == 0);
    // Check the test peer hasn't been added to the gateway's connected peers.
    assert!(gateway.connected_peers().read().is_empty());
    assert_eq!(gateway.tcp().num_connected(), 0);
}
