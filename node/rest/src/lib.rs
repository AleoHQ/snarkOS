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

#![forbid(unsafe_code)]

#[macro_use]
extern crate tracing;

mod helpers;
pub use helpers::*;

mod routes;

use snarkos_node_consensus::Consensus;
use snarkos_node_router::{
    messages::{Message, UnconfirmedTransaction},
    Routing,
};
use snarkvm::{
    console::{program::ProgramID, types::Field},
    ledger::narwhal::Data,
    prelude::{cfg_into_iter, store::ConsensusStorage, Ledger, Network},
};

use anyhow::Result;
use axum::{
    body::Body,
    error_handling::HandleErrorLayer,
    extract::{ConnectInfo, DefaultBodyLimit, Path, Query, State},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    middleware,
    middleware::Next,
    response::Response,
    routing::{get, post},
    BoxError,
    Json,
};
use axum_extra::response::ErasedJson;
use parking_lot::Mutex;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle};
use tower::{buffer::BufferLayer, limit::RateLimitLayer, ServiceBuilder};
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};
use tower_http::{
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};

/// A REST API server for the ledger.
#[derive(Clone)]
pub struct Rest<N: Network, C: ConsensusStorage<N>, R: Routing<N>> {
    /// The consensus module.
    consensus: Option<Consensus<N>>,
    /// The ledger.
    ledger: Ledger<N, C>,
    /// The node (routing).
    routing: Arc<R>,
    /// The server handles.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl<N: Network, C: 'static + ConsensusStorage<N>, R: Routing<N>> Rest<N, C, R> {
    /// Initializes a new instance of the server.
    pub async fn start(
        rest_ip: SocketAddr,
        rest_rps: u32,
        consensus: Option<Consensus<N>>,
        ledger: Ledger<N, C>,
        routing: Arc<R>,
    ) -> Result<Self> {
        // Initialize the server.
        let mut server = Self { consensus, ledger, routing, handles: Default::default() };
        // Spawn the server.
        server.spawn_server(rest_ip, rest_rps).await;
        // Return the server.
        Ok(server)
    }
}

impl<N: Network, C: ConsensusStorage<N>, R: Routing<N>> Rest<N, C, R> {
    /// Returns the ledger.
    pub const fn ledger(&self) -> &Ledger<N, C> {
        &self.ledger
    }

    /// Returns the handles.
    pub const fn handles(&self) -> &Arc<Mutex<Vec<JoinHandle<()>>>> {
        &self.handles
    }
}

impl<N: Network, C: ConsensusStorage<N>, R: Routing<N>> Rest<N, C, R> {
    async fn spawn_server(&mut self, rest_ip: SocketAddr, rest_rps: u32) {
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers([CONTENT_TYPE]);

        // Log the REST rate limit per IP.
        debug!("REST rate limit per IP - {rest_rps} RPS");

        // Prepare the rate limiting setup.
        let governor_config = Box::new(
            GovernorConfigBuilder::default()
                .per_second(1)
                .burst_size(rest_rps)
                .error_handler(|error| Response::new(error.to_string().into()))
                .finish()
                .expect("Couldn't set up rate limiting for the REST server!"),
        );

        let global_rate_limiter = ServiceBuilder::new()
            .layer(HandleErrorLayer::new(|err: BoxError| async move {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Unhandled error: {}", err),
                )
            }))
            // Use a buffer layer to allow rate limiting.
            .layer(BufferLayer::new(1024))
            // Apply a global rate limit of 100 requests/s.
            .layer(RateLimitLayer::new(100, Duration::from_secs(1)));

        let router = {
            axum::Router::new()

            // All the endpoints before the call to `route_layer` are protected with JWT auth.
            .route("/mainnet/node/address", get(Self::get_node_address))
            .route_layer(middleware::from_fn(auth_middleware))

            // ----------------- DEPRECATED ROUTES -----------------
            // The following `GET ../latest/..` routes will be removed before mainnet.
            // Please refer to the recommended routes for each endpoint:

            // Deprecated: use `/mainnet/block/height/latest` instead.
            .route("/mainnet/latest/height", get(Self::latest_height))
            // Deprecated: use `/mainnet/block/hash/latest` instead.
            .route("/mainnet/latest/hash", get(Self::latest_hash))
            // Deprecated: use `/mainnet/latest/block/height` instead.
            .route("/mainnet/latest/block", get(Self::latest_block))
            // Deprecated: use `/mainnet/stateRoot/latest` instead.
            .route("/mainnet/latest/stateRoot", get(Self::latest_state_root))
            // Deprecated: use `/mainnet/committee/latest` instead.
            .route("/mainnet/latest/committee", get(Self::latest_committee))
            // ------------------------------------------------------

            // GET ../block/..
            .route("/mainnet/block/height/latest", get(Self::get_block_height_latest))
            .route("/mainnet/block/hash/latest", get(Self::get_block_hash_latest))
            .route("/mainnet/block/latest", get(Self::get_block_latest))
            .route("/mainnet/block/:height_or_hash", get(Self::get_block))
            // The path param here is actually only the height, but the name must match the route
            // above, otherwise there'll be a conflict at runtime.
            .route("/mainnet/block/:height_or_hash/transactions", get(Self::get_block_transactions))

            // GET and POST ../transaction/..
            .route("/mainnet/transaction/:id", get(Self::get_transaction))
            .route("/mainnet/transaction/confirmed/:id", get(Self::get_confirmed_transaction))
            .route("/mainnet/transaction/broadcast", post(Self::transaction_broadcast))

            // POST ../solution/broadcast
            .route("/mainnet/solution/broadcast", post(Self::solution_broadcast))

            // GET ../find/..
            .route("/mainnet/find/blockHash/:tx_id", get(Self::find_block_hash))
            .route("/mainnet/find/transactionID/deployment/:program_id", get(Self::find_transaction_id_from_program_id))
            .route("/mainnet/find/transactionID/:transition_id", get(Self::find_transaction_id_from_transition_id))
            .route("/mainnet/find/transitionID/:input_or_output_id", get(Self::find_transition_id))

            // GET ../peers/..
            .route("/mainnet/peers/count", get(Self::get_peers_count))
            .route("/mainnet/peers/all", get(Self::get_peers_all))
            .route("/mainnet/peers/all/metrics", get(Self::get_peers_all_metrics))

            // GET ../program/..
            .route("/mainnet/program/:id", get(Self::get_program))
            .route("/mainnet/program/:id/mappings", get(Self::get_mapping_names))
            .route("/mainnet/program/:id/mapping/:name/:key", get(Self::get_mapping_value))

            // GET misc endpoints.
            .route("/mainnet/blocks", get(Self::get_blocks))
            .route("/mainnet/height/:hash", get(Self::get_height))
            .route("/mainnet/memoryPool/transmissions", get(Self::get_memory_pool_transmissions))
            .route("/mainnet/memoryPool/solutions", get(Self::get_memory_pool_solutions))
            .route("/mainnet/memoryPool/transactions", get(Self::get_memory_pool_transactions))
            .route("/mainnet/statePath/:commitment", get(Self::get_state_path_for_commitment))
            .route("/mainnet/stateRoot/latest", get(Self::get_state_root_latest))
            .route("/mainnet/committee/latest", get(Self::get_committee_latest))

            // Pass in `Rest` to make things convenient.
            .with_state(self.clone())
            // Enable tower-http tracing.
            .layer(TraceLayer::new_for_http())
            // Custom logging.
            .layer(middleware::from_fn(log_middleware))
            // Enable CORS.
            .layer(cors)
            // Cap body size at 10MB.
            .layer(DefaultBodyLimit::max(10 * 1024 * 1024))
            .layer(GovernorLayer {
                // We can leak this because it is created only once and it persists.
                config: Box::leak(governor_config),
            })
            .layer(global_rate_limiter)
        };

        let rest_listener = TcpListener::bind(rest_ip).await.unwrap();
        self.handles.lock().push(tokio::spawn(async move {
            axum::serve(rest_listener, router.into_make_service_with_connect_info::<SocketAddr>())
                .await
                .expect("couldn't start rest server");
        }))
    }
}

async fn log_middleware(
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    info!("Received '{} {}' from '{addr}'", request.method(), request.uri());

    Ok(next.run(request).await)
}
