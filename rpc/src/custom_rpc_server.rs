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

//! Logic for instantiating the RPC server.

use crate::{
    rpc_trait::RpcFunctions,
    rpc_types::{Meta, RpcCredentials},
    RpcImpl,
};
use snarkos_consensus::MerkleTreeLedger;
use snarkos_network::Node;
use snarkos_storage::Storage;

use hyper::{
    body::HttpBody,
    server::Server,
    service::{make_service_fn, service_fn},
    Body,
};
use json_rpc_types as jrt;
use jsonrpc_core::Params;
use serde::Serialize;
use tokio::task;

use std::{convert::Infallible, net::SocketAddr, sync::Arc};

const METHODS_EXPECTING_PARAMS: [&str; 14] = [
    // public
    "getblock",
    "getblockhash",
    "getrawtransaction",
    "gettransactioninfo",
    "decoderawtransaction",
    "sendtransaction",
    "validaterawtransaction",
    // private
    "createrawtransaction",
    "createtransactionkernel",
    "createtransaction",
    "getrawrecord",
    "decoderecord",
    "decryptrecord",
    "disconnect",
];

#[allow(clippy::too_many_arguments)]
pub fn start_rpc_server<S: Storage>(
    rpc_addr: SocketAddr,
    secondary_storage: Arc<MerkleTreeLedger<S>>,
    node_server: Node<S>,
    username: Option<String>,
    password: Option<String>,
) -> task::JoinHandle<()> {
    let credentials = match (username, password) {
        (Some(username), Some(password)) => Some(RpcCredentials { username, password }),
        _ => None,
    };

    let rpc_impl = RpcImpl::new(secondary_storage, credentials, node_server);

    let service = make_service_fn(move |_conn| {
        let rpc = rpc_impl.clone();
        async move { Ok::<_, Infallible>(service_fn(move |req| handle_rpc(rpc.clone(), req))) }
    });

    let server = Server::bind(&rpc_addr).serve(service);

    task::spawn(async move {
        server.await.expect("The RPC server couldn't be started!");
    })
}

async fn handle_rpc<S: Storage>(
    rpc: RpcImpl<S>,
    req: hyper::Request<Body>,
) -> Result<hyper::Response<Body>, Infallible> {
    // Register the request in the metrics.
    metrics::increment_counter!(snarkos_network::MISC_RPC_REQUESTS);

    // Obtain the username and password, if present.
    let auth = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .map(|h| h.to_str().unwrap_or("").to_owned());
    let meta = Meta { auth };

    // Ready the body of the request
    let mut body = req.into_body();
    let data = match body.data().await {
        Some(Ok(data)) => data,
        _ => {
            let resp = jrt::Response::<(), ()>::error(
                jrt::Version::V2,
                jrt::Error::from_code(jrt::ErrorCode::ParseError),
                None,
            );
            let body = serde_json::to_vec(&resp).unwrap_or_default();

            return Ok(hyper::Response::new(body.into()));
        }
    };

    // Deserialize the JSON-RPC request.
    let req: jrt::Request<Params> = match serde_json::from_slice(&data) {
        Ok(req) => req,
        Err(_) => {
            let resp = jrt::Response::<(), ()>::error(
                jrt::Version::V2,
                jrt::Error::from_code(jrt::ErrorCode::ParseError),
                None,
            );
            let body = serde_json::to_vec(&resp).unwrap_or_default();

            return Ok(hyper::Response::new(body.into()));
        }
    };

    // Read the request params.
    let mut params = match read_params(&req) {
        Ok(params) => params,
        Err(err) => {
            let resp = jrt::Response::<(), ()>::error(jrt::Version::V2, err, req.id.clone());
            let body = serde_json::to_vec(&resp).unwrap_or_default();

            return Ok(hyper::Response::new(body.into()));
        }
    };

    // Handle the request method.
    let response = match &*req.method {
        // public
        "getblock" => {
            let result = rpc
                .get_block(params[0].as_str().unwrap_or("").into())
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getblockcount" => {
            let result = rpc.get_block_count().map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getbestblockhash" => {
            let result = rpc.get_best_block_hash().map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getblockhash" => match serde_json::from_value::<u32>(params.remove(0)) {
            Ok(height) => {
                let result = rpc.get_block_hash(height).map_err(convert_crate_err);
                result_to_response(&req, result)
            }
            Err(_) => {
                let err = jrt::Error::with_custom_msg(jrt::ErrorCode::ParseError, "Invalid block height!");
                jrt::Response::error(jrt::Version::V2, err, req.id.clone())
            }
        },
        "getrawtransaction" => {
            let result = rpc
                .get_raw_transaction(params[0].as_str().unwrap_or("").into())
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "gettransactioninfo" => {
            let result = rpc
                .get_transaction_info(params[0].as_str().unwrap_or("").into())
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "decoderawtransaction" => {
            let result = rpc
                .decode_raw_transaction(params[0].as_str().unwrap_or("").into())
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "sendtransaction" => {
            let result = rpc
                .send_raw_transaction(params[0].as_str().unwrap_or("").into())
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "validaterawtransaction" => {
            let result = rpc
                .validate_raw_transaction(params[0].as_str().unwrap_or("").into())
                .map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getconnectioncount" => {
            let result = rpc.get_connection_count().map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getpeerinfo" => {
            let result = rpc.get_peer_info().map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getnodeinfo" => {
            let result = rpc.get_node_info().map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getnodestats" => {
            let result = rpc.get_node_stats().map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        "getblocktemplate" => {
            let result = rpc.get_block_template().map_err(convert_crate_err);
            result_to_response(&req, result)
        }
        // private
        "createaccount" => {
            let result = rpc
                .create_account_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "createrawtransaction" => {
            let result = rpc
                .create_raw_transaction_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "createtransactionkernel" => {
            let result = rpc
                .create_transaction_kernel_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "createtransaction" => {
            let result = rpc
                .create_transaction_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "getrecordcommitments" => {
            let result = rpc
                .get_record_commitments_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "getrawrecord" => {
            let result = rpc
                .get_raw_record_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "decoderecord" => {
            let result = rpc
                .decode_record_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "decryptrecord" => {
            let result = rpc
                .decrypt_record_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        "disconnect" => {
            let result = rpc
                .disconnect_protected(Params::Array(params), meta)
                .await
                .map_err(convert_core_err);
            result_to_response(&req, result)
        }
        _ => {
            let err = jrt::Error::from_code(jrt::ErrorCode::MethodNotFound);
            jrt::Response::error(jrt::Version::V2, err, req.id.clone())
        }
    };

    // Serialize the response object.
    let body = serde_json::to_vec(&response).unwrap_or_default();

    // Send the HTTP response.
    Ok(hyper::Response::new(body.into()))
}

/// Ensures that the params are a non-empty (this assumption is taken advantage of later) array and returns them.
fn read_params(req: &jrt::Request<Params>) -> Result<Vec<serde_json::Value>, jrt::Error<()>> {
    if METHODS_EXPECTING_PARAMS.contains(&&*req.method) {
        match &req.params {
            Some(Params::Array(arr)) if !arr.is_empty() => Ok(arr.clone()),
            Some(_) => Err(jrt::Error::from_code(jrt::ErrorCode::InvalidParams)),
            None => Err(jrt::Error::from_code(jrt::ErrorCode::InvalidParams)),
        }
    } else {
        Ok(vec![]) // unused in methods other than METHODS_EXPECTING_PARAMS
    }
}

/// Converts the crate's RpcError into a jrt::RpcError
fn convert_crate_err(err: crate::error::RpcError) -> jrt::Error<()> {
    let mut err = err.to_string();
    err.truncate(31); // json-rpc-type Error length limit
    jrt::Error::with_custom_msg(jrt::ErrorCode::ServerError(0), &err)
}

/// Converts the jsonrpc-core's Error into a jrt::RpcError
fn convert_core_err(err: jsonrpc_core::Error) -> jrt::Error<()> {
    let mut err = err.to_string();
    err.truncate(31); // json-rpc-type Error length limit
    jrt::Error::with_custom_msg(jrt::ErrorCode::InternalError, &err)
}

fn result_to_response<T: Serialize>(
    request: &jrt::Request<Params>,
    result: Result<T, jrt::Error<()>>,
) -> jrt::Response<serde_json::Value, ()> {
    match result {
        Ok(res) => {
            let result = serde_json::to_value(&res).unwrap_or_default();
            jrt::Response::result(jrt::Version::V2, result, request.id.clone())
        }
        Err(err) => jrt::Response::error(jrt::Version::V2, err, request.id.clone()),
    }
}
