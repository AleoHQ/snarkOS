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

//! Definition of the public and private RPC endpoints.

use crate::{error::RpcError, rpc_types::*};
use snarkos_metrics::snapshots::NodeStats;

use jsonrpc_derive::rpc;

use std::net::SocketAddr;

/// Definition of public RPC endpoints.
#[rpc]
pub trait RpcFunctions {
    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/getblock.md"))]
    #[rpc(name = "getblock")]
    fn get_block(&self, block_hash_string: String) -> Result<BlockInfo, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/getblockcount.md"))]
    #[rpc(name = "getblockcount")]
    fn get_block_count(&self) -> Result<u32, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/getbestblockhash.md"))]
    #[rpc(name = "getbestblockhash")]
    fn get_best_block_hash(&self) -> Result<String, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/getblockhash.md"))]
    #[rpc(name = "getblockhash")]
    fn get_block_hash(&self, block_height: u32) -> Result<String, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/getrawtransaction.md"))]
    #[rpc(name = "getrawtransaction")]
    fn get_raw_transaction(&self, transaction_id: String) -> Result<String, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/gettransactioninfo.md"))]
    #[rpc(name = "gettransactioninfo")]
    fn get_transaction_info(&self, transaction_id: String) -> Result<TransactionInfo, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/decoderawtransaction.md"))]
    #[rpc(name = "decoderawtransaction")]
    fn decode_raw_transaction(&self, transaction_bytes: String) -> Result<TransactionInfo, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/sendtransaction.md"))]
    #[rpc(name = "sendtransaction")]
    fn send_raw_transaction(&self, transaction_bytes: String) -> Result<String, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(
    //     nightly,
    //     doc(include = "../documentation/public_endpoints/validaterawtransaction.md")
    // )]
    #[rpc(name = "validaterawtransaction")]
    fn validate_raw_transaction(&self, transaction_bytes: String) -> Result<bool, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/getconnectioncount.md"))]
    #[rpc(name = "getconnectioncount")]
    fn get_connection_count(&self) -> Result<usize, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/getpeerinfo.md"))]
    #[rpc(name = "getpeerinfo")]
    fn get_peer_info(&self) -> Result<PeerInfo, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/getnodeinfo.md"))]
    #[rpc(name = "getnodeinfo")]
    fn get_node_info(&self) -> Result<NodeInfo, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/getnodestats.md"))]
    #[rpc(name = "getnodestats")]
    fn get_node_stats(&self) -> Result<NodeStats, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/public_endpoints/getblocktemplate.md"))]
    #[rpc(name = "getblocktemplate")]
    fn get_block_template(&self) -> Result<BlockTemplate, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include ="../documentation/public_endpoints/getnetworkgraph.md"))]
    #[rpc(name = "getnetworkgraph")]
    fn get_network_graph(&self) -> Result<NetworkGraph, RpcError>;
}

/// Definition of private RPC endpoints that require authentication.
pub trait ProtectedRpcFunctions {
    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/private_endpoints/createaccount.md"))]
    fn create_account(&self) -> Result<RpcAccount, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/private_endpoints/createrawtransaction.md"))]
    fn create_raw_transaction(
        &self,
        transaction_input: TransactionInputs,
    ) -> Result<CreateRawTransactionOuput, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(
    //     nightly,
    //     doc(include = "../documentation/private_endpoints/createtransactionkernel.md")
    // )]
    fn create_transaction_kernel(&self, transaction_input: TransactionInputs) -> Result<String, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/private_endpoints/createtransaction.md"))]
    fn create_transaction(
        &self,
        private_keys: [String; 2], // TODO (howardwu): Genericize this.
        transaction_kernel: String,
    ) -> Result<CreateRawTransactionOuput, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/private_endpoints/getrecordcommitments.md"))]
    fn get_record_commitments(&self) -> Result<Vec<String>, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(
    //     nightly,
    //     doc(include = "../documentation/private_endpoints/getrecordcommitmentcount.md")
    // )]
    fn get_record_commitment_count(&self) -> Result<usize, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/private_endpoints/getrawrecord.md"))]
    fn get_raw_record(&self, record_commitment: String) -> Result<String, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/private_endpoints/decoderecord.md"))]
    fn decode_record(&self, record_bytes: String) -> Result<RecordInfo, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/private_endpoints/decryptrecord.md"))]
    fn decrypt_record(&self, decryption_input: DecryptRecordInput) -> Result<String, RpcError>;

    // todo: readd in Rust 1.54
    // #[cfg_attr(nightly, doc(include = "../documentation/private_endpoints/disconnect.md"))]
    fn disconnect(&self, address: SocketAddr);
}
