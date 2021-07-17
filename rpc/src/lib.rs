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

// Compilation
#![warn(unused_extern_crates)]
#![forbid(unsafe_code)]
// Documentation
// #![cfg_attr(nightly, warn(missing_docs))]
// todo: readd in Rust 1.54
// #![cfg_attr(nightly, doc(include = "../documentation/concepts/rpc_server.md"))]

#[macro_use]
extern crate derivative;
#[macro_use]
extern crate thiserror;
#[macro_use]
extern crate tracing;

pub mod custom_rpc_server;
#[doc(inline)]
pub use custom_rpc_server::*;

pub mod error;

pub mod rpc_impl;
#[doc(inline)]
pub use rpc_impl::*;

pub mod rpc_impl_protected;
#[doc(inline)]
pub use rpc_impl_protected::*;
/*
pub mod rpc_server;
#[doc(inline)]
pub use rpc_server::*;
*/
pub mod rpc_trait;
#[doc(inline)]
pub use rpc_trait::*;

pub mod rpc_types;
#[doc(inline)]
pub use rpc_types::*;

pub(crate) mod empty_ledger;
pub(crate) mod transaction_kernel_builder;
