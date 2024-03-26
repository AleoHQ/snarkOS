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

pub mod primary;
pub mod test_peer;
pub mod utils;

pub type CurrentNetwork = snarkvm::prelude::MainnetV0;

pub type TranslucentLedgerService<N, C> = snarkos_node_bft_ledger_service::TranslucentLedgerService<N, C>;
