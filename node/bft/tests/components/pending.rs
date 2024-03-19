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

use crate::common::{primary::new_test_committee, utils::sample_ledger, CurrentNetwork};
use snarkos_node_bft::helpers::max_redundant_requests;
use snarkvm::{ledger::committee::Committee, prelude::TestRng};

#[test]
fn test_max_redundant_requests() {
    const NUM_NODES: u16 = Committee::<CurrentNetwork>::MAX_COMMITTEE_SIZE;

    // Initialize the RNG.
    let mut rng = TestRng::default();
    // Initialize the accounts and the committee.
    let (accounts, committee) = new_test_committee(NUM_NODES, &mut rng);
    // Sample a ledger.
    let ledger = sample_ledger(&accounts, &committee, &mut rng);
    // Ensure the maximum number of redundant requests is correct and consistent across iterations.
    assert_eq!(max_redundant_requests(ledger, 0), 34, "Update me if the formula changes");
}
