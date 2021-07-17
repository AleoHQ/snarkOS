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

use std::sync::Arc;

use criterion::*;
use rand::Rng;
use snarkvm_algorithms::{merkle_tree::MerkleTree, MerkleParameters, CRH};
use snarkvm_dpc::testnet1::instantiated::*;
use snarkvm_parameters::{LedgerMerkleTreeParameters, Parameter};
use snarkvm_utilities::bytes::FromBytes;

fn merkle_build(c: &mut Criterion) {
    let crh_parameters =
        <MerkleTreeCRH as CRH>::Parameters::read_le(&LedgerMerkleTreeParameters::load_bytes().unwrap()[..])
            .expect("read bytes as hash for MerkleParameters in ledger");
    let merkle_tree_hash_parameters = <CommitmentMerkleParameters as MerkleParameters>::H::from(crh_parameters);
    let ledger_merkle_tree_parameters: Arc<CommitmentMerkleParameters> =
        Arc::new(From::from(merkle_tree_hash_parameters));

    let mut commitments = Vec::with_capacity(10000);
    for _ in 0..10000 {
        let mut buf = [0u8; 32];
        rand::thread_rng().fill(&mut buf[..]);
        commitments.push(buf);
    }

    c.bench_function("merkle_build", move |b| {
        b.iter(|| {
            MerkleTree::new(ledger_merkle_tree_parameters.clone(), &commitments[..]).unwrap();
        });
    });
}

criterion_group! {
    name = merkle;
    config = Criterion::default().sample_size(10);
    targets = merkle_build
}

criterion_main!(merkle);
