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

use snarkos_testing::sync::*;
use snarkvm_dpc::{block::Transactions, merkle_root};
use snarkvm_posw::txids_to_roots;

use criterion::{criterion_group, criterion_main, Criterion};

fn bench_txids_to_roots(c: &mut Criterion) {
    let transactions = Transactions(vec![TestTestnet1Transaction; 200]);

    c.bench_function("txids_to_roots", |b| {
        b.iter(|| txids_to_roots(&transactions.to_transaction_ids().unwrap()))
    });
}

fn bench_merkle_root(c: &mut Criterion) {
    use std::convert::TryInto;

    let mut txs: Vec<[u8; 32]> = Vec::with_capacity(5);

    for hash in &[
        "c06fbab289f723c6261d3030ddb6be121f7d2508d77862bb1e484f5cd7f92b25",
        "5a4ebf66822b0b2d56bd9dc64ece0bc38ee7844a23ff1d7320a88c5fdb2ad3e2",
        "513507fa209db823541caf7b9742bb9999b4a399cf604ba8da7037f3acced649",
        "6bf5d2e02b8432d825c5dff692d435b6c5f685d94efa6b3d8fb818f2ecdcfb66",
        "8a5ad423bc54fb7c76718371fd5a73b8c42bf27beaf2ad448761b13bcafb8895",
    ] {
        txs.push(hex::decode(hash).unwrap().as_slice().try_into().unwrap())
    }

    c.bench_function("merkle_root", |b| b.iter(|| merkle_root(&txs)));
}

criterion_group!(
    name = benches_transactions;
    config = Criterion::default();
    targets = bench_txids_to_roots,
    bench_merkle_root,
);

criterion_main!(benches_transactions);
