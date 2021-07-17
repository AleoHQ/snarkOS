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

#[macro_use]
extern crate tracing;

use snarkos_consensus::{error::ConsensusError, Consensus, Miner};
use snarkos_testing::sync::*;
use snarkvm_dpc::{
    block::Transactions,
    testnet1::{
        instantiated::*,
        record::{payload::Payload as RecordPayload, Record},
    },
    Account,
    Address,
    Block,
    DPCComponents,
    ProgramScheme,
    RecordScheme,
    Storage,
};
use tracing_subscriber::EnvFilter;

use rand::{CryptoRng, Rng};
use std::{fs::File, path::PathBuf, sync::Arc};

async fn mine_block<S: Storage>(
    miner: &Miner<S>,
    txs: Vec<Testnet1Transaction>,
) -> Result<(Block<Testnet1Transaction>, Vec<Record<Components>>), ConsensusError> {
    info!("Mining block!");

    let transactions = Transactions(txs);

    let (previous_block_header, transactions, coinbase_records) = miner.establish_block(&transactions)?;

    let header = miner.find_block(&transactions, &previous_block_header)?;

    let block = Block { header, transactions };

    let old_block_height = miner.consensus.ledger.get_current_block_height();

    // Duplicate blocks dont do anything
    miner.consensus.receive_block(&block, false).await.ok(); // throws a duplicate error -- seemingly intentional

    let new_block_height = miner.consensus.ledger.get_current_block_height();
    assert_eq!(old_block_height + 1, new_block_height);

    Ok((block, coinbase_records))
}

/// Spends some value from inputs owned by the sender, to the receiver,
/// and pays back whatever we are left with.
#[allow(clippy::too_many_arguments)]
fn send<R: Rng + CryptoRng, S: Storage>(
    consensus: &Consensus<S>,
    from: &Account<Components>,
    inputs: Vec<Record<Components>>,
    receiver: &Address<Components>,
    amount: u64,
    rng: &mut R,
    memo: [u8; 32],
) -> Result<(Vec<Record<Components>>, Testnet1Transaction), ConsensusError> {
    let mut sum = 0;
    for inp in &inputs {
        sum += inp.value();
    }
    assert!(sum >= amount, "not enough balance in inputs");
    let change = sum - amount;

    let input_programs = vec![FIXTURE.program.id(); Components::NUM_INPUT_RECORDS];
    let output_programs = vec![FIXTURE.program.id(); Components::NUM_OUTPUT_RECORDS];

    let to = vec![receiver.clone(), from.address.clone()];
    let values = vec![amount, change];
    let output = vec![RecordPayload::default(); Components::NUM_OUTPUT_RECORDS];
    let dummy_flags = vec![false; Components::NUM_OUTPUT_RECORDS];

    let from = vec![from.private_key.clone(); Components::NUM_INPUT_RECORDS];
    consensus.create_transaction(
        inputs,
        from,
        to,
        input_programs,
        output_programs,
        dummy_flags,
        values,
        output,
        memo,
        rng,
    )
}

async fn mine_blocks(n: u32) -> Result<TestBlocks, ConsensusError> {
    info!("Creating test account");
    let [miner_acc, acc_1, _] = FIXTURE.test_accounts.clone();
    let mut rng = FIXTURE.rng.clone();
    info!("Creating sync");
    let consensus = Arc::new(crate::create_test_consensus());

    // setup the miner
    info!("Creating miner");
    let miner = Miner::new(miner_acc.address.clone(), consensus.clone());
    info!("Creating memory pool");

    let mut txs = vec![];
    let mut blocks = vec![];

    for i in 0..n {
        // mine an empty block
        let (block, coinbase_records) = mine_block(&miner, txs.clone()).await?;

        txs.clear();
        let mut memo = [0u8; 32];
        memo[0] = i as u8;
        // make a tx which spends 10 to the Testnet1Components receiver
        let (_records, tx) = send(
            &consensus,
            &miner_acc,
            coinbase_records.clone(),
            &acc_1.address,
            (10 + i).into(),
            &mut rng,
            memo,
        )?;

        txs.push(tx);
        blocks.push(block);
    }

    Ok(TestBlocks::new(blocks))
}

#[tokio::main]
pub async fn main() {
    let filter = EnvFilter::from_default_env();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    let block_count = 100;

    info!("Setting up test data");
    let test_blocks = mine_blocks(block_count).await.unwrap();

    let file = std::io::BufWriter::new(
        File::create(PathBuf::from(format!("test_blocks_{}", block_count))).expect("could not open file"),
    );
    test_blocks.write_le(file).expect("could not write to file");
}
