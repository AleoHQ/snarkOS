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

use crate::sync::{create_test_consensus, TestBlocks};
use snarkos_storage::*;
use snarkvm_dpc::{DatabaseTransaction, Op, Storage};

use rand::prelude::*;

#[tokio::test]
async fn valid_storage_validates() {
    let consensus = create_test_consensus();

    let blocks = TestBlocks::load(Some(5), "test_blocks_100_1").0;
    for block in blocks {
        consensus.receive_block(&block, false).await.unwrap();
    }

    assert!(consensus.ledger.validate(None, FixMode::Nothing));
}

#[tokio::test]
async fn validator_vs_a_missing_serial_number() {
    let consensus = create_test_consensus();

    let blocks = TestBlocks::load(Some(5), "test_blocks_100_1").0;
    for block in blocks {
        consensus.receive_block(&block, false).await.unwrap();
    }

    // Remove a random tx serial number.
    let stored_sns = consensus.ledger.storage.get_col(COL_SERIAL_NUMBER).unwrap();
    let random_sn = &stored_sns.choose(&mut thread_rng()).unwrap().0;
    let mut database_transaction = DatabaseTransaction::new();
    database_transaction.push(Op::Delete {
        col: COL_SERIAL_NUMBER,
        key: random_sn.to_vec(),
    });
    consensus.ledger.storage.batch(database_transaction).unwrap();

    assert!(!consensus.ledger.validate(None, FixMode::Nothing));
    // Currently unsupported.
    // assert!(consensus.ledger.validate(None, FixMode::MissingTestnet1TransactionComponents));
}

#[tokio::test]
async fn validator_vs_a_missing_commitment() {
    let consensus = create_test_consensus();

    let blocks = TestBlocks::load(Some(5), "test_blocks_100_1").0;
    for block in blocks {
        consensus.receive_block(&block, false).await.unwrap();
    }

    // Remove a random tx commitment.
    let stored_cms = consensus.ledger.storage.get_col(COL_COMMITMENT).unwrap();
    let random_cm = &stored_cms.choose(&mut thread_rng()).unwrap().0;
    let mut database_transaction = DatabaseTransaction::new();
    database_transaction.push(Op::Delete {
        col: COL_COMMITMENT,
        key: random_cm.to_vec(),
    });
    consensus.ledger.storage.batch(database_transaction).unwrap();

    assert!(!consensus.ledger.validate(None, FixMode::Nothing));
    // Currently unsupported
    // assert!(consensus.ledger.validate(None, FixMode::MissingTestnet1TransactionComponents));
}

#[tokio::test]
async fn validator_vs_a_missing_memorandum() {
    let consensus = create_test_consensus();

    let blocks = TestBlocks::load(Some(5), "test_blocks_100_1").0;
    for block in blocks {
        consensus.receive_block(&block, false).await.unwrap();
    }

    // Remove a random memo.
    let stored_memos = consensus.ledger.storage.get_col(COL_MEMO).unwrap();
    let random_memo = &stored_memos.choose(&mut thread_rng()).unwrap().0;
    let mut database_transaction = DatabaseTransaction::new();
    database_transaction.push(Op::Delete {
        col: COL_MEMO,
        key: random_memo.to_vec(),
    });
    consensus.ledger.storage.batch(database_transaction).unwrap();

    assert!(!consensus.ledger.validate(None, FixMode::Nothing));
    // Currently unsupported
    // assert!(consensus.ledger.validate(None, FixMode::MissingTestnet1TransactionComponents));
}

#[tokio::test]
async fn validator_vs_a_missing_digest() {
    let consensus = create_test_consensus();

    let blocks = TestBlocks::load(Some(5), "test_blocks_100_1").0;
    for block in blocks {
        consensus.receive_block(&block, false).await.unwrap();
    }

    // Remove a random digest.
    let stored_digests = consensus.ledger.storage.get_col(COL_DIGEST).unwrap();
    let random_digest = &stored_digests.choose(&mut thread_rng()).unwrap().0;
    let mut database_transaction = DatabaseTransaction::new();
    database_transaction.push(Op::Delete {
        col: COL_DIGEST,
        key: random_digest.to_vec(),
    });
    consensus.ledger.storage.batch(database_transaction).unwrap();

    assert!(!consensus.ledger.validate(None, FixMode::Nothing));
    assert!(
        consensus
            .ledger
            .validate(None, FixMode::MissingTestnet1TransactionComponents)
    );
}

#[tokio::test]
async fn validator_vs_a_superfluous_serial_number() {
    let consensus = create_test_consensus();

    let blocks = TestBlocks::load(Some(5), "test_blocks_100_1").0;
    for block in blocks {
        consensus.receive_block(&block, false).await.unwrap();
    }

    // Add an extra random tx serial number.
    let mut database_transaction = DatabaseTransaction::new();
    let current_sn_idx = consensus.ledger.current_sn_index().unwrap() as u32;
    database_transaction.push(Op::Insert {
        col: COL_SERIAL_NUMBER,
        key: vec![0; 32],
        value: (current_sn_idx + 1).to_le_bytes().to_vec(),
    });
    database_transaction.push(Op::Insert {
        col: COL_META,
        key: KEY_CURR_SN_INDEX.as_bytes().to_vec(),
        value: (current_sn_idx + 1).to_le_bytes().to_vec(),
    });
    consensus.ledger.storage.batch(database_transaction).unwrap();

    assert!(!consensus.ledger.validate(None, FixMode::Nothing));
    assert!(
        consensus
            .ledger
            .validate(None, FixMode::SuperfluousTestnet1TransactionComponents)
    );
}

#[tokio::test]
async fn validator_vs_a_superfluous_commitment() {
    let consensus = create_test_consensus();

    let blocks = TestBlocks::load(Some(5), "test_blocks_100_1").0;
    for block in blocks {
        consensus.receive_block(&block, false).await.unwrap();
    }

    // Add an extra random tx commitment.
    let mut database_transaction = DatabaseTransaction::new();
    let current_cm_idx = consensus.ledger.current_cm_index().unwrap() as u32;
    database_transaction.push(Op::Insert {
        col: COL_COMMITMENT,
        key: vec![0; 32],
        value: (current_cm_idx + 1).to_le_bytes().to_vec(),
    });
    database_transaction.push(Op::Insert {
        col: COL_META,
        key: KEY_CURR_CM_INDEX.as_bytes().to_vec(),
        value: (current_cm_idx + 1).to_le_bytes().to_vec(),
    });
    consensus.ledger.storage.batch(database_transaction).unwrap();

    assert!(!consensus.ledger.validate(None, FixMode::Nothing));
    assert!(
        consensus
            .ledger
            .validate(None, FixMode::SuperfluousTestnet1TransactionComponents)
    );
}

#[tokio::test]
async fn validator_vs_a_superfluous_memorandum() {
    let consensus = create_test_consensus();

    let blocks = TestBlocks::load(Some(5), "test_blocks_100_1").0;
    for block in blocks {
        consensus.receive_block(&block, false).await.unwrap();
    }

    // Add an extra random memo.
    let mut database_transaction = DatabaseTransaction::new();
    let current_memo_idx = consensus.ledger.current_memo_index().unwrap() as u32;
    database_transaction.push(Op::Insert {
        col: COL_MEMO,
        key: vec![9; 32], // apparently a memo filled with zeros is already stored
        value: (current_memo_idx + 1).to_le_bytes().to_vec(),
    });
    database_transaction.push(Op::Insert {
        col: COL_META,
        key: KEY_CURR_MEMO_INDEX.as_bytes().to_vec(),
        value: (current_memo_idx + 1).to_le_bytes().to_vec(),
    });
    consensus.ledger.storage.batch(database_transaction).unwrap();

    assert!(!consensus.ledger.validate(None, FixMode::Nothing));
    assert!(
        consensus
            .ledger
            .validate(None, FixMode::SuperfluousTestnet1TransactionComponents)
    );
}

#[tokio::test]
async fn validator_vs_a_superfluous_digest() {
    let consensus = create_test_consensus();

    let blocks = TestBlocks::load(Some(5), "test_blocks_100_1").0;
    for block in blocks {
        consensus.receive_block(&block, false).await.unwrap();
    }

    // Add an extra random digest.
    let mut database_transaction = DatabaseTransaction::new();
    database_transaction.push(Op::Insert {
        col: COL_DIGEST,
        key: vec![0; 32],
        value: (consensus.ledger.get_current_block_height() + 1).to_le_bytes().to_vec(),
    });
    consensus.ledger.storage.batch(database_transaction).unwrap();

    assert!(!consensus.ledger.validate(None, FixMode::Nothing));
    assert!(
        consensus
            .ledger
            .validate(None, FixMode::SuperfluousTestnet1TransactionComponents)
    );
}

#[ignore]
#[tokio::test]
async fn validator_vs_a_very_broken_db() {
    tracing_subscriber::fmt::init();

    let consensus = create_test_consensus();

    let blocks = TestBlocks::load(Some(10), "test_blocks_100_1").0;
    for block in blocks {
        consensus.receive_block(&block, false).await.unwrap();
    }

    let mut database_transaction = DatabaseTransaction::new();

    // Delete all tx-related items.
    for col in [COL_SERIAL_NUMBER, COL_COMMITMENT, COL_MEMO, COL_DIGEST].iter() {
        let stored_col = consensus.ledger.storage.get_col(*col).unwrap();
        for key in stored_col.into_iter().map(|(key, _val)| key) {
            database_transaction.push(Op::Delete {
                col: *col,
                key: key.into_vec(),
            });
        }
    }

    consensus.ledger.storage.batch(database_transaction).unwrap();

    let now = std::time::Instant::now();
    assert!(!consensus.ledger.validate(None, FixMode::Nothing));
    tracing::info!("Storage validated in {}ms", now.elapsed().as_millis());
}
