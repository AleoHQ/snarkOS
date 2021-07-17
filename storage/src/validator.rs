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

use crate::{
    Ledger,
    TransactionLocation,
    COL_COMMITMENT,
    COL_DIGEST,
    COL_MEMO,
    COL_SERIAL_NUMBER,
    COL_TRANSACTION_LOCATION,
};
use snarkvm_algorithms::traits::LoadableMerkleParameters;
use snarkvm_dpc::{Block, BlockHeaderHash, DatabaseTransaction, LedgerScheme, Op, Storage, TransactionScheme};
use snarkvm_utilities::{to_bytes_le, ToBytes};

use parking_lot::Mutex;
use rayon::prelude::*;
use tracing::*;

use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

macro_rules! check_for_superfluous_tx_components {
    ($fn_name:ident, $component_name:expr, $component_col:expr) => {
        fn $fn_name(
            &self,
            tx_entries: &HashSet<Vec<u8>>,
            db_ops: &Mutex<Option<DatabaseTransaction>>,
            fix_mode: FixMode,
            is_storage_valid: &AtomicBool,
        ) {
            let storage_entries_and_indices = match self.storage.get_col($component_col) {
                Ok(col) => col,
                Err(e) => {
                    error!("Couldn't obtain the column with tx {}s: {}", $component_name, e);
                    is_storage_valid.store(false, Ordering::SeqCst);

                    return;
                }
            };

            let storage_entries = storage_entries_and_indices
                .into_iter()
                .map(|(entry, _)| entry.into_vec())
                .collect::<HashSet<_>>();

            let superfluous_items = storage_entries.difference(&tx_entries).collect::<Vec<_>>();

            if !superfluous_items.is_empty() {
                warn!(
                    "There are {} more {}s stored than there are in canon transactions",
                    superfluous_items.len(),
                    $component_name
                );

                if let Some(ref mut ops) = &mut *db_ops.lock() {
                    if [
                        FixMode::SuperfluousTestnet1TransactionComponents,
                        FixMode::Everything,
                    ]
                    .contains(&fix_mode)
                    {
                        for superfluous_item in superfluous_items {
                            ops.push(Op::Delete {
                                col: $component_col,
                                key: superfluous_item.to_vec(),
                            });
                        }
                    } else {
                        is_storage_valid.store(false, Ordering::SeqCst);
                    }
                } else {
                    is_storage_valid.store(false, Ordering::SeqCst);
                }
            }
        }
    };
}

#[derive(Clone, Copy, PartialEq)]
pub enum FixMode {
    /// Don't fix anything in the storage.
    Nothing,
    /// Update transaction locations if need be.
    Testnet1TransactionLocations,
    /// Store transaction serial numbers, commitments and memorandums that are missing in the storage.
    MissingTestnet1TransactionComponents,
    /// Remove transaction serial numbers, commitments and memorandums for missing transactions.
    SuperfluousTestnet1TransactionComponents,
    /// Apply all the available fixes.
    Everything,
}

impl<T: TransactionScheme + Send + Sync, P: LoadableMerkleParameters, S: Storage + Sync> Ledger<T, P, S> {
    check_for_superfluous_tx_components!(check_for_superfluous_tx_memos, "memorandum", COL_MEMO);

    check_for_superfluous_tx_components!(check_for_superfluous_tx_digests, "digest", COL_DIGEST);

    check_for_superfluous_tx_components!(check_for_superfluous_tx_sns, "serial number", COL_SERIAL_NUMBER);

    check_for_superfluous_tx_components!(check_for_superfluous_tx_cms, "commitment", COL_COMMITMENT);

    /// Validates the storage of the canon blocks, their child-parent relationships, and their transactions; starts
    /// at the current block height and goes down until the genesis block, making sure that the block-related data
    /// stored in the database is coherent. The optional limit restricts the number of blocks to check, as
    /// it is likely that any issues are applicable only to the last few blocks. The `fix` argument determines whether
    /// the validation process should also attempt to fix the issues it encounters.
    pub fn validate(&self, mut limit: Option<u32>, fix_mode: FixMode) -> bool {
        if limit.is_some()
            && [FixMode::SuperfluousTestnet1TransactionComponents, FixMode::Everything].contains(&fix_mode)
        {
            panic!(
                "The validator can perform the specified fixes only if there is no limit on the number of blocks to process"
            );
        }

        info!("Validating the storage...");

        let is_valid = AtomicBool::new(true);

        if limit == Some(0) {
            info!("The limit of blocks to validate is 0; nothing to check.");

            return is_valid.load(Ordering::SeqCst);
        }

        let db_ops = if fix_mode != FixMode::Nothing {
            Arc::new(Mutex::new(Some(DatabaseTransaction::new())))
        } else {
            Arc::new(Mutex::new(None))
        };

        let mut current_height = self.get_current_block_height();

        if current_height == 0 {
            info!("Only the genesis block is currently available; nothing to check.");

            return is_valid.load(Ordering::SeqCst);
        }

        debug!("The block height is {}", current_height);

        match self.get_best_block_number() {
            Err(_) => {
                is_valid.store(false, Ordering::SeqCst);
                error!("Can't obtain the best block number from storage!");
            }
            Ok(number) => {
                // Initial block height comes from KEY_BEST_BLOCK_NUMBER.
                if number != current_height {
                    is_valid.store(false, Ordering::SeqCst);
                    error!("Current best block number doesn't match the block height!");
                }
            }
        }

        let mut true_height_mismatch = false;

        // get_block_hash uses COL_BLOCK_LOCATOR, as it should have been committed (i.e. be canon).
        let mut current_hash = self.get_block_hash(current_height);
        while let Err(e) = current_hash {
            is_valid.store(false, Ordering::SeqCst);
            error!(
                "Couldn't find the latest block (height {}): {}! Trying a lower height next.",
                current_height, e
            );

            true_height_mismatch = true;

            current_height -= 1;

            if let Some(ref mut limit) = limit {
                *limit -= 1;
                if *limit == 0 {
                    info!("Specified block limit reached; the check is complete.");

                    return is_valid.load(Ordering::SeqCst);
                }
            }

            current_hash = self.get_block_hash(current_height);
        }

        if true_height_mismatch {
            debug!("The true block height is {}", current_height);
        }

        let tx_memos = Default::default();
        let tx_sns = Default::default();
        let tx_cms = Default::default();
        let tx_digests = Default::default();

        let to_process = if let Some(ref mut limit) = limit {
            *limit
        } else {
            current_height
        };

        (0..=to_process).into_par_iter().for_each(|i| {
            self.validate_block(
                current_height - i,
                &tx_memos,
                &tx_sns,
                &tx_cms,
                &tx_digests,
                &db_ops,
                fix_mode,
                &is_valid,
            );
        });

        // Superfluous items can only be removed after a full storage pass.
        if limit.is_none() {
            self.check_for_superfluous_tx_memos(&*tx_memos.lock(), &db_ops, fix_mode, &is_valid);
            self.check_for_superfluous_tx_digests(&*tx_digests.lock(), &db_ops, fix_mode, &is_valid);
            self.check_for_superfluous_tx_sns(&*tx_sns.lock(), &db_ops, fix_mode, &is_valid);
            self.check_for_superfluous_tx_cms(&*tx_cms.lock(), &db_ops, fix_mode, &is_valid);
        }

        if let Some(ops) = db_ops.lock().take() {
            if !ops.0.is_empty() {
                info!("Fixing the detected storage issues ({} fixes)", ops.0.len());
                if let Err(e) = self.storage.batch(ops) {
                    error!("Couldn't fix the storage issues: {}", e);
                }
            }
        }

        let is_valid = is_valid.load(Ordering::SeqCst);

        if is_valid {
            info!("The storage is valid!");
        } else {
            error!("The storage is invalid!");
        }

        is_valid
    }

    /// Validates the storage of the given block.
    #[allow(clippy::too_many_arguments)]
    fn validate_block(
        &self,
        block_height: u32,
        tx_memos: &Mutex<HashSet<Vec<u8>>>,
        tx_sns: &Mutex<HashSet<Vec<u8>>>,
        tx_cms: &Mutex<HashSet<Vec<u8>>>,
        tx_digests: &Mutex<HashSet<Vec<u8>>>,
        database_fix: &Mutex<Option<DatabaseTransaction>>,
        fix_mode: FixMode,
        is_storage_valid: &AtomicBool,
    ) {
        let block = if let Ok(block) = self.get_block_from_block_number(block_height) {
            block
        } else {
            // Block not found; register the failure and attempt to carry on.
            is_storage_valid.store(false, Ordering::SeqCst);
            return;
        };

        let block_hash = block.header.get_hash();

        // This is extremely verbose and shouldn't be used outside of debugging.
        // trace!("Validating block at height {} ({})", block_height, block_hash);

        if !self.block_hash_exists(&block_hash) {
            is_storage_valid.store(false, Ordering::SeqCst);
            error!("The header for block at height {} is missing!", block_height);
        }

        self.validate_block_transactions(
            &block,
            block_height,
            tx_memos,
            tx_sns,
            tx_cms,
            tx_digests,
            database_fix,
            fix_mode,
            is_storage_valid,
        );

        // The genesis block has no parent.
        if block_height == 0 {
            return;
        }

        let previous_hash = match self.get_block_hash(block_height - 1) {
            Ok(hash) => hash,
            Err(e) => {
                error!("Couldn't find a block at height {}: {}!", block_height - 1, e);
                is_storage_valid.store(false, Ordering::SeqCst);

                return;
            }
        };

        if block.header.previous_block_hash != previous_hash {
            is_storage_valid.store(false, Ordering::SeqCst);
            error!(
                "The parent hash of block at height {} doesn't match its child at {}!",
                block_height,
                block_height - 1,
            );
        }

        match self.get_child_block_hashes(&previous_hash) {
            Err(e) => {
                is_storage_valid.store(false, Ordering::SeqCst);
                error!("Can't find the children of block at height {}: {}!", previous_hash, e);
            }
            Ok(child_hashes) => {
                if !child_hashes.contains(&block_hash) {
                    is_storage_valid.store(false, Ordering::SeqCst);
                    error!(
                        "The list of children hash of block at height {} don't contain the child at {}!",
                        block_height - 1,
                        block_height,
                    );
                }
            }
        }
    }

    /// Validates the storage of transactions belonging to the given block.
    #[allow(clippy::too_many_arguments)]
    fn validate_block_transactions(
        &self,
        block: &Block<T>,
        block_height: u32,
        tx_memos: &Mutex<HashSet<Vec<u8>>>,
        tx_sns: &Mutex<HashSet<Vec<u8>>>,
        tx_cms: &Mutex<HashSet<Vec<u8>>>,
        tx_digests: &Mutex<HashSet<Vec<u8>>>,
        database_fix: &Mutex<Option<DatabaseTransaction>>,
        fix_mode: FixMode,
        is_storage_valid: &AtomicBool,
    ) {
        block.transactions.as_slice().par_iter().enumerate().for_each(|(block_tx_idx, tx)| {
            let tx_id = match tx.transaction_id() {
                Ok(hash) => hash,
                Err(e) => {
                    error!(
                        "The id of a transaction from block {} can't be parsed: {}",
                        block.header.get_hash(),
                        e
                    );
                    is_storage_valid.store(false, Ordering::SeqCst);

                    return;
                }
            };

            let tx = match self.get_transaction_bytes(&tx_id) {
                Ok(tx) => match T::read_le(&tx[..]) {
                    Ok(tx) => tx,
                    Err(e) => {
                        error!("Transaction {} can't be parsed: {}", hex::encode(tx_id), e);
                        is_storage_valid.store(false, Ordering::SeqCst);

                        return;
                    }
                },
                Err(e) => {
                    error!(
                        "Transaction {} can't be found in the storage: {}",
                        hex::encode(tx_id),
                        e
                    );
                    is_storage_valid.store(false, Ordering::SeqCst);

                    return;
                }
            };

            for sn in tx.old_serial_numbers() {
                if !self.contains_sn(sn) {
                    error!(
                        "Transaction {} doesn't have an old serial number stored",
                        hex::encode(tx_id)
                    );
                    is_storage_valid.store(false, Ordering::SeqCst);
                }
                tx_sns.lock().insert(to_bytes_le!(sn).unwrap()); // to_bytes can't fail
            }

            for cm in tx.new_commitments() {
                if !self.contains_cm(cm) {
                    error!(
                        "Transaction {} doesn't have a new commitment stored",
                        hex::encode(tx_id)
                    );
                    is_storage_valid.store(false, Ordering::SeqCst);
                }
                tx_cms.lock().insert(to_bytes_le!(cm).unwrap()); // to_bytes can't fail
            }

            let tx_digest = tx.ledger_digest();
            // to_bytes can't fail
            if !self.storage.exists(COL_DIGEST, &to_bytes_le![tx_digest].unwrap()) {
                warn!(
                    "Transaction {} doesn't have the ledger digest stored",
                    hex::encode(tx_id),
                );

                if let Some(ref mut db_ops) = &mut *database_fix.lock() {
                    if [FixMode::MissingTestnet1TransactionComponents, FixMode::Everything].contains(&fix_mode) {
                        db_ops.push(Op::Insert {
                            col: COL_DIGEST,
                            key: to_bytes_le![tx_digest].unwrap(), // to_bytes can't fail
                            value: block_height.to_le_bytes().to_vec(),
                        });
                    } else {
                        is_storage_valid.store(false, Ordering::SeqCst);
                    }
                } else {
                    is_storage_valid.store(false, Ordering::SeqCst);
                }
            }
            tx_digests.lock().insert(to_bytes_le!(tx_digest).unwrap()); // to_bytes can't fail

            let tx_memo = tx.memorandum();
            if !self.contains_memo(tx_memo) {
                error!("Transaction {} doesn't have its memo stored", hex::encode(tx_id));
                is_storage_valid.store(false, Ordering::SeqCst);
            }
            tx_memos.lock().insert(to_bytes_le!(tx_memo).unwrap()); // to_bytes can't fail

            match self.get_transaction_location(&tx_id) {
                Ok(Some(tx_location)) => match self.get_block_number(&BlockHeaderHash(tx_location.block_hash)) {
                    Ok(block_number) => {
                        if block_number != block_height {
                            error!(
                                "The block indicated by the location of tx {} doesn't match the current height ({} != {})",
                                hex::encode(tx_id),
                                block_number,
                                block_height,
                            );
                            is_storage_valid.store(false, Ordering::SeqCst);
                        }
                    }
                    Err(_) => {
                        warn!(
                            "Can't get the block number for tx {}! The block locator entry for hash {} is missing",
                            hex::encode(tx_id),
                            BlockHeaderHash(tx_location.block_hash)
                        );

                        if let Some(ref mut db_ops) = &mut *database_fix.lock() {
                            if [FixMode::Testnet1TransactionLocations, FixMode::Everything].contains(&fix_mode) {
                                let corrected_location = TransactionLocation {
                                    index: block_tx_idx as u32,
                                    block_hash: block.header.get_hash().0,
                                };

                                db_ops.push(Op::Insert {
                                    col: COL_TRANSACTION_LOCATION,
                                    key: tx_id.to_vec(),
                                    value: to_bytes_le!(corrected_location).unwrap(), // to_bytes_le can't fail
                                });
                            } else {
                                is_storage_valid.store(false, Ordering::SeqCst);
                            }
                        } else {
                            is_storage_valid.store(false, Ordering::SeqCst);
                        }
                    }
                },
                Err(e) => {
                    error!("Can't get the location of tx {}: {}", hex::encode(tx_id), e);
                    is_storage_valid.store(false, Ordering::SeqCst);
                }
                Ok(None) => {
                    error!("Can't get the location of tx {}", hex::encode(tx_id));
                    is_storage_valid.store(false, Ordering::SeqCst);
                }
            }
        });
    }
}
