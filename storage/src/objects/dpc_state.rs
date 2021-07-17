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

use crate::*;
use snarkvm_algorithms::traits::LoadableMerkleParameters;
use snarkvm_dpc::{errors::StorageError, DatabaseTransaction, Op, Storage, TransactionScheme};
use snarkvm_utilities::{
    bytes::{FromBytes, ToBytes},
    to_bytes_le,
};

use std::{collections::HashSet, sync::Arc};

impl<T: TransactionScheme, P: LoadableMerkleParameters, S: Storage> Ledger<T, P, S> {
    /// Get the current commitment index
    pub fn current_cm_index(&self) -> Result<usize, StorageError> {
        match self.storage.get(COL_META, KEY_CURR_CM_INDEX.as_bytes())? {
            Some(cm_index_bytes) => Ok(bytes_to_u32(&cm_index_bytes) as usize),
            None => Ok(0),
        }
    }

    /// Get the current serial number index
    pub fn current_sn_index(&self) -> Result<usize, StorageError> {
        match self.storage.get(COL_META, KEY_CURR_SN_INDEX.as_bytes())? {
            Some(sn_index_bytes) => Ok(bytes_to_u32(&sn_index_bytes) as usize),
            None => Ok(0),
        }
    }

    /// Get the current memo index
    pub fn current_memo_index(&self) -> Result<usize, StorageError> {
        match self.storage.get(COL_META, KEY_CURR_MEMO_INDEX.as_bytes())? {
            Some(memo_index_bytes) => Ok(bytes_to_u32(&memo_index_bytes) as usize),
            None => Ok(0),
        }
    }

    /// Get the current ledger digest
    pub fn current_digest(&self) -> Result<Vec<u8>, StorageError> {
        match self.storage.get(COL_META, KEY_CURR_DIGEST.as_bytes())? {
            Some(current_digest) => Ok(current_digest),
            None => Ok(to_bytes_le![self.cm_merkle_tree.load().root()].unwrap()),
        }
    }

    /// Get the set of past ledger digests
    pub fn past_digests(&self) -> Result<HashSet<Box<[u8]>>, StorageError> {
        let keys = self.storage.get_keys(COL_DIGEST)?;
        let digests = keys.into_iter().collect();

        Ok(digests)
    }

    /// Get serial number index.
    pub fn get_sn_index(&self, sn_bytes: &[u8]) -> Result<Option<usize>, StorageError> {
        match self.storage.get(COL_SERIAL_NUMBER, sn_bytes)? {
            Some(sn_index_bytes) => {
                let mut sn_index = [0u8; 4];
                sn_index.copy_from_slice(&sn_index_bytes[0..4]);

                Ok(Some(u32::from_le_bytes(sn_index) as usize))
            }
            None => Ok(None),
        }
    }

    /// Get commitment index
    pub fn get_cm_index(&self, cm_bytes: &[u8]) -> Result<Option<usize>, StorageError> {
        match self.storage.get(COL_COMMITMENT, cm_bytes)? {
            Some(cm_index_bytes) => {
                let mut cm_index = [0u8; 4];
                cm_index.copy_from_slice(&cm_index_bytes[0..4]);

                Ok(Some(u32::from_le_bytes(cm_index) as usize))
            }
            None => Ok(None),
        }
    }

    /// Get memo index
    pub fn get_memo_index(&self, memo_bytes: &[u8]) -> Result<Option<usize>, StorageError> {
        match self.storage.get(COL_MEMO, memo_bytes)? {
            Some(memo_index_bytes) => {
                let mut memo_index = [0u8; 4];
                memo_index.copy_from_slice(&memo_index_bytes[0..4]);

                Ok(Some(u32::from_le_bytes(memo_index) as usize))
            }
            None => Ok(None),
        }
    }

    /// Build a new commitment merkle tree from the stored commitments
    pub fn rebuild_merkle_tree(&self, additional_cms: Vec<(T::Commitment, usize)>) -> Result<(), StorageError> {
        let mut new_cm_and_indices = additional_cms;

        let mut old_cm_and_indices = vec![];
        for (commitment_key, index_value) in self.storage.get_col(COL_COMMITMENT)? {
            let commitment: T::Commitment = FromBytes::read_le(&commitment_key[..])?;
            let index = bytes_to_u32(&index_value) as usize;

            old_cm_and_indices.push((commitment, index));
        }

        old_cm_and_indices.sort_by(|&(_, i), &(_, j)| i.cmp(&j));
        new_cm_and_indices.sort_by(|&(_, i), &(_, j)| i.cmp(&j));

        let old_commitments = old_cm_and_indices.into_iter().map(|(cm, _)| cm);
        let new_commitments = new_cm_and_indices.into_iter().map(|(cm, _)| cm).collect::<Vec<_>>();

        let merkle = self.cm_merkle_tree.load();
        self.cm_merkle_tree
            .store(Arc::new(merkle.rebuild(old_commitments, &new_commitments[..])?));

        Ok(())
    }

    /// Rebuild the stored merkle tree with the current stored commitments
    pub fn update_merkle_tree(&self, new_best_block_number: u32) -> Result<(), StorageError> {
        self.rebuild_merkle_tree(vec![])?;
        let new_digest = self.cm_merkle_tree.load().root();

        let mut database_transaction = DatabaseTransaction::new();

        database_transaction.push(Op::Insert {
            col: COL_DIGEST,
            key: to_bytes_le![new_digest]?,
            value: new_best_block_number.to_le_bytes().to_vec(),
        });
        database_transaction.push(Op::Insert {
            col: COL_META,
            key: KEY_CURR_DIGEST.as_bytes().to_vec(),
            value: to_bytes_le![new_digest]?,
        });

        self.storage.batch(database_transaction)
    }
}
