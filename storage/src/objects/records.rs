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
use snarkvm_dpc::{errors::StorageError, DatabaseTransaction, Op, RecordScheme, Storage, TransactionScheme};
use snarkvm_utilities::{
    bytes::{FromBytes, ToBytes},
    to_bytes_le,
};

// TODO (howardwu): Remove this from `Ledger` as it is not used for ledger state.
//  This is merely for local node / miner functionality.
impl<T: TransactionScheme, P: LoadableMerkleParameters, S: Storage> Ledger<T, P, S> {
    /// Get all stored record commitments of the node
    pub fn get_record_commitments(&self, limit: Option<usize>) -> Result<Vec<Vec<u8>>, StorageError> {
        let mut record_commitments = vec![];

        for commitment_key in self.storage.get_keys(COL_RECORDS)? {
            if let Some(limit) = limit {
                if record_commitments.len() >= limit {
                    break;
                }
            }

            record_commitments.push(commitment_key.to_vec());
        }

        Ok(record_commitments)
    }

    /// Get a transaction bytes given the transaction id.
    pub fn get_record<R: RecordScheme>(&self, record_commitment: &[u8]) -> Result<Option<R>, StorageError> {
        match self.storage.get(COL_RECORDS, record_commitment)? {
            Some(record_bytes) => {
                let record: R = FromBytes::read_le(&record_bytes[..])?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Get a transaction bytes given the transaction id.
    pub fn store_record<R: RecordScheme>(&self, record: &R) -> Result<(), StorageError> {
        let mut database_transaction = DatabaseTransaction::new();

        database_transaction.push(Op::Insert {
            col: COL_RECORDS,
            key: to_bytes_le![record.commitment()]?.to_vec(),
            value: to_bytes_le![record]?.to_vec(),
        });

        self.storage.batch(database_transaction)
    }

    /// Get a transaction bytes given the transaction id.
    pub fn store_records<R: RecordScheme>(&self, records: &[R]) -> Result<(), StorageError> {
        let mut database_transaction = DatabaseTransaction::new();

        for record in records {
            database_transaction.push(Op::Insert {
                col: COL_RECORDS,
                key: to_bytes_le![record.commitment()]?.to_vec(),
                value: to_bytes_le![record]?.to_vec(),
            });
        }

        self.storage.batch(database_transaction)
    }

    /// Removes a record from storage.
    pub fn delete_record<R: RecordScheme>(&self, record: R) -> Result<(), StorageError> {
        let mut database_transaction = DatabaseTransaction::new();

        database_transaction.push(Op::Delete {
            col: COL_RECORDS,
            key: to_bytes_le![record.commitment()]?.to_vec(),
        });

        self.storage.batch(database_transaction)
    }
}
