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

use snarkvm_algorithms::errors::CRHError;
use snarkvm_dpc::{BlockError, DPCError, ProgramError, RecordError, StorageError, TransactionError};
use snarkvm_posw::error::PoswError;

use std::fmt::Debug;

/// Possible block verification errors
#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error("UTXO has already been spent {:?} index: {:?}", _0, _1)]
    AlreadySpent(Vec<u8>, u32),

    #[error("{}", _0)]
    BlockError(#[from] BlockError),

    #[error("Block is too large: {}. Exceeds {} maximum", _0, _1)]
    BlockTooLarge(usize, usize),

    #[error("A coinbase transaction already exists in the block")]
    CoinbaseTransactionAlreadyExists(),

    #[error("conflicting network ids: {}, {}", _0, _1)]
    ConflictingNetworkId(u8, u8),

    #[error("{}: {}", _0, _1)]
    Crate(&'static str, String),

    #[error("{}", _0)]
    CRHError(#[from] CRHError),

    #[error("wrong difficulty, expected {0} got {1}")]
    DifficultyMismatch(u64, u64),

    #[error("{}", _0)]
    DPCError(#[from] DPCError),

    #[error("timestamp more than 2 hours into the future {:?} actual {:?}", _0, _1)]
    FuturisticTimestamp(i64, i64),

    #[error("invalid block {:?}", _0)]
    InvalidBlock(Vec<u8>),

    #[error("invalid coinbase transaction")]
    InvalidCoinbaseTransaction,

    #[error("block transactions do not hash to merkle root {:?}", _0)]
    MerkleRoot(String),

    #[error("{}", _0)]
    Message(String),

    #[error("the block has multiple coinbase transactions: {:?}", _0)]
    MultipleCoinbaseTransactions(u32),

    #[error("nonce {:?} is greater than nonce limit {:?}", _0, _1)]
    NonceInvalid(u32, u32),

    #[error("all nonces have been tried for the current block header")]
    NonceLimitError,

    #[error("Missing genesis block")]
    NoGenesisBlock,

    #[error("expected {:?} actual {:?}", _0, _1)]
    NoParent(String, String),

    #[error("block subroots do not hash to the pedersen merkle root {0}")]
    PedersenMerkleRoot(String),

    #[error("header greater than difficulty target {:?} actual {:?}", _0, _1)]
    PowInvalid(u64, u64),

    #[error(transparent)]
    PoswError(#[from] PoswError),

    #[error("{}", _0)]
    ProgramError(#[from] ProgramError),

    #[error("{}", _0)]
    RecordError(#[from] RecordError),

    #[error("{}", _0)]
    StorageError(#[from] StorageError),

    #[error("timestamp {:?} is less than parent timestamp {:?}", _0, _1)]
    TimestampInvalid(i64, i64),

    #[error("{}", _0)]
    TransactionError(#[from] TransactionError),

    #[error("Transactions are spending more funds than they have available")]
    TransactionOverspending,

    #[error("The block is already known")]
    PreExistingBlock,
}

impl From<anyhow::Error> for ConsensusError {
    fn from(error: anyhow::Error) -> Self {
        ConsensusError::Crate("anyhow", format!("{:?}", error))
    }
}

impl From<std::io::Error> for ConsensusError {
    fn from(error: std::io::Error) -> Self {
        ConsensusError::Crate("std::io", format!("{:?}", error))
    }
}
