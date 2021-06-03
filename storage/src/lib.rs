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

pub mod ledger;
pub use ledger::*;

pub mod storage;
pub use storage::*;

pub mod key_value;
pub use key_value::*;

pub mod objects;
pub use objects::*;

pub mod mem;
#[cfg(feature = "mem_storage")]
pub use mem::MemDb as LedgerStorage;

#[cfg(feature = "rocksdb_storage")]
mod rocks;
#[cfg(feature = "rocksdb_storage")]
pub use rocks::RocksDb as LedgerStorage;
