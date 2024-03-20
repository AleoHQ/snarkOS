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

use super::*;

use snarkvm::{
    ledger::narwhal::Data,
    prelude::{FromBytes, ToBytes},
};

use std::borrow::Cow;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnconfirmedTransaction<N: Network> {
    pub transaction_id: N::TransactionID,
    pub transaction: Data<Transaction<N>>,
}

impl<N: Network> From<Transaction<N>> for UnconfirmedTransaction<N> {
    /// Initializes a new `UnconfirmedTransaction` message.
    fn from(transaction: Transaction<N>) -> Self {
        Self { transaction_id: transaction.id(), transaction: Data::Object(transaction) }
    }
}

impl<N: Network> MessageTrait for UnconfirmedTransaction<N> {
    /// Returns the message name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "UnconfirmedTransaction".into()
    }
}

impl<N: Network> ToBytes for UnconfirmedTransaction<N> {
    fn write_le<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        self.transaction_id.write_le(&mut writer)?;
        self.transaction.write_le(&mut writer)?;
        Ok(())
    }
}

impl<N: Network> FromBytes for UnconfirmedTransaction<N> {
    fn read_le<R: io::Read>(mut reader: R) -> io::Result<Self> {
        Ok(Self { transaction_id: N::TransactionID::read_le(&mut reader)?, transaction: Data::read_le(reader)? })
    }
}

#[cfg(test)]
pub mod prop_tests {
    use crate::{Transaction, UnconfirmedTransaction};
    use snarkvm::{
        ledger::{narwhal::Data},
        prelude::{FromBytes, ToBytes},
    };

    use crate::block_response::prop_tests::any_block;
    use bytes::{Buf, BufMut, BytesMut};
    use proptest::{
        prelude::{any, BoxedStrategy, Strategy},
        sample::Selector,
    };
    
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_transaction() -> BoxedStrategy<Transaction<CurrentNetwork>> {
        (any_block(), any::<Selector>())
            .prop_map(|(block, selector)| {
                let tx: Transaction<CurrentNetwork> =
                    selector.select(block.transactions().clone().into_iter().map(|tx| tx.into_transaction()));
                tx
            })
            .boxed()
    }

    pub fn any_unconfirmed_transaction() -> BoxedStrategy<UnconfirmedTransaction<CurrentNetwork>> {
        any_transaction()
            .prop_map(|tx| UnconfirmedTransaction { transaction_id: tx.id(), transaction: Data::Object(tx) })
            .boxed()
    }

    #[proptest]
    fn unconfirmed_transaction_roundtrip(
        #[strategy(any_unconfirmed_transaction())] original: UnconfirmedTransaction<CurrentNetwork>,
    ) {
        let mut buf = BytesMut::default().writer();
        UnconfirmedTransaction::write_le(&original, &mut buf).unwrap();

        let deserialized: UnconfirmedTransaction<CurrentNetwork> =
            UnconfirmedTransaction::read_le(buf.into_inner().reader()).unwrap();
        assert_eq!(original.transaction_id, deserialized.transaction_id);
        assert_eq!(
            original.transaction.deserialize_blocking().unwrap(),
            deserialized.transaction.deserialize_blocking().unwrap(),
        );
    }
}
