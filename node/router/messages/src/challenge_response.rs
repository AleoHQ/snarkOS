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
pub struct ChallengeResponse<N: Network> {
    pub genesis_header: Header<N>,
    pub signature: Data<Signature<N>>,
    pub nonce: u64,
}

impl<N: Network> MessageTrait for ChallengeResponse<N> {
    /// Returns the message name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        "ChallengeResponse".into()
    }
}

impl<N: Network> ToBytes for ChallengeResponse<N> {
    fn write_le<W: io::Write>(&self, mut writer: W) -> io::Result<()> {
        self.genesis_header.write_le(&mut writer)?;
        self.signature.write_le(&mut writer)?;
        self.nonce.write_le(&mut writer)
    }
}

impl<N: Network> FromBytes for ChallengeResponse<N> {
    fn read_le<R: io::Read>(mut reader: R) -> io::Result<Self> {
        Ok(Self {
            genesis_header: Header::read_le(&mut reader)?,
            signature: Data::read_le(&mut reader)?,
            nonce: u64::read_le(reader)?,
        })
    }
}

#[cfg(test)]
pub mod prop_tests {
    use crate::ChallengeResponse;
    use snarkvm::{
        console::prelude::{FromBytes, ToBytes},
        ledger::narwhal::Data,
        prelude::{block::Header, PrivateKey, Signature},
        utilities::rand::{TestRng, Uniform},
    };

    use crate::block_response::prop_tests;
    use bytes::{Buf, BufMut, BytesMut};
    use proptest::prelude::{any, BoxedStrategy, Strategy};
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_signature() -> BoxedStrategy<Signature<CurrentNetwork>> {
        (any::<u64>(), 0..64)
            .prop_map(|(seed, message_size)| {
                let rng = &mut TestRng::from_seed(seed);
                let message: Vec<_> = (0..message_size).map(|_| Uniform::rand(rng)).collect();
                let private_key = PrivateKey::new(rng).unwrap();
                Signature::sign(&private_key, &message, rng).unwrap()
            })
            .boxed()
    }

    pub fn any_genesis_header() -> BoxedStrategy<Header<CurrentNetwork>> {
        any::<u64>().prop_map(|seed| *prop_tests::sample_genesis_block(&mut TestRng::from_seed(seed)).header()).boxed()
    }

    pub fn any_challenge_response() -> BoxedStrategy<ChallengeResponse<CurrentNetwork>> {
        (any_signature(), any_genesis_header(), any::<u64>())
            .prop_map(|(sig, genesis_header, nonce)| ChallengeResponse {
                signature: Data::Object(sig),
                genesis_header,
                nonce,
            })
            .boxed()
    }

    #[proptest]
    fn challenge_response_roundtrip(#[strategy(any_challenge_response())] original: ChallengeResponse<CurrentNetwork>) {
        let mut buf = BytesMut::default().writer();
        ChallengeResponse::write_le(&original, &mut buf).unwrap();

        let deserialized: ChallengeResponse<CurrentNetwork> =
            ChallengeResponse::read_le(buf.into_inner().reader()).unwrap();

        assert_eq!(original.genesis_header, deserialized.genesis_header);
        assert_eq!(
            original.signature.deserialize_blocking().unwrap(),
            deserialized.signature.deserialize_blocking().unwrap()
        );
    }
}
