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

#[derive(Clone, PartialEq, Eq)]
pub struct BlockResponse<N: Network> {
    /// The original block request.
    pub request: BlockRequest,
    /// The blocks.
    pub blocks: Data<DataBlocks<N>>,
}

impl<N: Network> EventTrait for BlockResponse<N> {
    /// Returns the event name.
    #[inline]
    fn name(&self) -> Cow<'static, str> {
        let start = self.request.start_height;
        let end = self.request.end_height;
        match start + 1 == end {
            true => format!("BlockResponse {start}"),
            false => format!("BlockResponse {start}..{end}"),
        }
        .into()
    }
}

impl<N: Network> ToBytes for BlockResponse<N> {
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        self.request.write_le(&mut writer)?;
        self.blocks.write_le(&mut writer)
    }
}

impl<N: Network> FromBytes for BlockResponse<N> {
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        let request = BlockRequest::read_le(&mut reader)?;
        let blocks = Data::read_le(&mut reader)?;

        Ok(Self { request, blocks })
    }
}

impl<N: Network> std::fmt::Debug for BlockResponse<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// A wrapper for a list of blocks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DataBlocks<N: Network>(pub Vec<Block<N>>);

impl<N: Network> DataBlocks<N> {
    /// The maximum number of blocks that can be sent in a single message.
    pub const MAXIMUM_NUMBER_OF_BLOCKS: u8 = 5;

    /// Ensures that the blocks are well-formed in a block response.
    pub fn ensure_response_is_well_formed(
        &self,
        peer_ip: SocketAddr,
        start_height: u32,
        end_height: u32,
    ) -> Result<()> {
        // Ensure the blocks are not empty.
        ensure!(!self.0.is_empty(), "Peer '{peer_ip}' sent an empty block response ({start_height}..{end_height})");
        // Check that the blocks are sequentially ordered.
        if !self.0.windows(2).all(|w| w[0].height() + 1 == w[1].height()) {
            bail!("Peer '{peer_ip}' sent an invalid block response (blocks are not sequentially ordered)")
        }

        // Retrieve the start (inclusive) and end (exclusive) block height.
        let candidate_start_height = self.first().map(|b| b.height()).unwrap_or(0);
        let candidate_end_height = 1 + self.last().map(|b| b.height()).unwrap_or(0);
        // Check that the range matches the block request.
        if start_height != candidate_start_height || end_height != candidate_end_height {
            bail!("Peer '{peer_ip}' sent an invalid block response (range does not match block request)")
        }
        Ok(())
    }
}

impl<N: Network> std::ops::Deref for DataBlocks<N> {
    type Target = Vec<Block<N>>;

    /// Returns the list of blocks.
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<N: Network> ToBytes for DataBlocks<N> {
    /// Writes the blocks to the given writer.
    #[inline]
    fn write_le<W: Write>(&self, mut writer: W) -> IoResult<()> {
        // Prepare the number of blocks.
        let num_blocks = self.0.len() as u8;
        // Ensure that the number of blocks is within the allowed range.
        if num_blocks > Self::MAXIMUM_NUMBER_OF_BLOCKS {
            return Err(error("Block response exceeds maximum number of blocks"));
        }
        // Write the number of blocks.
        num_blocks.write_le(&mut writer)?;
        // Write the blocks.
        self.0.iter().take(num_blocks as usize).try_for_each(|block| block.write_le(&mut writer))
    }
}

impl<N: Network> FromBytes for DataBlocks<N> {
    /// Reads the message from the given reader.
    #[inline]
    fn read_le<R: Read>(mut reader: R) -> IoResult<Self> {
        // Read the number of blocks.
        let num_blocks = u8::read_le(&mut reader)?;
        // Ensure that the number of blocks is within the allowed range.
        if num_blocks > Self::MAXIMUM_NUMBER_OF_BLOCKS {
            return Err(error("Block response exceeds maximum number of blocks"));
        }
        // Read the blocks.
        let blocks = (0..num_blocks).map(|_| Block::read_le(&mut reader)).collect::<Result<Vec<_>, _>>()?;
        Ok(Self(blocks))
    }
}

#[cfg(test)]
pub mod prop_tests {
    use crate::{block_request::prop_tests::any_block_request, BlockResponse, DataBlocks};
    use snarkvm::{
        ledger::ledger_test_helpers::sample_genesis_block,
        prelude::{block::Block, narwhal::Data, FromBytes, TestRng, ToBytes},
    };

    use bytes::{Buf, BufMut, BytesMut};
    use proptest::{
        collection::vec,
        prelude::{any, BoxedStrategy, Strategy},
    };
    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    pub fn any_block() -> BoxedStrategy<Block<CurrentNetwork>> {
        any::<u64>().prop_map(|seed| sample_genesis_block(&mut TestRng::fixed(seed))).boxed()
    }

    pub fn any_data_blocks() -> BoxedStrategy<DataBlocks<CurrentNetwork>> {
        vec(any_block(), 0..=1).prop_map(DataBlocks).boxed()
    }

    pub fn any_block_response() -> BoxedStrategy<BlockResponse<CurrentNetwork>> {
        (any_block_request(), any_data_blocks())
            .prop_map(|(request, data_blocks)| BlockResponse { request, blocks: Data::Object(data_blocks) })
            .boxed()
    }

    #[proptest]
    fn block_response_roundtrip(#[strategy(any_block_response())] block_response: BlockResponse<CurrentNetwork>) {
        let mut bytes = BytesMut::default().writer();
        block_response.write_le(&mut bytes).unwrap();
        let decoded = BlockResponse::<CurrentNetwork>::read_le(&mut bytes.into_inner().reader()).unwrap();
        assert_eq!(block_response.request, decoded.request);
        assert_eq!(
            block_response.blocks.deserialize_blocking().unwrap(),
            decoded.blocks.deserialize_blocking().unwrap(),
        );
    }
}
