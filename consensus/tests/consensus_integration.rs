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

mod consensus_integration {
    use snarkos_consensus::miner::Miner;
    use snarkos_testing::sync::*;
    use snarkvm_dpc::{block::Transactions, testnet1::instantiated::Testnet1Transaction, BlockHeader};
    use snarkvm_posw::txids_to_roots;

    use std::sync::Arc;

    // this test ensures that a block is found by running the proof of work
    // and that it doesnt loop forever
    fn test_find_block(transactions: &Transactions<Testnet1Transaction>, parent_header: &BlockHeader) {
        let consensus = Arc::new(snarkos_testing::sync::create_test_consensus());
        let miner_address = FIXTURE_VK.test_accounts[0].address.clone();
        let miner = Miner::new(miner_address, consensus.clone());

        let header = miner.find_block(transactions, parent_header).unwrap();

        let expected_prev_block_hash = parent_header.get_hash();
        assert_eq!(header.previous_block_hash, expected_prev_block_hash);

        let expected_merkle_root_hash = snarkvm_dpc::merkle_root(&transactions.to_transaction_ids().unwrap());
        assert_eq!(&header.merkle_root_hash.0[..], &expected_merkle_root_hash[..]);

        // generate the verifier args
        let (merkle_root, pedersen_merkle_root, _) = txids_to_roots(&transactions.to_transaction_ids().unwrap());

        // ensure that our POSW proof passes
        consensus
            .parameters
            .verify_header(&header, parent_header, &merkle_root, &pedersen_merkle_root)
            .unwrap();
    }

    #[tokio::test]
    async fn find_valid_block() {
        let transactions = Transactions(vec![
            DATA.block_1.transactions.0[0].clone(),
            DATA.block_2.transactions.0[0].clone(),
        ]);
        let parent_header = genesis().header;
        test_find_block(&transactions, &parent_header);
    }
}
