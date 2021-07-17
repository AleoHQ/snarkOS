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

mod miner {
    use snarkos_consensus::Miner;
    use snarkos_testing::sync::*;
    use snarkvm_algorithms::traits::{
        commitment::CommitmentScheme,
        encryption::EncryptionScheme,
        signature::SignatureScheme,
    };
    use snarkvm_dpc::{block::Transactions, Address, BlockHeader, DPCComponents, PrivateKey};
    use snarkvm_posw::txids_to_roots;

    use rand::{CryptoRng, Rng, SeedableRng};
    use rand_chacha::ChaChaRng;

    use std::sync::Arc;

    fn keygen<C: DPCComponents, R: Rng + CryptoRng>(rng: &mut R) -> (PrivateKey<C>, Address<C>) {
        let sig_params = C::AccountSignature::setup(rng).unwrap();
        let comm_params = C::AccountCommitment::setup(rng);
        let enc_params = <C::AccountEncryption as EncryptionScheme>::setup(rng);

        let private_key = PrivateKey::<C>::new(&sig_params, &comm_params, rng).unwrap();
        let address = Address::from_private_key(&sig_params, &comm_params, &enc_params, &private_key).unwrap();

        (private_key, address)
    }

    // this test ensures that a block is found by running the proof of work
    // and that it doesnt loop forever
    fn test_find_block(transactions: &Transactions<TestTestnet1Transaction>, parent_header: &BlockHeader) {
        let consensus = Arc::new(snarkos_testing::sync::create_test_consensus());
        let mut rng = ChaChaRng::seed_from_u64(3); // use this rng so that a valid solution is found quickly

        let (_, miner_address) = keygen(&mut rng);
        let miner = Miner::new(miner_address, consensus.clone());

        let header = miner.find_block(transactions, parent_header).unwrap();

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
        let transactions = Transactions(vec![TestTestnet1Transaction; 3]);
        let parent_header = genesis().header;
        test_find_block(&transactions, &parent_header);
    }
}
