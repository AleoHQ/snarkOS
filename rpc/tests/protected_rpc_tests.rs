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

/// Tests for protected RPC endpoints
mod protected_rpc_tests {
    use snarkos_consensus::{Consensus, MerkleTreeLedger};
    use snarkos_network::Node;
    use snarkos_rpc::*;
    use snarkos_storage::LedgerStorage;
    use snarkos_testing::{
        network::{test_config, ConsensusSetup, TestSetup},
        sync::*,
    };

    use snarkvm_dpc::{
        testnet1::{
            instantiated::{Components, Testnet1Transaction},
            record::Record as DPCRecord,
            TransactionKernel,
        },
        Address,
        PrivateKey,
        RecordScheme,
        ViewKey,
    };
    use snarkvm_utilities::{
        bytes::{FromBytes, ToBytes},
        to_bytes_le,
    };

    use jsonrpc_core::MetaIoHandler;
    use serde_json::Value;
    use std::{str::FromStr, sync::Arc, time::Duration};

    const TEST_USERNAME: &str = "TEST_USERNAME";
    const TEST_PASSWORD: &str = "TEST_PASSWORD";

    fn invalid_authentication() -> Meta {
        let basic_auth_encoding = format!(
            "Basic {}",
            base64::encode(format!("{}:{}", "INVALID_USERNAME", "INVALID_PASSWORD"))
        );

        Meta {
            auth: Some(basic_auth_encoding),
        }
    }

    fn authentication() -> Meta {
        let basic_auth_encoding = format!(
            "Basic {}",
            base64::encode(format!("{}:{}", TEST_USERNAME, TEST_PASSWORD))
        );

        Meta {
            auth: Some(basic_auth_encoding),
        }
    }

    async fn initialize_test_rpc(
        ledger: Arc<MerkleTreeLedger<LedgerStorage>>,
    ) -> (MetaIoHandler<Meta>, Arc<Consensus<LedgerStorage>>) {
        let credentials = RpcCredentials {
            username: TEST_USERNAME.to_string(),
            password: TEST_PASSWORD.to_string(),
        };

        let environment = test_config(TestSetup::default());
        let mut node = Node::new(environment).await.unwrap();
        let consensus_setup = ConsensusSetup::default();
        let consensus = Arc::new(snarkos_testing::sync::create_test_consensus_from_ledger(ledger.clone()));

        let node_consensus = snarkos_network::Sync::new(
            consensus.clone(),
            consensus_setup.is_miner,
            Duration::from_secs(consensus_setup.block_sync_interval),
            Duration::from_secs(consensus_setup.tx_sync_interval),
        );

        node.set_sync(node_consensus);

        let rpc_impl = RpcImpl::new(ledger, Some(credentials), node);
        let mut io = jsonrpc_core::MetaIoHandler::default();

        rpc_impl.add_protected(&mut io);

        (io, consensus)
    }

    #[tokio::test]
    async fn test_rpc_authentication() {
        let storage = Arc::new(FIXTURE_VK.ledger());
        let meta = invalid_authentication();
        let (rpc, _consensus) = initialize_test_rpc(storage).await;

        let method = "getrecordcommitments".to_string();
        let request = format!("{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\" }}", method);
        let response = rpc.handle_request_sync(&request, meta).unwrap();

        let extracted: Value = serde_json::from_str(&response).unwrap();

        let expected_result = Value::String("Authentication Error".to_string());
        assert_eq!(extracted["error"]["message"], expected_result);
    }

    #[tokio::test]
    async fn test_rpc_fetch_record_commitment_count() {
        let storage = Arc::new(FIXTURE_VK.ledger());
        storage.store_record(&DATA.records_1[0]).unwrap();

        let meta = authentication();
        let (rpc, _consensus) = initialize_test_rpc(storage).await;

        let method = "getrecordcommitmentcount".to_string();
        let request = format!("{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\" }}", method);
        let response = rpc.handle_request_sync(&request, meta).unwrap();

        let extracted: Value = serde_json::from_str(&response).unwrap();

        assert_eq!(extracted["result"], 1);
    }

    #[tokio::test]
    async fn test_rpc_fetch_record_commitments() {
        let storage = Arc::new(FIXTURE_VK.ledger());
        storage.store_record(&DATA.records_1[0]).unwrap();

        let meta = authentication();
        let (rpc, _consensus) = initialize_test_rpc(storage).await;

        let method = "getrecordcommitments".to_string();
        let request = format!("{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\" }}", method);
        let response = rpc.handle_request_sync(&request, meta).unwrap();

        let extracted: Value = serde_json::from_str(&response).unwrap();

        let expected_result = Value::Array(vec![Value::String(hex::encode(
            to_bytes_le![DATA.records_1[0].commitment()].unwrap(),
        ))]);

        assert_eq!(extracted["result"], expected_result);
    }

    #[tokio::test]
    async fn test_rpc_get_raw_record() {
        let storage = Arc::new(FIXTURE_VK.ledger());
        storage.store_record(&DATA.records_1[0]).unwrap();

        let meta = authentication();
        let (rpc, _consensus) = initialize_test_rpc(storage).await;

        let method = "getrawrecord".to_string();
        let params = hex::encode(to_bytes_le![DATA.records_1[0].commitment()].unwrap());
        let request = format!(
            "{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\", \"params\": [\"{}\"] }}",
            method, params
        );
        let response = rpc.handle_request_sync(&request, meta).unwrap();

        let extracted: Value = serde_json::from_str(&response).unwrap();

        let expected_result = Value::String(hex::encode(to_bytes_le![DATA.records_1[0]].unwrap()));

        assert_eq!(extracted["result"], expected_result);
    }

    #[tokio::test]
    async fn test_rpc_decode_record() {
        let storage = Arc::new(FIXTURE_VK.ledger());
        let meta = authentication();
        let (rpc, _consensus) = initialize_test_rpc(storage).await;

        let record = &DATA.records_1[0];

        let method = "decoderecord";
        let params = hex::encode(to_bytes_le![record].unwrap());
        let request = format!(
            "{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\", \"params\": [\"{}\"] }}",
            method, params
        );

        let response = rpc.handle_request_sync(&request, meta).unwrap();

        let record_info: Value = serde_json::from_str(&response).unwrap();

        let record_info = record_info["result"].clone();

        let owner = record.owner().to_string();
        let is_dummy = record.is_dummy();
        let value = record.value();
        let birth_program_id = hex::encode(to_bytes_le![record.birth_program_id()].unwrap());
        let death_program_id = hex::encode(to_bytes_le![record.death_program_id()].unwrap());
        let serial_number_nonce = hex::encode(to_bytes_le![record.serial_number_nonce()].unwrap());
        let commitment = hex::encode(to_bytes_le![record.commitment()].unwrap());
        let commitment_randomness = hex::encode(to_bytes_le![record.commitment_randomness()].unwrap());

        assert_eq!(owner, record_info["owner"]);
        assert_eq!(is_dummy, record_info["is_dummy"]);
        assert_eq!(value, record_info["value"]);
        assert_eq!(birth_program_id, record_info["birth_program_id"]);
        assert_eq!(death_program_id, record_info["death_program_id"]);
        assert_eq!(serial_number_nonce, record_info["serial_number_nonce"]);
        assert_eq!(commitment, record_info["commitment"]);
        assert_eq!(commitment_randomness, record_info["commitment_randomness"]);
    }

    #[tokio::test]
    async fn test_rpc_decrypt_record() {
        let storage = Arc::new(FIXTURE_VK.ledger());
        let meta = authentication();
        let (rpc, _consensus) = initialize_test_rpc(storage).await;

        let system_parameters = &FIXTURE_VK.dpc.system_parameters;
        let [miner_acc, _, _] = FIXTURE_VK.test_accounts.clone();

        let transaction = Testnet1Transaction::read_le(&TRANSACTION_1[..]).unwrap();
        let ciphertexts = transaction.encrypted_records;

        let records = &DATA.records_1;

        let view_key = ViewKey::from_private_key(
            &system_parameters.account_signature,
            &system_parameters.account_commitment,
            &miner_acc.private_key,
        )
        .unwrap();

        for (ciphertext, record) in ciphertexts.iter().zip(records) {
            let ciphertext_string = hex::encode(to_bytes_le![ciphertext].unwrap());
            let account_view_key = view_key.to_string();

            let params = DecryptRecordInput {
                encrypted_record: ciphertext_string,
                account_view_key,
            };
            let params = serde_json::to_value(params).unwrap();

            let method = "decryptrecord";
            let request = format!(
                "{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\", \"params\": [{}] }}",
                method, params
            );
            let response = rpc.handle_request_sync(&request, meta.clone()).unwrap();

            let extracted: Value = serde_json::from_str(&response).unwrap();

            let expected_result = Value::String(hex::encode(to_bytes_le![record].unwrap()).to_string());
            assert_eq!(extracted["result"], expected_result);
        }
    }

    #[tokio::test]
    async fn test_rpc_create_raw_transaction() {
        let storage = Arc::new(FIXTURE.ledger());
        let meta = authentication();

        let (rpc, consensus) = initialize_test_rpc(storage).await;

        consensus.receive_block(&DATA.block_1, false).await.unwrap();

        let method = "createrawtransaction".to_string();

        let [sender, receiver, _] = &FIXTURE_VK.test_accounts;

        let old_records = vec![hex::encode(to_bytes_le![DATA.records_1[0]].unwrap())];
        let old_account_private_keys = vec![sender.private_key.to_string()];

        let recipients = vec![TransactionRecipient {
            address: receiver.address.to_string(),
            amount: 100,
        }];

        let network_id = 0;

        let params = TransactionInputs {
            old_records,
            old_account_private_keys,
            recipients,
            memo: None,
            network_id,
        };

        let params = serde_json::to_value(params).unwrap();
        let request = format!(
            "{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\", \"params\": [{}] }}",
            method, params
        );
        let response = rpc.handle_request_sync(&request, meta).unwrap();

        let extracted: Value = serde_json::from_str(&response).unwrap();

        let result = extracted["result"].clone();

        for record_value in result["encoded_records"].as_array().unwrap() {
            let record_bytes = hex::decode(record_value.as_str().unwrap()).unwrap();
            let _record: DPCRecord<Components> = FromBytes::read_le(&record_bytes[..]).unwrap();
        }

        let transaction_string = result["encoded_transaction"].as_str().unwrap();
        let transaction_bytes = hex::decode(transaction_string).unwrap();
        let _transaction: Testnet1Transaction = FromBytes::read_le(&transaction_bytes[..]).unwrap();
    }

    #[tokio::test]
    async fn test_rpc_create_transaction_kernel() {
        let storage = Arc::new(FIXTURE_VK.ledger());
        let meta = authentication();

        let (rpc, consensus) = initialize_test_rpc(storage).await;

        consensus.receive_block(&DATA.block_1, false).await.unwrap();

        let method = "createtransactionkernel".to_string();

        let [sender, receiver, _] = &FIXTURE_VK.test_accounts;

        let old_records = vec![hex::encode(to_bytes_le![DATA.records_1[0]].unwrap())];
        let old_account_private_keys = vec![sender.private_key.to_string()];

        let recipients = vec![TransactionRecipient {
            address: receiver.address.to_string(),
            amount: 100,
        }];

        let network_id = 0;

        let params = TransactionInputs {
            old_records,
            old_account_private_keys,
            recipients,
            memo: None,
            network_id,
        };

        let params = serde_json::to_value(params).unwrap();
        let request = format!(
            "{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\", \"params\": [{}] }}",
            method, params
        );
        let response = rpc.handle_request_sync(&request, meta).unwrap();

        let extracted: Value = serde_json::from_str(&response).unwrap();

        let result = extracted["result"].clone();

        let transaction_kernel_bytes = hex::decode(result.as_str().unwrap()).unwrap();
        let _transaction_kernel: TransactionKernel<Components> =
            FromBytes::read_le(&transaction_kernel_bytes[..]).unwrap();
    }

    #[tokio::test]
    async fn test_rpc_create_transaction() {
        let storage = Arc::new(FIXTURE_VK.ledger());
        let meta = authentication();

        let (rpc, consensus) = initialize_test_rpc(storage).await;

        consensus.receive_block(&DATA.block_1, false).await.unwrap();

        let method = "createtransaction".to_string();

        // TODO (raychu86): Generate the transaction kernel with the test data.
        let private_keys = FIXTURE_VK.test_accounts.iter().map(|acc| &acc.private_key);
        let mut private_keys_str = String::from("[");
        for pk in private_keys.take(2) {
            private_keys_str.push_str(&format!("\"{}\",", pk));
        }
        private_keys_str.pop();
        private_keys_str.push(']');
        let transaction_kernel = "9121c47a5e10158beb772e62ff52bfc6c4ab9ff554af3e5201ab50dea2e1de5529e99121c47a5e10158beb772e62ff52bfc6c4ab9ff554af3e5201ab50dea2e1de5529e9ae7c26acc4e65698fc71ec04d6b55554e920d55a71903396b5f395f2cf4242010080d1f008000000000000000000000000000000000000000000000000000000000000000000000000304e7ae3ef9577877ddcef8f8c5d9b5e3bf544c78c50c51213857f35c33c3502df12f0fb72a0d7c56ccd31a87dada92b00304e7ae3ef9577877ddcef8f8c5d9b5e3bf544c78c50c51213857f35c33c3502df12f0fb72a0d7c56ccd31a87dada92b00d0cb5667bcd6d6f1c82c997e900549632a0e411cf83e396953dcad27b4cdf105b8e07d3830a65bb0f160cd1c04c411abdb130ba71c2a31e7b4e853b57923190bbd60a387eafb1e5bbe5c734ad77b9ea41ff7181d2f507e6af54eac782e844d02ae7c26acc4e65698fc71ec04d6b55554e920d55a71903396b5f395f2cf4242010100000000000000000000000000000000000000000000000000000000000000000000000000000000304e7ae3ef9577877ddcef8f8c5d9b5e3bf544c78c50c51213857f35c33c3502df12f0fb72a0d7c56ccd31a87dada92b00304e7ae3ef9577877ddcef8f8c5d9b5e3bf544c78c50c51213857f35c33c3502df12f0fb72a0d7c56ccd31a87dada92b007880d843efe092113a53b0603730ffc5d216ac3dacaf57419f139de08c59a710752f792c3a1136ba427e542e12766aa0e0465ca9543047cf875ba497bda3910972bb5d24db287e3bcdbb7ab2421752a50a741de0838d02479f7f6c3c41b95c0248c11ff2a55deabbd980c41703cd52217c290e70a56cfe2f64c19db4b87a2702e6476fd0b35601def2831bfbcb2a34bbe542e0a415f33d2ee318c408ea536908111e052258ba62d26ae8647b3e5707dc96390a867836b0a6723b3462cb295b060c6e64d43adf69992cccc82f8bc7e35772c9c37361fc7fb3063d6c431db9680e203d0dcb32884ef33c68dbd476a5d2cfd9b073ff85aa3141b8e8df2d985431fcc5205074d9c788b913d3a43fceebf87da1f018373ce27b8ba201fadd75c8181c2689f5065e80dc1cfe0ebb99dd4e08b538fba5dafa4f5e41f6aff7cf62f3fe8ca9120064000000000000000000000000000000000000000000000000000000000000000000000000000000304e7ae3ef9577877ddcef8f8c5d9b5e3bf544c78c50c51213857f35c33c3502df12f0fb72a0d7c56ccd31a87dada92b00304e7ae3ef9577877ddcef8f8c5d9b5e3bf544c78c50c51213857f35c33c3502df12f0fb72a0d7c56ccd31a87dada92b00bce776ac9acee7adc32b21a683a1eaf84b71c0dbc35cb7921330b89e8dbe10091236d7779ce0c71ad6fa9287630459bc0d8762d5bec61e728c6ebb9b7b7aed03443cedc72404c1a4a9de83eb6d87dcadaced740125c2c6c0c1ea3e8ad1b1af02f5065e80dc1cfe0ebb99dd4e08b538fba5dafa4f5e41f6aff7cf62f3fe8ca9120100000000000000000000000000000000000000000000000000000000000000000000000000000000304e7ae3ef9577877ddcef8f8c5d9b5e3bf544c78c50c51213857f35c33c3502df12f0fb72a0d7c56ccd31a87dada92b00304e7ae3ef9577877ddcef8f8c5d9b5e3bf544c78c50c51213857f35c33c3502df12f0fb72a0d7c56ccd31a87dada92b00077b4d1741a73df55548268386aef4710ce67287c60731ba303d914ca211470c7f5410f05c7ad6bdf7c74b8b8b14540095bcc2ad7eec8f54b4f7e0e0cd497f062020009e5061796d5514c743ef0a605e708744f20e7989e1dde9a1cf120163006dacc3209132ba1c93736ddeb4ac392d2fe81235481a4d25cdc6d15ce0125be1196c62028a95b415680afcd74a86db60d314d0c7285a0327b113b5f4ad781ba81236d7779ce0c71ad6fa9287630459bc0d8762d5bec61e728c6ebb9b7b7aed037f5410f05c7ad6bdf7c74b8b8b14540095bcc2ad7eec8f54b4f7e0e0cd497f065576d9509e54af937b321c90f82cd135ce2ed8512816786f45016567d9a6de031e2b8a9cbb9eac2d545efb86a957f10500a27f3658fb2a7d023fc586f6a0670208c14e0b55a425a291c630d54a963a7abb294091b4580d4db4639d6d1428fdd40b222dc2d814e615b34f491567175d2fdbbfb72b0a44817a3bf1249c88b2a76111a9aac8fa9c3ba198338941bc090fdba63307906e539a1c52ca65affc560aa708b80f495ee5661f18d3852f1c3076c19857fafd65871cf9f0d70a61fbf9150e11d1470eb9c2f79fc5cfd1c29b3e5685a6b265fd9790305a972866f2506025300b566529d9d13ef66b33a08b531a04f48839baa331ce6aab747c8671b034ee0b11408d6503a4dee856e4e5dc4ebce29a34f525e5b9d688cd2bf399699400e1f80bf85d45b836a5dd75beb9eb0c404649c39ed52a44fcfc171736168d5a60ad29002f00080f92edf44cc99bd58dd39d86c3902495dd512a4c88ceeeee9734898c2c1fab08f4ec98da0d065f24abed6f401b4533033497234ec9e2c05f9805415e7f239207fc62eb95926006335d9d57d83fff8f01290d64291f98b3aa0093356ec78b140e8f79ad61278ee1617136d8f5b0a29919a91058f2bab3567d6cbf5c820e9e270ff5d1c9190f92888197fabbc0a32f72119c4e5ed73da3b20475a1ced19b947802ccb16639da79dba98ba7d5a9c43ecd2ef7f62e752d04a8dcf9aed97d6349390ee43ca66a368fc0d90ab67198650c6dee2180ae42e17cb1cf11b9f25e11a60c058db18b85fe9611bcc7bbe342b22fabec844caaeb2b7a6db532f0d6b7889bdf012f0041caa6b0118105026a8b9b0638fba26ed0bc765fb87788daa2c4c2e7aac19f07768994f8975c43cc81837d5d0779973dd785ac6c072358d609c8e04a3af5f5104deb04779c5cf79f03f1e502d7addb3aed90ec249ecc84eb47784f87a5cfd671a3585cc1bf15969e4ef1aa5da31c55ad718987b6837946eea5a5f828104ef1abf4ac6722b2305cf8c2699b9eff8d6985204091d87474757092eaa8f813ee0a0103c6de6fb653b2df5168fc3703bf5132520412e0d35923dbac287e068b2ced0921b535a58973c7eb3ba82fc9d2d37ede6b4ba67171bcec83f28a2e8dec82501232ee8dee640c86e45c414817aab9d7d677c12d7ee81b783e5ce3832727f11606e159384ff072d92b2571308ec1a0addfcbcd223bcd4023a1d1c2bc91c839900fd1b1e1dc122c5a8bdeaf354a53a9ec798d95f21827741a7364f70d1416eeff004b9c16672d56716afa2031ba07c9e9f16f02d102e03a0eca600d752146ccfc021cb5138791e3a8a00970e511166d2cdfe33a914f1a379d05589c646eb6b2e801578c3111f9570faa6140050edccbabe1f19a0188d3f27da248790d976def1e01adc7a7f72d8f0bb56d33017061ef5f79e37a33d2514ff27b28e7859a02747202057b92e63d7b5ecf0fde4f3e489beeb250e08397127edf67305a1c873cd91a001cd1f0080000000034065f3d07646686e983ac8d260a6d28476a87d3adbb1e90260cf40614e5151300";

        let request = format!(
            "{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\", \"params\": [{}, \"{}\"] }}",
            method, private_keys_str, transaction_kernel
        );
        let response = rpc.handle_request_sync(&request, meta).unwrap();

        println!("extracted: {}", response);

        let extracted: Value = serde_json::from_str(&response).unwrap();

        println!("extracted: {}", extracted);

        let result = extracted["result"].clone();

        for record_value in result["encoded_records"].as_array().unwrap() {
            let record_bytes = hex::decode(record_value.as_str().unwrap()).unwrap();
            let _record: DPCRecord<Components> = FromBytes::read_le(&record_bytes[..]).unwrap();
        }

        let transaction_string = result["encoded_transaction"].as_str().unwrap();
        let transaction_bytes = hex::decode(transaction_string).unwrap();
        let _transaction: Testnet1Transaction = FromBytes::read_le(&transaction_bytes[..]).unwrap();
    }

    #[tokio::test]
    async fn test_create_account() {
        let storage = Arc::new(FIXTURE_VK.ledger());
        let meta = authentication();
        let (rpc, _consensus) = initialize_test_rpc(storage).await;

        let method = "createaccount".to_string();

        let request = format!("{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\" }}", method);
        let response = rpc.handle_request_sync(&request, meta.clone()).unwrap();

        let extracted: Value = serde_json::from_str(&response).unwrap();

        let account: RpcAccount = serde_json::from_value(extracted["result"].clone()).unwrap();

        let _private_key = PrivateKey::<Components>::from_str(&account.private_key).unwrap();
        let _address = Address::<Components>::from_str(&account.address).unwrap();

        let request = format!("{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\" }}", method);
        let response = rpc.handle_request_sync(&request, meta).unwrap();

        let extracted: Value = serde_json::from_str(&response).unwrap();

        let account: RpcAccount = serde_json::from_value(extracted["result"].clone()).unwrap();

        let _private_key = PrivateKey::<Components>::from_str(&account.private_key).unwrap();
        let _address = Address::<Components>::from_str(&account.address).unwrap();
    }
}
