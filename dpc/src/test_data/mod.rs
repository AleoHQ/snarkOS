use crate::base_dpc::{
    instantiated::*,
    parameters::PublicParameters,
    record_payload::PaymentRecordPayload,
    BaseDPCComponents,
    DPC,
};
use snarkos_algorithms::merkle_tree::MerkleParameters;
use snarkos_models::{
    dpc::{DPCScheme, Record},
    objects::{AccountScheme, Ledger, Transaction},
};
use snarkos_objects::Account;
use snarkvm_models::algorithms::CRH;
use snarkvm_utilities::{
    bytes::{FromBytes, ToBytes},
    to_bytes,
};

use rand::Rng;
use std::{fs::File, path::PathBuf};

pub struct Wallet {
    pub private_key: &'static str,
    pub public_key: &'static str,
}

pub fn setup_or_load_parameters<R: Rng>(
    verify_only: bool,
    rng: &mut R,
) -> (
    <Components as BaseDPCComponents>::MerkleParameters,
    <InstantiatedDPC as DPCScheme<MerkleTreeLedger>>::Parameters,
) {
    let mut path = std::env::current_dir().unwrap();
    path.push("../dpc/src/parameters/");
    let ledger_parameter_path = path.join("ledger.params");

    fn load_ledger_parameters(ledger_parameter_path: &PathBuf) -> Option<CommitmentMerkleParameters> {
        let mut file = match File::open(ledger_parameter_path) {
            Ok(file) => file,
            Err(_) => return None,
        };

        let crh_params: <<<Components as BaseDPCComponents>::MerkleParameters as MerkleParameters>::H as CRH>::Parameters = match FromBytes::read(&mut file) {
            Ok(crh_parameters) => crh_parameters,
            Err(_) => return None,
        };

        let crh = <<Components as BaseDPCComponents>::MerkleParameters as MerkleParameters>::H::from(crh_params);

        Some(<Components as BaseDPCComponents>::MerkleParameters::from(crh))
    }

    let (ledger_parameters, parameters) = match load_ledger_parameters(&ledger_parameter_path) {
        Some(ledger_parameters) => {
            let parameters =
                match <InstantiatedDPC as DPCScheme<MerkleTreeLedger>>::Parameters::load(&path, verify_only) {
                    Ok(parameters) => parameters,
                    Err(_) => {
                        println!("Parameter Setup");
                        let parameters =
                            <InstantiatedDPC as DPCScheme<MerkleTreeLedger>>::setup(&ledger_parameters, rng)
                                .expect("DPC setup failed");

                        //  parameters.store(&path).unwrap();
                        parameters
                    }
                };

            (ledger_parameters, parameters)
        }
        None => {
            println!("Ledger parameter Setup");
            let ledger_parameters = MerkleTreeLedger::setup(rng).expect("Ledger setup failed");

            println!("Parameter Setup");
            let parameters = <InstantiatedDPC as DPCScheme<MerkleTreeLedger>>::setup(&ledger_parameters, rng)
                .expect("DPC setup failed");

            (ledger_parameters, parameters)
        }
    };

    // Store parameters - Uncomment this to store parameters to the specified paths
    //    let mut file = File::create(ledger_parameter_path).unwrap();
    //    file.write_all(&to_bytes![ledger_parameters.parameters()].unwrap()).unwrap();
    //    drop(file);
    //    parameters.store(&path).unwrap();

    (ledger_parameters, parameters)
}

pub fn load_verifying_parameters() -> PublicParameters<Components> {
    PublicParameters::<Components>::load_vk_direct().unwrap()
}

pub fn generate_test_accounts<R: Rng>(
    parameters: &<InstantiatedDPC as DPCScheme<MerkleTreeLedger>>::Parameters,
    rng: &mut R,
) -> [Account<Components>; 3] {
    let signature_parameters = &parameters.circuit_parameters.signature;
    let commitment_parameters = &parameters.circuit_parameters.account_commitment;

    let genesis_metadata = [1u8; 32];
    let genesis_account = Account::new(signature_parameters, commitment_parameters, &genesis_metadata, rng).unwrap();

    let metadata_1 = [2u8; 32];
    let account_1 = Account::new(signature_parameters, commitment_parameters, &metadata_1, rng).unwrap();

    let metadata_2 = [3u8; 32];
    let account_2 = Account::new(signature_parameters, commitment_parameters, &metadata_2, rng).unwrap();

    [genesis_account, account_1, account_2]
}

pub fn ledger_genesis_setup<R: Rng>(
    parameters: &<InstantiatedDPC as DPCScheme<MerkleTreeLedger>>::Parameters,
    genesis_account: &Account<Components>,
    rng: &mut R,
) -> (
    <Tx as Transaction>::Commitment,
    <Tx as Transaction>::SerialNumber,
    <Tx as Transaction>::Memorandum,
    Vec<u8>,
    Vec<u8>,
) {
    let genesis_sn_nonce =
        SerialNumberNonce::hash(&parameters.circuit_parameters.serial_number_nonce, &[34u8; 1]).unwrap();
    let genesis_predicate_vk_bytes = to_bytes![
        PredicateVerificationKeyHash::hash(
            &parameters.circuit_parameters.predicate_verification_key_hash,
            &to_bytes![parameters.predicate_snark_parameters.verification_key].unwrap()
        )
        .unwrap()
    ]
    .unwrap();

    let genesis_record = DPC::generate_record(
        &parameters.circuit_parameters,
        &genesis_sn_nonce,
        &genesis_account.public_key,
        true, // The inital record should be dummy
        &PaymentRecordPayload::default(),
        &Predicate::new(genesis_predicate_vk_bytes.clone()),
        &Predicate::new(genesis_predicate_vk_bytes.clone()),
        rng,
    )
    .unwrap();

    // Generate serial number for the genesis record.
    let (genesis_sn, _) = DPC::generate_sn(
        &parameters.circuit_parameters,
        &genesis_record,
        &genesis_account.private_key,
    )
    .unwrap();
    let genesis_memo = [0u8; 32];

    (
        genesis_record.commitment(),
        genesis_sn,
        genesis_memo,
        genesis_predicate_vk_bytes.to_vec(),
        to_bytes![genesis_account].unwrap().to_vec(),
    )
}
