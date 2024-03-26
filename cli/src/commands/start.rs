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

use snarkos_account::Account;
use snarkos_display::Display;
use snarkos_node::{bft::MEMORY_POOL_PORT, router::messages::NodeType, Node};
use snarkvm::{
    console::{
        account::{Address, PrivateKey},
        algorithms::Hash,
        network::{MainnetV0, Network},
    },
    ledger::{
        block::Block,
        committee::{Committee, MIN_DELEGATOR_STAKE, MIN_VALIDATOR_STAKE},
        store::{helpers::memory::ConsensusMemory, ConsensusStore},
    },
    prelude::{FromBytes, ToBits, ToBytes},
    synthesizer::VM,
    utilities::to_bytes_le,
};

use aleo_std::StorageMode;
use anyhow::{bail, ensure, Result};
use clap::Parser;
use colored::Colorize;
use core::str::FromStr;
use indexmap::IndexMap;
use rand::SeedableRng;
use rand_chacha::ChaChaRng;
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};
use tokio::runtime::{self, Runtime};

/// The recommended minimum number of 'open files' limit for a validator.
/// Validators should be able to handle at least 1000 concurrent connections, each requiring 2 sockets.
#[cfg(target_family = "unix")]
const RECOMMENDED_MIN_NOFILES_LIMIT: u64 = 2048;

/// The development mode RNG seed.
const DEVELOPMENT_MODE_RNG_SEED: u64 = 1234567890u64;
/// The development mode number of genesis committee members.
const DEVELOPMENT_MODE_NUM_GENESIS_COMMITTEE_MEMBERS: u16 = 4;

/// A mapping of `staker_address` to `(validator_address, withdrawal_address, amount)`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
struct BondedBalances(IndexMap<String, (String, String, u64)>);

impl FromStr for BondedBalances {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

/// Starts the snarkOS node.
#[derive(Clone, Debug, Parser)]
pub struct Start {
    /// Specify the network ID of this node
    #[clap(default_value = "0", long = "network")]
    pub network: u16,

    /// Specify this node as a validator
    #[clap(long = "validator")]
    pub validator: bool,
    /// Specify this node as a prover
    #[clap(long = "prover")]
    pub prover: bool,
    /// Specify this node as a client
    #[clap(long = "client")]
    pub client: bool,

    /// Specify the account private key of the node
    #[clap(long = "private-key")]
    pub private_key: Option<String>,
    /// Specify the path to a file containing the account private key of the node
    #[clap(long = "private-key-file")]
    pub private_key_file: Option<PathBuf>,

    /// Specify the IP address and port for the node server
    #[clap(long = "node")]
    pub node: Option<SocketAddr>,
    /// Specify the IP address and port for the BFT
    #[clap(long = "bft")]
    pub bft: Option<SocketAddr>,
    /// Specify the IP address and port of the peer(s) to connect to
    #[clap(default_value = "", long = "peers")]
    pub peers: String,
    /// Specify the IP address and port of the validator(s) to connect to
    #[clap(default_value = "", long = "validators")]
    pub validators: String,

    /// Specify the IP address and port for the REST server
    #[clap(default_value = "0.0.0.0:3030", long = "rest")]
    pub rest: SocketAddr,
    /// Specify the requests per second (RPS) rate limit per IP for the REST server
    #[clap(default_value = "10", long = "rest-rps")]
    pub rest_rps: u32,
    /// If the flag is set, the node will not initialize the REST server
    #[clap(long)]
    pub norest: bool,

    /// If the flag is set, the node will not render the display
    #[clap(long)]
    pub nodisplay: bool,
    /// Specify the verbosity of the node [options: 0, 1, 2, 3, 4]
    #[clap(default_value = "1", long = "verbosity")]
    pub verbosity: u8,
    /// Specify the path to the file where logs will be stored
    #[clap(default_value_os_t = std::env::temp_dir().join("snarkos.log"), long = "logfile")]
    pub logfile: PathBuf,
    /// Enables the metrics exporter
    #[clap(default_value = "false", long = "metrics")]
    pub metrics: bool,

    /// Enables the node to prefetch initial blocks from a CDN
    #[clap(default_value = "https://s3.us-west-1.amazonaws.com/testnet3.blocks/phase3", long = "cdn")]
    pub cdn: String,
    /// If the flag is set, the node will not prefetch from a CDN
    #[clap(long)]
    pub nocdn: bool,

    /// Enables development mode, specify a unique ID for this node
    #[clap(long)]
    pub dev: Option<u16>,
    /// If development mode is enabled, specify the number of genesis validators (default: 4)
    #[clap(long)]
    pub dev_num_validators: Option<u16>,
    /// If developtment mode is enabled, specify whether node 0 should generate traffic to drive the network
    #[clap(default_value = "false", long = "no-dev-txs")]
    pub no_dev_txs: bool,
    /// Specify the path to a directory containing the ledger
    #[clap(long = "storage_path")]
    pub storage_path: Option<PathBuf>,

    /// If development mode is enabled, specify the custom bonded balances as a json object. (default: None)
    #[clap(long)]
    dev_bonded_balances: Option<BondedBalances>,

    /// If the flag is set, the validator will allow untrusted peers to connect
    #[clap(long = "allow-external-peers")]
    allow_external_peers: bool,
}

impl Start {
    /// Starts the snarkOS node.
    pub fn parse(self) -> Result<String> {
        // Initialize the logger.
        let log_receiver = crate::helpers::initialize_logger(self.verbosity, self.nodisplay, self.logfile.clone());
        // Initialize the runtime.
        Self::runtime().block_on(async move {
            // Clone the configurations.
            let mut cli = self.clone();
            // Parse the network.
            match cli.network {
                0 => {
                    // Parse the node from the configurations.
                    let node = cli.parse_node::<MainnetV0>().await.expect("Failed to parse the node");
                    // If the display is enabled, render the display.
                    if !cli.nodisplay {
                        // Initialize the display.
                        Display::start(node, log_receiver).expect("Failed to initialize the display");
                    }
                }
                _ => panic!("Invalid network ID specified"),
            };
            // Note: Do not move this. The pending await must be here otherwise
            // other snarkOS commands will not exit.
            std::future::pending::<()>().await;
        });

        Ok(String::new())
    }
}

impl Start {
    /// Returns the initial peer(s) to connect to, from the given configurations.
    fn parse_trusted_peers(&self) -> Result<Vec<SocketAddr>> {
        match self.peers.is_empty() {
            true => Ok(vec![]),
            false => Ok(self
                .peers
                .split(',')
                .flat_map(|ip| match ip.parse::<SocketAddr>() {
                    Ok(ip) => Some(ip),
                    Err(e) => {
                        eprintln!("The IP supplied to --peers ('{ip}') is malformed: {e}");
                        None
                    }
                })
                .collect()),
        }
    }

    /// Returns the initial validator(s) to connect to, from the given configurations.
    fn parse_trusted_validators(&self) -> Result<Vec<SocketAddr>> {
        match self.validators.is_empty() {
            true => Ok(vec![]),
            false => Ok(self
                .validators
                .split(',')
                .flat_map(|ip| match ip.parse::<SocketAddr>() {
                    Ok(ip) => Some(ip),
                    Err(e) => {
                        eprintln!("The IP supplied to --validators ('{ip}') is malformed: {e}");
                        None
                    }
                })
                .collect()),
        }
    }

    /// Returns the CDN to prefetch initial blocks from, from the given configurations.
    fn parse_cdn(&self) -> Option<String> {
        // Determine if the node type is not declared.
        let is_no_node_type = !(self.validator || self.prover || self.client);

        // Disable CDN if:
        //  1. The node is in development mode.
        //  2. The user has explicitly disabled CDN.
        //  3. The node is a prover (no need to sync).
        //  4. The node type is not declared (defaults to client) (no need to sync).
        if self.dev.is_some() || self.cdn.is_empty() || self.nocdn || self.prover || is_no_node_type {
            None
        }
        // Enable the CDN otherwise.
        else {
            Some(self.cdn.clone())
        }
    }

    /// Read the private key directly from an argument or from a filesystem location,
    /// returning the Aleo account.
    fn parse_private_key<N: Network>(&self) -> Result<Account<N>> {
        match self.dev {
            None => match (&self.private_key, &self.private_key_file) {
                // Parse the private key directly.
                (Some(private_key), None) => Account::from_str(private_key.trim()),
                // Parse the private key from a file.
                (None, Some(path)) => {
                    check_permissions(path)?;
                    Account::from_str(std::fs::read_to_string(path)?.trim())
                }
                // Ensure the private key is provided to the CLI, except for clients or nodes in development mode.
                (None, None) => match self.client {
                    true => Account::new(&mut rand::thread_rng()),
                    false => bail!("Missing the '--private-key' or '--private-key-file' argument"),
                },
                // Ensure only one private key flag is provided to the CLI.
                (Some(_), Some(_)) => {
                    bail!("Cannot use '--private-key' and '--private-key-file' simultaneously, please use only one")
                }
            },
            Some(dev) => {
                // Sample the private key of this node.
                Account::try_from({
                    // Initialize the (fixed) RNG.
                    let mut rng = ChaChaRng::seed_from_u64(DEVELOPMENT_MODE_RNG_SEED);
                    // Iterate through 'dev' address instances to match the account.
                    for _ in 0..dev {
                        let _ = PrivateKey::<N>::new(&mut rng)?;
                    }
                    let private_key = PrivateKey::<N>::new(&mut rng)?;
                    println!("🔑 Your development private key for node {dev} is {}.\n", private_key.to_string().bold());
                    private_key
                })
            }
        }
    }

    /// Updates the configurations if the node is in development mode.
    fn parse_development(
        &mut self,
        trusted_peers: &mut Vec<SocketAddr>,
        trusted_validators: &mut Vec<SocketAddr>,
    ) -> Result<()> {
        // If `--dev` is set, assume the dev nodes are initialized from 0 to `dev`,
        // and add each of them to the trusted peers. In addition, set the node IP to `4130 + dev`,
        // and the REST IP to `3030 + dev`.
        if let Some(dev) = self.dev {
            // Add the dev nodes to the trusted peers.
            if trusted_peers.is_empty() {
                for i in 0..dev {
                    if i != dev {
                        trusted_peers.push(SocketAddr::from_str(&format!("127.0.0.1:{}", 4130 + i))?);
                    }
                }
            }
            // Add the dev nodes to the trusted validators.
            if trusted_validators.is_empty() {
                // To avoid ambiguity, we define the first few nodes to be the trusted validators to connect to.
                for i in 0..2 {
                    if i != dev {
                        trusted_validators.push(SocketAddr::from_str(&format!("127.0.0.1:{}", MEMORY_POOL_PORT + i))?);
                    }
                }
            }
            // Set the node IP to `4130 + dev`.
            //
            // Note: the `node` flag is an option to detect remote devnet testing.
            if self.node.is_none() {
                self.node = Some(SocketAddr::from_str(&format!("0.0.0.0:{}", 4130 + dev))?);
            }
            // If the `norest` flag is not set, and the `bft` flag was not overridden,
            // then set the REST IP to `3030 + dev`.
            //
            // Note: the `bft` flag is an option to detect remote devnet testing.
            if !self.norest && self.bft.is_none() {
                self.rest = SocketAddr::from_str(&format!("0.0.0.0:{}", 3030 + dev))?;
            }
        }
        Ok(())
    }

    /// Returns an alternative genesis block if the node is in development mode.
    /// Otherwise, returns the actual genesis block.
    fn parse_genesis<N: Network>(&self) -> Result<Block<N>> {
        if self.dev.is_some() {
            // Determine the number of genesis committee members.
            let num_committee_members = match self.dev_num_validators {
                Some(num_committee_members) => num_committee_members,
                None => DEVELOPMENT_MODE_NUM_GENESIS_COMMITTEE_MEMBERS,
            };
            ensure!(
                num_committee_members >= DEVELOPMENT_MODE_NUM_GENESIS_COMMITTEE_MEMBERS,
                "Number of genesis committee members is too low"
            );

            // Initialize the (fixed) RNG.
            let mut rng = ChaChaRng::seed_from_u64(DEVELOPMENT_MODE_RNG_SEED);
            // Initialize the development private keys.
            let development_private_keys =
                (0..num_committee_members).map(|_| PrivateKey::<N>::new(&mut rng)).collect::<Result<Vec<_>>>()?;
            // Initialize the development addresses.
            let development_addresses =
                development_private_keys.iter().map(Address::<N>::try_from).collect::<Result<Vec<_>>>()?;

            // Construct the committee based on the state of the bonded balances.
            let (committee, bonded_balances) = match &self.dev_bonded_balances {
                Some(bonded_balances) => {
                    // Parse the bonded balances.
                    let bonded_balances = bonded_balances
                        .0
                        .iter()
                        .map(|(staker_address, (validator_address, withdrawal_address, amount))| {
                            let staker_addr = Address::<N>::from_str(staker_address)?;
                            let validator_addr = Address::<N>::from_str(validator_address)?;
                            let withdrawal_addr = Address::<N>::from_str(withdrawal_address)?;
                            Ok((staker_addr, (validator_addr, withdrawal_addr, *amount)))
                        })
                        .collect::<Result<IndexMap<_, _>>>()?;

                    // Construct the committee members.
                    let mut members = IndexMap::new();
                    for (staker_address, (validator_address, _, amount)) in bonded_balances.iter() {
                        // Ensure that the staking amount is sufficient.
                        match staker_address == validator_address {
                            true => ensure!(amount >= &MIN_VALIDATOR_STAKE, "Validator stake is too low"),
                            false => ensure!(amount >= &MIN_DELEGATOR_STAKE, "Delegator stake is too low"),
                        }

                        // Ensure that the validator address is included in the list of development addresses.
                        ensure!(
                            development_addresses.contains(validator_address),
                            "Validator address {validator_address} is not included in the list of development addresses"
                        );

                        // Add or update the validator entry in the list of members.
                        members
                            .entry(*validator_address)
                            .and_modify(|(stake, _)| *stake += amount)
                            .or_insert((*amount, true));
                    }
                    // Construct the committee.
                    let committee = Committee::<N>::new(0u64, members)?;
                    (committee, bonded_balances)
                }
                None => {
                    // Calculate the committee stake per member.
                    let stake_per_member =
                        N::STARTING_SUPPLY.saturating_div(2).saturating_div(num_committee_members as u64);
                    ensure!(stake_per_member >= MIN_VALIDATOR_STAKE, "Committee stake per member is too low");

                    // Construct the committee members and distribute stakes evenly among committee members.
                    let members = development_addresses
                        .iter()
                        .map(|address| (*address, (stake_per_member, true)))
                        .collect::<IndexMap<_, _>>();

                    // Construct the bonded balances.
                    // Note: The withdrawal address is set to the staker address.
                    let bonded_balances = members
                        .iter()
                        .map(|(address, (stake, _))| (*address, (*address, *address, *stake)))
                        .collect::<IndexMap<_, _>>();
                    // Construct the committee.
                    let committee = Committee::<N>::new(0u64, members)?;

                    (committee, bonded_balances)
                }
            };

            // Ensure that the number of committee members is correct.
            ensure!(
                committee.members().len() == num_committee_members as usize,
                "Number of committee members {} does not match the expected number of members {num_committee_members}",
                committee.members().len()
            );

            // Calculate the public balance per validator.
            let remaining_balance = N::STARTING_SUPPLY.saturating_sub(committee.total_stake());
            let public_balance_per_validator = remaining_balance.saturating_div(num_committee_members as u64);

            // Construct the public balances with fairly equal distribution.
            let mut public_balances = development_private_keys
                .iter()
                .map(|private_key| Ok((Address::try_from(private_key)?, public_balance_per_validator)))
                .collect::<Result<indexmap::IndexMap<_, _>>>()?;

            // If there is some leftover balance, add it to the 0-th validator.
            let leftover =
                remaining_balance.saturating_sub(public_balance_per_validator * num_committee_members as u64);
            if leftover > 0 {
                let (_, balance) = public_balances.get_index_mut(0).unwrap();
                *balance += leftover;
            }

            // Check if the sum of committee stakes and public balances equals the total starting supply.
            let public_balances_sum: u64 = public_balances.values().copied().sum();
            if committee.total_stake() + public_balances_sum != N::STARTING_SUPPLY {
                bail!("Sum of committee stakes and public balances does not equal total starting supply.");
            }

            // Construct the genesis block.
            load_or_compute_genesis(development_private_keys[0], committee, public_balances, bonded_balances, &mut rng)
        } else {
            // If the `dev_num_validators` flag is set, inform the user that it is ignored.
            if self.dev_num_validators.is_some() {
                eprintln!("The '--dev-num-validators' flag is ignored because '--dev' is not set");
            }

            Block::from_bytes_le(N::genesis_bytes())
        }
    }

    /// Returns the node type, from the given configurations.
    const fn parse_node_type(&self) -> NodeType {
        if self.validator {
            NodeType::Validator
        } else if self.prover {
            NodeType::Prover
        } else {
            NodeType::Client
        }
    }

    /// Returns the node type corresponding to the given configurations.
    #[rustfmt::skip]
    async fn parse_node<N: Network>(&mut self) -> Result<Node<N>> {
        // Print the welcome.
        println!("{}", crate::helpers::welcome_message());

        // Parse the trusted peers to connect to.
        let mut trusted_peers = self.parse_trusted_peers()?;
        // Parse the trusted validators to connect to.
        let mut trusted_validators = self.parse_trusted_validators()?;
        // Parse the development configurations.
        self.parse_development(&mut trusted_peers, &mut trusted_validators)?;

        // Parse the CDN.
        let cdn = self.parse_cdn();

        // Parse the genesis block.
        let genesis = self.parse_genesis::<N>()?;
        // Parse the private key of the node.
        let account = self.parse_private_key::<N>()?;
        // Parse the node type.
        let node_type = self.parse_node_type();

        // Parse the REST IP.
        let rest_ip = match self.norest {
            true => None,
            false => Some(self.rest),
        };
        // Parse the bft ip.
        let bft_ip = if self.dev.is_some() { self.bft } else { None };
        // Parse the node ip.
        let node_ip = if let Some(node_ip) = self.node { node_ip } else { SocketAddr::from_str("0.0.0.0:4130").unwrap() };

        // If the display is not enabled, render the welcome message.
        if self.nodisplay {
            // Print the Aleo address.
            println!("👛 Your Aleo address is {}.\n", account.address().to_string().bold());
            // Print the node type and network.
            println!(
                "🧭 Starting {} on {} at {}.\n",
                node_type.description().bold(),
                N::NAME.bold(),
                node_ip.to_string().bold()
            );

            // If the node is running a REST server, print the REST IP and JWT.
            if node_type.is_validator() {
                if let Some(rest_ip) = rest_ip {
                    println!("🌐 Starting the REST server at {}.\n", rest_ip.to_string().bold());

                    if let Ok(jwt_token) = snarkos_node_rest::Claims::new(account.address()).to_jwt_string() {
                        println!("🔑 Your one-time JWT token is {}\n", jwt_token.dimmed());
                    }
                }
            }
        }

        // If the node is a validator, check if the open files limit is lower than recommended.
        #[cfg(target_family = "unix")]
        if node_type.is_validator() {
            crate::helpers::check_open_files_limit(RECOMMENDED_MIN_NOFILES_LIMIT);
        }
        // Check if the machine meets the minimum requirements for a validator.
        crate::helpers::check_validator_machine(node_type);

        // Initialize the metrics.
        if self.metrics {
            metrics::initialize_metrics();
        }

        // Initialize the storage mode.
        let storage_mode = match &self.storage_path {
            Some(path) => StorageMode::Custom(path.clone()),
            None => StorageMode::from(self.dev),
        };

        // Determine whether to generate background transactions in dev mode.
        let dev_txs = match self.dev {
            Some(_) => !self.no_dev_txs,
            None => {
                // If the `no_dev_txs` flag is set, inform the user that it is ignored.
                if self.no_dev_txs {
                    eprintln!("The '--no-dev-txs' flag is ignored because '--dev' is not set");
                }
                false
            }
        };

        // Initialize the node.
        match node_type {
            NodeType::Validator => Node::new_validator(node_ip, bft_ip, rest_ip, self.rest_rps, account, &trusted_peers, &trusted_validators, genesis, cdn, storage_mode, self.allow_external_peers, dev_txs).await,
            NodeType::Prover => Node::new_prover(node_ip, account, &trusted_peers, genesis, storage_mode).await,
            NodeType::Client => Node::new_client(node_ip, rest_ip, self.rest_rps, account, &trusted_peers, genesis, cdn, storage_mode).await,
        }
    }

    /// Returns a runtime for the node.
    fn runtime() -> Runtime {
        // Retrieve the number of cores.
        let num_cores = num_cpus::get();

        // Initialize the number of tokio worker threads, max tokio blocking threads, and rayon cores.
        // Note: We intentionally set the number of tokio worker threads and number of rayon cores to be
        // more than the number of physical cores, because the node is expected to be I/O-bound.
        let (num_tokio_worker_threads, max_tokio_blocking_threads, num_rayon_cores_global) =
            (2 * num_cores, 512, num_cores);

        // Initialize the parallelization parameters.
        rayon::ThreadPoolBuilder::new()
            .stack_size(8 * 1024 * 1024)
            .num_threads(num_rayon_cores_global)
            .build_global()
            .unwrap();

        // Initialize the runtime configuration.
        runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(8 * 1024 * 1024)
            .worker_threads(num_tokio_worker_threads)
            .max_blocking_threads(max_tokio_blocking_threads)
            .build()
            .expect("Failed to initialize a runtime for the router")
    }
}

fn check_permissions(path: &PathBuf) -> Result<(), snarkvm::prelude::Error> {
    #[cfg(target_family = "unix")]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(path.exists(), "The file '{:?}' does not exist", path);
        let parent = path.parent();
        if let Some(parent) = parent {
            let parent_permissions = parent.metadata()?.permissions().mode();
            ensure!(
                parent_permissions & 0o777 == 0o700,
                "The folder {:?} must be readable only by the owner (0700)",
                parent
            );
        }
        let permissions = path.metadata()?.permissions().mode();
        ensure!(permissions & 0o777 == 0o600, "The file {:?} must be readable only by the owner (0600)", path);
    }
    Ok(())
}

/// Loads or computes the genesis block.
fn load_or_compute_genesis<N: Network>(
    genesis_private_key: PrivateKey<N>,
    committee: Committee<N>,
    public_balances: indexmap::IndexMap<Address<N>, u64>,
    bonded_balances: indexmap::IndexMap<Address<N>, (Address<N>, Address<N>, u64)>,
    rng: &mut ChaChaRng,
) -> Result<Block<N>> {
    // Construct the preimage.
    let mut preimage = Vec::new();

    // Input the genesis private key, committee, and public balances.
    preimage.extend(genesis_private_key.to_bytes_le()?);
    preimage.extend(committee.to_bytes_le()?);
    preimage.extend(&to_bytes_le![public_balances.iter().collect::<Vec<(_, _)>>()]?);
    preimage.extend(&to_bytes_le![
        bonded_balances
            .iter()
            .flat_map(|(staker, (validator, withdrawal, amount))| to_bytes_le![staker, validator, withdrawal, amount])
            .collect::<Vec<_>>()
    ]?);

    // Input the parameters' metadata.
    preimage.extend(snarkvm::parameters::mainnet::BondPublicVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::UnbondPublicVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::UnbondDelegatorAsValidatorVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::ClaimUnbondPublicVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::SetValidatorStateVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::TransferPrivateVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::TransferPublicVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::TransferPrivateToPublicVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::TransferPublicToPrivateVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::FeePrivateVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::FeePublicVerifier::METADATA.as_bytes());
    preimage.extend(snarkvm::parameters::mainnet::InclusionVerifier::METADATA.as_bytes());

    // Initialize the hasher.
    let hasher = snarkvm::console::algorithms::BHP256::<N>::setup("aleo.dev.block")?;
    // Compute the hash.
    let hash = hasher.hash(&preimage.to_bits_le())?.to_string();

    // A closure to load the block.
    let load_block = |file_path| -> Result<Block<N>> {
        // Attempts to load the genesis block file locally.
        let buffer = std::fs::read(file_path)?;
        // Return the genesis block.
        Block::from_bytes_le(&buffer)
    };

    // Construct the file path.
    let file_path = std::env::temp_dir().join(hash);
    // Check if the genesis block exists.
    if file_path.exists() {
        // If the block loads successfully, return it.
        if let Ok(block) = load_block(&file_path) {
            return Ok(block);
        }
    }

    /* Otherwise, compute the genesis block and store it. */

    // Initialize a new VM.
    let vm = VM::from(ConsensusStore::<N, ConsensusMemory<N>>::open(Some(0))?)?;
    // Initialize the genesis block.
    let block = vm.genesis_quorum(&genesis_private_key, committee, public_balances, bonded_balances, rng)?;
    // Write the genesis block to the file.
    std::fs::write(&file_path, block.to_bytes_le()?)?;
    // Return the genesis block.
    Ok(block)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::{Command, CLI};
    use snarkvm::prelude::MainnetV0;

    type CurrentNetwork = MainnetV0;

    #[test]
    fn test_parse_trusted_peers() {
        let config = Start::try_parse_from(["snarkos", "--peers", ""].iter()).unwrap();
        assert!(config.parse_trusted_peers().is_ok());
        assert!(config.parse_trusted_peers().unwrap().is_empty());

        let config = Start::try_parse_from(["snarkos", "--peers", "1.2.3.4:5"].iter()).unwrap();
        assert!(config.parse_trusted_peers().is_ok());
        assert_eq!(config.parse_trusted_peers().unwrap(), vec![SocketAddr::from_str("1.2.3.4:5").unwrap()]);

        let config = Start::try_parse_from(["snarkos", "--peers", "1.2.3.4:5,6.7.8.9:0"].iter()).unwrap();
        assert!(config.parse_trusted_peers().is_ok());
        assert_eq!(config.parse_trusted_peers().unwrap(), vec![
            SocketAddr::from_str("1.2.3.4:5").unwrap(),
            SocketAddr::from_str("6.7.8.9:0").unwrap()
        ]);
    }

    #[test]
    fn test_parse_trusted_validators() {
        let config = Start::try_parse_from(["snarkos", "--validators", ""].iter()).unwrap();
        assert!(config.parse_trusted_validators().is_ok());
        assert!(config.parse_trusted_validators().unwrap().is_empty());

        let config = Start::try_parse_from(["snarkos", "--validators", "1.2.3.4:5"].iter()).unwrap();
        assert!(config.parse_trusted_validators().is_ok());
        assert_eq!(config.parse_trusted_validators().unwrap(), vec![SocketAddr::from_str("1.2.3.4:5").unwrap()]);

        let config = Start::try_parse_from(["snarkos", "--validators", "1.2.3.4:5,6.7.8.9:0"].iter()).unwrap();
        assert!(config.parse_trusted_validators().is_ok());
        assert_eq!(config.parse_trusted_validators().unwrap(), vec![
            SocketAddr::from_str("1.2.3.4:5").unwrap(),
            SocketAddr::from_str("6.7.8.9:0").unwrap()
        ]);
    }

    #[test]
    fn test_parse_cdn() {
        // Validator (Prod)
        let config = Start::try_parse_from(["snarkos", "--validator", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn().is_some());
        let config =
            Start::try_parse_from(["snarkos", "--validator", "--private-key", "aleo1xx", "--cdn", "url"].iter())
                .unwrap();
        assert!(config.parse_cdn().is_some());
        let config =
            Start::try_parse_from(["snarkos", "--validator", "--private-key", "aleo1xx", "--cdn", ""].iter()).unwrap();
        assert!(config.parse_cdn().is_none());

        // Validator (Dev)
        let config =
            Start::try_parse_from(["snarkos", "--dev", "0", "--validator", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn().is_none());
        let config = Start::try_parse_from(
            ["snarkos", "--dev", "0", "--validator", "--private-key", "aleo1xx", "--cdn", "url"].iter(),
        )
        .unwrap();
        assert!(config.parse_cdn().is_none());
        let config = Start::try_parse_from(
            ["snarkos", "--dev", "0", "--validator", "--private-key", "aleo1xx", "--cdn", ""].iter(),
        )
        .unwrap();
        assert!(config.parse_cdn().is_none());

        // Prover (Prod)
        let config = Start::try_parse_from(["snarkos", "--prover", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn().is_none());
        let config =
            Start::try_parse_from(["snarkos", "--prover", "--private-key", "aleo1xx", "--cdn", "url"].iter()).unwrap();
        assert!(config.parse_cdn().is_none());
        let config =
            Start::try_parse_from(["snarkos", "--prover", "--private-key", "aleo1xx", "--cdn", ""].iter()).unwrap();
        assert!(config.parse_cdn().is_none());

        // Prover (Dev)
        let config =
            Start::try_parse_from(["snarkos", "--dev", "0", "--prover", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn().is_none());
        let config = Start::try_parse_from(
            ["snarkos", "--dev", "0", "--prover", "--private-key", "aleo1xx", "--cdn", "url"].iter(),
        )
        .unwrap();
        assert!(config.parse_cdn().is_none());
        let config = Start::try_parse_from(
            ["snarkos", "--dev", "0", "--prover", "--private-key", "aleo1xx", "--cdn", ""].iter(),
        )
        .unwrap();
        assert!(config.parse_cdn().is_none());

        // Client (Prod)
        let config = Start::try_parse_from(["snarkos", "--client", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn().is_some());
        let config =
            Start::try_parse_from(["snarkos", "--client", "--private-key", "aleo1xx", "--cdn", "url"].iter()).unwrap();
        assert!(config.parse_cdn().is_some());
        let config =
            Start::try_parse_from(["snarkos", "--client", "--private-key", "aleo1xx", "--cdn", ""].iter()).unwrap();
        assert!(config.parse_cdn().is_none());

        // Client (Dev)
        let config =
            Start::try_parse_from(["snarkos", "--dev", "0", "--client", "--private-key", "aleo1xx"].iter()).unwrap();
        assert!(config.parse_cdn().is_none());
        let config = Start::try_parse_from(
            ["snarkos", "--dev", "0", "--client", "--private-key", "aleo1xx", "--cdn", "url"].iter(),
        )
        .unwrap();
        assert!(config.parse_cdn().is_none());
        let config = Start::try_parse_from(
            ["snarkos", "--dev", "0", "--client", "--private-key", "aleo1xx", "--cdn", ""].iter(),
        )
        .unwrap();
        assert!(config.parse_cdn().is_none());

        // Default (Prod)
        let config = Start::try_parse_from(["snarkos"].iter()).unwrap();
        assert!(config.parse_cdn().is_none());
        let config = Start::try_parse_from(["snarkos", "--cdn", "url"].iter()).unwrap();
        assert!(config.parse_cdn().is_none());
        let config = Start::try_parse_from(["snarkos", "--cdn", ""].iter()).unwrap();
        assert!(config.parse_cdn().is_none());

        // Default (Dev)
        let config = Start::try_parse_from(["snarkos", "--dev", "0"].iter()).unwrap();
        assert!(config.parse_cdn().is_none());
        let config = Start::try_parse_from(["snarkos", "--dev", "0", "--cdn", "url"].iter()).unwrap();
        assert!(config.parse_cdn().is_none());
        let config = Start::try_parse_from(["snarkos", "--dev", "0", "--cdn", ""].iter()).unwrap();
        assert!(config.parse_cdn().is_none());
    }

    #[test]
    fn test_parse_development_and_genesis() {
        let prod_genesis = Block::from_bytes_le(CurrentNetwork::genesis_bytes()).unwrap();

        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config = Start::try_parse_from(["snarkos"].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        let candidate_genesis = config.parse_genesis::<CurrentNetwork>().unwrap();
        assert_eq!(trusted_peers.len(), 0);
        assert_eq!(trusted_validators.len(), 0);
        assert_eq!(candidate_genesis, prod_genesis);

        let _config = Start::try_parse_from(["snarkos", "--dev", ""].iter()).unwrap_err();

        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config = Start::try_parse_from(["snarkos", "--dev", "0"].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        let expected_genesis = config.parse_genesis::<CurrentNetwork>().unwrap();
        assert_eq!(config.node, Some(SocketAddr::from_str("0.0.0.0:4130").unwrap()));
        assert_eq!(config.rest, SocketAddr::from_str("0.0.0.0:3030").unwrap());
        assert_eq!(trusted_peers.len(), 0);
        assert_eq!(trusted_validators.len(), 1);
        assert!(!config.validator);
        assert!(!config.prover);
        assert!(!config.client);
        assert_ne!(expected_genesis, prod_genesis);

        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config =
            Start::try_parse_from(["snarkos", "--dev", "1", "--validator", "--private-key", ""].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        let genesis = config.parse_genesis::<CurrentNetwork>().unwrap();
        assert_eq!(config.node, Some(SocketAddr::from_str("0.0.0.0:4131").unwrap()));
        assert_eq!(config.rest, SocketAddr::from_str("0.0.0.0:3031").unwrap());
        assert_eq!(trusted_peers.len(), 1);
        assert_eq!(trusted_validators.len(), 1);
        assert!(config.validator);
        assert!(!config.prover);
        assert!(!config.client);
        assert_eq!(genesis, expected_genesis);

        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config =
            Start::try_parse_from(["snarkos", "--dev", "2", "--prover", "--private-key", ""].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        let genesis = config.parse_genesis::<CurrentNetwork>().unwrap();
        assert_eq!(config.node, Some(SocketAddr::from_str("0.0.0.0:4132").unwrap()));
        assert_eq!(config.rest, SocketAddr::from_str("0.0.0.0:3032").unwrap());
        assert_eq!(trusted_peers.len(), 2);
        assert_eq!(trusted_validators.len(), 2);
        assert!(!config.validator);
        assert!(config.prover);
        assert!(!config.client);
        assert_eq!(genesis, expected_genesis);

        let mut trusted_peers = vec![];
        let mut trusted_validators = vec![];
        let mut config =
            Start::try_parse_from(["snarkos", "--dev", "3", "--client", "--private-key", ""].iter()).unwrap();
        config.parse_development(&mut trusted_peers, &mut trusted_validators).unwrap();
        let genesis = config.parse_genesis::<CurrentNetwork>().unwrap();
        assert_eq!(config.node, Some(SocketAddr::from_str("0.0.0.0:4133").unwrap()));
        assert_eq!(config.rest, SocketAddr::from_str("0.0.0.0:3033").unwrap());
        assert_eq!(trusted_peers.len(), 3);
        assert_eq!(trusted_validators.len(), 2);
        assert!(!config.validator);
        assert!(!config.prover);
        assert!(config.client);
        assert_eq!(genesis, expected_genesis);
    }

    #[test]
    fn clap_snarkos_start() {
        let arg_vec = vec![
            "snarkos",
            "start",
            "--nodisplay",
            "--dev",
            "2",
            "--validator",
            "--private-key",
            "PRIVATE_KEY",
            "--cdn",
            "CDN",
            "--peers",
            "IP1,IP2,IP3",
            "--validators",
            "IP1,IP2,IP3",
            "--rest",
            "127.0.0.1:3030",
        ];
        let cli = CLI::parse_from(arg_vec);

        if let Command::Start(start) = cli.command {
            assert!(start.nodisplay);
            assert_eq!(start.dev, Some(2));
            assert!(start.validator);
            assert_eq!(start.private_key.as_deref(), Some("PRIVATE_KEY"));
            assert_eq!(start.cdn, "CDN");
            assert_eq!(start.rest, "127.0.0.1:3030".parse().unwrap());
            assert_eq!(start.network, 0);
            assert_eq!(start.peers, "IP1,IP2,IP3");
            assert_eq!(start.validators, "IP1,IP2,IP3");
        } else {
            panic!("Unexpected result of clap parsing!");
        }
    }
}
