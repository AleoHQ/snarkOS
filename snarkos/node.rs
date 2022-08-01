// Copyright (C) 2019-2022 Aleo Systems Inc.
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

use crate::{Display, Server, Updater};
use snarkos_environment::{
    helpers::NodeType,
    Client,
    ClientTrial,
    CurrentNetwork,
    Environment,
    Miner,
    MinerTrial,
    Operator,
    OperatorTrial,
    Prover,
    ProverTrial,
    SyncNode,
};
use snarkos_storage::{
    storage::{rocksdb::RocksDB, ReadOnly},
    LedgerState,
};
use snarkvm::dpc::prelude::*;

use anyhow::{anyhow, Result};
use clap::Parser;
use colored::*;
use crossterm::tty::IsTty;
use std::{fmt::Write, io, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[clap(name = "snarkos", author = "The Aleo Team <hello@aleo.org>")]
pub struct Node {
    /// Specify the IP address and port of a peer to connect to.
    #[clap(long, action)]
    pub connect: Option<String>,
    /// Specify this as a mining node, with the given miner address.
    #[clap(long, action)]
    pub miner: Option<String>,
    /// Specify this as an operating node, with the given operator address.
    #[clap(long, action)]
    pub operator: Option<String>,
    /// Specify this as a prover node, with the given prover address.
    #[clap(long, action)]
    pub prover: Option<String>,
    /// Specify the pool that a prover node is contributing to.
    #[clap(long, action)]
    pub pool: Option<SocketAddr>,
    /// Specify the network of this node.
    #[clap(long, default_value = "2", action)]
    pub network: u16,
    /// Specify the IP address and port for the node server.
    #[clap(long, default_value = "0.0.0.0:4132", action)]
    pub node: SocketAddr,
    /// Specify the IP address and port for the RPC server.
    #[clap(long, default_value = "0.0.0.0:3032", action)]
    pub rpc: SocketAddr,
    /// Specify the username for the RPC server.
    #[clap(default_value = "root", long = "username", action)]
    pub rpc_username: String,
    /// Specify the password for the RPC server.
    #[clap(default_value = "pass", long = "password", action)]
    pub rpc_password: String,
    /// Specify the verbosity of the node [options: 0, 1, 2, 3]
    #[clap(default_value = "2", long, action)]
    pub verbosity: u8,
    /// Enables development mode, specify a unique ID for the local node.
    #[clap(long, action)]
    pub dev: Option<u16>,
    /// If the flag is set, the node will render a read-only display.
    #[clap(long, action)]
    pub display: bool,
    /// If the flag is set, the node will not initialize the RPC server.
    #[clap(long, action)]
    pub norpc: bool,
    #[clap(hide = true, long, action)]
    pub trial: bool,
    #[clap(hide = true, long, action)]
    pub sync: bool,
    /// Specify an optional subcommand.
    #[clap(subcommand)]
    commands: Option<Command>,
}

impl Node {
    /// Starts the node.
    pub async fn start(self) -> Result<()> {
        // Parse optional subcommands first.
        match self.commands {
            Some(command) => {
                println!("{}", command.parse()?);
                Ok(())
            }
            None => match &self.get_node_type() {
                (NodeType::Client, false) => self.start_server::<CurrentNetwork, Client<CurrentNetwork>>(&None).await,
                (NodeType::Miner, false) => self.start_server::<CurrentNetwork, Miner<CurrentNetwork>>(&self.miner).await,
                (NodeType::Operator, false) => self.start_server::<CurrentNetwork, Operator<CurrentNetwork>>(&self.operator).await,
                (NodeType::Prover, false) => self.start_server::<CurrentNetwork, Prover<CurrentNetwork>>(&self.prover).await,
                (NodeType::Client, true) => self.start_server::<CurrentNetwork, ClientTrial<CurrentNetwork>>(&None).await,
                (NodeType::Miner, true) => self.start_server::<CurrentNetwork, MinerTrial<CurrentNetwork>>(&self.miner).await,
                (NodeType::Operator, true) => {
                    self.start_server::<CurrentNetwork, OperatorTrial<CurrentNetwork>>(&self.operator)
                        .await
                }
                (NodeType::Prover, true) => self.start_server::<CurrentNetwork, ProverTrial<CurrentNetwork>>(&self.prover).await,
                (NodeType::Sync, _) => self.start_server::<CurrentNetwork, SyncNode<CurrentNetwork>>(&None).await,
                _ => panic!("Unsupported node configuration"),
            },
        }
    }

    fn get_node_type(&self) -> (NodeType, bool) {
        (
            match (self.network, &self.miner, &self.operator, &self.prover, self.sync) {
                (2, None, None, None, false) => NodeType::Client,
                (2, Some(_), None, None, false) => NodeType::Miner,
                (2, None, Some(_), None, false) => NodeType::Operator,
                (2, None, None, Some(_), false) => NodeType::Prover,
                (2, None, None, None, true) => NodeType::Sync,
                _ => panic!("Unsupported node configuration"),
            },
            self.trial,
        )
    }

    /// Returns the storage path of the ledger.
    pub(crate) fn ledger_storage_path(&self, _local_ip: SocketAddr) -> PathBuf {
        if cfg!(feature = "test") {
            // Tests may use any available ports, and removes the storage artifacts afterwards,
            // so that there is no need to adhere to a specific number assignment logic.
            PathBuf::from(format!("/tmp/snarkos-test-ledger-{}", _local_ip.port()))
        } else {
            aleo_std::aleo_ledger_dir(self.network, self.dev)
        }
    }

    /// Returns the storage path of the operator.
    pub(crate) fn operator_storage_path(&self, _local_ip: SocketAddr) -> PathBuf {
        if cfg!(feature = "test") {
            // Tests may use any available ports, and removes the storage artifacts afterwards,
            // so that there is no need to adhere to a specific number assignment logic.
            PathBuf::from(format!("/tmp/snarkos-test-operator-{}", _local_ip.port()))
        } else {
            aleo_std::aleo_operator_dir(self.network, self.dev)
        }
    }

    /// Returns the storage path of the prover.
    pub(crate) fn prover_storage_path(&self, _local_ip: SocketAddr) -> PathBuf {
        if cfg!(feature = "test") {
            // Tests may use any available ports, and removes the storage artifacts afterwards,
            // so that there is no need to adhere to a specific number assignment logic.
            PathBuf::from(format!("/tmp/snarkos-test-prover-{}", _local_ip.port()))
        } else {
            aleo_std::aleo_prover_dir(self.network, self.dev)
        }
    }

    async fn start_server<N: Network, E: Environment>(&self, address: &Option<String>) -> Result<()> {
        println!("{}", crate::display::welcome_message());

        let address = match (E::NODE_TYPE, address) {
            (NodeType::Miner, Some(address)) | (NodeType::Operator, Some(address)) | (NodeType::Prover, Some(address)) => {
                let address = Address::<N>::from_str(address)?;
                println!("Your Aleo address is {}.\n", address);
                Some(address)
            }
            _ => None,
        };

        println!("Starting {} on {}.", E::NODE_TYPE.description(), N::NETWORK_NAME);
        println!("{}", crate::display::notification_message::<N>(address));

        // Initialize the node's server.
        let server = Server::<N, E>::initialize(self, address, self.pool).await?;

        // Initialize signal handling; it also maintains ownership of the Server
        // in order for it to not go out of scope.
        handle_signals(server.clone());

        // Initialize the display, if enabled.
        if self.display {
            println!("\nThe snarkOS console is initializing...\n");
            Display::<N, E>::start(server.clone(), self.verbosity)?;
        };

        // Connect to peer(s) if given as an argument.
        if let Some(peer_ips) = &self.connect {
            for peer_ip in peer_ips.split(',') {
                let addr: SocketAddr = if let Ok(addr) = peer_ip.parse() {
                    addr
                } else {
                    error!("The address supplied to --connect ('{}') is malformed.", peer_ip);
                    continue;
                };
                let _ = server.connect_to(addr).await;
            }
        }

        #[cfg(feature = "auto-update")]
        initialize_auto_updater(server).await;

        // Note: Do not move this. The pending await must be here otherwise
        // other snarkOS commands will not exit.
        #[cfg(not(feature = "auto-update"))]
        std::future::pending::<()>().await;

        Ok(())
    }
}

#[cfg(feature = "auto-update")]
async fn initialize_auto_updater<N: Network, E: Environment>(server: Server<N, E>) {
    let mut auto_updater = crate::AutoUpdater::new().await.expect("Auto-updater failed");

    loop {
        match auto_updater.check_for_updates().await {
            Ok(true) => {
                warn!("[auto-updater]: You are using an outdated version of the snarkOS client! Automatically updating...");
                if let Err(e) = auto_updater.update_local_repo().await {
                    error!("[auto-updater]: Couldn't automatically update the local snarkOS repo: {}", e);
                    continue;
                }

                server.shut_down().await;

                if let Err(e) = auto_updater.rebuild_local_repo().await {
                    error!("[auto-updater]: Couldn't automatically rebuild the local snarkOS repo: {}", e);
                }

                if let Err(e) = auto_updater.restart() {
                    error!("[auto-updater]: Couldn't automatically restart the node: {}", e);
                }

                break;
            }
            Ok(false) => {
                debug!("[auto-updater]: You are using an up-to-date version of the snarkOS client.");
            }
            Err(e) => {
                error!("[auto-updater]: Couldn't check the snarkOS repo for updates: {}", e);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(crate::AUTO_UPDATE_INTERVAL_SECS)).await;
    }
}

pub fn initialize_logger(verbosity: u8, log_sender: Option<mpsc::Sender<Vec<u8>>>) {
    match verbosity {
        0 => std::env::set_var("RUST_LOG", "info"),
        1 => std::env::set_var("RUST_LOG", "debug"),
        2 | 3 => std::env::set_var("RUST_LOG", "trace"),
        _ => std::env::set_var("RUST_LOG", "info"),
    };

    // Filter out undesirable logs.
    let filter = EnvFilter::from_default_env()
        .add_directive("mio=off".parse().unwrap())
        .add_directive("tokio_util=off".parse().unwrap())
        .add_directive("hyper::proto::h1::conn=off".parse().unwrap())
        .add_directive("hyper::proto::h1::decode=off".parse().unwrap())
        .add_directive("hyper::proto::h1::io=off".parse().unwrap())
        .add_directive("hyper::proto::h1::role=off".parse().unwrap())
        .add_directive("jsonrpsee=off".parse().unwrap());

    // Initialize tracing.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(log_sender.is_none() && io::stdout().is_tty())
        .with_writer(move || LogWriter::new(&log_sender))
        .with_target(verbosity == 3)
        .try_init();
}

#[derive(Debug, Parser)]
pub enum Command {
    #[clap(name = "clean", about = "Removes the ledger files from storage")]
    Clean(Clean),
    #[clap(name = "update", about = "Updates snarkOS to the latest version")]
    Update(Update),
    #[clap(name = "experimental", about = "Experimental features")]
    Experimental(Experimental),
    #[clap(name = "miner", about = "Miner commands and settings")]
    Miner(MinerSubcommand),
}

impl Command {
    pub fn parse(self) -> Result<String> {
        match self {
            Self::Clean(command) => command.parse(),
            Self::Update(command) => command.parse(),
            Self::Experimental(command) => command.parse(),
            Self::Miner(command) => command.parse(),
        }
    }
}

pub enum LogWriter {
    Stdout(io::Stdout),
    Sender(mpsc::Sender<Vec<u8>>),
}

impl LogWriter {
    pub fn new(log_sender: &Option<mpsc::Sender<Vec<u8>>>) -> Self {
        if let Some(sender) = log_sender {
            Self::Sender(sender.clone())
        } else {
            Self::Stdout(io::stdout())
        }
    }
}

impl io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Stdout(stdout) => stdout.write(buf),
            Self::Sender(sender) => {
                let log = buf.to_vec();
                let _ = sender.try_send(log);
                Ok(buf.len())
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Parser)]
pub struct Clean {
    /// Specify the network of the ledger to remove from storage.
    #[clap(long, default_value = "2", action)]
    pub network: u16,
    /// Enables development mode, specify the unique ID of the local node to clean.
    #[clap(long, action)]
    pub dev: Option<u16>,
}

impl Clean {
    pub fn parse(self) -> Result<String> {
        // Remove the specified ledger from storage.
        Self::remove_ledger(self.network, self.dev)
    }

    /// Removes the specified ledger from storage.
    fn remove_ledger(network: u16, dev: Option<u16>) -> Result<String> {
        // Construct the path to the ledger in storage.
        let path = aleo_std::aleo_ledger_dir(network, dev);
        // Check if the path to the ledger exists in storage.
        if path.exists() {
            // Remove the ledger files from storage.
            match std::fs::remove_dir_all(&path) {
                Ok(_) => Ok(format!("Successfully removed the ledger files from storage. ({})", path.display())),
                Err(error) => Err(anyhow!(
                    "Failed to remove the ledger files from storage. ({})\n{}",
                    path.display(),
                    error
                )),
            }
        } else {
            Ok(format!("No ledger files were found in storage. ({})", path.display()))
        }
    }
}

#[derive(Debug, Parser)]
pub struct Update {
    /// Lists all available versions of snarkOS
    #[clap(short, long, action)]
    list: bool,
    /// Suppress outputs to terminal
    #[clap(short, long, action)]
    quiet: bool,
    /// Update to specified version
    #[clap(short, long, action)]
    version: Option<String>,
}

impl Update {
    pub fn parse(self) -> Result<String> {
        match self.list {
            true => match Updater::show_available_releases() {
                Ok(output) => Ok(output),
                Err(error) => Ok(format!("Failed to list the available versions of snarkOS\n{}\n", error)),
            },
            false => {
                let result = Updater::update_to_release(!self.quiet, self.version);
                if !self.quiet {
                    match result {
                        Ok(status) => {
                            if status.uptodate() {
                                Ok("\nsnarkOS is already on the latest version".to_string())
                            } else if status.updated() {
                                Ok(format!("\nsnarkOS has updated to version {}", status.version()))
                            } else {
                                Ok(String::new())
                            }
                        }
                        Err(e) => Ok(format!("\nFailed to update snarkOS to the latest version\n{}\n", e)),
                    }
                } else {
                    Ok(String::new())
                }
            }
        }
    }
}

#[derive(Debug, Parser)]
pub struct Experimental {
    #[clap(subcommand)]
    commands: ExperimentalCommands,
}

impl Experimental {
    pub fn parse(self) -> Result<String> {
        match self.commands {
            ExperimentalCommands::NewAccount(command) => command.parse(),
        }
    }
}

#[derive(Debug, Parser)]
pub enum ExperimentalCommands {
    #[clap(name = "new_account", about = "Generate a new Aleo account.")]
    NewAccount(NewAccount),
}

#[derive(Debug, Parser)]
pub struct NewAccount {}

impl NewAccount {
    pub fn parse(self) -> Result<String> {
        let account = Account::<CurrentNetwork>::new(&mut rand::thread_rng());

        // Print the new Aleo account.
        let mut output = "".to_string();
        write!(
            output,
            "\n {:>12}\n",
            "Attention - Remember to store this account private key and view key.".red().bold()
        )?;
        writeln!(output, "\n {:>12}  {}", "Private Key".cyan().bold(), account.private_key())?;
        writeln!(output, " {:>12}  {}", "View Key".cyan().bold(), account.view_key())?;
        writeln!(output, " {:>12}  {}", "Address".cyan().bold(), account.address())?;

        Ok(output)
    }
}

#[derive(Debug, Parser)]
pub struct MinerSubcommand {
    #[clap(subcommand)]
    commands: MinerCommands,
}

impl MinerSubcommand {
    pub fn parse(self) -> Result<String> {
        match self.commands {
            MinerCommands::Stats(command) => command.parse(),
        }
    }
}

#[derive(Debug, Parser)]
pub enum MinerCommands {
    #[clap(name = "stats", about = "Prints statistics for the miner.")]
    Stats(MinerStats),
}

#[derive(Debug, Parser)]
pub struct MinerStats {
    #[clap(action)]
    address: String,
}

impl MinerStats {
    pub fn parse(self) -> Result<String> {
        // Parse the input address.
        let miner = Address::<CurrentNetwork>::from_str(&self.address)?;

        // Initialize the node.
        let node = Node::parse_from(&["snarkos", "--norpc", "--verbosity", "0"]);

        let ip = "0.0.0.0:1000".parse().unwrap();

        // Initialize the ledger storage.
        let ledger_storage_path = node.ledger_storage_path(ip);
        let (ledger, ledger_resource): (Arc<LedgerState<CurrentNetwork, _>>, _) =
            snarkos_storage::LedgerState::open_reader::<RocksDB<ReadOnly>, _>(ledger_storage_path).unwrap();

        // Initialize the prover storage.
        let prover_storage_path = node.prover_storage_path(ip);
        let prover = snarkos_storage::ProverState::open::<RocksDB<ReadOnly>, _>(prover_storage_path).unwrap();

        // Retrieve the latest block height.
        let latest_block_height = ledger.latest_block_height();

        // Prepare a list of confirmed and pending coinbase records.
        let mut confirmed = vec![];
        let mut pending = vec![];

        // Iterate through the coinbase records from storage.
        for (block_height, record) in prover.to_coinbase_records() {
            // Filter the coinbase records by determining if they exist on the canonical chain.
            if let Ok(true) = ledger.contains_commitment(&record.commitment()) {
                // Ensure the record owner matches.
                if record.owner() == miner {
                    // Add the block to the appropriate list.
                    match block_height + 2048 < latest_block_height {
                        true => confirmed.push((block_height, record)),
                        false => pending.push((block_height, record)),
                    }
                }
            }
        }

        tokio::spawn(ledger_resource.abort());

        Ok(format!(
            "Mining Report (confirmed_blocks = {}, pending_blocks = {}, miner_address = {})",
            confirmed.len(),
            pending.len(),
            miner
        ))
    }
}

// This function is responsible for handling OS signals in order
// for the node to be able to intercept them and perform a clean shutdown.
// Note: Only Ctrl-C is supported; it should work on both Unix-family systems and Windows.
pub fn handle_signals<N: Network, E: Environment>(server: Server<N, E>) {
    E::resources().register_task(
        None, // No need to provide an id, as the task will run indefinitely.
        tokio::task::spawn(async move {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    server.shut_down().await;
                    std::process::exit(0);
                }
                Err(error) => error!("tokio::signal::ctrl_c encountered an error: {}", error),
            }
        }),
    );
}
