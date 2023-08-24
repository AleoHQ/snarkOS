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

mod router;

use crate::traits::NodeInterface;
use snarkos_account::Account;
use snarkos_node_consensus::Consensus;
use snarkos_node_messages::{Data, NodeType, PuzzleResponse, UnconfirmedSolution, UnconfirmedTransaction};
use snarkos_node_narwhal::helpers::init_primary_channels;
use snarkos_node_rest::Rest;
use snarkos_node_router::{Heartbeat, Inbound, Outbound, Router, Routing};
use snarkos_node_tcp::{
    protocols::{Disconnect, Handshake, OnConnect, Reading, Writing},
    P2P,
};
use snarkvm::prelude::{
    block::{Block, Transaction},
    coinbase::ProverSolution,
    store::ConsensusStorage,
    Ledger,
    Network,
    Value,
};

use aleo_std::prelude::{finish, lap, timer};
use anyhow::{bail, Result};
use core::{str::FromStr, time::Duration};
use parking_lot::Mutex;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::task::JoinHandle;

/// A beacon is a full node, capable of producing blocks.
#[derive(Clone)]
pub struct Beacon<N: Network, C: ConsensusStorage<N>> {
    /// The account of the node.
    account: Account<N>,
    /// The ledger of the node.
    ledger: Ledger<N, C>,
    /// The consensus module of the node.
    consensus: Consensus<N, C>,
    /// The router of the node.
    router: Router<N>,
    /// The REST server of the node.
    rest: Option<Rest<N, C, Self>>,
    // /// The unspent records.
    // unspent_records: Arc<RwLock<RecordMap<N>>>,
    /// The spawned handles.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// The shutdown signal.
    shutdown: Arc<AtomicBool>,
}

impl<N: Network, C: ConsensusStorage<N>> Beacon<N, C> {
    /// Initializes a new beacon node.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        node_ip: SocketAddr,
        rest_ip: Option<SocketAddr>,
        bft_ip: Option<SocketAddr>,
        account: Account<N>,
        trusted_peers: &[SocketAddr],
        genesis: Block<N>,
        cdn: Option<String>,
        dev: Option<u16>,
    ) -> Result<Self> {
        let timer = timer!("Beacon::new");

        // Initialize the ledger.
        let ledger = Ledger::load(genesis, dev)?;
        lap!(timer, "Initialize the ledger");

        // Initialize the CDN.
        if let Some(base_url) = cdn {
            // Sync the ledger with the CDN.
            if let Err((_, error)) = snarkos_node_cdn::sync_ledger_with_cdn(&base_url, ledger.clone()).await {
                crate::helpers::log_clean_error(dev);
                return Err(error);
            }
            lap!(timer, "Initialize the CDN");
        }

        // Initialize the consensus.
        let mut consensus = Consensus::new(account.clone(), ledger.clone(), bft_ip, dev)?;
        // Initialize the primary channels.
        let (primary_sender, primary_receiver) = init_primary_channels::<N>();
        // Start the consensus.
        consensus.run(primary_sender, primary_receiver).await?;
        lap!(timer, "Initialize consensus");

        // // Retrieve the unspent records.
        // let unspent_records = ledger.find_unspent_credits_records(account.view_key())?;
        // lap!(timer, "Retrieve the unspent credits records");

        // Initialize the node router.
        let router = Router::new(
            node_ip,
            NodeType::Beacon,
            account.clone(),
            trusted_peers,
            Self::MAXIMUM_NUMBER_OF_PEERS as u16,
            dev.is_some(),
        )
        .await?;
        lap!(timer, "Initialize the router");

        // Initialize the node.
        let mut node = Self {
            account,
            ledger: ledger.clone(),
            consensus: consensus.clone(),
            router,
            rest: None,
            // unspent_records: Arc::new(RwLock::new(unspent_records)),
            handles: Default::default(),
            shutdown: Default::default(),
        };

        // Initialize the REST server.
        if let Some(rest_ip) = rest_ip {
            node.rest = Some(Rest::start(rest_ip, Some(consensus), ledger, Arc::new(node.clone()))?);
            lap!(timer, "Initialize REST server");
        }
        // Initialize the routing.
        node.initialize_routing().await;
        // Initialize the block production.
        node.initialize_block_production().await;
        // Initialize the signal handler.
        node.handle_signals();
        lap!(timer, "Initialize the handlers");

        finish!(timer);
        // Return the node.
        Ok(node)
    }

    /// Returns the ledger.
    pub fn ledger(&self) -> &Ledger<N, C> {
        &self.ledger
    }

    /// Returns the REST server.
    pub fn rest(&self) -> &Option<Rest<N, C, Self>> {
        &self.rest
    }
}

#[async_trait]
impl<N: Network, C: ConsensusStorage<N>> NodeInterface<N> for Beacon<N, C> {
    /// Shuts down the node.
    async fn shut_down(&self) {
        info!("Shutting down...");

        // Shut down block production.
        trace!("Shutting down block production...");
        self.shutdown.store(true, Ordering::Relaxed);

        // Abort the tasks.
        trace!("Shutting down the beacon...");
        self.handles.lock().iter().for_each(|handle| handle.abort());

        // Shut down the router.
        self.router.shut_down().await;

        // Shut down the ledger.
        trace!("Shutting down the ledger...");
        // self.ledger.shut_down().await;

        info!("Node has shut down.");
    }
}

// /// A helper method to check if the coinbase target has been met.
// async fn check_for_coinbase<N: Network, C: ConsensusStorage<N>>(consensus: Consensus<N, C>) {
//     loop {
//         // Check if the coinbase target has been met.
//         match consensus.is_coinbase_target_met() {
//             Ok(true) => break,
//             Ok(false) => (),
//             Err(error) => error!("Failed to check if coinbase target is met: {error}"),
//         }
//         // Sleep for one second.
//         tokio::time::sleep(Duration::from_secs(1)).await
//     }
// }

impl<N: Network, C: ConsensusStorage<N>> Beacon<N, C> {
    /// Initialize a new instance of block production.
    async fn initialize_block_production(&self) {
        let beacon = self.clone();
        self.handles.lock().push(tokio::spawn(async move {
            //         // Expected time per block.
            //         const ROUND_TIME: u64 = 15; // 15 seconds per block

            // Produce blocks.
            loop {
                //             // Fetch the current timestamp.
                //             let current_timestamp = OffsetDateTime::now_utc().unix_timestamp();
                //             // Compute the elapsed time.
                //             let elapsed_time = current_timestamp.saturating_sub(beacon.ledger.latest_timestamp()) as u64;
                //
                //             // Do not produce a block if the elapsed time has not exceeded `ROUND_TIME - block_generation_time`.
                //             // This will ensure a block is produced at intervals of approximately `ROUND_TIME`.
                //             let time_to_wait = ROUND_TIME.saturating_sub(beacon.block_generation_time.load(Ordering::Acquire));
                //             trace!("Waiting for {time_to_wait} seconds before producing a block...");
                //             if elapsed_time < time_to_wait {
                //                 if let Err(error) = timeout(
                //                     Duration::from_secs(time_to_wait.saturating_sub(elapsed_time)),
                //                     check_for_coinbase(beacon.consensus.clone()),
                //                 )
                //                 .await
                //                 {
                //                     trace!("Check for coinbase - {error}");
                //                 }
                //             }
                //
                //             // Start a timer.
                //             let timer = std::time::Instant::now();

                tokio::time::sleep(Duration::from_millis(100)).await;

                // Produce the next block and propagate it to all peers.
                match beacon.produce_next_block().await {
                    // Update the block generation time.
                    // Ok(()) => beacon.block_generation_time.store(timer.elapsed().as_secs(), Ordering::Release),
                    Ok(()) => (),
                    Err(error) => error!("{error}"),
                }

                // If the Ctrl-C handler registered the signal, stop the node once the current block is complete.
                if beacon.shutdown.load(Ordering::Relaxed) {
                    info!("Shutting down block production");
                    break;
                }
            }
        }));
    }

    /// Produces the next block and propagates it to all peers.
    async fn produce_next_block(&self) -> Result<()> {
        // let mut beacon_transaction: Option<Transaction<N>> = None;

        // Produce a transaction if the mempool is empty.
        if self.consensus.num_unconfirmed_transactions() == 0 {
            // Create a transfer transaction.
            let beacon = self.clone();
            let transaction = match tokio::task::spawn_blocking(move || {
                // Initialize an RNG.
                let rng = &mut rand::thread_rng();

                // Prepare the inputs for a transfer.
                let to = beacon.account.address();
                let amount = 1;
                let inputs = vec![Value::from_str(&format!("{to}"))?, Value::from_str(&format!("{amount}u64"))?];

                // Create a new transaction.
                let transaction = beacon.ledger.vm().execute(
                    beacon.account.private_key(),
                    ("credits.aleo", "mint"),
                    inputs.iter(),
                    None,
                    None,
                    rng,
                );

                match transaction {
                    Ok(transaction) => Ok(transaction),
                    Err(error) => {
                        bail!("Failed to create a transaction: {error}")
                    }
                }
            })
            .await
            {
                Ok(Ok(transaction)) => transaction,
                Ok(Err(error)) => bail!("Failed to create a transfer transaction for the next block: {error}"),
                Err(error) => bail!("Failed to create a transfer transaction for the next block: {error}"),
            };
            // Save the beacon transaction.
            // beacon_transaction = Some(transaction.clone());

            // Add the transaction to the memory pool.
            let beacon = self.clone();
            match beacon.consensus.add_unconfirmed_transaction(transaction).await {
                Ok(()) => (),
                Err(error) => bail!("Failed to add the transaction to the memory pool: {error}"),
            }
        }

        // // Propose the next block.
        // let beacon = self.clone();
        // let next_block = match tokio::task::spawn_blocking(move || {
        //     let next_block = beacon.consensus.propose_next_block(beacon.private_key(), &mut rand::thread_rng())?;
        //
        //     // Ensure the block is a valid next block.
        //     if let Err(error) = beacon.ledger.check_next_block(&next_block) {
        //         // Clear the memory pool of all solutions and transactions.
        //         trace!("Clearing the memory pool...");
        //         // beacon.consensus.clear_memory_pool()?;
        //         trace!("Cleared the memory pool");
        //         bail!("Proposed an invalid block: {error}")
        //     }

        //     // Advance to the next block.
        //     match beacon.consensus.advance_to_next_block(&next_block) {
        //         Ok(()) => {
        //             // If the beacon produced a transaction, save its output records.
        //             if let Some(transaction) = beacon_transaction {
        //                 // Save the unspent records.
        //                 if let Err(error) = transaction.into_transitions().try_for_each(|transition| {
        //                     for (commitment, record) in transition.into_records() {
        //                         let record = record.decrypt(beacon.account.view_key())?;
        //
        //                         if let Some(Entry::Private(Plaintext::Literal(Literal::U64(amount), _))) =
        //                             record.data().get(&Identifier::from_str("microcredits")?)
        //                         {
        //                             if !amount.is_zero() {
        //                                 beacon.unspent_records.write().insert(commitment, record);
        //                             }
        //                         }
        //                     }
        //                     Ok::<_, anyhow::Error>(())
        //                 }) {
        //                     warn!("Unable to save the beacon unspent records, recomputing unspent records: {error}");
        //                     // Recompute the unspent records.
        //                     *beacon.unspent_records.write() =
        //                         beacon.ledger.find_unspent_credits_records(beacon.account.view_key())?;
        //                 };
        //             }
        //             // Log the next block.
        //             match serde_json::to_string_pretty(&next_block.header()) {
        //                 Ok(header) => info!("Block {}: {header}", next_block.height()),
        //                 Err(error) => info!("Block {}: (serde failed: {error})", next_block.height()),
        //             }
        //         }
        //         Err(error) => {
        //             // Clear the memory pool of all solutions and transactions.
        //             trace!("Clearing the memory pool...");
        //             // beacon.consensus.clear_memory_pool()?;
        //             trace!("Cleared the memory pool");
        //             bail!("Failed to advance to the next block: {error}")
        //         }
        //     }
        //
        //     Ok(next_block)
        // })
        // .await
        // {
        //     Ok(Ok(next_block)) => next_block,
        //     Ok(Err(error)) => {
        //         // Sleep for one second.
        //         tokio::time::sleep(Duration::from_secs(1)).await;
        //         bail!("Failed to propose the next block: {error}")
        //     }
        //     Err(error) => {
        //         // Sleep for one second.
        //         tokio::time::sleep(Duration::from_secs(1)).await;
        //         bail!("Failed to propose the next block (JoinError): {error}")
        //     }
        // };

        // // Ensure the block is a valid next block.
        // if let Err(error) = self.consensus.check_next_block(&next_block) {
        //     // Clear the memory pool of all solutions and transactions.
        //     trace!("Clearing the memory pool...");
        //     self.consensus.clear_memory_pool()?;
        //     trace!("Cleared the memory pool");
        //     // Sleep for one second.
        //     tokio::time::sleep(Duration::from_secs(1)).await;
        //     bail!("Proposed an invalid block: {error}")
        // }
        //
        // // Advance to the next block.
        // match self.consensus.advance_to_next_block(&next_block) {
        //     Ok(()) => match serde_json::to_string_pretty(&next_block.header()) {
        //         Ok(header) => info!("Block {next_block_height}: {header}"),
        //         Err(error) => info!("Block {next_block_height}: (serde failed: {error})"),
        //     },
        //     Err(error) => {
        //         // Clear the memory pool of all solutions and transactions.
        //         trace!("Clearing the memory pool...");
        //         self.consensus.clear_memory_pool()?;
        //         trace!("Cleared the memory pool");
        //         // Sleep for one second.
        //         tokio::time::sleep(Duration::from_secs(1)).await;
        //         bail!("Failed to advance to the next block: {error}")
        //     }
        // }

        // // Prepare the message.
        // let next_block_round = next_block.round();
        // let next_block_height = next_block.height();
        // let next_block_hash = next_block.hash();
        //
        // // Serialize the block ahead of time to not do it for each peer.
        // let serialized_block = match Data::Object(next_block).serialize().await {
        //     Ok(serialized_block) => Data::Buffer(serialized_block),
        //     Err(error) => bail!("Failed to serialize the next block for propagation: {error}"),
        // };
        //
        // // Prepare the block to be sent to all peers.
        // let message = Message::<N>::BeaconPropose(BeaconPropose::new(
        //     next_block_round,
        //     next_block_height,
        //     next_block_hash,
        //     serialized_block,
        // ));
        //
        // // Propagate the block to all beacons.
        // self.propagate_to_beacons(message, &[]);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkvm::prelude::{
        store::{helpers::memory::ConsensusMemory, ConsensusStore},
        Testnet3,
        VM,
    };

    use rand::SeedableRng;
    use rand_chacha::ChaChaRng;

    type CurrentNetwork = Testnet3;

    /// Use `RUST_MIN_STACK=67108864 cargo test --release profiler --features timer` to run this test.
    #[ignore]
    #[tokio::test]
    async fn test_profiler() -> Result<()> {
        // Specify the node attributes.
        let node = SocketAddr::from_str("0.0.0.0:4133").unwrap();
        let rest = SocketAddr::from_str("0.0.0.0:3033").unwrap();
        let dev = Some(0);

        // Initialize an (insecure) fixed RNG.
        let mut rng = ChaChaRng::seed_from_u64(1234567890u64);
        // Initialize the beacon account.
        let beacon_account = Account::<CurrentNetwork>::new(&mut rng).unwrap();
        // Initialize a new VM.
        let vm = VM::from(ConsensusStore::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>::open(None)?)?;
        // Initialize the genesis block.
        let genesis = vm.genesis(beacon_account.private_key(), &mut rng)?;

        println!("Initializing beacon node...");

        let beacon = Beacon::<CurrentNetwork, ConsensusMemory<CurrentNetwork>>::new(
            node,
            Some(rest),
            None,
            beacon_account,
            &[],
            genesis,
            None,
            dev,
        )
        .await
        .unwrap();

        println!(
            "Loaded beacon node with {} blocks",
            beacon.ledger.latest_height(),
            //     beacon.unspent_records.read().len()
        );

        bail!("\n\nRemember to #[ignore] this test!\n\n")
    }
}
