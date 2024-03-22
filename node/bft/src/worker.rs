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

use crate::{
    events::{Event, TransmissionRequest, TransmissionResponse},
    helpers::{fmt_id, max_redundant_requests, Pending, Ready, Storage, WorkerReceiver},
    spawn_blocking,
    ProposedBatch,
    Transport,
    MAX_FETCH_TIMEOUT_IN_MS,
    MAX_WORKERS,
};
use snarkos_node_bft_ledger_service::LedgerService;
use snarkvm::{
    console::prelude::*,
    ledger::{
        block::Transaction,
        coinbase::{ProverSolution, PuzzleCommitment},
        narwhal::{BatchHeader, Data, Transmission, TransmissionID},
    },
};

use indexmap::{IndexMap, IndexSet};
use parking_lot::Mutex;
use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{sync::oneshot, task::JoinHandle, time::timeout};

#[derive(Clone)]
pub struct Worker<N: Network> {
    /// The worker ID.
    id: u8,
    /// The gateway.
    gateway: Arc<dyn Transport<N>>,
    /// The storage.
    storage: Storage<N>,
    /// The ledger service.
    ledger: Arc<dyn LedgerService<N>>,
    /// The proposed batch.
    proposed_batch: Arc<ProposedBatch<N>>,
    /// The ready queue.
    ready: Ready<N>,
    /// The pending transmissions queue.
    pending: Arc<Pending<TransmissionID<N>, Transmission<N>>>,
    /// The spawned handles.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl<N: Network> Worker<N> {
    /// Initializes a new worker instance.
    pub fn new(
        id: u8,
        gateway: Arc<dyn Transport<N>>,
        storage: Storage<N>,
        ledger: Arc<dyn LedgerService<N>>,
        proposed_batch: Arc<ProposedBatch<N>>,
    ) -> Result<Self> {
        // Ensure the worker ID is valid.
        ensure!(id < MAX_WORKERS, "Invalid worker ID '{id}'");
        // Return the worker.
        Ok(Self {
            id,
            gateway,
            storage,
            ledger,
            proposed_batch,
            ready: Default::default(),
            pending: Default::default(),
            handles: Default::default(),
        })
    }

    /// Run the worker instance.
    pub fn run(&self, receiver: WorkerReceiver<N>) {
        info!("Starting worker instance {} of the memory pool...", self.id);
        // Start the worker handlers.
        self.start_handlers(receiver);
    }

    /// Returns the worker ID.
    pub const fn id(&self) -> u8 {
        self.id
    }

    /// Returns a reference to the pending transmissions queue.
    pub fn pending(&self) -> &Arc<Pending<TransmissionID<N>, Transmission<N>>> {
        &self.pending
    }
}

impl<N: Network> Worker<N> {
    /// The maximum number of transmissions allowed in a worker.
    pub const MAX_TRANSMISSIONS_PER_WORKER: usize =
        BatchHeader::<N>::MAX_TRANSMISSIONS_PER_BATCH / MAX_WORKERS as usize;
    /// The maximum number of transmissions allowed in a worker ping.
    pub const MAX_TRANSMISSIONS_PER_WORKER_PING: usize = BatchHeader::<N>::MAX_TRANSMISSIONS_PER_BATCH / 10;

    // transmissions

    /// Returns the number of transmissions in the ready queue.
    pub fn num_transmissions(&self) -> usize {
        self.ready.num_transmissions()
    }

    /// Returns the number of ratifications in the ready queue.
    pub fn num_ratifications(&self) -> usize {
        self.ready.num_ratifications()
    }

    /// Returns the number of solutions in the ready queue.
    pub fn num_solutions(&self) -> usize {
        self.ready.num_solutions()
    }

    /// Returns the number of transactions in the ready queue.
    pub fn num_transactions(&self) -> usize {
        self.ready.num_transactions()
    }
}

impl<N: Network> Worker<N> {
    /// Returns the transmission IDs in the ready queue.
    pub fn transmission_ids(&self) -> IndexSet<TransmissionID<N>> {
        self.ready.transmission_ids()
    }

    /// Returns the transmissions in the ready queue.
    pub fn transmissions(&self) -> IndexMap<TransmissionID<N>, Transmission<N>> {
        self.ready.transmissions()
    }

    /// Returns the solutions in the ready queue.
    pub fn solutions(&self) -> impl '_ + Iterator<Item = (PuzzleCommitment<N>, Data<ProverSolution<N>>)> {
        self.ready.solutions()
    }

    /// Returns the transactions in the ready queue.
    pub fn transactions(&self) -> impl '_ + Iterator<Item = (N::TransactionID, Data<Transaction<N>>)> {
        self.ready.transactions()
    }
}

impl<N: Network> Worker<N> {
    /// Returns `true` if the transmission ID exists in the ready queue, proposed batch, storage, or ledger.
    pub fn contains_transmission(&self, transmission_id: impl Into<TransmissionID<N>>) -> bool {
        let transmission_id = transmission_id.into();
        // Check if the transmission ID exists in the ready queue, proposed batch, storage, or ledger.
        self.ready.contains(transmission_id)
            || self.proposed_batch.read().as_ref().map_or(false, |p| p.contains_transmission(transmission_id))
            || self.storage.contains_transmission(transmission_id)
            || self.ledger.contains_transmission(&transmission_id).unwrap_or(false)
    }

    /// Returns the transmission if it exists in the ready queue, proposed batch, storage.
    ///
    /// Note: We explicitly forbid retrieving a transmission from the ledger, as transmissions
    /// in the ledger are not guaranteed to be invalid for the current batch.
    pub fn get_transmission(&self, transmission_id: TransmissionID<N>) -> Option<Transmission<N>> {
        // Check if the transmission ID exists in the ready queue.
        if let Some(transmission) = self.ready.get(transmission_id) {
            return Some(transmission);
        }
        // Check if the transmission ID exists in storage.
        if let Some(transmission) = self.storage.get_transmission(transmission_id) {
            return Some(transmission);
        }
        // Check if the transmission ID exists in the proposed batch.
        if let Some(transmission) =
            self.proposed_batch.read().as_ref().and_then(|p| p.get_transmission(transmission_id))
        {
            return Some(transmission.clone());
        }
        None
    }

    /// Returns the transmissions if it exists in the worker, or requests it from the specified peer.
    pub async fn get_or_fetch_transmission(
        &self,
        peer_ip: SocketAddr,
        transmission_id: TransmissionID<N>,
    ) -> Result<(TransmissionID<N>, Transmission<N>)> {
        // Attempt to get the transmission from the worker.
        if let Some(transmission) = self.get_transmission(transmission_id) {
            return Ok((transmission_id, transmission));
        }
        // Send a transmission request to the peer.
        let (candidate_id, transmission) = self.send_transmission_request(peer_ip, transmission_id).await?;
        // Ensure the transmission ID matches.
        ensure!(candidate_id == transmission_id, "Invalid transmission ID");
        // Return the transmission.
        Ok((transmission_id, transmission))
    }

    /// Removes up to the specified number of transmissions from the ready queue, and returns them.
    pub(crate) fn drain(&self, num_transmissions: usize) -> impl Iterator<Item = (TransmissionID<N>, Transmission<N>)> {
        self.ready.drain(num_transmissions).into_iter()
    }

    /// Reinserts the specified transmission into the ready queue.
    pub(crate) fn reinsert(&self, transmission_id: TransmissionID<N>, transmission: Transmission<N>) -> bool {
        // Check if the transmission ID exists.
        if !self.contains_transmission(transmission_id) {
            // Insert the transmission into the ready queue.
            return self.ready.insert(transmission_id, transmission);
        }
        false
    }

    /// Broadcasts a worker ping event.
    pub(crate) fn broadcast_ping(&self) {
        // Retrieve the transmission IDs.
        let transmission_ids = self
            .ready
            .transmission_ids()
            .into_iter()
            .take(Self::MAX_TRANSMISSIONS_PER_WORKER_PING)
            .collect::<IndexSet<_>>();

        // Broadcast the ping event.
        if !transmission_ids.is_empty() {
            self.gateway.broadcast(Event::WorkerPing(transmission_ids.into()));
        }
    }
}

impl<N: Network> Worker<N> {
    /// Handles the incoming transmission ID from a worker ping event.
    fn process_transmission_id_from_ping(&self, peer_ip: SocketAddr, transmission_id: TransmissionID<N>) {
        // Check if the transmission ID exists.
        if self.contains_transmission(transmission_id) {
            return;
        }
        // If the ready queue is full, then skip this transmission.
        // Note: We must prioritize the unconfirmed solutions and unconfirmed transactions, not transmissions.
        if self.ready.num_transmissions() > Self::MAX_TRANSMISSIONS_PER_WORKER {
            return;
        }
        // Attempt to fetch the transmission from the peer.
        let self_ = self.clone();
        tokio::spawn(async move {
            // Send a transmission request to the peer.
            match self_.send_transmission_request(peer_ip, transmission_id).await {
                // If the transmission was fetched, then process it.
                Ok((candidate_id, transmission)) => {
                    // Ensure the transmission ID matches.
                    if candidate_id == transmission_id {
                        // Insert the transmission into the ready queue.
                        // Note: This method checks `contains_transmission` again, because by the time the transmission is fetched,
                        // it could have already been inserted into the ready queue.
                        self_.process_transmission_from_peer(peer_ip, transmission_id, transmission);
                    }
                }
                // If the transmission was not fetched, then attempt to fetch it again.
                Err(e) => {
                    warn!(
                        "Worker {} - Failed to fetch transmission '{}' from '{peer_ip}' (ping) - {e}",
                        self_.id,
                        fmt_id(transmission_id)
                    );
                }
            }
        });
    }

    /// Handles the incoming transmission from a peer.
    pub(crate) fn process_transmission_from_peer(
        &self,
        peer_ip: SocketAddr,
        transmission_id: TransmissionID<N>,
        transmission: Transmission<N>,
    ) {
        // If the transmission ID already exists, then do not store it.
        if self.contains_transmission(transmission_id) {
            return;
        }
        // Ensure the transmission ID and transmission type matches.
        let is_well_formed = match (&transmission_id, &transmission) {
            (TransmissionID::Solution(_), Transmission::Solution(_)) => true,
            (TransmissionID::Transaction(_), Transmission::Transaction(_)) => true,
            // Note: We explicitly forbid inserting ratifications into the ready queue,
            // as the protocol currently does not support ratifications.
            (TransmissionID::Ratification, Transmission::Ratification) => false,
            // All other combinations are clearly invalid.
            _ => false,
        };
        // If the transmission ID and transmission type matches, then insert the transmission into the ready queue.
        if is_well_formed && self.ready.insert(transmission_id, transmission) {
            trace!("Worker {} - Added transmission '{}' from '{peer_ip}'", self.id, fmt_id(transmission_id));
        }
    }

    /// Handles the incoming unconfirmed solution.
    /// Note: This method assumes the incoming solution is valid and does not exist in the ledger.
    pub(crate) async fn process_unconfirmed_solution(
        &self,
        puzzle_commitment: PuzzleCommitment<N>,
        prover_solution: Data<ProverSolution<N>>,
    ) -> Result<()> {
        // Construct the transmission.
        let transmission = Transmission::Solution(prover_solution.clone());
        // Remove the puzzle commitment from the pending queue.
        self.pending.remove(puzzle_commitment, Some(transmission.clone()));
        // Check if the solution exists.
        if self.contains_transmission(puzzle_commitment) {
            bail!("Solution '{}' already exists.", fmt_id(puzzle_commitment));
        }
        // Check that the solution is well-formed and unique.
        if let Err(e) = self.ledger.check_solution_basic(puzzle_commitment, prover_solution).await {
            bail!("Invalid unconfirmed solution '{}': {e}", fmt_id(puzzle_commitment));
        }
        // Adds the prover solution to the ready queue.
        if self.ready.insert(puzzle_commitment, transmission) {
            trace!("Worker {} - Added unconfirmed solution '{}'", self.id, fmt_id(puzzle_commitment));
        }
        Ok(())
    }

    /// Handles the incoming unconfirmed transaction.
    pub(crate) async fn process_unconfirmed_transaction(
        &self,
        transaction_id: N::TransactionID,
        transaction: Data<Transaction<N>>,
    ) -> Result<()> {
        // Construct the transmission.
        let transmission = Transmission::Transaction(transaction.clone());
        // Remove the transaction from the pending queue.
        self.pending.remove(&transaction_id, Some(transmission.clone()));
        // Check if the transaction ID exists.
        if self.contains_transmission(&transaction_id) {
            bail!("Transaction '{}' already exists.", fmt_id(transaction_id));
        }
        // Check that the transaction is well-formed and unique.
        if let Err(e) = self.ledger.check_transaction_basic(transaction_id, transaction).await {
            bail!("Invalid unconfirmed transaction '{}': {e}", fmt_id(transaction_id));
        }
        // Adds the transaction to the ready queue.
        if self.ready.insert(&transaction_id, transmission) {
            trace!("Worker {} - Added unconfirmed transaction '{}'", self.id, fmt_id(transaction_id));
        }
        Ok(())
    }
}

impl<N: Network> Worker<N> {
    /// Starts the worker handlers.
    fn start_handlers(&self, receiver: WorkerReceiver<N>) {
        let WorkerReceiver { mut rx_worker_ping, mut rx_transmission_request, mut rx_transmission_response } = receiver;

        // Start the pending queue expiration loop.
        let self_ = self.clone();
        self.spawn(async move {
            loop {
                // Sleep briefly.
                tokio::time::sleep(Duration::from_millis(MAX_FETCH_TIMEOUT_IN_MS)).await;

                // Remove the expired pending certificate requests.
                self_.pending.clear_expired_callbacks();
            }
        });

        // Process the ping events.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, transmission_id)) = rx_worker_ping.recv().await {
                self_.process_transmission_id_from_ping(peer_ip, transmission_id);
            }
        });

        // Process the transmission requests.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, transmission_request)) = rx_transmission_request.recv().await {
                self_.send_transmission_response(peer_ip, transmission_request);
            }
        });

        // Process the transmission responses.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, transmission_response)) = rx_transmission_response.recv().await {
                // Process the transmission response.
                let self__ = self_.clone();
                let _ = spawn_blocking!({
                    self__.finish_transmission_request(peer_ip, transmission_response);
                    Ok(())
                });
            }
        });
    }

    /// Sends a transmission request to the specified peer.
    async fn send_transmission_request(
        &self,
        peer_ip: SocketAddr,
        transmission_id: TransmissionID<N>,
    ) -> Result<(TransmissionID<N>, Transmission<N>)> {
        // Initialize a oneshot channel.
        let (callback_sender, callback_receiver) = oneshot::channel();
        // Determine how many sent requests are pending.
        let num_sent_requests = self.pending.num_sent_requests(transmission_id);
        // Determine the maximum number of redundant requests.
        let num_redundant_requests = max_redundant_requests(self.ledger.clone(), self.storage.current_round());
        // Determine if we should send a transmission request to the peer.
        let should_send_request = num_sent_requests < num_redundant_requests;

        // Insert the transmission ID into the pending queue.
        self.pending.insert(transmission_id, peer_ip, Some((callback_sender, should_send_request)));

        // If the number of requests is less than or equal to the the redundancy factor, send the transmission request to the peer.
        if should_send_request {
            // Send the transmission request to the peer.
            if self.gateway.send(peer_ip, Event::TransmissionRequest(transmission_id.into())).await.is_none() {
                bail!("Unable to fetch transmission - failed to send request")
            }
        } else {
            debug!(
                "Skipped sending request for transmission {} to '{peer_ip}' ({num_sent_requests} redundant requests)",
                fmt_id(transmission_id)
            );
        }
        // Wait for the transmission to be fetched.
        match timeout(Duration::from_millis(MAX_FETCH_TIMEOUT_IN_MS), callback_receiver).await {
            // If the transmission was fetched, return it.
            Ok(result) => Ok((transmission_id, result?)),
            // If the transmission was not fetched, return an error.
            Err(e) => bail!("Unable to fetch transmission - (timeout) {e}"),
        }
    }

    /// Handles the incoming transmission response.
    /// This method ensures the transmission response is well-formed and matches the transmission ID.
    fn finish_transmission_request(&self, peer_ip: SocketAddr, response: TransmissionResponse<N>) {
        let TransmissionResponse { transmission_id, mut transmission } = response;
        // Check if the peer IP exists in the pending queue for the given transmission ID.
        let exists = self.pending.get_peers(transmission_id).unwrap_or_default().contains(&peer_ip);
        // If the peer IP exists, finish the pending request.
        if exists {
            // Ensure the transmission is not a fee and matches the transmission ID.
            match self.ledger.ensure_transmission_is_well_formed(transmission_id, &mut transmission) {
                Ok(()) => {
                    // Remove the transmission ID from the pending queue.
                    self.pending.remove(transmission_id, Some(transmission));
                }
                Err(err) => warn!("Failed to finish transmission response from peer '{peer_ip}': {err}"),
            };
        }
    }

    /// Sends the requested transmission to the specified peer.
    fn send_transmission_response(&self, peer_ip: SocketAddr, request: TransmissionRequest<N>) {
        let TransmissionRequest { transmission_id } = request;
        // Attempt to retrieve the transmission.
        if let Some(transmission) = self.get_transmission(transmission_id) {
            // Send the transmission response to the peer.
            let self_ = self.clone();
            tokio::spawn(async move {
                self_.gateway.send(peer_ip, Event::TransmissionResponse((transmission_id, transmission).into())).await;
            });
        }
    }

    /// Spawns a task with the given future; it should only be used for long-running tasks.
    fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        self.handles.lock().push(tokio::spawn(future));
    }

    /// Shuts down the worker.
    pub(crate) fn shut_down(&self) {
        trace!("Shutting down worker {}...", self.id);
        // Abort the tasks.
        self.handles.lock().iter().for_each(|handle| handle.abort());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkos_node_bft_ledger_service::LedgerService;
    use snarkos_node_bft_storage_service::BFTMemoryService;
    use snarkvm::{
        console::{network::Network, types::Field},
        ledger::{
            block::Block,
            committee::Committee,
            narwhal::{BatchCertificate, Subdag, Transmission, TransmissionID},
        },
        prelude::Address,
    };

    use bytes::Bytes;
    use indexmap::IndexMap;
    use mockall::mock;
    use std::{io, ops::Range};

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    const ITERATIONS: usize = 100;

    mock! {
        Gateway<N: Network> {}
        #[async_trait]
        impl<N:Network> Transport<N> for Gateway<N> {
            fn broadcast(&self, event: Event<N>);
            async fn send(&self, peer_ip: SocketAddr, event: Event<N>) -> Option<oneshot::Receiver<io::Result<()>>>;
        }
    }

    mock! {
        #[derive(Debug)]
        Ledger<N: Network> {}
        #[async_trait]
        impl<N: Network> LedgerService<N> for Ledger<N> {
            fn latest_round(&self) -> u64;
            fn latest_block_height(&self) -> u32;
            fn latest_block(&self) -> Block<N>;
            fn latest_leader(&self) -> Option<(u64, Address<N>)>;
            fn update_latest_leader(&self, round: u64, leader: Address<N>);
            fn contains_block_height(&self, height: u32) -> bool;
            fn get_block_height(&self, hash: &N::BlockHash) -> Result<u32>;
            fn get_block_hash(&self, height: u32) -> Result<N::BlockHash>;
            fn get_block_round(&self, height: u32) -> Result<u64>;
            fn get_block(&self, height: u32) -> Result<Block<N>>;
            fn get_blocks(&self, heights: Range<u32>) -> Result<Vec<Block<N>>>;
            fn get_solution(&self, solution_id: &PuzzleCommitment<N>) -> Result<ProverSolution<N>>;
            fn get_unconfirmed_transaction(&self, transaction_id: N::TransactionID) -> Result<Transaction<N>>;
            fn get_batch_certificate(&self, certificate_id: &Field<N>) -> Result<BatchCertificate<N>>;
            fn current_committee(&self) -> Result<Committee<N>>;
            fn get_committee_for_round(&self, round: u64) -> Result<Committee<N>>;
            fn get_committee_lookback_for_round(&self, round: u64) -> Result<Committee<N>>;
            fn contains_certificate(&self, certificate_id: &Field<N>) -> Result<bool>;
            fn contains_transmission(&self, transmission_id: &TransmissionID<N>) -> Result<bool>;
            fn ensure_transmission_is_well_formed(
                &self,
                transmission_id: TransmissionID<N>,
                transmission: &mut Transmission<N>,
            ) -> Result<()>;
            async fn check_solution_basic(
                &self,
                puzzle_commitment: PuzzleCommitment<N>,
                solution: Data<ProverSolution<N>>,
            ) -> Result<()>;
            async fn check_transaction_basic(
                &self,
                transaction_id: N::TransactionID,
                transaction: Data<Transaction<N>>,
            ) -> Result<()>;
            fn check_next_block(&self, block: &Block<N>) -> Result<()>;
            fn prepare_advance_to_next_quorum_block(
                &self,
                subdag: Subdag<N>,
                transmissions: IndexMap<TransmissionID<N>, Transmission<N>>,
            ) -> Result<Block<N>>;
            fn advance_to_next_block(&self, block: &Block<N>) -> Result<()>;
        }
    }

    #[tokio::test]
    async fn test_max_redundant_requests() {
        const NUM_NODES: u16 = Committee::<CurrentNetwork>::MAX_COMMITTEE_SIZE;

        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee =
            snarkvm::ledger::committee::test_helpers::sample_committee_for_round_and_size(0, NUM_NODES, rng);
        let committee_clone = committee.clone();
        // Setup the mock ledger.
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_solution_basic().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);

        // Ensure the maximum number of redundant requests is correct and consistent across iterations.
        assert_eq!(max_redundant_requests(ledger, 0), 34, "Update me if the formula changes");
    }

    #[tokio::test]
    async fn test_process_transmission() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let gateway = MockGateway::default();
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_solution_basic().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1);

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let data = |rng: &mut TestRng| Data::Buffer(Bytes::from((0..512).map(|_| rng.gen::<u8>()).collect::<Vec<_>>()));
        let transmission_id = TransmissionID::Solution(PuzzleCommitment::from_g1_affine(rng.gen()));
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let transmission = Transmission::Solution(data(rng));

        // Process the transmission.
        worker.process_transmission_from_peer(peer_ip, transmission_id, transmission.clone());
        assert!(worker.contains_transmission(transmission_id));
        assert!(worker.ready.contains(transmission_id));
        assert_eq!(worker.get_transmission(transmission_id), Some(transmission));
        // Take the transmission from the ready set.
        let transmission: Vec<_> = worker.drain(1).collect();
        assert_eq!(transmission.len(), 1);
        assert!(!worker.ready.contains(transmission_id));
    }

    #[tokio::test]
    async fn test_send_transmission() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_ensure_transmission_is_well_formed().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1);

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let transmission_id = TransmissionID::Solution(PuzzleCommitment::from_g1_affine(rng.gen()));
        let worker_ = worker.clone();
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
        assert!(worker.pending.contains(transmission_id));
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        // Fake the transmission response.
        worker.finish_transmission_request(peer_ip, TransmissionResponse {
            transmission_id,
            transmission: Transmission::Solution(Data::Buffer(Bytes::from(vec![0; 512]))),
        });
        // Check the transmission was removed from the pending set.
        assert!(!worker.pending.contains(transmission_id));
    }

    #[tokio::test]
    async fn test_process_solution_ok() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_solution_basic().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1);

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let puzzle = PuzzleCommitment::from_g1_affine(rng.gen());
        let transmission_id = TransmissionID::Solution(puzzle);
        let worker_ = worker.clone();
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
        assert!(worker.pending.contains(transmission_id));
        let result = worker
            .process_unconfirmed_solution(
                puzzle,
                Data::Buffer(Bytes::from((0..512).map(|_| rng.gen::<u8>()).collect::<Vec<_>>())),
            )
            .await;
        assert!(result.is_ok());
        assert!(!worker.pending.contains(transmission_id));
        assert!(worker.ready.contains(puzzle));
    }

    #[tokio::test]
    async fn test_process_solution_nok() {
        let rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_solution_basic().returning(|_, _| Err(anyhow!("")));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1);

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let puzzle = PuzzleCommitment::from_g1_affine(rng.gen());
        let transmission_id = TransmissionID::Solution(puzzle);
        let worker_ = worker.clone();
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
        assert!(worker.pending.contains(transmission_id));
        let result = worker
            .process_unconfirmed_solution(
                puzzle,
                Data::Buffer(Bytes::from((0..512).map(|_| rng.gen::<u8>()).collect::<Vec<_>>())),
            )
            .await;
        assert!(result.is_err());
        assert!(!worker.pending.contains(puzzle));
        assert!(!worker.ready.contains(puzzle));
    }

    #[tokio::test]
    async fn test_process_transaction_ok() {
        let mut rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_transaction_basic().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1);

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let transaction_id: <CurrentNetwork as Network>::TransactionID = Field::<CurrentNetwork>::rand(&mut rng).into();
        let transmission_id = TransmissionID::Transaction(transaction_id);
        let worker_ = worker.clone();
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
        assert!(worker.pending.contains(transmission_id));
        let result = worker
            .process_unconfirmed_transaction(
                transaction_id,
                Data::Buffer(Bytes::from((0..512).map(|_| rng.gen::<u8>()).collect::<Vec<_>>())),
            )
            .await;
        assert!(result.is_ok());
        assert!(!worker.pending.contains(transmission_id));
        assert!(worker.ready.contains(transmission_id));
    }

    #[tokio::test]
    async fn test_process_transaction_nok() {
        let mut rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_transaction_basic().returning(|_, _| Err(anyhow!("")));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1);

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let transaction_id: <CurrentNetwork as Network>::TransactionID = Field::<CurrentNetwork>::rand(&mut rng).into();
        let transmission_id = TransmissionID::Transaction(transaction_id);
        let worker_ = worker.clone();
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));
        let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
        assert!(worker.pending.contains(transmission_id));
        let result = worker
            .process_unconfirmed_transaction(
                transaction_id,
                Data::Buffer(Bytes::from((0..512).map(|_| rng.gen::<u8>()).collect::<Vec<_>>())),
            )
            .await;
        assert!(result.is_err());
        assert!(!worker.pending.contains(transmission_id));
        assert!(!worker.ready.contains(transmission_id));
    }

    #[tokio::test]
    async fn test_flood_transmission_requests() {
        let mut rng = &mut TestRng::default();
        // Sample a committee.
        let committee = snarkvm::ledger::committee::test_helpers::sample_committee(rng);
        let committee_clone = committee.clone();
        // Setup the mock gateway and ledger.
        let mut gateway = MockGateway::default();
        gateway.expect_send().returning(|_, _| {
            let (_tx, rx) = oneshot::channel();
            Some(rx)
        });
        let mut mock_ledger = MockLedger::default();
        mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));
        mock_ledger.expect_get_committee_lookback_for_round().returning(move |_| Ok(committee_clone.clone()));
        mock_ledger.expect_contains_transmission().returning(|_| Ok(false));
        mock_ledger.expect_check_transaction_basic().returning(|_, _| Ok(()));
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
        // Initialize the storage.
        let storage = Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 1);

        // Create the Worker.
        let worker = Worker::new(0, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        let transaction_id: <CurrentNetwork as Network>::TransactionID = Field::<CurrentNetwork>::rand(&mut rng).into();
        let transmission_id = TransmissionID::Transaction(transaction_id);
        let peer_ip = SocketAddr::from(([127, 0, 0, 1], 1234));

        // Determine the number of redundant requests are sent.
        let num_redundant_requests = max_redundant_requests(worker.ledger.clone(), worker.storage.current_round());
        let num_flood_requests = num_redundant_requests * 10;
        // Flood the pending queue with transmission requests.
        for i in 1..=num_flood_requests {
            let worker_ = worker.clone();
            tokio::spawn(async move {
                let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
            // Check that the number of sent requests does not exceed the maximum number of redundant requests.
            assert!(worker.pending.num_sent_requests(transmission_id) <= num_redundant_requests);
            assert_eq!(worker.pending.num_callbacks(transmission_id), i);
        }
        // Check that the number of sent requests does not exceed the maximum number of redundant requests.
        assert_eq!(worker.pending.num_sent_requests(transmission_id), num_redundant_requests);
        assert_eq!(worker.pending.num_callbacks(transmission_id), num_flood_requests);

        // Let all the requests expire.
        tokio::time::sleep(Duration::from_millis(MAX_FETCH_TIMEOUT_IN_MS + 1000)).await;
        assert_eq!(worker.pending.num_sent_requests(transmission_id), 0);
        assert_eq!(worker.pending.num_callbacks(transmission_id), 0);

        // Flood the pending queue with transmission requests again.
        for i in 1..=num_flood_requests {
            let worker_ = worker.clone();
            tokio::spawn(async move {
                let _ = worker_.send_transmission_request(peer_ip, transmission_id).await;
            });
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert!(worker.pending.num_sent_requests(transmission_id) <= num_redundant_requests);
            assert_eq!(worker.pending.num_callbacks(transmission_id), i);
        }
        // Check that the number of sent requests does not exceed the maximum number of redundant requests.
        assert_eq!(worker.pending.num_sent_requests(transmission_id), num_redundant_requests);
        assert_eq!(worker.pending.num_callbacks(transmission_id), num_flood_requests);

        // Check that fulfilling a transmission request clears the pending queue.
        let result = worker
            .process_unconfirmed_transaction(
                transaction_id,
                Data::Buffer(Bytes::from((0..512).map(|_| rng.gen::<u8>()).collect::<Vec<_>>())),
            )
            .await;
        assert!(result.is_ok());
        assert_eq!(worker.pending.num_sent_requests(transmission_id), 0);
        assert_eq!(worker.pending.num_callbacks(transmission_id), 0);
        assert!(!worker.pending.contains(transmission_id));
        assert!(worker.ready.contains(transmission_id));
    }

    #[tokio::test]
    async fn test_storage_gc_on_initialization() {
        let rng = &mut TestRng::default();

        for _ in 0..ITERATIONS {
            // Mock the ledger round.
            let max_gc_rounds = rng.gen_range(50..=100);
            let latest_ledger_round = rng.gen_range((max_gc_rounds + 1)..1000);
            let expected_gc_round = latest_ledger_round - max_gc_rounds;

            // Sample a committee.
            let committee =
                snarkvm::ledger::committee::test_helpers::sample_committee_for_round(latest_ledger_round, rng);

            // Setup the mock gateway and ledger.
            let mut mock_ledger = MockLedger::default();
            mock_ledger.expect_current_committee().returning(move || Ok(committee.clone()));

            let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(mock_ledger);
            // Initialize the storage.
            let storage =
                Storage::<CurrentNetwork>::new(ledger.clone(), Arc::new(BFTMemoryService::new()), max_gc_rounds);

            // Ensure that the storage GC round is correct.
            assert_eq!(storage.gc_round(), expected_gc_round);
        }
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use crate::Gateway;
    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkvm::{
        console::account::Address,
        ledger::committee::{Committee, MIN_VALIDATOR_STAKE},
    };

    use test_strategy::proptest;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    // Initializes a new test committee.
    fn new_test_committee(n: u16) -> Committee<CurrentNetwork> {
        let mut members = IndexMap::with_capacity(n as usize);
        for i in 0..n {
            // Sample the address.
            let rng = &mut TestRng::fixed(i as u64);
            let address = Address::new(rng.gen());
            info!("Validator {i}: {address}");
            members.insert(address, (MIN_VALIDATOR_STAKE, false));
        }
        // Initialize the committee.
        Committee::<CurrentNetwork>::new(1u64, members).unwrap()
    }

    #[proptest]
    fn worker_initialization(
        #[strategy(0..MAX_WORKERS)] id: u8,
        gateway: Gateway<CurrentNetwork>,
        storage: Storage<CurrentNetwork>,
    ) {
        let committee = new_test_committee(4);
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(MockLedgerService::new(committee));
        let worker = Worker::new(id, Arc::new(gateway), storage, ledger, Default::default()).unwrap();
        assert_eq!(worker.id(), id);
    }

    #[proptest]
    fn invalid_worker_id(
        #[strategy(MAX_WORKERS..)] id: u8,
        gateway: Gateway<CurrentNetwork>,
        storage: Storage<CurrentNetwork>,
    ) {
        let committee = new_test_committee(4);
        let ledger: Arc<dyn LedgerService<CurrentNetwork>> = Arc::new(MockLedgerService::new(committee));
        let worker = Worker::new(id, Arc::new(gateway), storage, ledger, Default::default());
        // TODO once Worker implements Debug, simplify this with `unwrap_err`
        if let Err(error) = worker {
            assert_eq!(error.to_string(), format!("Invalid worker ID '{}'", id));
        }
    }
}
