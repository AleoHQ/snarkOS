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
    events::{BatchPropose, BatchSignature, Event},
    helpers::{
        assign_to_worker,
        assign_to_workers,
        fmt_id,
        init_sync_channels,
        init_worker_channels,
        now,
        BFTSender,
        PrimaryReceiver,
        PrimarySender,
        Proposal,
        Storage,
    },
    spawn_blocking,
    Gateway,
    Sync,
    Transport,
    Worker,
    MAX_BATCH_DELAY_IN_MS,
    MAX_WORKERS,
    PRIMARY_PING_IN_MS,
    WORKER_PING_IN_MS,
};
use snarkos_account::Account;
use snarkos_node_bft_events::PrimaryPing;
use snarkos_node_bft_ledger_service::LedgerService;
use snarkos_node_sync::MAX_BLOCKS_BEHIND;
use snarkvm::{
    console::{
        account::Signature,
        prelude::*,
        types::{Address, Field},
    },
    ledger::{
        block::Transaction,
        narwhal::{BatchCertificate, BatchHeader, Data, Transmission, TransmissionID},
        puzzle::{Solution, SolutionID},
    },
    prelude::committee::Committee,
};

use colored::Colorize;
use futures::stream::{FuturesUnordered, StreamExt};
use indexmap::{IndexMap, IndexSet};
use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex as TMutex, OnceCell},
    task::JoinHandle,
};

/// A helper type for an optional proposed batch.
pub type ProposedBatch<N> = RwLock<Option<Proposal<N>>>;

#[derive(Clone)]
pub struct Primary<N: Network> {
    /// The sync module.
    sync: Sync<N>,
    /// The gateway.
    gateway: Gateway<N>,
    /// The storage.
    storage: Storage<N>,
    /// The ledger service.
    ledger: Arc<dyn LedgerService<N>>,
    /// The workers.
    workers: Arc<[Worker<N>]>,
    /// The BFT sender.
    bft_sender: Arc<OnceCell<BFTSender<N>>>,
    /// The batch proposal, if the primary is currently proposing a batch.
    proposed_batch: Arc<ProposedBatch<N>>,
    /// The recently-signed batch proposals (a map from the address to the round, batch ID, and signature).
    signed_proposals: Arc<RwLock<HashMap<Address<N>, (u64, Field<N>, Signature<N>)>>>,
    /// The spawned handles.
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// The lock for propose_batch.
    propose_lock: Arc<TMutex<u64>>,
}

impl<N: Network> Primary<N> {
    /// The maximum number of unconfirmed transmissions to send to the primary.
    pub const MAX_TRANSMISSIONS_TOLERANCE: usize = BatchHeader::<N>::MAX_TRANSMISSIONS_PER_BATCH * 2;

    /// Initializes a new primary instance.
    pub fn new(
        account: Account<N>,
        storage: Storage<N>,
        ledger: Arc<dyn LedgerService<N>>,
        ip: Option<SocketAddr>,
        trusted_validators: &[SocketAddr],
        dev: Option<u16>,
    ) -> Result<Self> {
        // Initialize the gateway.
        let gateway = Gateway::new(account, storage.clone(), ledger.clone(), ip, trusted_validators, dev)?;
        // Initialize the sync module.
        let sync = Sync::new(gateway.clone(), storage.clone(), ledger.clone());
        // Initialize the primary instance.
        Ok(Self {
            sync,
            gateway,
            storage,
            ledger,
            workers: Arc::from(vec![]),
            bft_sender: Default::default(),
            proposed_batch: Default::default(),
            signed_proposals: Default::default(),
            handles: Default::default(),
            propose_lock: Default::default(),
        })
    }

    /// Run the primary instance.
    pub async fn run(
        &mut self,
        bft_sender: Option<BFTSender<N>>,
        primary_sender: PrimarySender<N>,
        primary_receiver: PrimaryReceiver<N>,
    ) -> Result<()> {
        info!("Starting the primary instance of the memory pool...");

        // Set the BFT sender.
        if let Some(bft_sender) = &bft_sender {
            // Set the BFT sender in the primary.
            self.bft_sender.set(bft_sender.clone()).expect("BFT sender already set");
        }

        // Construct a map of the worker senders.
        let mut worker_senders = IndexMap::new();
        // Construct a map for the workers.
        let mut workers = Vec::new();
        // Initialize the workers.
        for id in 0..MAX_WORKERS {
            // Construct the worker channels.
            let (tx_worker, rx_worker) = init_worker_channels();
            // Construct the worker instance.
            let worker = Worker::new(
                id,
                Arc::new(self.gateway.clone()),
                self.storage.clone(),
                self.ledger.clone(),
                self.proposed_batch.clone(),
            )?;
            // Run the worker instance.
            worker.run(rx_worker);
            // Add the worker to the list of workers.
            workers.push(worker);
            // Add the worker sender to the map.
            worker_senders.insert(id, tx_worker);
        }
        // Set the workers.
        self.workers = Arc::from(workers);

        // First, initialize the sync channels.
        let (sync_sender, sync_receiver) = init_sync_channels();
        // Next, initialize the sync module.
        self.sync.run(bft_sender, sync_receiver).await?;
        // Next, initialize the gateway.
        self.gateway.run(primary_sender, worker_senders, Some(sync_sender)).await;
        // Lastly, start the primary handlers.
        // Note: This ensures the primary does not start communicating before syncing is complete.
        self.start_handlers(primary_receiver);

        Ok(())
    }

    /// Returns the current round.
    pub fn current_round(&self) -> u64 {
        self.storage.current_round()
    }

    /// Returns `true` if the primary is synced.
    pub fn is_synced(&self) -> bool {
        self.sync.is_synced()
    }

    /// Returns the gateway.
    pub const fn gateway(&self) -> &Gateway<N> {
        &self.gateway
    }

    /// Returns the storage.
    pub const fn storage(&self) -> &Storage<N> {
        &self.storage
    }

    /// Returns the ledger.
    pub const fn ledger(&self) -> &Arc<dyn LedgerService<N>> {
        &self.ledger
    }

    /// Returns the number of workers.
    pub fn num_workers(&self) -> u8 {
        u8::try_from(self.workers.len()).expect("Too many workers")
    }

    /// Returns the workers.
    pub const fn workers(&self) -> &Arc<[Worker<N>]> {
        &self.workers
    }

    /// Returns the batch proposal of our primary, if one currently exists.
    pub fn proposed_batch(&self) -> &Arc<ProposedBatch<N>> {
        &self.proposed_batch
    }
}

impl<N: Network> Primary<N> {
    /// Returns the number of unconfirmed transmissions.
    pub fn num_unconfirmed_transmissions(&self) -> usize {
        self.workers.iter().map(|worker| worker.num_transmissions()).sum()
    }

    /// Returns the number of unconfirmed ratifications.
    pub fn num_unconfirmed_ratifications(&self) -> usize {
        self.workers.iter().map(|worker| worker.num_ratifications()).sum()
    }

    /// Returns the number of solutions.
    pub fn num_unconfirmed_solutions(&self) -> usize {
        self.workers.iter().map(|worker| worker.num_solutions()).sum()
    }

    /// Returns the number of unconfirmed transactions.
    pub fn num_unconfirmed_transactions(&self) -> usize {
        self.workers.iter().map(|worker| worker.num_transactions()).sum()
    }
}

impl<N: Network> Primary<N> {
    /// Returns the unconfirmed transmission IDs.
    pub fn unconfirmed_transmission_ids(&self) -> impl '_ + Iterator<Item = TransmissionID<N>> {
        self.workers.iter().flat_map(|worker| worker.transmission_ids())
    }

    /// Returns the unconfirmed transmissions.
    pub fn unconfirmed_transmissions(&self) -> impl '_ + Iterator<Item = (TransmissionID<N>, Transmission<N>)> {
        self.workers.iter().flat_map(|worker| worker.transmissions())
    }

    /// Returns the unconfirmed solutions.
    pub fn unconfirmed_solutions(&self) -> impl '_ + Iterator<Item = (SolutionID<N>, Data<Solution<N>>)> {
        self.workers.iter().flat_map(|worker| worker.solutions())
    }

    /// Returns the unconfirmed transactions.
    pub fn unconfirmed_transactions(&self) -> impl '_ + Iterator<Item = (N::TransactionID, Data<Transaction<N>>)> {
        self.workers.iter().flat_map(|worker| worker.transactions())
    }
}

impl<N: Network> Primary<N> {
    /// Proposes the batch for the current round.
    ///
    /// This method performs the following steps:
    /// 1. Drain the workers.
    /// 2. Sign the batch.
    /// 3. Set the batch proposal in the primary.
    /// 4. Broadcast the batch header to all validators for signing.
    pub async fn propose_batch(&self) -> Result<()> {
        // This function isn't re-entrant.
        let mut lock_guard = self.propose_lock.lock().await;

        // Check if the proposed batch has expired, and clear it if it has expired.
        if let Err(e) = self.check_proposed_batch_for_expiration().await {
            warn!("Failed to check the proposed batch for expiration - {e}");
            return Ok(());
        }

        // If there is a batch being proposed already,
        // rebroadcast the batch header to the non-signers, and return early.
        if let Some(proposal) = self.proposed_batch.read().as_ref() {
            // Construct the event.
            // TODO(ljedrz): the BatchHeader should be serialized only once in advance before being sent to non-signers.
            let event = Event::BatchPropose(proposal.batch_header().clone().into());
            // Iterate through the non-signers.
            for address in proposal.nonsigners(&self.ledger.get_committee_lookback_for_round(proposal.round())?) {
                // Resolve the address to the peer IP.
                match self.gateway.resolver().get_peer_ip_for_address(address) {
                    // Resend the batch proposal to the validator for signing.
                    Some(peer_ip) => {
                        let (gateway, event_, round) = (self.gateway.clone(), event.clone(), proposal.round());
                        tokio::spawn(async move {
                            debug!("Resending batch proposal for round {round} to peer '{peer_ip}'");
                            // Resend the batch proposal to the peer.
                            if gateway.send(peer_ip, event_).await.is_none() {
                                warn!("Failed to resend batch proposal for round {round} to peer '{peer_ip}'");
                            }
                        });
                    }
                    None => continue,
                }
            }
            debug!("Proposed batch for round {} is still valid", proposal.round());
            return Ok(());
        }

        // Retrieve the current round.
        let round = self.current_round();

        #[cfg(feature = "metrics")]
        metrics::gauge(metrics::bft::PROPOSAL_ROUND, round as f64);

        // Ensure the primary has not proposed a batch for this round before.
        if self.storage.contains_certificate_in_round_from(round, self.gateway.account().address()) {
            // If a BFT sender was provided, attempt to advance the current round.
            if let Some(bft_sender) = self.bft_sender.get() {
                match bft_sender.send_primary_round_to_bft(self.current_round()).await {
                    // 'is_ready' is true if the primary is ready to propose a batch for the next round.
                    Ok(true) => (), // continue,
                    // 'is_ready' is false if the primary is not ready to propose a batch for the next round.
                    Ok(false) => return Ok(()),
                    // An error occurred while attempting to advance the current round.
                    Err(e) => {
                        warn!("Failed to update the BFT to the next round - {e}");
                        return Err(e);
                    }
                }
            }
            bail!("Primary is safely skipping {}", format!("(round {round} was already certified)").dimmed());
        }

        // Retrieve the committee to check against.
        let committee_lookback = self.ledger.get_committee_lookback_for_round(round)?;
        // Check if the primary is connected to enough validators to reach quorum threshold.
        {
            // Retrieve the connected validator addresses.
            let mut connected_validators = self.gateway.connected_addresses();
            // Append the primary to the set.
            connected_validators.insert(self.gateway.account().address());
            // If quorum threshold is not reached, return early.
            if !committee_lookback.is_quorum_threshold_reached(&connected_validators) {
                debug!(
                    "Primary is safely skipping a batch proposal {}",
                    "(please connect to more validators)".dimmed()
                );
                trace!("Primary is connected to {} validators", connected_validators.len() - 1);
                return Ok(());
            }
        }

        // Compute the previous round.
        let previous_round = round.saturating_sub(1);
        // Retrieve the previous certificates.
        let previous_certificates = self.storage.get_certificates_for_round(previous_round);

        // Check if the batch is ready to be proposed.
        // Note: The primary starts at round 1, and round 0 contains no certificates, by definition.
        let mut is_ready = previous_round == 0;
        // If the previous round is not 0, check if the previous certificates have reached the quorum threshold.
        if previous_round > 0 {
            // Retrieve the committee lookback for the round.
            let Ok(previous_committee_lookback) = self.ledger.get_committee_lookback_for_round(previous_round) else {
                bail!("Cannot propose a batch for round {round}: the committee lookback is not known yet")
            };
            // Construct a set over the authors.
            let authors = previous_certificates.iter().map(BatchCertificate::author).collect();
            // Check if the previous certificates have reached the quorum threshold.
            if previous_committee_lookback.is_quorum_threshold_reached(&authors) {
                is_ready = true;
            }
        }
        // If the batch is not ready to be proposed, return early.
        if !is_ready {
            debug!(
                "Primary is safely skipping a batch proposal {}",
                format!("(previous round {previous_round} has not reached quorum)").dimmed()
            );
            return Ok(());
        }

        // Determined the required number of transmissions per worker.
        let num_transmissions_per_worker = BatchHeader::<N>::MAX_TRANSMISSIONS_PER_BATCH / self.num_workers() as usize;
        // Initialize the map of transmissions.
        let mut transmissions: IndexMap<_, _> = Default::default();
        // Initialize a tracker for the number of transactions.
        let mut num_transactions = 0;
        // Take the transmissions from the workers.
        for worker in self.workers.iter() {
            // Initialize a tracker for included transmissions for the current worker.
            let mut num_transmissions_included_for_worker = 0;
            // Keep draining the worker until the desired number of transmissions is reached or the worker is empty.
            'outer: while num_transmissions_included_for_worker < num_transmissions_per_worker {
                // Determine the number of remaining transmissions for the worker.
                let num_remaining_transmissions =
                    num_transmissions_per_worker.saturating_sub(num_transmissions_included_for_worker);
                // Drain the worker.
                let mut worker_transmissions = worker.drain(num_remaining_transmissions).peekable();
                // If the worker is empty, break early.
                if worker_transmissions.peek().is_none() {
                    break 'outer;
                }
                // Iterate through the worker transmissions.
                'inner: for (id, transmission) in worker_transmissions {
                    // Check if the ledger already contains the transmission.
                    if self.ledger.contains_transmission(&id).unwrap_or(true) {
                        trace!("Proposing - Skipping transmission '{}' - Already in ledger", fmt_id(id));
                        continue 'inner;
                    }
                    // Check if the storage already contain the transmission.
                    // Note: We do not skip if this is the first transmission in the proposal, to ensure that
                    // the primary does not propose a batch with no transmissions.
                    if !transmissions.is_empty() && self.storage.contains_transmission(id) {
                        trace!("Proposing - Skipping transmission '{}' - Already in storage", fmt_id(id));
                        continue 'inner;
                    }
                    // Check the transmission is still valid.
                    match (id, transmission.clone()) {
                        (TransmissionID::Solution(solution_id), Transmission::Solution(solution)) => {
                            // Check if the solution is still valid.
                            if let Err(e) = self.ledger.check_solution_basic(solution_id, solution).await {
                                trace!("Proposing - Skipping solution '{}' - {e}", fmt_id(solution_id));
                                continue 'inner;
                            }
                        }
                        (TransmissionID::Transaction(transaction_id), Transmission::Transaction(transaction)) => {
                            // Check if the transaction is still valid.
                            if let Err(e) = self.ledger.check_transaction_basic(transaction_id, transaction).await {
                                trace!("Proposing - Skipping transaction '{}' - {e}", fmt_id(transaction_id));
                                continue 'inner;
                            }
                            // Increment the number of transactions.
                            num_transactions += 1;
                        }
                        // Note: We explicitly forbid including ratifications,
                        // as the protocol currently does not support ratifications.
                        (TransmissionID::Ratification, Transmission::Ratification) => continue,
                        // All other combinations are clearly invalid.
                        _ => continue 'inner,
                    }
                    // Insert the transmission into the map.
                    transmissions.insert(id, transmission);
                    num_transmissions_included_for_worker += 1;
                }
            }
        }
        // If there are no unconfirmed transmissions to propose, return early.
        if transmissions.is_empty() {
            debug!("Primary is safely skipping a batch proposal {}", "(no unconfirmed transmissions)".dimmed());
            return Ok(());
        }
        // If there are no unconfirmed transactions to propose, return early.
        if num_transactions == 0 {
            debug!("Primary is safely skipping a batch proposal {}", "(no unconfirmed transactions)".dimmed());
            return Ok(());
        }
        // Ditto if the batch had already been proposed.
        ensure!(round > 0, "Round 0 cannot have transaction batches");
        if *lock_guard == round {
            warn!("Primary is safely skipping a batch proposal - round {round} already proposed");
            return Ok(());
        }

        *lock_guard = round;

        /* Proceeding to sign & propose the batch. */
        info!("Proposing a batch with {} transmissions for round {round}...", transmissions.len());

        // Retrieve the private key.
        let private_key = *self.gateway.account().private_key();
        // Retrieve the committee ID.
        let committee_id = committee_lookback.id();
        // Prepare the transmission IDs.
        let transmission_ids = transmissions.keys().copied().collect();
        // Prepare the previous batch certificate IDs.
        let previous_certificate_ids = previous_certificates.into_iter().map(|c| c.id()).collect();
        // Sign the batch header.
        let batch_header = spawn_blocking!(BatchHeader::new(
            &private_key,
            round,
            now(),
            committee_id,
            transmission_ids,
            previous_certificate_ids,
            &mut rand::thread_rng()
        ))?;
        // Construct the proposal.
        let proposal = Proposal::new(committee_lookback, batch_header.clone(), transmissions)?;
        // Broadcast the batch to all validators for signing.
        self.gateway.broadcast(Event::BatchPropose(batch_header.into()));
        // Set the proposed batch.
        *self.proposed_batch.write() = Some(proposal);
        Ok(())
    }

    /// Processes a batch propose from a peer.
    ///
    /// This method performs the following steps:
    /// 1. Verify the batch.
    /// 2. Sign the batch.
    /// 3. Broadcast the signature back to the validator.
    ///
    /// If our primary is ahead of the peer, we will not sign the batch.
    /// If our primary is behind the peer, but within GC range, we will sync up to the peer's round, and then sign the batch.
    async fn process_batch_propose_from_peer(&self, peer_ip: SocketAddr, batch_propose: BatchPropose<N>) -> Result<()> {
        let BatchPropose { round: batch_round, batch_header } = batch_propose;

        // Deserialize the batch header.
        let batch_header = spawn_blocking!(batch_header.deserialize_blocking())?;
        // Ensure the round matches in the batch header.
        if batch_round != batch_header.round() {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!("Malicious peer - proposed round {batch_round}, but sent batch for round {}", batch_header.round());
        }

        // Retrieve the batch author.
        let batch_author = batch_header.author();

        // Ensure the batch proposal is from the validator.
        match self.gateway.resolver().get_address(peer_ip) {
            // If the peer is a validator, then ensure the batch proposal is from the validator.
            Some(address) => {
                if address != batch_author {
                    // Proceed to disconnect the validator.
                    self.gateway.disconnect(peer_ip);
                    bail!("Malicious peer - proposed batch from a different validator ({batch_author})");
                }
            }
            None => bail!("Batch proposal from a disconnected validator"),
        }
        // Ensure the batch author is a current committee member.
        if !self.gateway.is_authorized_validator_address(batch_author) {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!("Malicious peer - proposed batch from a non-committee member ({batch_author})");
        }
        // Ensure the batch proposal is not from the current primary.
        if self.gateway.account().address() == batch_author {
            bail!("Invalid peer - proposed batch from myself ({batch_author})");
        }

        // Ensure that the batch proposal's committee ID matches the expected committee ID.
        let expected_committee_id = self.ledger.get_committee_lookback_for_round(batch_round)?.id();
        if expected_committee_id != batch_header.committee_id() {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!(
                "Malicious peer - proposed batch has a different committee ID ({expected_committee_id} != {})",
                batch_header.committee_id()
            );
        }

        // Retrieve the cached round and batch ID for this validator.
        if let Some((signed_round, signed_batch_id, signature)) =
            self.signed_proposals.read().get(&batch_author).copied()
        {
            // If the signed round is ahead of the peer's batch round, then the validator is malicious.
            if signed_round > batch_header.round() {
                // Proceed to disconnect the validator.
                self.gateway.disconnect(peer_ip);
                bail!("Malicious peer - proposed a batch for a previous round ({})", batch_header.round());
            }

            // If the round matches and the batch ID differs, then the validator is malicious.
            if signed_round == batch_header.round() && signed_batch_id != batch_header.batch_id() {
                // Proceed to disconnect the validator.
                self.gateway.disconnect(peer_ip);
                bail!("Malicious peer - proposed another batch for the same round ({signed_round})");
            }
            // If the round and batch ID matches, then skip signing the batch a second time.
            // Instead, rebroadcast the cached signature to the peer.
            if signed_round == batch_header.round() && signed_batch_id == batch_header.batch_id() {
                let gateway = self.gateway.clone();
                tokio::spawn(async move {
                    debug!("Resending a signature for a batch in round {batch_round} from '{peer_ip}'");
                    let event = Event::BatchSignature(BatchSignature::new(batch_header.batch_id(), signature));
                    // Resend the batch signature to the peer.
                    if gateway.send(peer_ip, event).await.is_none() {
                        warn!("Failed to resend a signature for a batch in round {batch_round} to '{peer_ip}'");
                    }
                });
                // Return early.
                return Ok(());
            }
        }

        // If the peer is ahead, use the batch header to sync up to the peer.
        let mut transmissions = self.sync_with_batch_header_from_peer(peer_ip, &batch_header).await?;

        // Check that the transmission ids match and are not fee transactions.
        if let Err(err) = cfg_iter_mut!(transmissions).try_for_each(|(transmission_id, transmission)| {
            // If the transmission is not well-formed, then return early.
            self.ledger.ensure_transmission_is_well_formed(*transmission_id, transmission)
        }) {
            debug!("Batch propose from '{peer_ip}' contains an invalid transmission - {err}",);
            return Ok(());
        }

        // Ensure the batch is for the current round.
        // This method must be called after fetching previous certificates (above),
        // and prior to checking the batch header (below).
        if let Err(e) = self.ensure_is_signing_round(batch_round) {
            // If the primary is not signing for the peer's round, then return early.
            debug!("{e} from '{peer_ip}'");
            return Ok(());
        }

        // Ensure the batch header from the peer is valid.
        let (storage, header) = (self.storage.clone(), batch_header.clone());
        let missing_transmissions = spawn_blocking!(storage.check_batch_header(&header, transmissions))?;
        // Inserts the missing transmissions into the workers.
        self.insert_missing_transmissions_into_workers(peer_ip, missing_transmissions.into_iter())?;

        /* Proceeding to sign the batch. */

        // Retrieve the batch ID.
        let batch_id = batch_header.batch_id();
        // Sign the batch ID.
        let account = self.gateway.account().clone();
        let signature = spawn_blocking!(account.sign(&[batch_id], &mut rand::thread_rng()))?;

        // Ensure the proposal has not already been signed.
        //
        // Note: Due to the need to sync the batch header with the peer, it is possible
        // for the primary to receive the same 'BatchPropose' event again, whereby only
        // one instance of this handler should sign the batch. This check guarantees this.
        match self.signed_proposals.write().entry(batch_author) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                // If the validator has already signed a batch for this round, then return early,
                // since, if the peer still has not received the signature, they will request it again,
                // and the logic at the start of this function will resend the (now cached) signature
                // to the peer if asked to sign this batch proposal again.
                if entry.get().0 == batch_round {
                    return Ok(());
                }
                // Otherwise, cache the round, batch ID, and signature for this validator.
                entry.insert((batch_round, batch_id, signature));
            }
            // If the validator has not signed a batch before, then continue.
            std::collections::hash_map::Entry::Vacant(entry) => {
                // Cache the round, batch ID, and signature for this validator.
                entry.insert((batch_round, batch_id, signature));
            }
        };

        // Broadcast the signature back to the validator.
        let self_ = self.clone();
        tokio::spawn(async move {
            let event = Event::BatchSignature(BatchSignature::new(batch_id, signature));
            // Send the batch signature to the peer.
            if self_.gateway.send(peer_ip, event).await.is_some() {
                debug!("Signed a batch for round {batch_round} from '{peer_ip}'");
            }
        });
        Ok(())
    }

    /// Processes a batch signature from a peer.
    ///
    /// This method performs the following steps:
    /// 1. Ensure the proposed batch has not expired.
    /// 2. Verify the signature, ensuring it corresponds to the proposed batch.
    /// 3. Store the signature.
    /// 4. Certify the batch if enough signatures have been received.
    /// 5. Broadcast the batch certificate to all validators.
    async fn process_batch_signature_from_peer(
        &self,
        peer_ip: SocketAddr,
        batch_signature: BatchSignature<N>,
    ) -> Result<()> {
        // Ensure the proposed batch has not expired, and clear the proposed batch if it has expired.
        self.check_proposed_batch_for_expiration().await?;

        // Retrieve the signature and timestamp.
        let BatchSignature { batch_id, signature } = batch_signature;

        // Retrieve the signer.
        let signer = signature.to_address();

        // Ensure the batch signature is signed by the validator.
        if self.gateway.resolver().get_address(peer_ip).map_or(true, |address| address != signer) {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!("Malicious peer - batch signature is from a different validator ({signer})");
        }
        // Ensure the batch signature is not from the current primary.
        if self.gateway.account().address() == signer {
            bail!("Invalid peer - received a batch signature from myself ({signer})");
        }

        let self_ = self.clone();
        let Some(proposal) = spawn_blocking!({
            // Acquire the write lock.
            let mut proposed_batch = self_.proposed_batch.write();
            // Add the signature to the batch, and determine if the batch is ready to be certified.
            match proposed_batch.as_mut() {
                Some(proposal) => {
                    // Ensure the batch ID matches the currently proposed batch ID.
                    if proposal.batch_id() != batch_id {
                        match self_.storage.contains_batch(batch_id) {
                            true => bail!("This batch was already certified"),
                            false => bail!(
                                "Unknown batch ID '{batch_id}', expected '{}' for round {}",
                                proposal.batch_id(),
                                proposal.round()
                            ),
                        }
                    }
                    // Retrieve the committee lookback for the round.
                    let committee_lookback = self_.ledger.get_committee_lookback_for_round(proposal.round())?;
                    // Retrieve the address of the validator.
                    let Some(signer) = self_.gateway.resolver().get_address(peer_ip) else {
                        bail!("Signature is from a disconnected validator");
                    };
                    // Add the signature to the batch.
                    proposal.add_signature(signer, signature, &committee_lookback)?;
                    info!("Received a batch signature for round {} from '{peer_ip}'", proposal.round());
                    // Check if the batch is ready to be certified.
                    if !proposal.is_quorum_threshold_reached(&committee_lookback) {
                        // If the batch is not ready to be certified, return early.
                        return Ok(None);
                    }
                }
                // There is no proposed batch, so return early.
                None => return Ok(None),
            };
            // Retrieve the batch proposal, clearing the proposed batch.
            match proposed_batch.take() {
                Some(proposal) => Ok(Some(proposal)),
                None => Ok(None),
            }
        })?
        else {
            return Ok(());
        };

        /* Proceeding to certify the batch. */

        info!("Quorum threshold reached - Preparing to certify our batch for round {}...", proposal.round());

        // Retrieve the committee lookback for the round.
        let committee_lookback = self.ledger.get_committee_lookback_for_round(proposal.round())?;
        // Store the certified batch and broadcast it to all validators.
        // If there was an error storing the certificate, reinsert the transmissions back into the ready queue.
        if let Err(e) = self.store_and_broadcast_certificate(&proposal, &committee_lookback).await {
            // Reinsert the transmissions back into the ready queue for the next proposal.
            self.reinsert_transmissions_into_workers(proposal)?;
            return Err(e);
        }

        #[cfg(feature = "metrics")]
        metrics::increment_gauge(metrics::bft::CERTIFIED_BATCHES, 1.0);
        Ok(())
    }

    /// Processes a batch certificate from a peer.
    ///
    /// This method performs the following steps:
    /// 1. Stores the given batch certificate, after ensuring it is valid.
    /// 2. If there are enough certificates to reach quorum threshold for the current round,
    ///  then proceed to advance to the next round.
    async fn process_batch_certificate_from_peer(
        &self,
        peer_ip: SocketAddr,
        certificate: BatchCertificate<N>,
    ) -> Result<()> {
        // Ensure storage does not already contain the certificate.
        if self.storage.contains_certificate(certificate.id()) {
            return Ok(());
        }

        // Retrieve the batch certificate author.
        let author = certificate.author();
        // Retrieve the batch certificate round.
        let certificate_round = certificate.round();
        // Retrieve the batch certificate committee ID.
        let committee_id = certificate.committee_id();

        // Ensure the batch certificate is from an authorized validator.
        if !self.gateway.is_authorized_validator_ip(peer_ip) {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!("Malicious peer - Received a batch certificate from an unauthorized validator IP ({peer_ip})");
        }
        // Ensure the batch certificate is not from the current primary.
        if self.gateway.account().address() == author {
            bail!("Received a batch certificate for myself ({author})");
        }

        // Store the certificate, after ensuring it is valid.
        self.sync_with_certificate_from_peer(peer_ip, certificate).await?;

        // If there are enough certificates to reach quorum threshold for the certificate round,
        // then proceed to advance to the next round.

        // Retrieve the committee lookback.
        let committee_lookback = self.ledger.get_committee_lookback_for_round(certificate_round)?;
        // Retrieve the certificates.
        let certificates = self.storage.get_certificates_for_round(certificate_round);
        // Construct a set over the authors.
        let authors = certificates.iter().map(BatchCertificate::author).collect();
        // Check if the certificates have reached the quorum threshold.
        let is_quorum = committee_lookback.is_quorum_threshold_reached(&authors);

        // Ensure that the batch certificate's committee ID matches the expected committee ID.
        let expected_committee_id = committee_lookback.id();
        if expected_committee_id != committee_id {
            // Proceed to disconnect the validator.
            self.gateway.disconnect(peer_ip);
            bail!("Batch certificate has a different committee ID ({expected_committee_id} != {committee_id})");
        }

        // Determine if we are currently proposing a round that is relevant.
        // Note: This is important, because while our peers have advanced,
        // they may not be proposing yet, and thus still able to sign our proposed batch.
        let should_advance = match &*self.proposed_batch.read() {
            // We advance if the proposal round is less than the current round that was just certified.
            Some(proposal) => proposal.round() < certificate_round,
            // If there's no proposal, we consider advancing.
            None => true,
        };

        // Retrieve the current round.
        let current_round = self.current_round();

        // Determine whether to advance to the next round.
        if is_quorum && should_advance && certificate_round >= current_round {
            // If we have reached the quorum threshold and the round should advance, then proceed to the next round.
            self.try_increment_to_the_next_round(current_round + 1).await?;
        }
        Ok(())
    }
}

impl<N: Network> Primary<N> {
    /// Starts the primary handlers.
    fn start_handlers(&self, primary_receiver: PrimaryReceiver<N>) {
        let PrimaryReceiver {
            mut rx_batch_propose,
            mut rx_batch_signature,
            mut rx_batch_certified,
            mut rx_primary_ping,
            mut rx_unconfirmed_solution,
            mut rx_unconfirmed_transaction,
        } = primary_receiver;

        // Start the primary ping.
        if self.sync.is_gateway_mode() {
            let self_ = self.clone();
            self.spawn(async move {
                loop {
                    // Sleep briefly.
                    tokio::time::sleep(Duration::from_millis(PRIMARY_PING_IN_MS)).await;

                    // Retrieve the block locators.
                    let self__ = self_.clone();
                    let block_locators = match spawn_blocking!(self__.sync.get_block_locators()) {
                        Ok(block_locators) => block_locators,
                        Err(e) => {
                            warn!("Failed to retrieve block locators - {e}");
                            continue;
                        }
                    };

                    // Retrieve the latest certificate of the primary.
                    let primary_certificate = {
                        // Retrieve the primary address.
                        let primary_address = self_.gateway.account().address();

                        // Iterate backwards from the latest round to find the primary certificate.
                        let mut certificate = None;
                        let mut current_round = self_.current_round();
                        while certificate.is_none() {
                            // If the current round is 0, then break the while loop.
                            if current_round == 0 {
                                break;
                            }
                            // Retrieve the certificates.
                            let certificates = self_.storage.get_certificates_for_round(current_round);
                            // Retrieve the primary certificate.
                            certificate =
                                certificates.into_iter().find(|certificate| certificate.author() == primary_address);
                            // If the primary certificate was not found, decrement the round.
                            if certificate.is_none() {
                                current_round = current_round.saturating_sub(1);
                            }
                        }

                        // Determine if the primary certificate was found.
                        match certificate {
                            Some(certificate) => certificate,
                            // Skip this iteration of the loop (do not send a primary ping).
                            None => continue,
                        }
                    };

                    // Construct the primary ping.
                    let primary_ping = PrimaryPing::from((<Event<N>>::VERSION, block_locators, primary_certificate));
                    // Broadcast the event.
                    self_.gateway.broadcast(Event::PrimaryPing(primary_ping));
                }
            });
        }

        // Start the primary ping handler.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, primary_certificate)) = rx_primary_ping.recv().await {
                // If the primary is not synced, then do not process the primary ping.
                if !self_.sync.is_synced() {
                    trace!("Skipping a primary ping from '{peer_ip}' {}", "(node is syncing)".dimmed());
                    continue;
                }

                // Spawn a task to process the primary certificate.
                {
                    let self_ = self_.clone();
                    tokio::spawn(async move {
                        // Deserialize the primary certificate in the primary ping.
                        let Ok(primary_certificate) = spawn_blocking!(primary_certificate.deserialize_blocking())
                        else {
                            warn!("Failed to deserialize primary certificate in 'PrimaryPing' from '{peer_ip}'");
                            return;
                        };
                        // Process the primary certificate.
                        if let Err(e) = self_.process_batch_certificate_from_peer(peer_ip, primary_certificate).await {
                            warn!("Cannot process a primary certificate in a 'PrimaryPing' from '{peer_ip}' - {e}");
                        }
                    });
                }
            }
        });

        // Start the worker ping(s).
        if self.sync.is_gateway_mode() {
            let self_ = self.clone();
            self.spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(WORKER_PING_IN_MS)).await;
                    // If the primary is not synced, then do not broadcast the worker ping(s).
                    if !self_.sync.is_synced() {
                        trace!("Skipping worker ping(s) {}", "(node is syncing)".dimmed());
                        continue;
                    }
                    // Broadcast the worker ping(s).
                    for worker in self_.workers.iter() {
                        worker.broadcast_ping();
                    }
                }
            });
        }

        // Start the batch proposer.
        let self_ = self.clone();
        self.spawn(async move {
            loop {
                // Sleep briefly, but longer than if there were no batch.
                tokio::time::sleep(Duration::from_millis(MAX_BATCH_DELAY_IN_MS)).await;
                // If the primary is not synced, then do not propose a batch.
                if !self_.sync.is_synced() {
                    debug!("Skipping batch proposal {}", "(node is syncing)".dimmed());
                    continue;
                }
                // A best-effort attempt to skip the scheduled batch proposal if
                // round progression already triggered one.
                if self_.propose_lock.try_lock().is_err() {
                    trace!("Skipping batch proposal {}", "(node is already proposing)".dimmed());
                    continue;
                };
                // If there is no proposed batch, attempt to propose a batch.
                // Note: Do NOT spawn a task around this function call. Proposing a batch is a critical path,
                // and only one batch needs be proposed at a time.
                if let Err(e) = self_.propose_batch().await {
                    warn!("Cannot propose a batch - {e}");
                }
            }
        });

        // Process the proposed batch.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, batch_propose)) = rx_batch_propose.recv().await {
                // If the primary is not synced, then do not sign the batch.
                if !self_.sync.is_synced() {
                    trace!("Skipping a batch proposal from '{peer_ip}' {}", "(node is syncing)".dimmed());
                    continue;
                }
                // Spawn a task to process the proposed batch.
                let self_ = self_.clone();
                tokio::spawn(async move {
                    // Process the batch proposal.
                    if let Err(e) = self_.process_batch_propose_from_peer(peer_ip, batch_propose).await {
                        warn!("Cannot sign a batch from '{peer_ip}' - {e}");
                    }
                });
            }
        });

        // Process the batch signature.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, batch_signature)) = rx_batch_signature.recv().await {
                // If the primary is not synced, then do not store the signature.
                if !self_.sync.is_synced() {
                    trace!("Skipping a batch signature from '{peer_ip}' {}", "(node is syncing)".dimmed());
                    continue;
                }
                // Process the batch signature.
                // Note: Do NOT spawn a task around this function call. Processing signatures from peers
                // is a critical path, and we should only store the minimum required number of signatures.
                // In addition, spawning a task can cause concurrent processing of signatures (even with a lock),
                // which means the RwLock for the proposed batch must become a 'tokio::sync' to be safe.
                if let Err(e) = self_.process_batch_signature_from_peer(peer_ip, batch_signature).await {
                    warn!("Cannot store a signature from '{peer_ip}' - {e}");
                }
            }
        });

        // Process the certified batch.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((peer_ip, batch_certificate)) = rx_batch_certified.recv().await {
                // If the primary is not synced and lagging by more than `MAX_BLOCKS_BEHIND + 1`, then do not store the certificate.
                // This allows us to start processing the certificate as soon as we are within `MAX_BLOCKS_BEHIND + 1` blocks.
                if !self_.sync.is_synced() && self_.sync.num_blocks_behind() > MAX_BLOCKS_BEHIND.saturating_add(1) {
                    trace!("Skipping a certified batch from '{peer_ip}' {}", "(node is syncing)".dimmed());
                    continue;
                }
                // Spawn a task to process the batch certificate.
                let self_ = self_.clone();
                tokio::spawn(async move {
                    // Deserialize the batch certificate.
                    let Ok(batch_certificate) = spawn_blocking!(batch_certificate.deserialize_blocking()) else {
                        warn!("Failed to deserialize the batch certificate from '{peer_ip}'");
                        return;
                    };
                    // Process the batch certificate.
                    if let Err(e) = self_.process_batch_certificate_from_peer(peer_ip, batch_certificate).await {
                        warn!("Cannot store a certificate from '{peer_ip}' - {e}");
                    }
                });
            }
        });

        // Process the unconfirmed solutions.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((solution_id, solution, callback)) = rx_unconfirmed_solution.recv().await {
                // Compute the worker ID.
                let Ok(worker_id) = assign_to_worker(solution_id, self_.num_workers()) else {
                    error!("Unable to determine the worker ID for the unconfirmed solution");
                    continue;
                };
                let self_ = self_.clone();
                tokio::spawn(async move {
                    // Retrieve the worker.
                    let worker = &self_.workers[worker_id as usize];
                    // Process the unconfirmed solution.
                    let result = worker.process_unconfirmed_solution(solution_id, solution).await;
                    // Send the result to the callback.
                    callback.send(result).ok();
                });
            }
        });

        // Process the unconfirmed transactions.
        let self_ = self.clone();
        self.spawn(async move {
            while let Some((transaction_id, transaction, callback)) = rx_unconfirmed_transaction.recv().await {
                trace!("Primary - Received an unconfirmed transaction '{}'", fmt_id(transaction_id));
                // Compute the worker ID.
                let Ok(worker_id) = assign_to_worker::<N>(&transaction_id, self_.num_workers()) else {
                    error!("Unable to determine the worker ID for the unconfirmed transaction");
                    continue;
                };
                let self_ = self_.clone();
                tokio::spawn(async move {
                    // Retrieve the worker.
                    let worker = &self_.workers[worker_id as usize];
                    // Process the unconfirmed transaction.
                    let result = worker.process_unconfirmed_transaction(transaction_id, transaction).await;
                    // Send the result to the callback.
                    callback.send(result).ok();
                });
            }
        });
    }

    /// Checks if the proposed batch is expired, and clears the proposed batch if it has expired.
    async fn check_proposed_batch_for_expiration(&self) -> Result<()> {
        // Check if the proposed batch is timed out or stale.
        let is_expired = match self.proposed_batch.read().as_ref() {
            Some(proposal) => proposal.round() < self.current_round(),
            None => false,
        };
        // If the batch is expired, clear the proposed batch.
        if is_expired {
            // Reset the proposed batch.
            let proposal = self.proposed_batch.write().take();
            if let Some(proposal) = proposal {
                self.reinsert_transmissions_into_workers(proposal)?;
            }
        }
        Ok(())
    }

    /// Increments to the next round.
    async fn try_increment_to_the_next_round(&self, next_round: u64) -> Result<()> {
        // If the next round is within GC range, then iterate to the penultimate round.
        if self.current_round() + self.storage.max_gc_rounds() >= next_round {
            let mut fast_forward_round = self.current_round();
            // Iterate until the penultimate round is reached.
            while fast_forward_round < next_round.saturating_sub(1) {
                // Update to the next round in storage.
                fast_forward_round = self.storage.increment_to_next_round(fast_forward_round)?;
                // Clear the proposed batch.
                *self.proposed_batch.write() = None;
            }
        }

        // Retrieve the current round.
        let current_round = self.current_round();
        // Attempt to advance to the next round.
        if current_round < next_round {
            // If a BFT sender was provided, send the current round to the BFT.
            let is_ready = if let Some(bft_sender) = self.bft_sender.get() {
                match bft_sender.send_primary_round_to_bft(current_round).await {
                    Ok(is_ready) => is_ready,
                    Err(e) => {
                        warn!("Failed to update the BFT to the next round - {e}");
                        return Err(e);
                    }
                }
            }
            // Otherwise, handle the Narwhal case.
            else {
                // Update to the next round in storage.
                self.storage.increment_to_next_round(current_round)?;
                // Set 'is_ready' to 'true'.
                true
            };

            // Log whether the next round is ready.
            match is_ready {
                true => debug!("Primary is ready to propose the next round"),
                false => debug!("Primary is not ready to propose the next round"),
            }

            // If the node is ready, propose a batch for the next round.
            if is_ready {
                self.propose_batch().await?;
            }
        }
        Ok(())
    }

    /// Ensures the primary is signing for the specified batch round.
    /// This method is used to ensure: for a given round, as soon as the primary starts proposing,
    /// it will no longer sign for the previous round (as it has enough previous certificates to proceed).
    fn ensure_is_signing_round(&self, batch_round: u64) -> Result<()> {
        // Retrieve the current round.
        let current_round = self.current_round();
        // Ensure the batch round is within GC range of the current round.
        if current_round + self.storage.max_gc_rounds() <= batch_round {
            bail!("Round {batch_round} is too far in the future")
        }
        // Ensure the batch round is at or one before the current round.
        // Intuition: Our primary has moved on to the next round, but has not necessarily started proposing,
        // so we can still sign for the previous round. If we have started proposing, the next check will fail.
        if current_round > batch_round + 1 {
            bail!("Primary is on round {current_round}, and no longer signing for round {batch_round}")
        }
        // Check if the primary is still signing for the batch round.
        if let Some(signing_round) = self.proposed_batch.read().as_ref().map(|proposal| proposal.round()) {
            if signing_round > batch_round {
                bail!("Our primary at round {signing_round} is no longer signing for round {batch_round}")
            }
        }
        Ok(())
    }

    /// Stores the certified batch and broadcasts it to all validators, returning the certificate.
    async fn store_and_broadcast_certificate(&self, proposal: &Proposal<N>, committee: &Committee<N>) -> Result<()> {
        // Create the batch certificate and transmissions.
        let (certificate, transmissions) = tokio::task::block_in_place(|| proposal.to_certificate(committee))?;
        // Convert the transmissions into a HashMap.
        // Note: Do not change the `Proposal` to use a HashMap. The ordering there is necessary for safety.
        let transmissions = transmissions.into_iter().collect::<HashMap<_, _>>();
        // Store the certified batch.
        let (storage, certificate_) = (self.storage.clone(), certificate.clone());
        spawn_blocking!(storage.insert_certificate(certificate_, transmissions))?;
        debug!("Stored a batch certificate for round {}", certificate.round());
        // If a BFT sender was provided, send the certificate to the BFT.
        if let Some(bft_sender) = self.bft_sender.get() {
            // Await the callback to continue.
            if let Err(e) = bft_sender.send_primary_certificate_to_bft(certificate.clone()).await {
                warn!("Failed to update the BFT DAG from primary - {e}");
                return Err(e);
            };
        }
        // Broadcast the certified batch to all validators.
        self.gateway.broadcast(Event::BatchCertified(certificate.clone().into()));
        // Log the certified batch.
        let num_transmissions = certificate.transmission_ids().len();
        let round = certificate.round();
        info!("\n\nOur batch with {num_transmissions} transmissions for round {round} was certified!\n");
        // Increment to the next round.
        self.try_increment_to_the_next_round(round + 1).await
    }

    /// Inserts the missing transmissions from the proposal into the workers.
    fn insert_missing_transmissions_into_workers(
        &self,
        peer_ip: SocketAddr,
        transmissions: impl Iterator<Item = (TransmissionID<N>, Transmission<N>)>,
    ) -> Result<()> {
        // Insert the transmissions into the workers.
        assign_to_workers(&self.workers, transmissions, |worker, transmission_id, transmission| {
            worker.process_transmission_from_peer(peer_ip, transmission_id, transmission);
        })
    }

    /// Re-inserts the transmissions from the proposal into the workers.
    fn reinsert_transmissions_into_workers(&self, proposal: Proposal<N>) -> Result<()> {
        // Re-insert the transmissions into the workers.
        assign_to_workers(
            &self.workers,
            proposal.into_transmissions().into_iter(),
            |worker, transmission_id, transmission| {
                worker.reinsert(transmission_id, transmission);
            },
        )
    }

    /// Recursively stores a given batch certificate, after ensuring:
    ///   - Ensure the round matches the committee round.
    ///   - Ensure the address is a member of the committee.
    ///   - Ensure the timestamp is within range.
    ///   - Ensure we have all of the transmissions.
    ///   - Ensure we have all of the previous certificates.
    ///   - Ensure the previous certificates are for the previous round (i.e. round - 1).
    ///   - Ensure the previous certificates have reached the quorum threshold.
    ///   - Ensure we have not already signed the batch ID.
    #[async_recursion::async_recursion]
    async fn sync_with_certificate_from_peer(
        &self,
        peer_ip: SocketAddr,
        certificate: BatchCertificate<N>,
    ) -> Result<()> {
        // Retrieve the batch header.
        let batch_header = certificate.batch_header();
        // Retrieve the batch round.
        let batch_round = batch_header.round();

        // If the certificate round is outdated, do not store it.
        if batch_round <= self.storage.gc_round() {
            return Ok(());
        }
        // If the certificate already exists in storage, return early.
        if self.storage.contains_certificate(certificate.id()) {
            return Ok(());
        }

        // If the peer is ahead, use the batch header to sync up to the peer.
        let missing_transmissions = self.sync_with_batch_header_from_peer(peer_ip, batch_header).await?;

        // Check if the certificate needs to be stored.
        if !self.storage.contains_certificate(certificate.id()) {
            // Store the batch certificate.
            let (storage, certificate_) = (self.storage.clone(), certificate.clone());
            spawn_blocking!(storage.insert_certificate(certificate_, missing_transmissions))?;
            debug!("Stored a batch certificate for round {batch_round} from '{peer_ip}'");
            // If a BFT sender was provided, send the round and certificate to the BFT.
            if let Some(bft_sender) = self.bft_sender.get() {
                // Send the certificate to the BFT.
                if let Err(e) = bft_sender.send_primary_certificate_to_bft(certificate).await {
                    warn!("Failed to update the BFT DAG from sync: {e}");
                    return Err(e);
                };
            }
        }
        Ok(())
    }

    /// Recursively syncs using the given batch header.
    async fn sync_with_batch_header_from_peer(
        &self,
        peer_ip: SocketAddr,
        batch_header: &BatchHeader<N>,
    ) -> Result<HashMap<TransmissionID<N>, Transmission<N>>> {
        // Retrieve the batch round.
        let batch_round = batch_header.round();

        // If the certificate round is outdated, do not store it.
        if batch_round <= self.storage.gc_round() {
            bail!("Round {batch_round} is too far in the past")
        }

        // Determine if quorum threshold is reached on the batch round.
        let is_quorum_threshold_reached = {
            let certificates = self.storage.get_certificates_for_round(batch_round);
            let authors = certificates.iter().map(BatchCertificate::author).collect();
            let committee_lookback = self.ledger.get_committee_lookback_for_round(batch_round)?;
            committee_lookback.is_quorum_threshold_reached(&authors)
        };

        // Check if our primary should move to the next round.
        // Note: Checking that quorum threshold is reached is important for mitigating a race condition,
        // whereby Narwhal requires 2f+1, however the BFT only requires f+1. Without this check, the primary
        // will advance to the next round assuming f+1, not 2f+1, which can lead to a network stall.
        let is_behind_schedule = is_quorum_threshold_reached && batch_round > self.current_round();
        // Check if our primary is far behind the peer.
        let is_peer_far_in_future = batch_round > self.current_round() + self.storage.max_gc_rounds();
        // If our primary is far behind the peer, update our committee to the batch round.
        if is_behind_schedule || is_peer_far_in_future {
            // If the batch round is greater than the current committee round, update the committee.
            self.try_increment_to_the_next_round(batch_round).await?;
        }

        // Ensure the primary has all of the previous certificates.
        let missing_previous_certificates =
            self.fetch_missing_previous_certificates(peer_ip, batch_header).await.map_err(|e| {
                anyhow!("Failed to fetch missing previous certificates for round {batch_round} from '{peer_ip}' - {e}")
            })?;

        // Ensure the primary has all of the transmissions.
        let missing_transmissions = self.fetch_missing_transmissions(peer_ip, batch_header).await.map_err(|e| {
            anyhow!("Failed to fetch missing transmissions for round {batch_round} from '{peer_ip}' - {e}")
        })?;

        // Iterate through the missing previous certificates.
        for batch_certificate in missing_previous_certificates {
            // Store the batch certificate (recursively fetching any missing previous certificates).
            self.sync_with_certificate_from_peer(peer_ip, batch_certificate).await?;
        }
        Ok(missing_transmissions)
    }

    /// Fetches any missing transmissions for the specified batch header.
    /// If a transmission does not exist, it will be fetched from the specified peer IP.
    async fn fetch_missing_transmissions(
        &self,
        peer_ip: SocketAddr,
        batch_header: &BatchHeader<N>,
    ) -> Result<HashMap<TransmissionID<N>, Transmission<N>>> {
        // If the round is <= the GC round, return early.
        if batch_header.round() <= self.storage.gc_round() {
            return Ok(Default::default());
        }

        // Ensure this batch ID is new, otherwise return early.
        if self.storage.contains_batch(batch_header.batch_id()) {
            trace!("Batch for round {} from peer has already been processed", batch_header.round());
            return Ok(Default::default());
        }

        // Retrieve the workers.
        let workers = self.workers.clone();

        // Initialize a list for the transmissions.
        let mut fetch_transmissions = FuturesUnordered::new();

        // Retrieve the number of workers.
        let num_workers = self.num_workers();
        // Iterate through the transmission IDs.
        for transmission_id in batch_header.transmission_ids() {
            // If the transmission does not exist in storage, proceed to fetch the transmission.
            if !self.storage.contains_transmission(*transmission_id) {
                // Determine the worker ID.
                let Ok(worker_id) = assign_to_worker(*transmission_id, num_workers) else {
                    bail!("Unable to assign transmission ID '{transmission_id}' to a worker")
                };
                // Retrieve the worker.
                let Some(worker) = workers.get(worker_id as usize) else { bail!("Unable to find worker {worker_id}") };
                // Push the callback onto the list.
                fetch_transmissions.push(worker.get_or_fetch_transmission(peer_ip, *transmission_id));
            }
        }

        // Initialize a set for the transmissions.
        let mut transmissions = HashMap::with_capacity(fetch_transmissions.len());
        // Wait for all of the transmissions to be fetched.
        while let Some(result) = fetch_transmissions.next().await {
            // Retrieve the transmission.
            let (transmission_id, transmission) = result?;
            // Insert the transmission into the set.
            transmissions.insert(transmission_id, transmission);
        }
        // Return the transmissions.
        Ok(transmissions)
    }

    /// Fetches any missing previous certificates for the specified batch header from the specified peer.
    async fn fetch_missing_previous_certificates(
        &self,
        peer_ip: SocketAddr,
        batch_header: &BatchHeader<N>,
    ) -> Result<HashSet<BatchCertificate<N>>> {
        // Retrieve the round.
        let round = batch_header.round();
        // If the previous round is 0, or is <= the GC round, return early.
        if round == 1 || round <= self.storage.gc_round() + 1 {
            return Ok(Default::default());
        }

        // Fetch the missing previous certificates.
        let missing_previous_certificates =
            self.fetch_missing_certificates(peer_ip, round, batch_header.previous_certificate_ids()).await?;
        if !missing_previous_certificates.is_empty() {
            debug!(
                "Fetched {} missing previous certificates for round {round} from '{peer_ip}'",
                missing_previous_certificates.len(),
            );
        }
        // Return the missing previous certificates.
        Ok(missing_previous_certificates)
    }

    /// Fetches any missing certificates for the specified batch header from the specified peer.
    async fn fetch_missing_certificates(
        &self,
        peer_ip: SocketAddr,
        round: u64,
        certificate_ids: &IndexSet<Field<N>>,
    ) -> Result<HashSet<BatchCertificate<N>>> {
        // Initialize a list for the missing certificates.
        let mut fetch_certificates = FuturesUnordered::new();
        // Iterate through the certificate IDs.
        for certificate_id in certificate_ids {
            // Check if the certificate already exists in the ledger.
            if self.ledger.contains_certificate(certificate_id)? {
                continue;
            }
            // If we do not have the certificate, request it.
            if !self.storage.contains_certificate(*certificate_id) {
                trace!("Primary - Found a new certificate ID for round {round} from '{peer_ip}'");
                // TODO (howardwu): Limit the number of open requests we send to a peer.
                // Send an certificate request to the peer.
                fetch_certificates.push(self.sync.send_certificate_request(peer_ip, *certificate_id));
            }
        }

        // If there are no missing certificates, return early.
        match fetch_certificates.is_empty() {
            true => return Ok(Default::default()),
            false => trace!(
                "Fetching {} missing certificates for round {round} from '{peer_ip}'...",
                fetch_certificates.len(),
            ),
        }

        // Initialize a set for the missing certificates.
        let mut missing_certificates = HashSet::with_capacity(fetch_certificates.len());
        // Wait for all of the missing certificates to be fetched.
        while let Some(result) = fetch_certificates.next().await {
            // Insert the missing certificate into the set.
            missing_certificates.insert(result?);
        }
        // Return the missing certificates.
        Ok(missing_certificates)
    }
}

impl<N: Network> Primary<N> {
    /// Spawns a task with the given future; it should only be used for long-running tasks.
    fn spawn<T: Future<Output = ()> + Send + 'static>(&self, future: T) {
        self.handles.lock().push(tokio::spawn(future));
    }

    /// Shuts down the primary.
    pub async fn shut_down(&self) {
        info!("Shutting down the primary...");
        // Shut down the workers.
        self.workers.iter().for_each(|worker| worker.shut_down());
        // Abort the tasks.
        self.handles.lock().iter().for_each(|handle| handle.abort());
        // Close the gateway.
        self.gateway.shut_down().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snarkos_node_bft_ledger_service::MockLedgerService;
    use snarkos_node_bft_storage_service::BFTMemoryService;
    use snarkvm::{
        ledger::committee::{Committee, MIN_VALIDATOR_STAKE},
        prelude::{Address, Signature},
    };

    use bytes::Bytes;
    use indexmap::IndexSet;
    use rand::RngCore;

    type CurrentNetwork = snarkvm::prelude::MainnetV0;

    // Returns a primary and a list of accounts in the configured committee.
    async fn primary_without_handlers(
        rng: &mut TestRng,
    ) -> (Primary<CurrentNetwork>, Vec<(SocketAddr, Account<CurrentNetwork>)>) {
        // Create a committee containing the primary's account.
        let (accounts, committee) = {
            const COMMITTEE_SIZE: usize = 4;
            let mut accounts = Vec::with_capacity(COMMITTEE_SIZE);
            let mut members = IndexMap::new();

            for i in 0..COMMITTEE_SIZE {
                let socket_addr = format!("127.0.0.1:{}", 5000 + i).parse().unwrap();
                let account = Account::new(rng).unwrap();
                members.insert(account.address(), (MIN_VALIDATOR_STAKE, true));
                accounts.push((socket_addr, account));
            }

            (accounts, Committee::<CurrentNetwork>::new(1, members).unwrap())
        };

        let account = accounts.first().unwrap().1.clone();
        let ledger = Arc::new(MockLedgerService::new(committee));
        let storage = Storage::new(ledger.clone(), Arc::new(BFTMemoryService::new()), 10);

        // Initialize the primary.
        let mut primary = Primary::new(account, storage, ledger, None, &[], None).unwrap();

        // Construct a worker instance.
        primary.workers = Arc::from([Worker::new(
            0, // id
            Arc::new(primary.gateway.clone()),
            primary.storage.clone(),
            primary.ledger.clone(),
            primary.proposed_batch.clone(),
        )
        .unwrap()]);
        for a in accounts.iter() {
            primary.gateway.insert_connected_peer(a.0, a.0, a.1.address());
        }

        (primary, accounts)
    }

    // Creates a mock solution.
    fn sample_unconfirmed_solution(rng: &mut TestRng) -> (SolutionID<CurrentNetwork>, Data<Solution<CurrentNetwork>>) {
        // Sample a random fake solution ID.
        let solution_id = rng.gen::<u64>().into();
        // Vary the size of the solutions.
        let size = rng.gen_range(1024..10 * 1024);
        // Sample random fake solution bytes.
        let mut vec = vec![0u8; size];
        rng.fill_bytes(&mut vec);
        let solution = Data::Buffer(Bytes::from(vec));
        // Return the solution ID and solution.
        (solution_id, solution)
    }

    // Creates a mock transaction.
    fn sample_unconfirmed_transaction(
        rng: &mut TestRng,
    ) -> (<CurrentNetwork as Network>::TransactionID, Data<Transaction<CurrentNetwork>>) {
        // Sample a random fake transaction ID.
        let id = Field::<CurrentNetwork>::rand(rng).into();
        // Vary the size of the transactions.
        let size = rng.gen_range(1024..10 * 1024);
        // Sample random fake transaction bytes.
        let mut vec = vec![0u8; size];
        rng.fill_bytes(&mut vec);
        let transaction = Data::Buffer(Bytes::from(vec));
        // Return the ID and transaction.
        (id, transaction)
    }

    // Creates a batch proposal with one solution and one transaction.
    fn create_test_proposal(
        author: &Account<CurrentNetwork>,
        committee: Committee<CurrentNetwork>,
        round: u64,
        previous_certificate_ids: IndexSet<Field<CurrentNetwork>>,
        timestamp: i64,
        rng: &mut TestRng,
    ) -> Proposal<CurrentNetwork> {
        let (solution_id, solution) = sample_unconfirmed_solution(rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(rng);

        // Retrieve the private key.
        let private_key = author.private_key();
        // Prepare the transmission IDs.
        let transmission_ids = [solution_id.into(), (&transaction_id).into()].into();
        let transmissions = [
            (solution_id.into(), Transmission::Solution(solution)),
            ((&transaction_id).into(), Transmission::Transaction(transaction)),
        ]
        .into();
        // Sign the batch header.
        let batch_header = BatchHeader::new(
            private_key,
            round,
            timestamp,
            committee.id(),
            transmission_ids,
            previous_certificate_ids,
            rng,
        )
        .unwrap();
        // Construct the proposal.
        Proposal::new(committee, batch_header, transmissions).unwrap()
    }

    // Creates a signature of the primary's current proposal for each committee member (excluding
    // the primary).
    fn peer_signatures_for_proposal(
        primary: &Primary<CurrentNetwork>,
        accounts: &[(SocketAddr, Account<CurrentNetwork>)],
        rng: &mut TestRng,
    ) -> Vec<(SocketAddr, BatchSignature<CurrentNetwork>)> {
        // Each committee member signs the batch.
        let mut signatures = Vec::with_capacity(accounts.len() - 1);
        for (socket_addr, account) in accounts {
            if account.address() == primary.gateway.account().address() {
                continue;
            }
            let batch_id = primary.proposed_batch.read().as_ref().unwrap().batch_id();
            let signature = account.sign(&[batch_id], rng).unwrap();
            signatures.push((*socket_addr, BatchSignature::new(batch_id, signature)));
        }

        signatures
    }

    /// Creates a signature of the batch ID for each committee member (excluding the primary).
    fn peer_signatures_for_batch(
        primary_address: Address<CurrentNetwork>,
        accounts: &[(SocketAddr, Account<CurrentNetwork>)],
        batch_id: Field<CurrentNetwork>,
        rng: &mut TestRng,
    ) -> IndexSet<Signature<CurrentNetwork>> {
        let mut signatures = IndexSet::new();
        for (_, account) in accounts {
            if account.address() == primary_address {
                continue;
            }
            let signature = account.sign(&[batch_id], rng).unwrap();
            signatures.insert(signature);
        }
        signatures
    }

    // Creates a batch certificate.
    fn create_batch_certificate(
        primary_address: Address<CurrentNetwork>,
        accounts: &[(SocketAddr, Account<CurrentNetwork>)],
        round: u64,
        previous_certificate_ids: IndexSet<Field<CurrentNetwork>>,
        rng: &mut TestRng,
    ) -> (BatchCertificate<CurrentNetwork>, HashMap<TransmissionID<CurrentNetwork>, Transmission<CurrentNetwork>>) {
        let timestamp = now();

        let author =
            accounts.iter().find(|&(_, acct)| acct.address() == primary_address).map(|(_, acct)| acct.clone()).unwrap();
        let private_key = author.private_key();

        let committee_id = Field::rand(rng);
        let (solution_id, solution) = sample_unconfirmed_solution(rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(rng);
        let transmission_ids = [solution_id.into(), (&transaction_id).into()].into();
        let transmissions = [
            (solution_id.into(), Transmission::Solution(solution)),
            ((&transaction_id).into(), Transmission::Transaction(transaction)),
        ]
        .into();

        let batch_header = BatchHeader::new(
            private_key,
            round,
            timestamp,
            committee_id,
            transmission_ids,
            previous_certificate_ids,
            rng,
        )
        .unwrap();
        let signatures = peer_signatures_for_batch(primary_address, accounts, batch_header.batch_id(), rng);
        let certificate = BatchCertificate::<CurrentNetwork>::from(batch_header, signatures).unwrap();
        (certificate, transmissions)
    }

    // Create a certificate chain up to round in primary storage.
    fn store_certificate_chain(
        primary: &Primary<CurrentNetwork>,
        accounts: &[(SocketAddr, Account<CurrentNetwork>)],
        round: u64,
        rng: &mut TestRng,
    ) -> IndexSet<Field<CurrentNetwork>> {
        let mut previous_certificates = IndexSet::<Field<CurrentNetwork>>::new();
        let mut next_certificates = IndexSet::<Field<CurrentNetwork>>::new();
        for cur_round in 1..round {
            for (_, account) in accounts.iter() {
                let (certificate, transmissions) = create_batch_certificate(
                    account.address(),
                    accounts,
                    cur_round,
                    previous_certificates.clone(),
                    rng,
                );
                next_certificates.insert(certificate.id());
                assert!(primary.storage.insert_certificate(certificate, transmissions).is_ok());
            }

            assert!(primary.storage.increment_to_next_round(cur_round).is_ok());
            previous_certificates = next_certificates;
            next_certificates = IndexSet::<Field<CurrentNetwork>>::new();
        }

        previous_certificates
    }

    // Insert the account socket addresses into the resolver so that
    // they are recognized as "connected".
    fn map_account_addresses(primary: &Primary<CurrentNetwork>, accounts: &[(SocketAddr, Account<CurrentNetwork>)]) {
        // First account is primary, which doesn't need to resolve.
        for (addr, acct) in accounts.iter().skip(1) {
            primary.gateway.resolver().insert_peer(*addr, *addr, acct.address());
        }
    }

    #[tokio::test]
    async fn test_propose_batch() {
        let mut rng = TestRng::default();
        let (primary, _) = primary_without_handlers(&mut rng).await;

        // Check there is no batch currently proposed.
        assert!(primary.proposed_batch.read().is_none());

        // Try to propose a batch. There are no transmissions in the workers so the method should
        // just return without proposing a batch.
        assert!(primary.propose_batch().await.is_ok());
        assert!(primary.proposed_batch.read().is_none());

        // Generate a solution and a transaction.
        let (solution_id, solution) = sample_unconfirmed_solution(&mut rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(&mut rng);

        // Store it on one of the workers.
        primary.workers[0].process_unconfirmed_solution(solution_id, solution).await.unwrap();
        primary.workers[0].process_unconfirmed_transaction(transaction_id, transaction).await.unwrap();

        // Try to propose a batch again. This time, it should succeed.
        assert!(primary.propose_batch().await.is_ok());
        assert!(primary.proposed_batch.read().is_some());
    }

    #[tokio::test]
    async fn test_propose_batch_in_round() {
        let round = 3;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng).await;

        // Fill primary storage.
        store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Try to propose a batch. There are no transmissions in the workers so the method should
        // just return without proposing a batch.
        assert!(primary.propose_batch().await.is_ok());
        assert!(primary.proposed_batch.read().is_none());

        // Generate a solution and a transaction.
        let (solution_id, solution) = sample_unconfirmed_solution(&mut rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(&mut rng);

        // Store it on one of the workers.
        primary.workers[0].process_unconfirmed_solution(solution_id, solution).await.unwrap();
        primary.workers[0].process_unconfirmed_transaction(transaction_id, transaction).await.unwrap();

        // Propose a batch again. This time, it should succeed.
        assert!(primary.propose_batch().await.is_ok());
        assert!(primary.proposed_batch.read().is_some());
    }

    #[tokio::test]
    async fn test_propose_batch_skip_transmissions_from_previous_certificates() {
        let round = 3;
        let prev_round = round - 1;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng).await;
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;

        // Fill primary storage.
        store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Get transmissions from previous certificates.
        let previous_certificate_ids: IndexSet<_> =
            primary.storage.get_certificates_for_round(prev_round).iter().map(|cert| cert.id()).collect();

        // Track the number of transmissions in the previous round.
        let mut num_transmissions_in_previous_round = 0;

        // Generate a solution and a transaction.
        let (solution_commitment, solution) = sample_unconfirmed_solution(&mut rng);
        let (transaction_id, transaction) = sample_unconfirmed_transaction(&mut rng);

        // Store it on one of the workers.
        primary.workers[0].process_unconfirmed_solution(solution_commitment, solution).await.unwrap();
        primary.workers[0].process_unconfirmed_transaction(transaction_id, transaction).await.unwrap();

        // Check that the worker has 2 transmissions.
        assert_eq!(primary.workers[0].num_transmissions(), 2);

        // Create certificates for the current round and add the transmissions to the worker before inserting the certificate to storage.
        for (_, account) in accounts.iter() {
            let (certificate, transmissions) = create_batch_certificate(
                account.address(),
                &accounts,
                round,
                previous_certificate_ids.clone(),
                &mut rng,
            );

            // Add the transmissions to the worker.
            for (transmission_id, transmission) in transmissions.iter() {
                primary.workers[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone());
            }

            // Insert the certificate to storage.
            num_transmissions_in_previous_round += transmissions.len();
            primary.storage.insert_certificate(certificate, transmissions).unwrap();
        }

        // Advance to the next round.
        assert!(primary.storage.increment_to_next_round(round).is_ok());

        // Check that the worker has `num_transmissions_in_previous_round + 2` transmissions.
        assert_eq!(primary.workers[0].num_transmissions(), num_transmissions_in_previous_round + 2);

        // Propose the batch.
        assert!(primary.propose_batch().await.is_ok());

        // Check that the proposal only contains the new transmissions that were not in previous certificates.
        let proposed_transmissions = primary.proposed_batch.read().as_ref().unwrap().transmissions().clone();
        assert_eq!(proposed_transmissions.len(), 2);
        assert!(proposed_transmissions.contains_key(&TransmissionID::Solution(solution_commitment)));
        assert!(proposed_transmissions.contains_key(&TransmissionID::Transaction(transaction_id)));
    }

    #[tokio::test]
    async fn test_batch_propose_from_peer() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng).await;

        // Create a valid proposal with an author that isn't the primary.
        let round = 1;
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;
        let timestamp = now();
        let proposal = create_test_proposal(
            &peer_account.1,
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            &mut rng,
        );

        // Make sure the primary is aware of the transmissions in the proposal.
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone())
        }

        // The author must be known to resolver to pass propose checks.
        primary.gateway.resolver().insert_peer(peer_ip, peer_ip, peer_account.1.address());

        // Try to process the batch proposal from the peer, should succeed.
        assert!(
            primary.process_batch_propose_from_peer(peer_ip, (*proposal.batch_header()).clone().into()).await.is_ok()
        );
    }

    #[tokio::test]
    async fn test_batch_propose_from_peer_in_round() {
        let round = 2;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng).await;

        // Generate certificates.
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Create a valid proposal with an author that isn't the primary.
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;
        let timestamp = now();
        let proposal = create_test_proposal(
            &peer_account.1,
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            timestamp,
            &mut rng,
        );

        // Make sure the primary is aware of the transmissions in the proposal.
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone())
        }

        // The author must be known to resolver to pass propose checks.
        primary.gateway.resolver().insert_peer(peer_ip, peer_ip, peer_account.1.address());

        // Try to process the batch proposal from the peer, should succeed.
        primary.process_batch_propose_from_peer(peer_ip, (*proposal.batch_header()).clone().into()).await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_propose_from_peer_wrong_round() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng).await;

        // Create a valid proposal with an author that isn't the primary.
        let round = 1;
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;
        let timestamp = now();
        let proposal = create_test_proposal(
            &peer_account.1,
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            &mut rng,
        );

        // Make sure the primary is aware of the transmissions in the proposal.
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone())
        }

        // The author must be known to resolver to pass propose checks.
        primary.gateway.resolver().insert_peer(peer_ip, peer_ip, peer_account.1.address());

        // Try to process the batch proposal from the peer, should error.
        assert!(
            primary
                .process_batch_propose_from_peer(peer_ip, BatchPropose {
                    round: round + 1,
                    batch_header: Data::Object(proposal.batch_header().clone())
                })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_batch_propose_from_peer_in_round_wrong_round() {
        let round = 4;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng).await;

        // Generate certificates.
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Create a valid proposal with an author that isn't the primary.
        let peer_account = &accounts[1];
        let peer_ip = peer_account.0;
        let timestamp = now();
        let proposal = create_test_proposal(
            &peer_account.1,
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            timestamp,
            &mut rng,
        );

        // Make sure the primary is aware of the transmissions in the proposal.
        for (transmission_id, transmission) in proposal.transmissions() {
            primary.workers[0].process_transmission_from_peer(peer_ip, *transmission_id, transmission.clone())
        }

        // The author must be known to resolver to pass propose checks.
        primary.gateway.resolver().insert_peer(peer_ip, peer_ip, peer_account.1.address());

        // Try to process the batch proposal from the peer, should error.
        assert!(
            primary
                .process_batch_propose_from_peer(peer_ip, BatchPropose {
                    round: round + 1,
                    batch_header: Data::Object(proposal.batch_header().clone())
                })
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_signature_from_peer() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng).await;
        map_account_addresses(&primary, &accounts);

        // Create a valid proposal.
        let round = 1;
        let timestamp = now();
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            &mut rng,
        );

        // Store the proposal on the primary.
        *primary.proposed_batch.write() = Some(proposal);

        // Each committee member signs the batch.
        let signatures = peer_signatures_for_proposal(&primary, &accounts, &mut rng);

        // Have the primary process the signatures.
        for (socket_addr, signature) in signatures {
            primary.process_batch_signature_from_peer(socket_addr, signature).await.unwrap();
        }

        // Check the certificate was created and stored by the primary.
        assert!(primary.storage.contains_certificate_in_round_from(round, primary.gateway.account().address()));
        // Check the round was incremented.
        assert_eq!(primary.current_round(), round + 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_batch_signature_from_peer_in_round() {
        let round = 5;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng).await;
        map_account_addresses(&primary, &accounts);

        // Generate certificates.
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Create a valid proposal.
        let timestamp = now();
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            timestamp,
            &mut rng,
        );

        // Store the proposal on the primary.
        *primary.proposed_batch.write() = Some(proposal);

        // Each committee member signs the batch.
        let signatures = peer_signatures_for_proposal(&primary, &accounts, &mut rng);

        // Have the primary process the signatures.
        for (socket_addr, signature) in signatures {
            primary.process_batch_signature_from_peer(socket_addr, signature).await.unwrap();
        }

        // Check the certificate was created and stored by the primary.
        assert!(primary.storage.contains_certificate_in_round_from(round, primary.gateway.account().address()));
        // Check the round was incremented.
        assert_eq!(primary.current_round(), round + 1);
    }

    #[tokio::test]
    async fn test_batch_signature_from_peer_no_quorum() {
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng).await;
        map_account_addresses(&primary, &accounts);

        // Create a valid proposal.
        let round = 1;
        let timestamp = now();
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            Default::default(),
            timestamp,
            &mut rng,
        );

        // Store the proposal on the primary.
        *primary.proposed_batch.write() = Some(proposal);

        // Each committee member signs the batch.
        let signatures = peer_signatures_for_proposal(&primary, &accounts, &mut rng);

        // Have the primary process only one signature, mimicking a lack of quorum.
        let (socket_addr, signature) = signatures.first().unwrap();
        primary.process_batch_signature_from_peer(*socket_addr, *signature).await.unwrap();

        // Check the certificate was not created and stored by the primary.
        assert!(!primary.storage.contains_certificate_in_round_from(round, primary.gateway.account().address()));
        // Check the round was incremented.
        assert_eq!(primary.current_round(), round);
    }

    #[tokio::test]
    async fn test_batch_signature_from_peer_in_round_no_quorum() {
        let round = 7;
        let mut rng = TestRng::default();
        let (primary, accounts) = primary_without_handlers(&mut rng).await;
        map_account_addresses(&primary, &accounts);

        // Generate certificates.
        let previous_certificates = store_certificate_chain(&primary, &accounts, round, &mut rng);

        // Create a valid proposal.
        let timestamp = now();
        let proposal = create_test_proposal(
            primary.gateway.account(),
            primary.ledger.current_committee().unwrap(),
            round,
            previous_certificates,
            timestamp,
            &mut rng,
        );

        // Store the proposal on the primary.
        *primary.proposed_batch.write() = Some(proposal);

        // Each committee member signs the batch.
        let signatures = peer_signatures_for_proposal(&primary, &accounts, &mut rng);

        // Have the primary process only one signature, mimicking a lack of quorum.
        let (socket_addr, signature) = signatures.first().unwrap();
        primary.process_batch_signature_from_peer(*socket_addr, *signature).await.unwrap();

        // Check the certificate was not created and stored by the primary.
        assert!(!primary.storage.contains_certificate_in_round_from(round, primary.gateway.account().address()));
        // Check the round was incremented.
        assert_eq!(primary.current_round(), round);
    }
}
