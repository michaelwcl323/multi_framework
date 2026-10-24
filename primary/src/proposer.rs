// Copyright(C) Facebook, Inc. and its affiliates.
use crate::epoch::{Epoch, Segment};
use crate::messages::{Batch, Instance, Position, PrePrepare, Sequence, Transaction};
use crypto::{Hash as _, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
#[cfg(feature = "benchmark")]
use std::convert::TryInto;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
mod tests;

pub(crate) struct Proposer {
    name: PublicKey,
    instance: Instance,
    segment: Segment,
    epoch_length: u64,
    segments: u64,
    rx_epoch: watch::Receiver<u64>,
    signatures: SignatureService,
    batch_size: usize,
    max_batch_delay: u64,
    rx_committed: Receiver<Position>,
    rx_transactions: Receiver<Transaction>,
    tx_core: Sender<PrePrepare>,
    sequence: Sequence,
    in_flight: bool,
    transactions: Vec<Transaction>,
    rx_view: Option<watch::Receiver<(u64, u64)>>,
    payload_size: usize,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        instance: Instance,
        epoch_length: u64,
        segments: u64,
        rx_epoch: watch::Receiver<u64>,
        signatures: SignatureService,
        batch_size: usize,
        max_batch_delay: u64,
        rx_committed: Receiver<Position>,
        rx_transactions: Receiver<Transaction>,
        tx_core: Sender<PrePrepare>,
        rx_view: Option<watch::Receiver<(u64, u64)>>,
    ) {
        tokio::spawn(async move {
            let epoch = Epoch::new(*rx_epoch.borrow(), epoch_length, segments)
                .expect("Invalid epoch layout");
            Self {
                name,
                instance,
                segment: epoch.segment(instance).expect("Unknown segment"),
                epoch_length,
                segments,
                rx_epoch,
                signatures,
                batch_size,
                max_batch_delay,
                rx_committed,
                rx_transactions,
                tx_core,
                sequence: 1,
                in_flight: false,
                transactions: Vec::new(),
                rx_view,
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    async fn make_proposal(&mut self) {
        let batch = Batch::new(self.transactions.drain(..).collect());
        let proposal = PrePrepare::new(
            Position::for_epoch(self.segment.epoch, self.instance, self.sequence),
            batch,
            self.name,
            &mut self.signatures,
        )
        .await;
        debug!(
            "Created {} -> {:?} position={:?} global_sn={} batch={:?} tx_count={} tx_bytes={}",
            proposal,
            proposal.digest(),
            proposal.position,
            self.segment.global_sn(self.sequence).unwrap(),
            proposal.batch.id,
            proposal.batch.transactions.len(),
            self.payload_size
        );
        #[cfg(feature = "benchmark")]
        if !proposal.batch.transactions.is_empty() {
            // The benchmark ID identifies the signed protocol position, including
            // for two identical payloads proposed at different sequence numbers.
            let id = proposal.digest();
            info!("Batch {:?} contains {} B", id, self.payload_size);
            for tx in &proposal.batch.transactions {
                if tx.len() > 8 && tx[0] == 0 {
                    let sample = u64::from_be_bytes(tx[1..9].try_into().unwrap());
                    info!("Batch {:?} contains sample tx {}", id, sample);
                }
            }
            info!("Created {} -> {:?}", proposal, id);
        }
        self.tx_core
            .send(proposal)
            .await
            .expect("Core channel closed");
        self.in_flight = true;
        self.payload_size = 0;
    }

    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);
        loop {
            let epoch_number = *self.rx_epoch.borrow_and_update();
            if epoch_number != self.segment.epoch {
                assert!(
                    epoch_number > self.segment.epoch,
                    "Epoch must not move backwards"
                );
                self.segment = Epoch::new(epoch_number, self.epoch_length, self.segments)
                    .expect("Global sequence space exhausted")
                    .segment(self.instance)
                    .unwrap();
                self.sequence = 1;
                self.in_flight = false;
                timer
                    .as_mut()
                    .reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
            }
            let stopped = self.rx_view.as_ref().map_or(false, |rx| {
                let (epoch, view) = *rx.borrow();
                epoch == self.segment.epoch && view > 0
            });
            if !stopped
                && self.sequence <= self.segment.slots
                && !self.in_flight
                && (timer.is_elapsed() || self.payload_size >= self.batch_size)
            {
                self.make_proposal().await;
                timer
                    .as_mut()
                    .reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
            }
            let rx_view = &mut self.rx_view;
            tokio::select! {
                Some(position) = self.rx_committed.recv() => {
                    if self.in_flight && position == Position::for_epoch(self.segment.epoch, self.instance, self.sequence) {
                        self.in_flight = false;
                        self.sequence += 1;
                    }
                }
                Some(transaction) = self.rx_transactions.recv(), if self.payload_size < self.batch_size => {
                    self.payload_size += transaction.len();
                    self.transactions.push(transaction);
                }
                changed = self.rx_epoch.changed() => {
                    if changed.is_err() { return; }
                },
                changed = async move {
                    match rx_view.as_mut() {
                        Some(rx) => rx.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if changed.is_err() { self.rx_view = None; }
                },
                _ = &mut timer, if !timer.is_elapsed() => {},
                else => break,
            }
        }
    }
}
