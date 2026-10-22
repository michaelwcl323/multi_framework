// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Batch, Instance, Position, PrePrepare, Sequence, Transaction};
use crypto::{Hash as _, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
#[cfg(feature = "benchmark")]
use std::convert::TryInto;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
mod tests;

pub(crate) struct Proposer {
    name: PublicKey,
    instance: Instance,
    signatures: SignatureService,
    batch_size: usize,
    max_batch_delay: u64,
    rx_committed: Receiver<Sequence>,
    rx_transactions: Receiver<Transaction>,
    tx_core: Sender<PrePrepare>,
    sequence: Sequence,
    in_flight: bool,
    transactions: Vec<Transaction>,
    payload_size: usize,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        instance: Instance,
        signatures: SignatureService,
        batch_size: usize,
        max_batch_delay: u64,
        rx_committed: Receiver<Sequence>,
        rx_transactions: Receiver<Transaction>,
        tx_core: Sender<PrePrepare>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                instance,
                signatures,
                batch_size,
                max_batch_delay,
                rx_committed,
                rx_transactions,
                tx_core,
                sequence: 1,
                in_flight: false,
                transactions: Vec::new(),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    async fn make_proposal(&mut self) {
        let batch = Batch::new(self.transactions.drain(..).collect());
        let proposal = PrePrepare::new(
            Position::for_instance(self.instance, self.sequence),
            batch,
            self.name,
            &mut self.signatures,
        )
        .await;
        debug!("Created {} -> {:?}", proposal, proposal.digest());
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
            if !self.in_flight && (timer.is_elapsed() || self.payload_size >= self.batch_size) {
                self.make_proposal().await;
                timer
                    .as_mut()
                    .reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
            }
            tokio::select! {
                Some(sequence) = self.rx_committed.recv() => {
                    if self.in_flight && sequence == self.sequence {
                        self.in_flight = false;
                        self.sequence += 1;
                    }
                }
                Some(transaction) = self.rx_transactions.recv(), if self.payload_size < self.batch_size => {
                    self.payload_size += transaction.len();
                    self.transactions.push(transaction);
                }
                _ = &mut timer, if !timer.is_elapsed() => {},
                else => break,
            }
        }
    }
}
