// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Batch, Certificate, Transaction};
use crate::primary::Round;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
#[cfg(feature = "benchmark")]
use std::convert::TryInto;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

pub struct Proposer {
    name: PublicKey,
    signature_service: SignatureService,
    batch_size: usize,
    max_batch_delay: u64,
    rx_core: Receiver<(Vec<Digest>, Round)>,
    rx_transactions: Receiver<Transaction>,
    tx_core: Sender<Batch>,
    round: Round,
    last_parents: Vec<Digest>,
    transactions: Vec<Transaction>,
    payload_size: usize,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: &Committee,
        signature_service: SignatureService,
        batch_size: usize,
        max_batch_delay: u64,
        rx_core: Receiver<(Vec<Digest>, Round)>,
        rx_transactions: Receiver<Transaction>,
        tx_core: Sender<Batch>,
    ) {
        let genesis = Certificate::genesis(committee)
            .iter()
            .map(|x| x.digest())
            .collect();
        tokio::spawn(async move {
            Self {
                name,
                signature_service,
                batch_size,
                max_batch_delay,
                rx_core,
                rx_transactions,
                tx_core,
                round: 1,
                last_parents: genesis,
                transactions: Vec::new(),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }

    async fn make_batch(&mut self) {
        let batch = Batch::new(
            self.name,
            self.round,
            self.transactions.drain(..).collect(),
            self.last_parents.drain(..).collect(),
            &mut self.signature_service,
        )
        .await;
        debug!("Created {:?}", batch);
        #[cfg(feature = "benchmark")]
        if !batch.transactions.is_empty() {
            // The batch ID identifies the complete signed proposal; no second payload ID exists.
            info!("Batch {:?} contains {} B", batch.id, self.payload_size);
            for tx in &batch.transactions {
                if tx.len() > 8 && tx[0] == 0 {
                    let id = u64::from_be_bytes(tx[1..9].try_into().unwrap());
                    info!("Batch {:?} contains sample tx {}", batch.id, id);
                }
            }
            info!("Created {} -> {:?}", batch, batch.id);
        }
        self.tx_core
            .send(batch)
            .await
            .expect("Failed to send batch proposal");
    }

    pub async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);
        loop {
            let timer_expired = timer.is_elapsed();
            if !self.last_parents.is_empty()
                && (timer_expired || self.payload_size >= self.batch_size)
            {
                // Empty proposals permit DAG progress when client traffic is idle.
                self.make_batch().await;
                self.payload_size = 0;
                timer
                    .as_mut()
                    .reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
            }
            tokio::select! {
                Some((parents, round)) = self.rx_core.recv() => {
                    if round >= self.round {
                        self.round = round + 1;
                        self.last_parents = parents;
                    }
                }
                // Stop reading once a batch is full until parent certificates let us propose it.
                Some(transaction) = self.rx_transactions.recv(), if self.payload_size < self.batch_size => {
                    self.payload_size += transaction.len();
                    self.transactions.push(transaction);
                }
                () = &mut timer, if !timer.is_elapsed() => {}
            }
        }
    }
}
