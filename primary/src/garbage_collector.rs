// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::Certificate;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

/// Receives the highest round reached by consensus and update it for all tasks.
pub struct GarbageCollector {
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// Receives the ordered certificates from consensus.
    rx_consensus: Receiver<Certificate>,
}

impl GarbageCollector {
    pub fn spawn(consensus_round: Arc<AtomicU64>, rx_consensus: Receiver<Certificate>) {
        tokio::spawn(async move {
            Self {
                consensus_round,
                rx_consensus,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut last_committed_round = 0;
        while let Some(certificate) = self.rx_consensus.recv().await {
            let round = certificate.round();
            if round > last_committed_round {
                last_committed_round = round;

                // Trigger cleanup on the primary.
                self.consensus_round.store(round, Ordering::Relaxed);
            }
        }
    }
}
