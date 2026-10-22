// Copyright(C) Facebook, Inc. and its affiliates.
use config::Committee;
use crypto::Hash as _;
use log::{info, warn};
use primary::{CommittedBatch, Sequence, SEQUENCE_WINDOW};
use std::collections::BTreeMap;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
mod tests;

/// Delivers a contiguous prefix of PBFT decisions. No DAG or additional voting.
pub struct Consensus;

impl Consensus {
    pub fn spawn(
        committee: Committee,
        mut decisions: Receiver<CommittedBatch>,
        output: Sender<CommittedBatch>,
    ) {
        tokio::spawn(async move {
            let mut next: Sequence = 1;
            let mut pending: BTreeMap<Sequence, CommittedBatch> = BTreeMap::new();
            while let Some(decision) = decisions.recv().await {
                if let Err(error) = decision.verify(&committee) {
                    warn!("Rejected PBFT decision: {}", error);
                    continue;
                }
                let sequence = decision.proposal.position.sequence;
                if sequence < next || sequence - next >= SEQUENCE_WINDOW {
                    continue;
                }
                if let Some(previous) = pending.get(&sequence) {
                    if previous.proposal.digest() != decision.proposal.digest() {
                        warn!("Conflicting PBFT decision at sequence {}", sequence);
                    }
                    continue;
                }
                pending.insert(sequence, decision);
                while let Some(decision) = pending.remove(&next) {
                    #[cfg(not(feature = "benchmark"))]
                    info!("Committed {}", decision.proposal);
                    #[cfg(feature = "benchmark")]
                    if !decision.proposal.batch.transactions.is_empty() {
                        info!(
                            "Committed {} -> {:?}",
                            decision.proposal,
                            decision.proposal.digest()
                        );
                    }
                    if output.send(decision).await.is_err() {
                        return;
                    }
                    next += 1;
                }
            }
        });
    }
}
