// Copyright(C) Facebook, Inc. and its affiliates.
use config::Committee;
use crypto::Hash as _;
use log::{info, warn};
use primary::{CommittedBatch, Instance, Sequence, SEQUENCE_WINDOW};
use std::collections::BTreeMap;
use tokio::sync::mpsc::{Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
mod tests;

#[derive(Default)]
struct DeliveryState {
    delivered: Sequence,
    pending: BTreeMap<Sequence, CommittedBatch>,
}

/// Each instance delivers its own contiguous prefix. Cross-instance arrival
/// order is unconstrained: this stage does not construct a global total order.
pub struct Consensus;

impl Consensus {
    pub fn spawn(
        committee: Committee,
        mut decisions: Receiver<CommittedBatch>,
        output: Sender<CommittedBatch>,
    ) {
        tokio::spawn(async move {
            let mut instances: BTreeMap<Instance, DeliveryState> = BTreeMap::new();
            while let Some(decision) = decisions.recv().await {
                if let Err(error) = decision.verify(&committee) {
                    warn!("Rejected PBFT decision: {}", error);
                    continue;
                }
                let instance = decision.proposal.position.instance;
                let state = instances.entry(instance).or_default();
                let next = state.delivered + 1;
                let sequence = decision.proposal.position.sequence;
                if sequence < next || sequence - next >= SEQUENCE_WINDOW {
                    continue;
                }
                if let Some(previous) = state.pending.get(&sequence) {
                    if previous.proposal.digest() != decision.proposal.digest() {
                        warn!(
                            "Conflicting PBFT decision at instance {}, sequence {}",
                            instance, sequence
                        );
                    }
                    continue;
                }
                state.pending.insert(sequence, decision);
                while let Some(decision) = state.pending.remove(&(state.delivered + 1)) {
                    #[cfg(not(feature = "benchmark"))]
                    info!("Committed {} (instance {})", decision.proposal, instance);
                    #[cfg(feature = "benchmark")]
                    if !decision.proposal.batch.transactions.is_empty() {
                        info!(
                            "Committed {} -> {:?} (instance {})",
                            decision.proposal,
                            decision.proposal.digest(),
                            instance
                        );
                    }
                    if output.send(decision).await.is_err() {
                        return;
                    }
                    state.delivered += 1;
                }
            }
        });
    }
}
