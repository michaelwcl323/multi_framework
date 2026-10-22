// Copyright(C) Facebook, Inc. and its affiliates.
use config::Committee;
use crypto::Hash as _;
use log::{debug, info, warn};
use primary::{CommittedBatch, Epoch, GlobalSn};
use std::collections::BTreeMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
mod tests;

/// Merge finite segments into one global log. Only a complete, delivered epoch
/// releases the local cores and proposer into the next epoch.
pub struct Consensus;

impl Consensus {
    pub fn spawn(
        committee: Committee,
        epoch_length: u64,
        tx_epoch: watch::Sender<u64>,
        mut decisions: Receiver<CommittedBatch>,
        output: Sender<CommittedBatch>,
    ) {
        let mut epoch = Epoch::new(*tx_epoch.borrow(), epoch_length, committee.size() as u64)
            .expect("Invalid epoch layout");
        tokio::spawn(async move {
            let mut next = epoch.first_sn();
            let mut pending: BTreeMap<GlobalSn, CommittedBatch> = BTreeMap::new();
            debug!(
                "Started epoch={} global_sn={}..={}",
                epoch.number,
                epoch.first_sn(),
                epoch.last_sn()
            );
            while let Some(decision) = decisions.recv().await {
                let position = decision.proposal.position;
                let sn = match epoch.global_sn(position) {
                    Some(sn) if sn >= next => sn,
                    _ => {
                        debug!("Ignoring decision outside current epoch/window position={:?} epoch={} next_global_sn={}", position, epoch.number, next);
                        continue;
                    }
                };
                if let Err(error) = decision.verify(&committee) {
                    warn!("Rejected PBFT decision: {}", error);
                    continue;
                }
                if let Some(previous) = pending.get(&sn) {
                    if previous.proposal.digest() != decision.proposal.digest() {
                        warn!(
                            "Conflicting PBFT decision at global_sn={} position={:?}",
                            sn, position
                        );
                    }
                    continue;
                }
                pending.insert(sn, decision);
                while let Some(decision) = pending.remove(&next) {
                    debug!(
                        "Delivering position={:?} global_sn={} proposal={:?} batch={:?} tx_count={} tx_bytes={}",
                        decision.proposal.position,
                        next,
                        decision.proposal.digest(),
                        decision.proposal.batch.id,
                        decision.proposal.batch.transactions.len(),
                        decision.proposal.batch.transactions.iter().map(Vec::len).sum::<usize>()
                    );
                    #[cfg(not(feature = "benchmark"))]
                    info!(
                        "Committed {} (instance {}) epoch={} global_sn={}",
                        decision.proposal, decision.proposal.position.instance, epoch.number, next
                    );
                    #[cfg(feature = "benchmark")]
                    if !decision.proposal.batch.transactions.is_empty() {
                        info!(
                            "Committed {} -> {:?} (instance {}) epoch={} global_sn={}",
                            decision.proposal,
                            decision.proposal.digest(),
                            decision.proposal.position.instance,
                            epoch.number,
                            next
                        );
                    }
                    if output.send(decision).await.is_err() {
                        return;
                    }
                    if next == epoch.last_sn() {
                        debug!(
                            "Completed epoch={} slots={} last_global_sn={}",
                            epoch.number,
                            epoch.length(),
                            next
                        );
                        assert!(pending.is_empty());
                        epoch = epoch.next().expect("Global sequence space exhausted");
                        next = epoch.first_sn();
                        if tx_epoch.send(epoch.number).is_err() {
                            return;
                        }
                        debug!(
                            "Started epoch={} global_sn={}..={}",
                            epoch.number,
                            epoch.first_sn(),
                            epoch.last_sn()
                        );
                        break;
                    }
                    next += 1;
                }
            }
        });
    }
}
