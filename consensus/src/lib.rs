// Copyright(C) Facebook, Inc. and its affiliates.
use config::Committee;
use crypto::Hash as _;
use log::{debug, info, warn};
use primary::{CommittedBatch, Epoch, GlobalSequence, PbftError, PbftResult};
use std::collections::BTreeMap;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::watch;

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/global_order_tests.rs"]
mod global_order_tests;

/// A verified PBFT decision in the contiguous global log across finite epochs.
#[derive(Clone, Debug)]
pub struct OrderedBatch {
    pub global_sequence: GlobalSequence,
    pub committed: CommittedBatch,
}

struct GlobalOrder {
    epoch: Epoch,
    delivered: GlobalSequence,
    pending: BTreeMap<GlobalSequence, CommittedBatch>,
}

impl GlobalOrder {
    fn new(epoch: Epoch) -> Self {
        Self {
            delivered: epoch.first_sn() - 1,
            epoch,
            pending: BTreeMap::new(),
        }
    }

    fn insert(&mut self, committee: &Committee, decision: CommittedBatch) -> PbftResult<()> {
        decision.verify(committee)?;
        let position = decision.proposal.position;
        if position.epoch != self.epoch.number {
            return Err(PbftError::WrongEpoch {
                expected: self.epoch.number,
                received: position.epoch,
            });
        }
        let global = position.global_sequence(committee.size() as u64, self.epoch.length())?;
        if global <= self.delivered {
            return Ok(());
        }
        if let Some(previous) = self.pending.get(&global) {
            if previous.proposal.digest() != decision.proposal.digest() {
                return Err(PbftError::ConflictingGlobalDecision(global));
            }
            return Ok(());
        }
        // Retain verified current-epoch decisions even beyond the PBFT receive
        // window. Finite segment quotas bound the normal epoch's backlog.
        self.pending.insert(global, decision);
        Ok(())
    }

    fn pop_next(&mut self) -> Option<OrderedBatch> {
        let global_sequence = self.delivered.checked_add(1)?;
        let committed = self.pending.remove(&global_sequence)?;
        self.delivered = global_sequence;
        Some(OrderedBatch {
            global_sequence,
            committed,
        })
    }

    fn advance_epoch(&mut self) {
        assert_eq!(self.delivered, self.epoch.last_sn());
        assert!(self.pending.is_empty());
        self.epoch = self.epoch.next().expect("Global sequence space exhausted");
    }
}

/// Merge finite segments into one global log. Only a complete, delivered epoch
/// releases the local cores and proposer into the next epoch.
pub struct Consensus;

impl Consensus {
    pub fn spawn(
        committee: Committee,
        epoch_length: u64,
        tx_epoch: watch::Sender<u64>,
        mut decisions: Receiver<CommittedBatch>,
        output: Sender<OrderedBatch>,
    ) {
        let epoch = Epoch::new(*tx_epoch.borrow(), epoch_length, committee.size() as u64)
            .expect("Invalid epoch layout");
        tokio::spawn(async move {
            let mut order = GlobalOrder::new(epoch);
            info!(
                "ISS global ordering enabled: {} instances, {} slots per epoch",
                committee.size(),
                epoch_length
            );
            debug!(
                "Started epoch={} global_sn={}..={}",
                order.epoch.number,
                order.epoch.first_sn(),
                order.epoch.last_sn()
            );
            while let Some(decision) = decisions.recv().await {
                let position = decision.proposal.position;
                if position.epoch != order.epoch.number {
                    // Local cores cannot decide a future epoch before their
                    // local barrier opens; next-epoch protocol messages buffer there.
                    debug!(
                        "Ignoring decision outside current epoch position={:?} epoch={}",
                        position, order.epoch.number
                    );
                    continue;
                }
                if let Err(error) = order.insert(&committee, decision) {
                    warn!("Rejected global ordering input: {}", error);
                    continue;
                }
                while let Some(ordered) = order.pop_next() {
                    let proposal = &ordered.committed.proposal;
                    let sn = ordered.global_sequence;
                    debug!(
                        "Globally ordered {} -> {:?} (instance {}, global {}) epoch={}",
                        proposal,
                        proposal.digest(),
                        proposal.position.instance,
                        sn,
                        order.epoch.number
                    );
                    debug!("Delivering position={:?} global_sn={} proposal={:?} batch={:?} tx_count={} tx_bytes={}",
                        proposal.position, sn, proposal.digest(), proposal.batch.id,
                        proposal.batch.transactions.len(), proposal.batch.transactions.iter().map(Vec::len).sum::<usize>());
                    #[cfg(not(feature = "benchmark"))]
                    info!(
                        "Committed {} (instance {}) epoch={} global_sn={}",
                        proposal, proposal.position.instance, order.epoch.number, sn
                    );
                    #[cfg(feature = "benchmark")]
                    if !proposal.batch.transactions.is_empty() {
                        info!(
                            "Committed {} -> {:?} (instance {}) epoch={} global_sn={}",
                            proposal,
                            proposal.digest(),
                            proposal.position.instance,
                            order.epoch.number,
                            sn
                        );
                    }
                    if output.send(ordered).await.is_err() {
                        return;
                    }
                    if sn == order.epoch.last_sn() {
                        debug!(
                            "Completed epoch={} slots={} last_global_sn={}",
                            order.epoch.number,
                            order.epoch.length(),
                            sn
                        );
                        order.advance_epoch();
                        if tx_epoch.send(order.epoch.number).is_err() {
                            return;
                        }
                        debug!(
                            "Started epoch={} global_sn={}..={}",
                            order.epoch.number,
                            order.epoch.first_sn(),
                            order.epoch.last_sn()
                        );
                        break;
                    }
                }
            }
        });
    }
}
