use crate::epoch::Segment;
use crate::error::{PbftError, PbftResult};
use crate::messages::{
    Commit, CommittedBatch, Instance, PbftMessage, PrePrepare, Prepare, Sequence, SEQUENCE_WINDOW,
};
use config::{Committee, Stake};
use crypto::{Hash as _, PublicKey};
use log::debug;
use std::collections::BTreeMap;

#[cfg(test)]
#[path = "tests/pbft_tests.rs"]
mod tests;

#[derive(Debug)]
pub(crate) enum Action {
    Prepare(PrePrepare),
    Commit(PrePrepare),
    Decide(CommittedBatch),
}

#[derive(Default)]
struct Slot {
    proposal: Option<PrePrepare>,
    prepares: BTreeMap<PublicKey, Prepare>,
    commits: BTreeMap<PublicKey, Commit>,
    prepare_sent: bool,
    commit_sent: bool,
    decided: bool,
}

/// Deterministic fixed-leader, fixed-view PBFT normal path. Local votes re-enter
/// through `process`, exactly like remote votes; transport ACKs never enter here.
pub(crate) struct Pbft {
    committee: Committee,
    instance: Instance,
    pub segment: Segment,
    slots: BTreeMap<Sequence, Slot>,
    pub low_watermark: Sequence,
}

impl Pbft {
    pub fn new(committee: Committee, segment: Segment) -> Self {
        let instance = segment.instance;
        assert!(
            crate::leader(&committee, instance).is_some(),
            "Unknown PBFT instance"
        );
        Self {
            committee,
            instance,
            segment,
            slots: BTreeMap::new(),
            low_watermark: 1,
        }
    }

    pub fn process(&mut self, message: PbftMessage) -> PbftResult<Vec<Action>> {
        ensure!(
            message.position().epoch == self.segment.epoch,
            PbftError::WrongEpoch {
                expected: self.segment.epoch,
                received: message.position().epoch,
            }
        );
        ensure!(
            message.position().instance == self.instance,
            PbftError::WrongInstance {
                expected: self.instance,
                received: message.position().instance
            }
        );
        message.verify(&self.committee)?;
        let sequence = message.position().sequence;
        ensure!(
            self.segment.global_sn(sequence).is_some(),
            PbftError::OutsideSegment(message.position())
        );
        if sequence < self.low_watermark {
            return Ok(Vec::new());
        }
        ensure!(
            sequence - self.low_watermark < SEQUENCE_WINDOW,
            PbftError::OutsideWindow(sequence)
        );

        let committee = &self.committee;
        let slot = self.slots.entry(sequence).or_default();
        match message {
            PbftMessage::PrePrepare(proposal) => {
                debug!(
                    "Received PrePrepare position={:?} sender={} proposal={:?} batch={:?} tx_count={} tx_bytes={}",
                    proposal.position, proposal.author, proposal.digest(), proposal.batch.id,
                    proposal.batch.transactions.len(),
                    proposal.batch.transactions.iter().map(Vec::len).sum::<usize>()
                );
                if let Some(accepted) = &slot.proposal {
                    ensure!(
                        accepted.digest() == proposal.digest(),
                        PbftError::ConflictingProposal(sequence)
                    );
                } else {
                    slot.proposal = Some(proposal);
                }
            }
            PbftMessage::Prepare(prepare) => {
                debug!(
                    "Received Prepare position={:?} sender={} proposal={:?}",
                    prepare.position, prepare.author, prepare.proposal
                );
                if let Some(previous) = slot.prepares.get(&prepare.author) {
                    ensure!(
                        previous.proposal == prepare.proposal,
                        PbftError::ConflictingVote(prepare.author, sequence)
                    );
                } else {
                    slot.prepares.insert(prepare.author, prepare);
                }
            }
            PbftMessage::Commit(commit) => {
                debug!(
                    "Received Commit position={:?} sender={} proposal={:?}",
                    commit.position, commit.author, commit.proposal
                );
                if let Some(previous) = slot.commits.get(&commit.author) {
                    ensure!(
                        previous.proposal == commit.proposal,
                        PbftError::ConflictingVote(commit.author, sequence)
                    );
                } else {
                    slot.commits.insert(commit.author, commit);
                }
            }
        }

        let mut actions = Vec::new();
        if let Some(proposal) = &slot.proposal {
            let digest = proposal.digest();
            if !slot.prepare_sent {
                slot.prepare_sent = true;
                actions.push(Action::Prepare(proposal.clone()));
            }
            let prepare_weight: Stake = slot
                .prepares
                .values()
                .filter(|x| x.proposal == digest)
                .map(|x| committee.stake(&x.author))
                .sum();
            if !slot.commit_sent && prepare_weight >= committee.quorum_threshold() {
                debug!(
                    "PBFT prepared position={:?} proposal={:?} prepare_weight={} quorum={}",
                    proposal.position,
                    digest,
                    prepare_weight,
                    committee.quorum_threshold()
                );
                slot.commit_sent = true;
                actions.push(Action::Commit(proposal.clone()));
            }
            let commit_weight: Stake = slot
                .commits
                .values()
                .filter(|x| x.proposal == digest)
                .map(|x| committee.stake(&x.author))
                .sum();
            debug!("PBFT votes position={:?} proposal={:?} prepare_weight={} commit_weight={} quorum={}", proposal.position, digest, prepare_weight, commit_weight, committee.quorum_threshold());
            // A commit quorum alone never bypasses local prepared state.
            if !slot.decided && slot.commit_sent && commit_weight >= committee.quorum_threshold() {
                debug!(
                    "PBFT commit quorum position={:?} proposal={:?} commit_weight={} quorum={}",
                    proposal.position,
                    digest,
                    commit_weight,
                    committee.quorum_threshold()
                );
                slot.decided = true;
                actions.push(Action::Decide(CommittedBatch {
                    proposal: proposal.clone(),
                    commits: slot
                        .commits
                        .values()
                        .filter(|x| x.proposal == digest)
                        .cloned()
                        .collect(),
                }));
            }
        }

        // Advance only across a contiguous decided prefix, never over a hole.
        while matches!(self.slots.get(&self.low_watermark), Some(slot) if slot.decided) {
            self.slots.remove(&self.low_watermark);
            self.low_watermark += 1;
        }
        if self.low_watermark > self.segment.slots {
            debug!(
                "Completed segment epoch={} instance={} slots={}",
                self.segment.epoch, self.segment.instance, self.segment.slots
            );
        }
        Ok(actions)
    }
}
