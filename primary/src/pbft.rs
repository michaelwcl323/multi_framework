use crate::epoch::Segment;
use crate::error::{PbftError, PbftResult};
use crate::messages::{
    Commit, CommittedBatch, Instance, PbftMessage, PrePrepare, Prepare, Sequence, SEQUENCE_WINDOW,
};
use crate::{NewView, PreparedCertificate, ViewChange};
use config::{Committee, Stake};
use crypto::{Hash as _, PublicKey};
use log::debug;
use std::collections::BTreeMap;
use std::sync::Arc;

#[cfg(test)]
#[path = "tests/pbft_tests.rs"]
mod tests;

#[derive(Debug)]
pub(crate) enum Action {
    Prepare(Arc<PrePrepare>),
    Commit(Arc<PrePrepare>),
    Decide(CommittedBatch),
    ChangeView(u64),
    NewView(Vec<ViewChange>),
}

#[derive(Default)]
struct Slot {
    proposal: Option<Arc<PrePrepare>>,
    prepares: BTreeMap<PublicKey, Prepare>,
    commits: BTreeMap<PublicKey, Commit>,
    prepare_sent: bool,
    commit_sent: bool,
    decided: bool,
}

/// Retained until the local epoch barrier, even after live slots are removed.
/// Share the batch with its live slot; materialize a wire certificate only when
/// a ViewChange actually needs to report it.
struct PreparedEvidence {
    proposal: Arc<PrePrepare>,
    prepares: Vec<Prepare>,
    commits: Vec<Commit>,
}

impl PreparedEvidence {
    fn certificate(&self) -> PreparedCertificate {
        PreparedCertificate {
            proposal: self.proposal.as_ref().clone(),
            prepares: self.prepares.clone(),
            commits: self.commits.clone(),
        }
    }
}

/// Deterministic segment-local PBFT. Local votes re-enter
/// through `process`, exactly like remote votes; transport ACKs never enter here.
pub(crate) struct Pbft {
    committee: Committee,
    instance: Instance,
    pub segment: Segment,
    slots: BTreeMap<Sequence, Slot>,
    pub low_watermark: Sequence,
    pub view: u64,
    pub target_view: u64,
    history: BTreeMap<Sequence, PreparedEvidence>,
    changes: BTreeMap<PublicKey, ViewChange>,
    proposed_view: u64,
    installed_view: Option<crypto::Digest>,
    recovery: BTreeMap<Sequence, crypto::Digest>,
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
            view: 0,
            target_view: 0,
            history: BTreeMap::new(),
            changes: BTreeMap::new(),
            proposed_view: 0,
            installed_view: None,
            recovery: BTreeMap::new(),
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
        match &message {
            PbftMessage::ViewChange(change) => return self.view_change(change.clone()),
            PbftMessage::NewView(view) => return self.install_view(view.clone()),
            _ => (),
        }
        let position = message.position();
        // Leaving a view freezes all of its normal-path voting immediately.
        if position.view < self.target_view {
            return Ok(Vec::new());
        }
        ensure!(
            position.view == self.view && self.view == self.target_view,
            PbftError::WrongView {
                expected: self.target_view,
                received: position.view
            }
        );
        message.verify(&self.committee)?;
        let sequence = message.position().sequence;
        ensure!(
            self.segment.global_sn(sequence).is_some(),
            PbftError::OutsideSegment(message.position())
        );
        if self
            .history
            .get(&sequence)
            .map_or(false, |x| !x.commits.is_empty())
        {
            return Ok(Vec::new());
        }
        if sequence < self.low_watermark {
            return Ok(Vec::new());
        }
        ensure!(
            sequence - self.low_watermark < SEQUENCE_WINDOW
                || self.recovery.contains_key(&sequence),
            PbftError::OutsideWindow(sequence)
        );

        let committee = &self.committee;
        let slot = self.slots.entry(sequence).or_default();
        match message {
            PbftMessage::PrePrepare(proposal) => {
                if self.view > 0 {
                    ensure!(
                        self.recovery.get(&sequence) == Some(&proposal.digest()),
                        PbftError::InvalidRecovery
                    );
                }
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
                    slot.proposal = Some(Arc::new(proposal));
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
            PbftMessage::ViewChange(_) | PbftMessage::NewView(_) => unreachable!(),
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
                self.history.insert(
                    sequence,
                    PreparedEvidence {
                        proposal: proposal.clone(),
                        prepares: slot
                            .prepares
                            .values()
                            .filter(|x| x.proposal == digest)
                            .cloned()
                            .collect(),
                        commits: Vec::new(),
                    },
                );
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
                let commits: Vec<_> = slot
                    .commits
                    .values()
                    .filter(|x| x.proposal == digest)
                    .cloned()
                    .collect();
                self.history.get_mut(&sequence).unwrap().commits = commits.clone();
                actions.push(Action::Decide(CommittedBatch {
                    proposal: proposal.as_ref().clone(),
                    commits,
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

    /// Manual/test trigger; signing is performed by Core after voting is frozen.
    pub fn begin_view_change(&mut self, view: u64) -> PbftResult<Vec<PreparedCertificate>> {
        ensure!(
            view > self.view && view >= self.target_view,
            PbftError::WrongView {
                expected: self.target_view,
                received: view
            }
        );
        self.target_view = view;
        Ok(self
            .history
            .values()
            .map(PreparedEvidence::certificate)
            .collect())
    }

    fn check_report(&self, change: &ViewChange) -> PbftResult<()> {
        change.verify(&self.committee)?;
        ensure!(
            change.epoch == self.segment.epoch && change.instance == self.instance,
            PbftError::InvalidRecovery
        );
        for certificate in &change.prepared {
            ensure!(
                self.segment
                    .global_sn(certificate.proposal.position.sequence)
                    .is_some(),
                PbftError::OutsideSegment(certificate.proposal.position)
            );
        }
        Ok(())
    }

    fn view_change(&mut self, change: ViewChange) -> PbftResult<Vec<Action>> {
        self.check_report(&change)?;
        if change.view <= self.view || change.view < self.target_view {
            return Ok(Vec::new());
        }
        if let Some(previous) = self.changes.get(&change.author) {
            if previous.view > change.view {
                return Ok(Vec::new());
            }
            if previous.view == change.view {
                ensure!(
                    previous.digest() == change.digest(),
                    PbftError::ConflictingViewChange(change.author)
                );
                return Ok(Vec::new());
            }
        }
        let view = change.view;
        self.changes.insert(change.author, change);
        let reports: Vec<_> = self
            .changes
            .values()
            .filter(|x| x.view == view)
            .cloned()
            .collect();
        let weight: Stake = reports
            .iter()
            .map(|x| self.committee.stake(&x.author))
            .sum();
        let mut actions = Vec::new();
        // A single faulty node cannot make the replica abandon its active view.
        if view > self.target_view && weight >= self.committee.validity_threshold() {
            actions.push(Action::ChangeView(view));
        }
        if weight >= self.committee.quorum_threshold() && self.proposed_view < view {
            self.proposed_view = view;
            actions.push(Action::NewView(reports));
        }
        Ok(actions)
    }

    fn install_view(&mut self, new_view: NewView) -> PbftResult<Vec<Action>> {
        new_view.verify(&self.committee)?;
        for change in &new_view.changes {
            self.check_report(change)?;
        }
        if new_view.view < self.target_view || new_view.view < self.view {
            return Ok(Vec::new());
        }
        if new_view.view == self.view {
            ensure!(
                self.installed_view.as_ref() == Some(&new_view.digest()),
                PbftError::InvalidRecovery
            );
            return Ok(Vec::new());
        }
        let selected: BTreeMap<_, _> = new_view
            .proposals
            .iter()
            .map(|p| (p.position.sequence, p))
            .collect();
        for (sequence, certificate) in &self.history {
            if !certificate.commits.is_empty() {
                ensure!(
                    selected.get(sequence).map(|p| &p.batch.id)
                        == Some(&certificate.proposal.batch.id),
                    PbftError::InvalidRecovery
                );
            }
        }
        // Verify the complete NewView before changing any protocol state.
        self.slots.clear();
        self.view = new_view.view;
        self.target_view = new_view.view;
        self.installed_view = Some(new_view.digest());
        self.recovery = new_view
            .proposals
            .iter()
            .map(|p| (p.position.sequence, p.digest()))
            .collect();
        let view = self.view;
        self.changes.retain(|_, report| report.view > view);
        let mut actions = Vec::new();
        // A complete old commit certificate can be delivered directly to a
        // lagging replica; replicas that already decided do not deliver twice.
        for change in &new_view.changes {
            for certificate in &change.prepared {
                if certificate.commits.is_empty() {
                    continue;
                }
                let sequence = certificate.proposal.position.sequence;
                let known = self
                    .history
                    .get(&sequence)
                    .map_or(false, |old| !old.commits.is_empty());
                if !known {
                    self.history.insert(
                        sequence,
                        PreparedEvidence {
                            proposal: Arc::new(certificate.proposal.clone()),
                            prepares: certificate.prepares.clone(),
                            commits: certificate.commits.clone(),
                        },
                    );
                    actions.push(Action::Decide(CommittedBatch {
                        proposal: certificate.proposal.clone(),
                        commits: certificate.commits.clone(),
                    }));
                }
            }
        }
        for (sequence, certificate) in &self.history {
            if *sequence >= self.low_watermark && !certificate.commits.is_empty() {
                self.slots.insert(
                    *sequence,
                    Slot {
                        decided: true,
                        ..Slot::default()
                    },
                );
            }
        }
        while matches!(self.slots.get(&self.low_watermark), Some(slot) if slot.decided) {
            self.slots.remove(&self.low_watermark);
            self.low_watermark += 1;
        }
        for proposal in new_view.proposals {
            if self
                .history
                .get(&proposal.position.sequence)
                .map_or(false, |x| !x.commits.is_empty())
            {
                continue;
            }
            actions.extend(self.process(PbftMessage::PrePrepare(proposal))?);
        }
        Ok(actions)
    }
}
