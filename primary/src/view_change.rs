//! Signed, segment-local recovery for finite epochs without checkpoints.
//!
//! Selection follows the highest prepared view in a quorum of reports. Slots
//! without evidence stay undecided in this first stage; they are not converted
//! into ordinary empty batches. A later SB-bottom phase can fill those holes.
use crate::{
    primary, Commit, CommittedBatch, Instance, PbftError, PbftResult, Position, PrePrepare,
    Prepare, Sequence,
};
use config::Committee;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::{Digest as _, Sha512};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::convert::TryInto;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PreparedCertificate {
    pub proposal: PrePrepare,
    pub prepares: Vec<Prepare>,
    /// Empty, or a complete commit quorum for this proposal.
    pub commits: Vec<Commit>,
}

impl PreparedCertificate {
    pub fn verify(&self, committee: &Committee) -> PbftResult<()> {
        self.proposal.verify(committee)?;
        let digest = self.proposal.digest();
        let mut authors = HashSet::new();
        let mut weight = 0;
        for prepare in &self.prepares {
            prepare.verify(committee)?;
            ensure!(
                prepare.position == self.proposal.position && prepare.proposal == digest,
                PbftError::InvalidRecovery
            );
            ensure!(
                authors.insert(prepare.author),
                PbftError::AuthorityReuse(prepare.author)
            );
            weight += committee.stake(&prepare.author);
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            PbftError::RequiresQuorum
        );
        if !self.commits.is_empty() {
            CommittedBatch {
                proposal: self.proposal.clone(),
                commits: self.commits.clone(),
            }
            .verify(committee)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ViewChange {
    pub epoch: u64,
    pub instance: Instance,
    pub view: u64,
    pub prepared: Vec<PreparedCertificate>,
    pub author: PublicKey,
    pub signature: Signature,
}

impl ViewChange {
    pub async fn new(
        epoch: u64,
        instance: Instance,
        view: u64,
        prepared: Vec<PreparedCertificate>,
        author: PublicKey,
        signatures: &mut SignatureService,
    ) -> Self {
        let mut message = Self {
            epoch,
            instance,
            view,
            prepared,
            author,
            signature: Signature::default(),
        };
        message.signature = signatures.request_signature(message.digest()).await;
        message
    }

    /// Sequence one is only an envelope for routing control messages.
    pub fn position(&self) -> Position {
        Position {
            epoch: self.epoch,
            instance: self.instance,
            view: self.view,
            sequence: 1,
        }
    }

    pub fn verify(&self, committee: &Committee) -> PbftResult<()> {
        verify_target(committee, self.instance, self.view, self.author)?;
        self.signature.verify(&self.digest(), &self.author)?;
        let mut slots = HashSet::new();
        for certificate in &self.prepared {
            let position = certificate.proposal.position;
            ensure!(
                position.epoch == self.epoch
                    && position.instance == self.instance
                    && position.view < self.view
                    && slots.insert(position.sequence),
                PbftError::InvalidRecovery
            );
            certificate.verify(committee)?;
        }
        Ok(())
    }
}

impl Hash for ViewChange {
    fn digest(&self) -> Digest {
        signed_digest(
            b"iss/pbft/view-change/v1",
            &(
                self.epoch,
                self.instance,
                self.view,
                &self.prepared,
                self.author,
            ),
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NewView {
    pub epoch: u64,
    pub instance: Instance,
    pub view: u64,
    pub changes: Vec<ViewChange>,
    pub proposals: Vec<PrePrepare>,
    pub author: PublicKey,
    pub signature: Signature,
}

impl NewView {
    pub async fn new(
        committee: &Committee,
        changes: Vec<ViewChange>,
        author: PublicKey,
        signatures: &mut SignatureService,
    ) -> PbftResult<Self> {
        let first = changes.first().ok_or(PbftError::RequiresQuorum)?;
        let (epoch, instance, view) = (first.epoch, first.instance, first.view);
        verify_target(committee, instance, view, author)?;
        ensure!(
            primary(committee, instance, view) == Some(author),
            PbftError::UnexpectedLeader(author)
        );
        let selected = select(committee, epoch, instance, view, &changes)?;
        let mut proposals = Vec::new();
        for previous in selected.values() {
            proposals.push(PrePrepare::recover(previous, view, author, signatures).await);
        }
        let mut message = Self {
            epoch,
            instance,
            view,
            changes,
            proposals,
            author,
            signature: Signature::default(),
        };
        message.signature = signatures.request_signature(message.digest()).await;
        Ok(message)
    }

    pub fn position(&self) -> Position {
        Position {
            epoch: self.epoch,
            instance: self.instance,
            view: self.view,
            sequence: 1,
        }
    }

    pub fn verify(&self, committee: &Committee) -> PbftResult<()> {
        verify_target(committee, self.instance, self.view, self.author)?;
        ensure!(
            primary(committee, self.instance, self.view) == Some(self.author),
            PbftError::UnexpectedLeader(self.author)
        );
        self.signature.verify(&self.digest(), &self.author)?;
        let mut selected = select(
            committee,
            self.epoch,
            self.instance,
            self.view,
            &self.changes,
        )?;
        ensure!(
            selected.len() == self.proposals.len(),
            PbftError::InvalidRecovery
        );
        for proposal in &self.proposals {
            proposal.verify(committee)?;
            let position = proposal.position;
            ensure!(
                position.epoch == self.epoch
                    && position.instance == self.instance
                    && position.view == self.view,
                PbftError::InvalidRecovery
            );
            let previous = selected
                .remove(&position.sequence)
                .ok_or(PbftError::InvalidRecovery)?;
            ensure!(
                previous.batch.id == proposal.batch.id,
                PbftError::InvalidRecovery
            );
        }
        ensure!(selected.is_empty(), PbftError::InvalidRecovery);
        Ok(())
    }
}

impl Hash for NewView {
    fn digest(&self) -> Digest {
        signed_digest(
            b"iss/pbft/new-view/v1",
            &(
                self.epoch,
                self.instance,
                self.view,
                &self.changes,
                &self.proposals,
                self.author,
            ),
        )
    }
}

fn verify_target(
    committee: &Committee,
    instance: Instance,
    view: u64,
    author: PublicKey,
) -> PbftResult<()> {
    ensure!(view > 0, PbftError::InvalidRecovery);
    ensure!(
        primary(committee, instance, view).is_some(),
        PbftError::UnknownInstance(instance)
    );
    ensure!(
        committee.stake(&author) > 0,
        PbftError::UnknownAuthority(author)
    );
    Ok(())
}

fn select<'a>(
    committee: &Committee,
    epoch: u64,
    instance: Instance,
    view: u64,
    changes: &'a [ViewChange],
) -> PbftResult<BTreeMap<Sequence, &'a PrePrepare>> {
    let mut authors = HashSet::new();
    let mut weight = 0;
    let mut selected: BTreeMap<Sequence, &PrePrepare> = BTreeMap::new();
    let mut committed = BTreeMap::new();
    for change in changes {
        change.verify(committee)?;
        ensure!(
            change.epoch == epoch && change.instance == instance && change.view == view,
            PbftError::InvalidRecovery
        );
        ensure!(
            authors.insert(change.author),
            PbftError::AuthorityReuse(change.author)
        );
        weight += committee.stake(&change.author);
        for certificate in &change.prepared {
            let proposal = &certificate.proposal;
            let sequence = proposal.position.sequence;
            if !certificate.commits.is_empty() {
                if let Some(previous) = committed.insert(sequence, proposal.batch.id.clone()) {
                    ensure!(previous == proposal.batch.id, PbftError::InvalidRecovery);
                }
            }
            if let Some(previous) = selected.get(&sequence) {
                ensure!(
                    previous.position.view != proposal.position.view
                        || previous.batch.id == proposal.batch.id,
                    PbftError::InvalidRecovery
                );
                if previous.position.view >= proposal.position.view {
                    continue;
                }
            }
            selected.insert(sequence, proposal);
        }
    }
    ensure!(
        weight >= committee.quorum_threshold(),
        PbftError::RequiresQuorum
    );
    for (sequence, digest) in committed {
        ensure!(
            selected[&sequence].batch.id == digest,
            PbftError::InvalidRecovery
        );
    }
    Ok(selected)
}

fn signed_digest<T: Serialize>(domain: &[u8], body: &T) -> Digest {
    let mut hasher = Sha512::new();
    hasher.update(domain);
    hasher.update(bincode::serialize(body).expect("serializable protocol message"));
    Digest(hasher.finalize()[..32].try_into().unwrap())
}

#[cfg(test)]
#[path = "tests/view_change_tests.rs"]
mod tests;
