// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{PbftError, PbftResult};
use config::Committee;
use crypto::{Digest, Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::{Digest as _, Sha512};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::convert::TryInto;
use std::fmt;

#[cfg(test)]
#[path = "tests/messages_tests.rs"]
mod messages_tests;

pub type Transaction = Vec<u8>;
/// One-based slot index within a finite, epoch-specific segment.
pub type Sequence = u64;
pub type GlobalSequence = crate::epoch::GlobalSn;
pub type Instance = u64;
/// Bounds slots and early votes retained before a proposal arrives.
pub const SEQUENCE_WINDOW: Sequence = 128;

/// Instance IDs are the ranks of their fixed leaders in committee key order.
pub fn leader(committee: &Committee, instance: Instance) -> Option<PublicKey> {
    if instance >= committee.size() as u64 {
        return None;
    }
    committee.authorities.keys().nth(instance as usize).copied()
}

/// The SB sender is fixed; only the segment's PBFT primary rotates with its view.
pub fn primary(committee: &Committee, instance: Instance, view: u64) -> Option<PublicKey> {
    if instance >= committee.size() as u64 {
        return None;
    }
    leader(
        committee,
        ((instance as u128 + view as u128) % committee.size() as u128) as u64,
    )
}

/// Logical log identity, independent of the view that supplies its certificate.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SlotKey {
    pub epoch: u64,
    pub instance: Instance,
    pub sequence: Sequence,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Position {
    pub epoch: u64,
    pub instance: Instance,
    pub view: u64,
    pub sequence: Sequence,
}

impl Position {
    pub fn new(sequence: Sequence) -> Self {
        Self::for_instance(0, sequence)
    }

    pub fn for_instance(instance: Instance, sequence: Sequence) -> Self {
        Self::for_epoch(0, instance, sequence)
    }

    pub fn for_epoch(epoch: u64, instance: Instance, sequence: Sequence) -> Self {
        Self {
            epoch,
            instance,
            view: 0,
            sequence,
        }
    }

    pub fn verify(&self) -> PbftResult<()> {
        ensure!(self.sequence > 0, PbftError::UnsupportedPosition(*self));
        Ok(())
    }

    pub fn slot(&self) -> SlotKey {
        SlotKey {
            epoch: self.epoch,
            instance: self.instance,
            sequence: self.sequence,
        }
    }

    /// Checked finite-epoch mapping, shared with the canonical ISS epoch layout.
    pub fn global_sequence(
        &self,
        instance_count: u64,
        epoch_length: u64,
    ) -> PbftResult<GlobalSequence> {
        self.verify()?;
        ensure!(
            self.instance < instance_count,
            PbftError::UnknownInstance(self.instance)
        );
        let epoch = crate::epoch::Epoch::new(self.epoch, epoch_length, instance_count)
            .ok_or(PbftError::GlobalSequenceOverflow(*self))?;
        epoch
            .global_sn(*self)
            .ok_or(PbftError::OutsideSegment(*self))
    }

    fn verify_instance(&self, committee: &Committee) -> PbftResult<PublicKey> {
        self.verify()?;
        primary(committee, self.instance, self.view)
            .ok_or(PbftError::UnknownInstance(self.instance))
    }

    fn hash_into(&self, hasher: &mut Sha512) {
        hasher.update(self.epoch.to_le_bytes());
        hasher.update(self.instance.to_le_bytes());
        hasher.update(self.view.to_le_bytes());
        hasher.update(self.sequence.to_le_bytes());
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Batch {
    pub transactions: Vec<Transaction>,
    pub id: Digest,
}

impl Batch {
    pub fn new(transactions: Vec<Transaction>) -> Self {
        let mut batch = Self {
            transactions,
            id: Digest::default(),
        };
        batch.id = batch.digest();
        batch
    }
}

impl Hash for Batch {
    fn digest(&self) -> Digest {
        let mut hasher = Sha512::new();
        hasher.update(b"iss/pbft/batch/v1");
        hasher.update((self.transactions.len() as u64).to_le_bytes());
        for transaction in &self.transactions {
            hasher.update((transaction.len() as u64).to_le_bytes());
            hasher.update(transaction);
        }
        Digest(hasher.finalize()[..32].try_into().unwrap())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrePrepare {
    pub position: Position,
    pub batch: Batch,
    pub author: PublicKey,
    pub signature: Signature,
    /// Original SB sender's view-zero signature. Recovery never authorizes a
    /// new client batch, even when the original sender becomes primary again.
    pub origin: Option<Signature>,
}

impl PrePrepare {
    pub async fn new(
        position: Position,
        batch: Batch,
        author: PublicKey,
        signatures: &mut SignatureService,
    ) -> Self {
        let mut proposal = Self {
            position,
            batch,
            author,
            signature: Signature::default(),
            origin: None,
        };
        proposal.signature = signatures.request_signature(proposal.digest()).await;
        proposal
    }

    pub async fn recover(
        previous: &PrePrepare,
        view: u64,
        author: PublicKey,
        signatures: &mut SignatureService,
    ) -> Self {
        let mut proposal = Self {
            position: Position {
                view,
                ..previous.position
            },
            batch: previous.batch.clone(),
            author,
            signature: Signature::default(),
            origin: Some(
                previous
                    .origin
                    .as_ref()
                    .unwrap_or(&previous.signature)
                    .clone(),
            ),
        };
        proposal.signature = signatures.request_signature(proposal.digest()).await;
        proposal
    }

    pub fn original_digest(&self, committee: &Committee) -> PbftResult<Digest> {
        let sender = leader(committee, self.position.instance)
            .ok_or(PbftError::UnknownInstance(self.position.instance))?;
        Ok(proposal_digest(
            Position {
                view: 0,
                ..self.position
            },
            sender,
            &self.batch.id,
        ))
    }

    pub fn verify(&self, committee: &Committee) -> PbftResult<()> {
        let expected_leader = self.position.verify_instance(committee)?;
        ensure!(
            committee.stake(&self.author) > 0,
            PbftError::UnknownAuthority(self.author)
        );
        ensure!(
            self.author == expected_leader,
            PbftError::UnexpectedLeader(self.author)
        );
        ensure!(
            self.batch.id == self.batch.digest(),
            PbftError::InvalidBatchId
        );
        self.signature.verify(&self.digest(), &self.author)?;
        if self.position.view == 0 {
            ensure!(self.origin.is_none(), PbftError::InvalidRecovery);
        } else {
            let sender = leader(committee, self.position.instance).unwrap();
            let origin = self.origin.as_ref().ok_or(PbftError::InvalidRecovery)?;
            let digest = proposal_digest(
                Position {
                    view: 0,
                    ..self.position
                },
                sender,
                &self.batch.id,
            );
            origin.verify(&digest, &sender)?;
        }
        Ok(())
    }
}

impl Hash for PrePrepare {
    fn digest(&self) -> Digest {
        proposal_digest(self.position, self.author, &self.batch.id)
    }
}

fn proposal_digest(position: Position, author: PublicKey, batch: &Digest) -> Digest {
    let mut hasher = Sha512::new();
    hasher.update(b"iss/pbft/preprepare/v1");
    position.hash_into(&mut hasher);
    hasher.update(author);
    hasher.update(batch);
    Digest(hasher.finalize()[..32].try_into().unwrap())
}

impl fmt::Display for PrePrepare {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "B{}({})", self.position.sequence, self.author)
    }
}

// Separate Rust types and signing domains prevent cross-phase signature replay.
macro_rules! phase_message {
    ($name:ident, $domain:expr) => {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct $name {
            pub position: Position,
            pub proposal: Digest,
            pub author: PublicKey,
            pub signature: Signature,
        }

        impl $name {
            pub async fn new(
                proposal: &PrePrepare,
                author: PublicKey,
                signatures: &mut SignatureService,
            ) -> Self {
                let mut message = Self {
                    position: proposal.position,
                    proposal: proposal.digest(),
                    author,
                    signature: Signature::default(),
                };
                message.signature = signatures.request_signature(message.digest()).await;
                message
            }

            pub fn verify(&self, committee: &Committee) -> PbftResult<()> {
                self.position.verify_instance(committee)?;
                ensure!(
                    committee.stake(&self.author) > 0,
                    PbftError::UnknownAuthority(self.author)
                );
                self.signature.verify(&self.digest(), &self.author)?;
                Ok(())
            }
        }

        impl Hash for $name {
            fn digest(&self) -> Digest {
                let mut hasher = Sha512::new();
                hasher.update($domain);
                self.position.hash_into(&mut hasher);
                hasher.update(&self.proposal);
                hasher.update(&self.author);
                Digest(hasher.finalize()[..32].try_into().unwrap())
            }
        }
    };
}

phase_message!(Prepare, b"iss/pbft/prepare/v1");
phase_message!(Commit, b"iss/pbft/commit/v1");

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PbftMessage {
    PrePrepare(PrePrepare),
    Prepare(Prepare),
    Commit(Commit),
    ViewChange(crate::ViewChange),
    NewView(crate::NewView),
}

impl PbftMessage {
    pub fn position(&self) -> Position {
        match self {
            Self::PrePrepare(x) => x.position,
            Self::Prepare(x) => x.position,
            Self::Commit(x) => x.position,
            Self::ViewChange(x) => x.position(),
            Self::NewView(x) => x.position(),
        }
    }

    pub fn verify(&self, committee: &Committee) -> PbftResult<()> {
        match self {
            Self::PrePrepare(x) => x.verify(committee),
            Self::Prepare(x) => x.verify(committee),
            Self::Commit(x) => x.verify(committee),
            Self::ViewChange(x) => x.verify(committee),
            Self::NewView(x) => x.verify(committee),
        }
    }

    pub(crate) fn phase_author(&self) -> (u8, PublicKey) {
        match self {
            Self::PrePrepare(x) => (0, x.author),
            Self::Prepare(x) => (1, x.author),
            Self::Commit(x) => (2, x.author),
            Self::ViewChange(x) => (3, x.author),
            Self::NewView(x) => (4, x.author),
        }
    }
}

/// A segment decision, multiplexed into the epoch's global log by delivery.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommittedBatch {
    pub proposal: PrePrepare,
    pub commits: Vec<Commit>,
}

impl CommittedBatch {
    pub fn verify(&self, committee: &Committee) -> PbftResult<()> {
        self.proposal.verify(committee)?;
        let digest = self.proposal.digest();
        let mut used = HashSet::new();
        let mut weight = 0;
        for commit in &self.commits {
            commit.verify(committee)?;
            ensure!(
                commit.position == self.proposal.position && commit.proposal == digest,
                PbftError::MismatchedCommit
            );
            ensure!(
                used.insert(commit.author),
                PbftError::AuthorityReuse(commit.author)
            );
            weight += committee.stake(&commit.author);
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            PbftError::RequiresQuorum
        );
        Ok(())
    }
}
