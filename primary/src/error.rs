// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Instance, Position, Sequence};
use crypto::{CryptoError, PublicKey};
use store::StoreError;
use thiserror::Error;

#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($e);
    };
}

#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!($e);
        }
    };
}

pub type PbftResult<T> = Result<T, PbftError>;

#[derive(Debug, Error)]
pub enum PbftError {
    #[error("Protocol state could not be stored")]
    StorageFailure,
    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),
    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),
    #[error("Invalid batch id")]
    InvalidBatchId,
    #[error("Unknown authority {0}")]
    UnknownAuthority(PublicKey),
    #[error("Unexpected leader {0}")]
    UnexpectedLeader(PublicKey),
    #[error("Authority {0} appears in quorum more than once")]
    AuthorityReuse(PublicKey),
    #[error("Certificate requires a quorum")]
    RequiresQuorum,
    #[error("Commit does not match its proposal")]
    MismatchedCommit,
    #[error("Unsupported protocol position {0:?}")]
    UnsupportedPosition(Position),
    #[error("Global sequence overflows for position {0:?}")]
    GlobalSequenceOverflow(Position),
    #[error("Conflicting decisions for global sequence {0}")]
    ConflictingGlobalDecision(Sequence),
    #[error("Unknown PBFT instance {0}")]
    UnknownInstance(Instance),
    #[error("Message targets instance {received}, expected {expected}")]
    WrongInstance {
        expected: Instance,
        received: Instance,
    },
    #[error("Message targets epoch {received}, expected {expected}")]
    WrongEpoch { expected: u64, received: u64 },
    #[error("Position is outside its finite segment: {0:?}")]
    OutsideSegment(Position),
    #[error("Sequence {0} is beyond the receive window")]
    OutsideWindow(Sequence),
    #[error("Message targets view {received}, expected {expected}")]
    WrongView { expected: u64, received: u64 },
    #[error("Invalid view-change recovery evidence")]
    InvalidRecovery,
    #[error("Recovery control message exceeds the size limit")]
    ControlMessageTooLarge,
    #[error("Conflicting view-change reports by {0}")]
    ConflictingViewChange(PublicKey),
    #[error("Conflicting proposal at sequence {0}")]
    ConflictingProposal(Sequence),
    #[error("Conflicting vote by {0} at sequence {1}")]
    ConflictingVote(PublicKey, Sequence),
}
