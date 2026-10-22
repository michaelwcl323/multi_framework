// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Position, Sequence};
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
    #[error("Commit proof requires a quorum")]
    RequiresQuorum,
    #[error("Commit does not match its proposal")]
    MismatchedCommit,
    #[error("Unsupported protocol position {0:?}")]
    UnsupportedPosition(Position),
    #[error("Sequence {0} is beyond the receive window")]
    OutsideWindow(Sequence),
    #[error("Conflicting proposal at sequence {0}")]
    ConflictingProposal(Sequence),
    #[error("Conflicting vote by {0} at sequence {1}")]
    ConflictingVote(PublicKey, Sequence),
}
