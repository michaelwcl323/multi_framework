// Copyright(C) Facebook, Inc. and its affiliates.
use crate::primary::Round;
use crypto::{CryptoError, Digest, PublicKey};
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

pub type DagResult<T> = Result<T, DagError>;

#[derive(Debug, Error)]
pub enum DagError {
    #[error("Full batch could not be stored before voting")]
    BatchStorageFailure,

    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),

    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Invalid batch id")]
    InvalidBatchId,

    #[error("Malformed batch {0}")]
    MalformedBatch(Digest),

    #[error("Received message from unknown authority {0}")]
    UnknownAuthority(PublicKey),

    #[error("Authority {0} appears in quorum more than once")]
    AuthorityReuse(PublicKey),

    #[error("Received unexpected vote fo batch {0}")]
    UnexpectedVote(Digest),

    #[error("Received certificate without a quorum")]
    CertificateRequiresQuorum,

    #[error("Parents of batch {0} are not a quorum")]
    BatchRequiresQuorum(Digest),

    #[error("Message {0} (round {1}) too old")]
    TooOld(Digest, Round),
}
