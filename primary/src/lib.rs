// Copyright(C) Facebook, Inc. and its affiliates.
#[macro_use]
mod error;
mod core;
mod epoch;
mod messages;
mod pbft;
mod primary;
mod proposer;
mod transport;

#[cfg(test)]
#[path = "tests/common.rs"]
mod common;

pub use crate::epoch::{Epoch, GlobalSn, Segment};
pub use crate::error::{PbftError, PbftResult};
pub use crate::messages::{
    leader, Batch, Commit, CommittedBatch, GlobalSequence, Instance, PbftMessage, Position,
    PrePrepare, Prepare, Sequence, Transaction, SEQUENCE_WINDOW,
};
pub use crate::primary::Primary;
