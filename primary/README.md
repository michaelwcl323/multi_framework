# Finite ISS segments and epochs

Each segment has `segment_length` entries (default 25), and each epoch has
`committee_size * segment_length` contiguous global log positions. One segment per
committee member owns a round-robin subset; segment IDs follow sorted committee
key order. `epoch.rs` defines the finite layout and the reversible mapping
between global SN and signed `(epoch, instance, view, local sequence)` positions.

`primary.rs` starts one Core per segment and one local proposer. `pbft.rs` checks
epoch, segment membership, finite slot quota, signatures, and stake quorums.
A segment stops after all its assigned slots decide. Partial and empty batches
fill slots when `max_batch_delay` expires. A segment with no slots never proposes.

`core.rs` persists only the final `Vec<Commit>` proof, keyed by the complete
protocol position. Batch contents, proposals, and intermediate local votes are
not stored. Proposer feedback carries the full position, preventing old-epoch
feedback from releasing a new proposal. Retry retention uses global SNs, so
local sequence resets do not mix retry handles across epochs.

`consensus` checks proofs and multiplexes decisions into a contiguous global
log, returning `OrderedBatch` with an explicit `global_sequence` and the original
`committed` proposal/proof. Duplicate and conflicting results cannot advance the
prefix. It releases a local watch notification only after every required slot of
the epoch has entered the output channel. Cores then reset their PBFT state and
the local proposer starts a fresh finite segment. Each replica advances from
its own complete log; no central epoch leader or network barrier is added.

Authenticated messages for the immediately next epoch can arrive before this
replica finishes the current epoch. Core buffers them by slot/phase/sender,
without voting, and replays them after the local barrier opens. Look-ahead is
bounded to the existing receive window and one epoch. Older messages cannot
change current state. State transfer for replicas falling further behind is
outside this stage.

Leaders remain fixed and view stays zero. Bucket assignment/rotation, view
change, checkpoints, and crash recovery are not implemented. An unavailable
leader leaves a gap that holds the epoch barrier. See the root README for
configuration, examples, and validation.
