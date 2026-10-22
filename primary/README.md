# Single-instance PBFT

`messages.rs` defines transaction batches, signed PrePrepare/Prepare/Commit
messages, and locally emitted `CommittedBatch` proofs. `pbft.rs` is the
deterministic state machine: it verifies messages, deduplicates senders, buffers
early votes, and emits one Prepare, Commit, and decision per accepted slot.

`core.rs` executes those actions: it stores the complete proposal before
preparing, stores local phase messages before broadcasting, feeds local votes
through the same state machine, and persists decisions before output. These
writes do not implement crash recovery. Transport ACKs are separate from votes.

`primary.rs` owns the common network port, client ingress, and backup-to-leader
transaction forwarding. Only the fixed leader runs `proposer.rs`, which seals
full/partial/empty batches and waits for its local decision before assigning the
next sequence number. `consensus` verifies commit proofs and delivers only a
contiguous sequence to the application; it does not run another voting phase.

The fixed leader is the first committee key in sorted order; only epoch 0,
instance 0, view 0 are supported. There is no DAG parent synchronization or
Tusk ordering. See the root README for scope and validation commands.
