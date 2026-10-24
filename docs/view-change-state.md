# Step 6: add view change state

Base: `e6dfeb0` (Step 5: Support Global Ordering), on branch `iss-step5`.
The raw transaction input and finite-epoch delivery barrier are retained.

## Messages and identities

- `PreparedCertificate` contains a full proposal, a unique Prepare quorum and
  either zero Commit votes or a complete Commit quorum. All vote signatures,
  phases, positions, digests, authors and stake weights are checked.
- `ViewChange` signs the epoch, segment, target view and complete evidence list.
  It cannot report evidence from another segment/epoch, duplicate a slot or
  report a proposal from the target or a future view.
- `NewView` is signed by `primary(committee, instance, view)`, whose index is
  `(instance + view) mod N`. The original segment sender remains fixed.
  Reports must have unique authors and a stake-weighted quorum for one target.
- Selection recovers the highest prepared view for each reported slot, rejects
  conflicting values in the same prepared view, and preserves complete Commit
  proofs. The proposed recovery set must match selection exactly: no omissions,
  replacements, fresh values, or invented empty batches.
- A recovered `PrePrepare` has the new view and primary signature, plus the
  original sender's view-zero signature over the same batch/slot. Recovery
  cannot authorize a new client batch, even when primary rotation returns to
  the original sender.
- `SlotKey { epoch, instance, sequence }` and global SN do not change with view.
  Votes remain bound to the full position including view. Global Ordering treats
  valid old/new-view proofs for the same batch as idempotent and rejects a
  conflicting batch, including after delivery within the current epoch.

## State transitions

PBFT keeps separate installed and target views. A single report cannot stop
normal voting. The validity threshold (`f+1` with equal stake) causes Core to
freeze old-view votes, load local evidence, sign its own report and broadcast it.
The quorum threshold (`2f+1` with equal stake) lets the designated primary build
NewView. NewView is fully checked against local committed history before state
is replaced. It may also safely skip views.

Prepared-but-uncommitted values execute the Prepare/Commit phases in the new
view. A complete old Commit proof can be delivered directly by a lagging
replica; replicas that already decided do not deliver it again. Imported
evidence is persisted and can be exported in another view change.

The original proposer watches its segment's target view and stops proposing
after leaving view zero. Each new epoch starts fresh at view zero. Authenticated
future-view votes are buffered, do not vote before NewView installation, and
are replayed when the installed view changes. Step 5's immediately-next-epoch
lookahead remains bounded; buffers are not rescanned on every normal message.

## Storage and transport

Before the first Commit is signed/broadcast, Core serializes the proposal and
Prepare quorum to `pbft/prepared/<full position>`. Final Commit votes remain
under `pbft/committed/<full position>`. PBFT's history only retains a
`PreparedRef` containing the position, proposal digest, batch digest and
committed flag. It does not retain a second epoch-long payload copy.

ViewChange loads the indexed records and verifies their identity and complete
certificates. Missing, corrupt or incomplete records fail closed; they are not
silently omitted. Prepared records from all views retire at the existing local
epoch barrier. Commit-proof retention remains unchanged.

Checked store writes acknowledge the RocksDB put result without copying and
reading back the serialized value. WAL/sync settings are unchanged: this does
not add fsync guarantees or reconstruct live indexes after a process restart.
Serialization and RocksDB buffers still consume memory. Prepared writes and
small reference/quorum bookkeeping add normal-path work, which must be measured
separately from protocol correctness.

ViewChange/NewView can exceed the transport's 8 MiB frame limit. Recovery-only
messages are split into signed 4 MiB fragments, reassembled by authenticated
sender/segment/phase, then validated as full protocol messages. Duplicate
fragments/messages do not deliver twice. The control-message limit is 512 MiB.
Normal PrePrepare/Prepare/Commit use the existing broadcast path.

The message encoding adds variants and original-signature metadata. Deploy a
homogeneous build; interoperability with Step 5 binaries is not provided.

## Scope and validation

Initiation uses explicitly constructed authenticated ViewChange reports; this
step adds no timeout loop, failure detector, or admin RPC. Receiving sufficient
valid reports joins the requested view automatically. A slot without Prepared
Evidence remains undecided, so an evidence-free segment can remain blocked.
Bottom/no-op selection, transaction resurrection, checkpoints, state transfer,
and crash/restart recovery are separate work.

Tests cover weighted/unique quorums, signature and context tampering, highest
prepared selection across multiple views, committed-value protection after
slot cleanup, local atomic rejection, future-view vote replay, proposer pause,
RocksDB evidence availability before Commit broadcast, imported proof reuse,
epoch record cleanup, recovered global ordering/idempotence, and real TCP
transport of ViewChange and NewView larger than 8 MiB. Existing normal-path
PBFT/epoch tests remain part of the workspace suite. Benchmark parsing maps
recovered digests to their original proposal time and client samples.

Validation in this workspace: **81 Rust tests and 6 Python tests passed**;
default-feature compilation, formatting and whitespace checks also passed.

```bash
cargo test --workspace --all-features
cargo check --workspace
cargo fmt --all -- --check
PYTHONPATH=benchmark python3 -m unittest discover -s benchmark/tests
git diff --check
```

These are functional checks. No new CloudLab throughput or latency measurements
were performed for this step.
