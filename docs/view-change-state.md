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
freeze old-view votes, export in-memory evidence, sign its own report and broadcast it.
The quorum threshold (`2f+1` with equal stake) lets the designated primary build
NewView. NewView is fully checked against local committed history before state
is replaced. It may also safely skip views.

Prepared-but-uncommitted values execute the Prepare/Commit phases in the new
view. A complete old Commit proof can be delivered directly by a lagging
replica; replicas that already decided do not deliver it again. Imported
evidence is retained in memory and can be exported in another view change.

The original proposer watches its segment's target view and stops proposing
after leaving view zero. Each new epoch starts fresh at view zero. Authenticated
future-view votes are buffered, do not vote before NewView installation, and
are replayed when the installed view changes. Step 5's immediately-next-epoch
lookahead remains bounded; buffers are not rescanned on every normal message.

## Storage and transport

Prepared Evidence is exclusively in memory. At Prepare quorum, PBFT retains the
proposal and matching Prepare votes; at Commit quorum, it attaches matching Commit
votes. The live slot, outgoing vote actions and retained evidence share the
proposal through `Arc`, avoiding an extra normal-path batch payload copy.
Signing and broadcasting Commit requires no Prepared serialization or database
operation. There are no `pbft/prepared/` writes, reads or disk-cleanup operations.

Live-slot cleanup preserves the evidence. ViewChange materializes wire
certificates directly from memory, including imported committed proofs. Replacing
PBFT at the local epoch barrier releases that epoch's evidence. Retaining payloads
and vote quorums uses memory until this barrier, even if no view change occurs.

Step 5's final Commit-proof write remains under `pbft/committed/<full position>`;
its retention and the existing WAL/sync settings are unchanged. Checked writes
acknowledge the put result without reading back its value. This does not add
fsync guarantees. In-memory Prepared Evidence is lost on process restart, and
crash/restart recovery is not implemented.

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
Commit broadcast without a Prepared record, shared payload ownership, imported
in-memory proof reuse, evidence retirement at the epoch barrier, recovered global
ordering/idempotence, and real TCP
transport of ViewChange and NewView larger than 8 MiB. Existing normal-path
PBFT/epoch tests remain part of the workspace suite. Benchmark parsing maps
recovered digests to their original proposal time and client samples.

The in-memory evidence revision passed **78 Rust tests**, default-feature
compilation, formatting and whitespace checks. The former RocksDB Prepared
loader/deletion tests were removed; coverage now exercises actual retained
evidence, shared payload ownership and epoch retirement.

Validation commands (functional checks, not performance measurements):

```bash
cargo test --workspace --all-features
cargo check --workspace
cargo fmt --all -- --check
PYTHONPATH=benchmark python3 -m unittest discover -s benchmark/tests
git diff --check
```

These are functional checks. No new CloudLab throughput or latency measurements
were performed for this step.
