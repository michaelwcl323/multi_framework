# ISS: finite segments and epochs

Each authority runs one node process and leads one PBFT segment per epoch.
Segment IDs are zero-based positions in sorted committee public-key order.
Every node participates in every segment using the full committee's voting
stake. The PBFT normal path is PrePrepare, all-to-all Prepare, and all-to-all
Commit; transport ACKs are never votes.

## Global sequence numbers and finite segments

`segment_length` defines the number of entries per segment, defaulting to
**25**. With N segments, `epoch_length = N * segment_length` is derived
automatically. Each entry is one consensus slot containing a batch. Global SNs
start at one. Epoch `e` owns
`[e * epoch_length + 1, (e + 1) * epoch_length]`. Within this finite range, slots
are assigned round-robin to the N segments, starting with segment 0.

The signed wire position remains `(epoch, instance, view, sequence)`, where
`instance` identifies the segment and `sequence` is its one-based local slot.
The global SN is derived deterministically as:

```text
global_sn = epoch * epoch_length + (sequence - 1) * N + instance + 1
```

Only local slots that map into that epoch's finite range are accepted. For an
three-entry segment length and three leaders (nine slots per epoch):

| Epoch | Segment 0 | Segment 1 | Segment 2 |
|---|---|---|---|
| 0 | 1, 4, 7 | 2, 5, 8 | 3, 6, 9 |
| 1 | 10, 13, 16 | 11, 14, 17 | 12, 15, 18 |

Every segment has exactly `segment_length` entries. Partial and empty batches
finish required slots when the batch timer expires.

## Delivery and epoch completion

Segments run concurrently and each leader has one proposal in flight. A local
segment stops at its finite quota. Decisions may arrive out of order, but the
application receives exactly one contiguous log ordered by global SN. A missing
slot blocks later delivery, including slots decided by other segments.

Application output is `OrderedBatch { global_sequence, committed }`: the explicit
global sequence accompanies the original proposal and Commit proof. Empty batches
consume their assigned slots and advance the prefix without executing transactions.
Duplicate results cannot repeat application output, conflicting buffered proofs
are rejected, and all sequence arithmetic is checked through the finite epoch
layout. Verified current-epoch results are retained even beyond the PBFT receive
window. Finite segment quotas bound normal backlog until the epoch barrier opens.

After **every required slot of the current epoch has entered the local output
channel**, delivery opens the next epoch through a watch notification. Cores
start fresh PBFT segment state and proposers reset their local slot counters.
A replica never votes in the next epoch merely because its own segment finished.
Epochs advance from local verified log completion, not a timer or a central
coordinator. This implements the finite segment lifecycle described in the
[ISS paper](https://research.protocol.ai/publications/state-machine-replication-scalability-made-simple/stathakopoulou2022.pdf).

Epoch, segment, view, and local sequence are bound into every signature. Duplicate
messages and decisions are idempotent. Conflicting values and votes cannot mix
quorum weight. Authenticated messages for the immediately following epoch are
buffered without voting and replayed after the local barrier; the buffer is
bounded by slot/phase/sender and the existing receive window. Older messages and
old proposer feedback cannot advance current-epoch state.

## Configuration and running

From `benchmark`, use `fab local` or the existing CloudLab runner. Parameters
may include `"segment_length": 25`; it must be positive and identical on all
nodes. With 10 nodes this allocates 25 entries per segment and **250 global
slots per epoch**. `epoch_length` is derived, rather than configured separately.

```bash
cargo build --release --features benchmark
./target/release/node -vvv run --keys keys.json --committee committee.json \
    --parameters parameters.json --store .db-node primary
```

`batch_size` counts transaction bytes; `max_batch_delay` is in milliseconds.
Client transactions feed only the proposer led by the receiving node. The
legacy `workers["0"].transactions` address remains the client ingress; all PBFT
traffic shares one outgoing connection per peer on `primary_to_primary`.
Legacy sync settings are accepted but inactive. `gc_depth` retains transport
retries across local slot resets using the equivalent distance in global SNs.

Diagnostic `debug!()` logs include global SNs, batch transaction counts and
payload bytes, segment completion, and epoch start/completion. Existing INFO
benchmark records retain their parser-compatible prefixes and digest IDs.
`PBFT decided` marks local agreement, while `Committed` marks the global delivery
condition; locally decided batches waiting behind a gap do not count toward
benchmark throughput or global ordering latency. `Globally ordered` debug logs
also include empty slots and explicit global sequence numbers.

Only the final commit-vote proof is written to RocksDB, under a key containing
the complete protocol position. Proposal/batch payload and intermediate local
votes are not persisted. This does not implement crash recovery.

## Scope and validation

This stage adds finite segments, global log ordering, and the epoch completion
barrier. Leaders remain fixed and view stays zero. Bucket assignment/rotation,
client authentication/deduplication, view change, checkpoints, state transfer,
and crash recovery are outside this stage. A missing leader can leave its
segment unfinished and prevent the next epoch from opening, even if other
segments finish. Use fresh stores and a homogeneous cluster for each run.

```bash
cargo test --release -p primary -p consensus -p node -p config --all-features -- --test-threads=1
cargo fmt --all -- --check
cd benchmark
python3 -m unittest discover -s tests -v
```

Tests cover finite round-robin coverage (including uneven and empty segments),
epoch overflow/bounds, quorum and replay isolation, out-of-order global delivery,
missing-slot barriers, old feedback, next-epoch buffering, and proof-only
storage. A real TCP four-node test holds one leader offline, checks that the
missing segment holds epoch 0, then starts it and verifies that all four nodes
deliver the same global log across three epochs, including empty batches and
real client data.

Derived from Narwhal/Tusk. [Apache 2.0](LICENSE).
