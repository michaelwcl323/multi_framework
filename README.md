# Single-instance PBFT normal path

Each authority runs one node process. The committee's first public key in sorted
order is the fixed leader. All nodes participate in epoch 0, instance 0, view 0.
The leader batches full transaction bytes and assigns sequence numbers starting
at 1. There are no DAG parents, old Vote/Certificate messages, or Tusk ordering.

```text
Client transactions (any node; backups forward to the leader)
  -> leader broadcasts signed PrePrepare { position, complete Batch }
  -> every replica, including the leader, broadcasts signed Prepare
  -> matching Prepare quorum: broadcast signed Commit
  -> locally prepared + matching Commit quorum: CommittedBatch
  -> deliver the contiguous sequence to the application
```

Prepare and Commit use distinct signature domains. The signed position includes
`epoch`, `instance`, `view`, and `sequence`; votes also bind the proposal digest
and sender. Quorums use the committee's stake threshold, strictly more than two
thirds of total stake (3 distinct unit-stake voters in a four-node committee).
All replicas send Prepare; this is the symmetric variant rather than the classic
PBFT rule of a PrePrepare plus 2f backup Prepares. Local votes are counted once.
Transport ACKs only drive retries and never contribute to a quorum.

Duplicate messages are idempotent. Conflicting proposals for the same position
are rejected. Each sender has at most one retained vote per phase and slot;
conflicting votes are rejected and different digests never share quorum weight.
Prepare/Commit messages received before their proposal are buffered within a
128-slot receive window. A Commit quorum cannot bypass local prepared state.
Decisions may complete out of order, but application output waits for every
preceding sequence number. The leader currently allows one proposal in flight.

## Run locally

From `benchmark`, run `fab local`. The runner starts one node and one client per
authority. Every client endpoint remains usable; a backup forwards its input to
the fixed leader. To run an individual authority after generating configuration:

```bash
cargo build --release --features benchmark
./target/release/node -vv run --keys keys.json --committee committee.json \
    --parameters parameters.json --store .db-node primary
```

`batch_size` counts transaction bytes; `max_batch_delay` seals partial or empty
batches. `batch_size` must be positive. A new proposal waits for the leader's local PBFT
decision on the preceding proposal. `gc_depth` retains recent transport retries
in sequence numbers; legacy sync settings are accepted but inactive.

For committee compatibility, `workers["0"].transactions` remains the client
ingress address. All PBFT messages and forwarded transactions use
`primary.primary_to_primary`. Former worker/data ports remain unused.

## Scope

This is the single-instance, fixed-view normal path, not the complete ISS
protocol. Multi-instance scheduling, buckets, epochs, view change, checkpoints,
state transfer, and crash recovery are not implemented. A failed backup can be
tolerated while enough voting stake remains, but a failed leader stalls progress.
A replica that falls behind transport retention has no catch-up mechanism yet.
Use fresh stores and a homogeneous cluster for each run; restarting replicas
with previously used identities/stores is not a supported recovery procedure.
Transaction bytes are opaque; client authentication and request deduplication
are outside this stage.

## Validation

```bash
cargo test --workspace --release --all-features --offline
cargo fmt --all -- --check
cd benchmark
python3 -m unittest discover -s tests -v
```

Tests cover quorum boundaries, duplicate messages, out-of-order phases,
conflicting proposals/votes, phase and position signature replay, stake weights,
ordered output, and real TCP all-to-all broadcasts. A four-node committee test
starts only two replicas (no decision), then starts a third and checks that all
three decide the same complete batch while the fourth remains offline.

Derived from Narwhal/Tusk. [Apache 2.0](LICENSE).
