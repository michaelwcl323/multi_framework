# MultiBFT: parallel PBFT instances

Each authority runs one node process and leads one fixed PBFT instance. Instance
IDs are zero-based positions in sorted committee public-key order: a committee
of N nodes has N instances. Every node runs a separate PBFT task for every
instance, participating with the full committee in each instance's quorums.
All instances currently use epoch 0 and view 0.

```text
Node 0 client input -> proposer 0 -> PBFT instance 0 (all nodes participate)
Node 1 client input -> proposer 1 -> PBFT instance 1 (all nodes participate)
...
Each instance: PrePrepare -> Prepare all-to-all -> Commit all-to-all
Each instance: ordered output (instance, local sequence, committed batch)
```

Instances advance independently. Each leader has one proposal in flight in its
own instance and waits only for that instance's local decision before proposing
again. Client transactions enter the instance led by the receiving node; they
are not forwarded to another leader or copied into other instances. The shared
node-to-node transport keeps one outgoing TCP connection per peer, multiplexing
messages by instance ID without allocating additional ports.

The signed position includes `epoch`, `instance`, `view`, and local `sequence`.
PrePrepare is accepted only from that instance's fixed leader. Prepare and
Commit use distinct signature domains and bind the position, proposal digest,
and sender. Quorums use strictly more than two thirds of committee stake (3
unique unit-stake voters for four nodes). All replicas, including the leader,
send Prepare; this is the symmetric PBFT normal path. Transport ACKs are not
protocol votes.

Duplicate protocol messages are idempotent. Conflicting proposals and votes are
rejected; different instances or digests never share quorum weight. Early
Prepare/Commit messages are buffered in a 128-slot window per instance. A Commit
quorum cannot bypass local prepared state. Proposal and vote state, commit-proof
storage keys, retry cleanup, and proposer feedback are isolated by instance.

## Output semantics

`CommittedBatch.proposal.position` identifies each decision by `(instance,
sequence)`. `consensus` verifies proofs and delivers a contiguous sequence within
each instance, suppressing duplicate outputs. A gap in one instance does not
block any other instance. Cross-instance delivery order can differ across
replicas: this stage intentionally does not establish a global total order or a
single replicated state-machine log. Applications should consume separate
instance logs until a later global ordering layer is added.

## Run locally

From `benchmark`, run `fab local`. The runner starts one node and one client per
authority; every node proposes its own client input. No additional instance-count
parameter is required. For an individual node after generating configuration:

```bash
cargo build --release --features benchmark
./target/release/node -vv run --keys keys.json --committee committee.json \
    --parameters parameters.json --store .db-node primary
```

`batch_size` counts transaction bytes and must be positive. `max_batch_delay`
seals partial or empty batches independently at each leader. `gc_depth` retains
recent transport retries in local sequence numbers per instance; legacy sync
settings are accepted but inactive. Benchmark proposal IDs include the signed
instance identity, so same-numbered batches from different leaders are distinct.

For committee compatibility, `workers["0"].transactions` remains the local client
ingress address. All PBFT traffic uses `primary.primary_to_primary`. Former
worker/data ports remain unused.

## Scope

This is parallel fixed-leader PBFT, not complete ISS. There are no bucket
assignments/rotation, epochs, cross-instance total ordering, view change,
checkpoints, state transfer, or crash recovery. A failed leader stalls its own
instance; other instances can continue when quorum stake remains available.
Client authentication and request deduplication, including the same request
submitted to multiple leaders, are outside this stage.

Use fresh stores and a homogeneous cluster for each run. Restarting replicas
with previously used identities/stores is not a supported recovery procedure.
A replica falling behind transport retention has no catch-up mechanism yet.

## Validation

```bash
cargo test --workspace --release --all-features --offline
cargo fmt --all -- --check
cd benchmark
python3 -m unittest discover -s tests -v
```

Tests cover existing PBFT quorum/duplicate/reordering/conflict checks, valid
nonzero instances, cross-instance signature replay and quorum isolation,
independent ordered delivery, and real TCP all-to-all broadcasts. A four-node
committee test starts two nodes (no instance can commit), adds a third, and
checks that all three live leaders' instances commit at every live replica while
the fourth leader remains offline. It also verifies separate persisted proofs
for the same local sequence and continued progress in one instance while the
others are idle.

Derived from Narwhal/Tusk. [Apache 2.0](LICENSE).
