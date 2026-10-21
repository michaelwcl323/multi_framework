# ISS branch: direct batch proposals with Tusk ordering

Each authority runs a single node process. Client transactions are assembled
straight into a signed `Batch` proposal containing the full transaction bytes,
author, DAG round, and parent certificate references. The node broadcasts that
complete proposal directly to its peers.

There is no separate batch-data broadcast, pre-distribution ACK quorum,
batch-digest aggregation, or `Header` object. Peers verify and store the complete
proposal before voting. A quorum of signed votes certifies that same batch;
Tusk orders its certificate in the DAG. The ISS ordering protocol itself is not
yet implemented in this branch.

```text
Client transactions
  -> proposer batches transactions + DAG metadata
  -> broadcast complete Batch
  -> peers verify/store Batch and vote
  -> Certificate { batch, votes }
  -> existing Tusk DAG ordering
```

Transport ACKs still support reliable retransmission. They are not an
availability-quorum phase and do not gate proposal construction. Certificates
currently include the complete batch, so certificate dissemination also sends
transaction bytes. Empty batches allow DAG rounds to advance while traffic is
idle. Missing parent certificates are fetched from peers; there is no separate
transaction-payload lookup.

## Run locally

From `benchmark`, run `fab local`. The runner builds the binaries, starts one
node and one client per authority, and parses primary/client logs. To run an
individual authority after generating keys and a committee:

```bash
cargo build --release --features benchmark
./target/release/node -vv run --keys keys.json --committee committee.json \
    --parameters parameters.json --store .db-node primary
```

`batch_size` counts transaction bytes and `max_batch_delay` controls when a
partial/empty proposal may be made. Both proposal triggers also require parent
certificates from the previous DAG round. Client intake applies backpressure
once the pending batch reaches its size threshold. `header_size` and
`max_header_delay` have been removed; old JSON files containing them are still
accepted, but those extra fields have no effect.

For committee-format compatibility, `workers["0"].transactions` remains the
client-ingress address on the primary host. All proposal, vote, certificate,
and parent-sync traffic uses `primary.primary_to_primary`. The legacy
`worker_to_primary`, `primary_to_worker`, and `worker_to_worker` ports are unused.
Benchmark parameters retain `workers=1` and `collocate=True`; no worker process
or standalone worker command exists. The former worker/batch-service crates
have been removed.

## Validation

```bash
cargo test --workspace --release --all-features
cd benchmark
python3 -m unittest discover -s tests -v
fab local
```

This is a research prototype derived from Narwhal/Tusk. Its transaction-carrying
proposal and certificate wire formats differ from upstream; use fresh stores
and a homogeneous cluster running this branch.

## License

[Apache 2.0](LICENSE).
