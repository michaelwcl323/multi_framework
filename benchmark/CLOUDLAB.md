# Sailfish on CloudLab

Run the commands from the `benchmark` directory. This workflow follows the
Angelfish CloudLab runner: a static SSH inventory, local key/config generation,
remote Rust builds, one primary and client per node, and downloaded logs. The
primary starts its embedded worker when real clients are enabled.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-cloudlab.txt
cp cloudlab_settings.example.json cloudlab_settings.json
# Set your SSH key, username/hosts, and Sailfish repository branch.
fab cloudlab-info
fab cloudlab-test
fab cloudlab-install
fab cloudlab-remote
```

The local `cloudlab_settings.json` has already been populated from this
workspace's CloudLab inventory (ten hosts, michael9, port base 5000). It is
ignored by Git. The example contains no private-key password. An encrypted key
can use `SSH_KEY_PASSWORD` or the ignored file's `ssh_key_password`. Remote
nodes clone the configured Sailfish repository/branch; that branch must contain
these changes before its binaries are built. The installer does not publish
local changes to GitHub.

`cloudlab-info` and `fab --list` work without GCP SDKs or a GCP credential file.
`python environment_setup.py --dry-run` prints setup actions without connecting
or installing anything. Use `--settings PATH` (Fabric) or `--settings PATH`
(environment setup) for a different inventory.

## Experiment parameters

Edit `cloudlab_tasks.py::cloudlab_remote`, or override its Fabric arguments:

| Setting | Default | Unit / meaning |
| --- | ---: | --- |
| nodes | 10 | One primary and one embedded worker per host |
| rates | 40000,80000,120000,160000,200000 | Aggregate input transactions/second |
| runs | 2 | Repetitions at each input rate |
| duration | 120 | Seconds per run |
| tx_size | 500 | Bytes per transaction |
| batch_size | 2048000 | Bytes, equivalent to 4096 transactions |
| header_size | 2048000 | Payload bytes, equivalent to 4096 transactions |
| max_batch_delay | 200 | Milliseconds; seal a partial batch on timeout |
| max_header_delay | 200 | Milliseconds; proposer timeout, subject to protocol progress |
| burst | 50 | Client burst interval in milliseconds |
| consensus_only | false | Default uses real benchmark clients |

```bash
fab cloudlab-remote --rates='40000,80000,120000,160000,200000' --runs=2
fab cloudlab-remote --max-batch-delay=200 --max-header-delay=200 \
    --batch-size=2048000 --header-size=2048000 --tx-size=500
fab cloudlab-kill
```

This Sailfish implementation has no Angelfish `propose_rate` input. Its normal
proposal rules and leader selection are retained, and results use `Sailfish`
without a fabricated propose-rate label. `client_rate` is per-node synthetic
load and only affects consensus-only mode. With real clients, the total input
rate is divided across client endpoints. The runner converts target tx/s into
the client's `rate/20` transactions-per-burst representation; arbitrary intervals
can round upward by less than one transaction per burst per client. Each repeat
starts with empty stores and fresh processes.

## Results and latency

Results default to `results/Sailfish/rate-R/run-N/`; use `--output-dir` to change
the root. Existing run directories are retained and new runs get unused numbers.
Each run contains `summary.txt`, `metrics.json`, `health.json`, `parameters.json`,
`committee.json`, `experiment.json`, and `logs.tar.gz`. Secret keys and passwords
are not archived. The runner preserves each repetition before the next starts.

Consensus latency is measured from the proposer's `Created ID` immediately before
header construction/signing to its first local `Committed ID`. Pair timestamps
inside one node's log; include completed nonempty headers and average them
equally. It includes header construction and excludes the preceding batch wait.
Summary and JSON expose this definition and sample count. Empty and uncommitted
headers are excluded; uncommitted counts are retained. End-to-end latency is
reported separately. Achieved consensus throughput retains the legacy saved
earliest-commit throughput calculation; changing the latency endpoint does not
silently change that throughput metric.

The adaptation fixes sealing without resetting accumulated batch bytes, the
off-by-one proposal log ID, and inconsistent sample/header IDs. These fixes are
required for meaningful batching and proposal-to-commit comparison. No-vote,
leader election, and quorum logic are unchanged.

## Validation

```bash
python -m unittest discover -s tests -p 'test_*.py' -v
cargo build --manifest-path ../Cargo.toml -p node --features benchmark
cargo test --manifest-path ../Cargo.toml -p primary --features benchmark --test batch_maker_cloudlab
python tests/smoke_cloudlab.py
```

The smoke test runs four real node/client processes over loopback for eight
seconds and verifies config parsing, proposals, common commits, both latency
metrics, and result archiving. It uses temporary files and does not contact the
CloudLab inventory. Run CloudLab connectivity/deployment commands to validate
the actual testbed.

Validation performed for this change: node/client debug build, eleven Python
CloudLab tests, two batch-maker integration tests, and the four-node smoke run.
The repository's older `primary/src/tests/common.rs` fixtures refer to removed
signature/header fields and do not compile as a complete primary unit-test
suite; the batch regression target above runs independently of those fixtures.
