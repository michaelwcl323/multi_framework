# Sailfish Implementation

This repository provides a simplified reference implementation of Sailfish protocol analyzed in the [Pipes framework paper](https://eprint.iacr.org/2025/1116.pdf). 

In our implementation, we introduce several simplifications. In particular, we use the CB primitive to disseminate proposed blocks and wait for all $n$ vertices in each layer, rather than $n - f$ as specified in the original protocol. Additionally, incoming transactions are first accumulated in a buffer and then proposed collectively in the next block. This differs from the original design, where transactions can be included in a block while it is still being disseminated.


## Quick Start

The protocol is implemented in Rust, while benchmarking scripts are written in Python and executed using [Fabric](http://www.fabfile.org/). 

To deploy and benchmark a 15-node local testbed, clone the repository and install the Python dependencies:

```
$ git clone https://github.com/nibeshrestha/pipes.git
$ git checkout sailfish
$ cd pipes/benchmark
$ pip install -r requirements.txt
```
You will also need Clang (required for RocksDB) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux)
 (used to run all nodes and clients in the background). Once installed, you can launch a local benchmark using Fabric:

```
$ fab local
```

The first execution may take longer, as it compiles the Rust code in `release` mode. You can customize various benchmarking parameters in fabfile.py before running. Once the benchmark completes, a summary of the execution will be printed, similar to the example shown below.

```
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Faults: 0 node(s)
 Committee size: 10 node(s)
 Worker(s) per node: 1 worker
 Collocate primary and workers: True
 Execution time: 20 s

 Header size: 1,000 B
 Max header delay: 200 ms
 GC depth: 50 round(s)
 Sync retry delay: 5,000 ms
 Sync retry nodes: 3 node(s)
 batch size: 500,000 B
 Max batch delay: 200 ms
 Client rate: 2,000 tx/s

 + RESULTS:
 Consensus BLPS: 564 Block/s
 Consensus TPS: 19,281 tx/s
 Consensus latency: 59 ms
 Consensus leader latency: 43 ms
 Consensus non leader latency: 61 ms
-----------------------------------------
```

## Remote Run

For a static CloudLab testbed, see [CloudLab setup and benchmarks](benchmark/CLOUDLAB.md).
The `cloudlab-*` Fabric tasks use SSH inventory and work independently of GCP.

The scripts are configured to deploy on GCP instances. Ensure that your GCP API credentials are placed in `benchmark/benchmark/key.json`. You can then install the required dependencies on the remote servers by running `fab install`.

We use the Linux tc command to introduce network delays and enforce bandwidth limits. To configure these settings, run `fab set-filter`. By default, the network delay (100 ms) and bandwidth (100 Mbit/s) are hardcoded in remote.py; you can modify these values in that file as needed.

Once the setup is complete, launch the experiment on the remote machines using `fab remote`.

## License

This software is licensed as [Apache 2.0](LICENSE).
