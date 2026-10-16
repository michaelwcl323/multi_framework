"""Local process smoke test for the CloudLab config/client/log pipeline.

Run after `cargo build -p node --features benchmark`:
  python tests/smoke_cloudlab.py
Uses temporary files and loopback; it never contacts CloudLab hosts.
"""
from collections import OrderedDict
import contextlib
import json
from multiprocessing.pool import ThreadPool
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

BENCHMARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK))
from benchmark.cloudlab_remote import CloudLabBench
from benchmark.cloudlab_results import save_results
from benchmark.config import BenchParameters, Committee, NodeParameters
from benchmark.logs import LogParser


def available_ports(count):
    for base in range(24000, 60000, 100):
        with contextlib.ExitStack() as stack:
            try:
                for port in range(base, base + count):
                    sock = stack.enter_context(socket.socket())
                    sock.bind(('127.0.0.1', port))
            except OSError:
                continue
            return base
    raise RuntimeError('No free block of loopback ports')


def main():
    binary = BENCHMARK.parent / 'target/debug/node'
    client = binary.with_name('benchmark_client')
    assert binary.is_file() and client.is_file(), 'Build benchmark binaries first'
    temporary = tempfile.TemporaryDirectory(prefix='sailfish-cloudlab-smoke-')
    directory = Path(temporary.name)
    print(f'Smoke directory: {directory}', flush=True)
    previous = Path.cwd()
    children, files = [], []
    try:
        os.chdir(directory)
        logs = Path('logs'); logs.mkdir()
        names = []
        for i in range(4):
            subprocess.run([str(binary), 'generate_keys', '--filename', f'.node-{i}.json'], check=True)
            names.append(json.loads(Path(f'.node-{i}.json').read_text())['name'])
        committee = Committee.from_address_list(
            OrderedDict((name, ['127.0.0.1'] * 2) for name in names), available_ports(24), 0)
        committee.print('.committee.json')
        params = NodeParameters(dict(
            consensus_only=False, header_size=32000, batch_size=32000, tx_size=500,
            max_header_delay=200, max_batch_delay=200, gc_depth=50,
            sync_retry_delay=1000, sync_retry_nodes=3, client_rate=1000, timeout_delay=200,
        ))
        params.print('.parameters.json')

        def launch(command, name):
            output = open(logs / name, 'w'); files.append(output)
            child = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                                     env={**os.environ, 'RUST_LOG': 'info'})
            children.append(child)

        for i in range(4):
            launch([str(binary), '-vv', 'run', '--keys', f'.node-{i}.json',
                    '--committee', '.committee.json', '--store', f'.db-{i}',
                    '--parameters', '.parameters.json', 'primary'], f'primary-{i}.log')
        time.sleep(.5)
        addresses = [address for group in committee.workers_addresses() for _, address in group]
        argument = CloudLabBench.__new__(CloudLabBench)._client_rate_argument(1000, 50)
        for i, address in enumerate(addresses):
            launch([str(client), address, '--size', '500', '--rate', str(argument),
                    '--burst', '50', '--nodes', *addresses], f'client-{i}-0.log')
        for _ in range(8):
            time.sleep(1)
            assert all(child.poll() is None for child in children), 'Node/client exited early'
        for child in children:
            child.terminate()
        for child in children:
            child.wait(timeout=10)
        for output in files:
            output.close()
        with patch('benchmark.logs.Pool', lambda: ThreadPool(4)):
            logger = LogParser.process('logs', faults=0, consensus_only=False)
        bench = BenchParameters(dict(faults=0, nodes=4, workers=1, collocate=True,
                                     rate=4000, tx_size=500, duration=8, runs=1, burst=50))
        target = save_results(logger, 4000, 4, bench, params, 50, 1, Path('results'),
                              SimpleNamespace(repo_url='local smoke', branch='working-tree'))
        metrics = json.loads((target / 'metrics.json').read_text())
        assert metrics['valid'], metrics
        assert metrics['consensus_samples'] > 0 and metrics['consensus_latency_ms'] > 0, metrics
        assert metrics['end_to_end_latency_ms'] > 0, metrics
        assert metrics['all_node_common_commits'] > 0, metrics
        print('PASS:', json.dumps(metrics, indent=2))
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()
        for output in files:
            output.close()
        os.chdir(previous)
        # Preserve logs only if the run failed, for diagnosis.
        if sys.exc_info()[0]:
            temporary._finalizer.detach()
            print(f'Failed smoke logs preserved at {directory}', file=sys.stderr)
        else:
            temporary.cleanup()


if __name__ == '__main__':
    main()
