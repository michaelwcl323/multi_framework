import asyncio
from collections import OrderedDict
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

BENCHMARK = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK))

from benchmark.cloudlab_remote import CloudLabBench, CloudLabExecutionError
from benchmark.cloudlab_results import consensus_metrics, save_results
from benchmark.cloudlab_settings import CloudLabSettings, CloudLabSettingsError
from benchmark.commands import CommandMaker
from benchmark.config import BenchParameters, Committee, NodeParameters
from benchmark.logs import LogParser
import cloudlab_tasks


@contextlib.contextmanager
def working_directory(path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def primary(index, offset=0):
    def event(milliseconds, message):
        # Keep all example timestamps in the same minute.
        seconds, millis = divmod(milliseconds, 1000)
        return f'[2026-10-16T00:{offset:02d}:{seconds:02d}.{millis:03d}Z INFO primary] {message}\n'
    config = ''.join(event(0, message) for message in (
        'Header size set to 2048000 B', 'Max header delay set to 200 ms',
        'Garbage collection depth set to 50 rounds', 'Sync retry delay set to 10000 ms',
        'Sync retry nodes set to 7 nodes', 'Batch size set to 2048000 B',
        'Max batch delay set to 200 ms', 'Transaction size set to 500 B',
        'Client rate set to 4000 tx/s', f'Primary {index} booted on 127.0.0.1',
    ))
    identifier = str(index + 1)
    return (config + event(100, f'Created {identifier}')
            + event(101, f'Header {identifier} contains 500 B')
            + event(300 + index * 100, f'Committed {identifier} Leader')
            + event(500, f'Committed {2 - index} NonLeader'))


class CloudLabTests(unittest.TestCase):
    def test_tasks_discover_without_gcp_dependencies_or_credentials(self):
        result = subprocess.run([sys.executable, '-m', 'fabric', '--list'], cwd=BENCHMARK,
                                text=True, capture_output=True, check=True)
        for name in ('cloudlab-info', 'cloudlab-test', 'cloudlab-install',
                     'cloudlab-remote', 'cloudlab-kill'):
            self.assertIn(name, result.stdout)
        self.assertNotIn('benchmark.instance', sys.modules)

    def test_settings_formats_and_duplicate_host_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'settings.json'
            for data in (
                dict(key={'path': '~/.ssh/key'}, hosts=[dict(hostname='127.0.0.1')]),
                dict(ssh_key_path='~/.ssh/key', servers=[dict(hostname='127.0.0.1', description='utah')]),
            ):
                data.update(port=5000, repo=dict(name='sailfish', url='https://example.com/repo', branch='sailfish'))
                path.write_text(json.dumps(data))
                settings = CloudLabSettings.load(path)
                self.assertEqual(settings.hosts[0]['port'], 22)
                self.assertEqual(settings.repo_name, 'sailfish')
                host_key = 'hosts' if 'hosts' in data else 'servers'
                data[host_key] *= 2
                path.write_text(json.dumps(data))
                with self.assertRaises(CloudLabSettingsError):
                    CloudLabSettings.load(path)

    def test_multiworker_addresses_do_not_duplicate_client_endpoints(self):
        committee = Committee.from_address_list(
            OrderedDict((str(i), ['127.0.0.1'] * 3) for i in range(4)), 5000, 0)
        workers = committee.workers_addresses()
        self.assertEqual(len(workers), 4)
        self.assertEqual(sum(map(len, workers)), 8)
        ports = [address for group in workers for _, address in group]
        self.assertEqual(len(set(ports)), 8)

    def test_rates_and_burst_match_actual_client_algorithm(self):
        runner = CloudLabBench.__new__(CloudLabBench)
        for total_rate in (40000, 80000, 120000, 160000, 200000):
            for burst in (10, 50, 100):
                argument = runner._client_rate_argument(total_rate // 10, burst)
                actual = (argument // 20) * 1000 / burst * 10
                self.assertEqual(actual, total_rate)
                command = shlex.split(CommandMaker.run_client('127.0.0.1:5000', 500,
                                                              argument, [], burst=burst))
                self.assertEqual(command[command.index('--burst') + 1], str(burst))
                self.assertEqual(command[command.index('--rate') + 1], str(argument))

    def test_task_defaults_reach_runner_in_bytes_and_milliseconds(self):
        with patch('environment_setup.setup_local_environment'), patch('cloudlab_tasks._bench') as factory:
            cloudlab_tasks.cloudlab_remote.body(None)
            bench, params = factory.return_value.run.call_args.args
            self.assertEqual(bench['nodes'], [10])
            self.assertEqual(bench['rate'], [40000, 80000, 120000, 160000, 200000])
            self.assertEqual(params['batch_size'], 4096 * 500)
            self.assertEqual(params['header_size'], 4096 * 500)
            self.assertEqual(params['max_batch_delay'], 200)
            self.assertEqual(params['max_header_delay'], 200)
            self.assertFalse(params['consensus_only'])
            self.assertNotIn('propose_rate', params)

    def test_configuration_generation_uses_sailfish_keys_and_preserves_settings(self):
        runner = CloudLabBench.__new__(CloudLabBench)
        runner.settings = SimpleNamespace(base_port=5000)
        bench = BenchParameters(dict(faults=0, nodes=4, workers=1, rate=40000,
                                     tx_size=500, duration=120, runs=1, burst=50))
        params = NodeParameters(dict(timeout_delay=200, header_size=2048000,
                                     gc_depth=50, batch_size=2048000))
        commands = []

        def execute(command, **kwargs):
            commands.append(command)
            if command[0] == './node':
                filename = command[command.index('--filename') + 1]
                Path(filename).write_text(json.dumps(dict(name=filename, secret='test-only-key')))
            return SimpleNamespace(returncode=0)

        with tempfile.TemporaryDirectory() as directory, working_directory(directory):
            Path('cloudlab_settings.json').write_text('preserved inventory')
            with patch('benchmark.cloudlab_remote.subprocess.run', side_effect=execute), \
                    patch.object(runner, '_replace_binary_links'):
                committee, names = runner._generate_config(['127.0.0.1'] * 4, params, bench)
            self.assertEqual(committee.size(), 4)
            self.assertEqual(len(names), 4)
            self.assertEqual([a['id'] for a in committee.json['authorities'].values()], list(range(4)))
            self.assertEqual(Path('cloudlab_settings.json').read_text(), 'preserved inventory')
            self.assertEqual(sum(c[0] == './node' for c in commands), 4)
            self.assertFalse(any('generate_bls_keys' in c for c in commands))

    def test_proposer_local_pairs_are_not_subtracted_across_clocks(self):
        parser = LogParser.__new__(LogParser)
        parser.consensus_only = True
        metrics = consensus_metrics(parser, [primary(0), primary(1, offset=10)])
        self.assertAlmostEqual(metrics['consensus_latency_ms'], 250, places=3)
        self.assertEqual(metrics['consensus_samples'], 2)
        self.assertEqual(metrics['all_node_common_commits'], 2)

    def test_real_parser_to_archived_results_does_not_overwrite_repetitions(self):
        from multiprocessing.pool import ThreadPool
        with tempfile.TemporaryDirectory() as directory, working_directory(directory):
            logs = Path('logs'); logs.mkdir()
            primaries = [primary(0), primary(1)]
            for i, text in enumerate(primaries):
                (logs / f'primary-{i}.log').write_text(text)
            with patch('benchmark.logs.Pool', lambda: ThreadPool(2)):
                parser = LogParser([], primaries, consensus_only=True)
            Path('.committee.json').write_text('{"authorities": {}}')
            bench = BenchParameters(dict(faults=0, nodes=2, workers=1, rate=40000,
                                         tx_size=500, duration=120, runs=2))
            params = NodeParameters(dict(timeout_delay=200, header_size=2048000,
                                         gc_depth=50, batch_size=2048000))
            settings = SimpleNamespace(repo_url='https://example.com/sailfish', branch='sailfish')
            with contextlib.redirect_stdout(io.StringIO()):
                first = save_results(parser, 40000, 2, bench, params, 50, 1, Path('results'), settings)
                second = save_results(parser, 40000, 2, bench, params, 50, 1, Path('results'), settings)
            self.assertNotEqual(first, second)
            metrics = json.loads((first / 'metrics.json').read_text())
            self.assertAlmostEqual(metrics['consensus_latency_ms'], 250, places=3)
            self.assertEqual(metrics['consensus_samples'], 2)
            self.assertTrue(metrics['valid'])
            self.assertEqual(metrics['end_to_end_latency_ms'], None)
            self.assertTrue((first / 'logs.tar.gz').is_file())
            self.assertIn('propose -> proposer-local commit', (first / 'summary.txt').read_text())


class CloudLabAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_background_process_forces_benchmark_logging(self):
        connection = SimpleNamespace(run=AsyncMock(return_value=SimpleNamespace(
            exit_status=0, stdout='', stderr='')))
        await CloudLabBench._start_process('127.0.0.1', connection,
                                          './node -vv run primary', 'logs/primary-0.log')
        command = connection.run.call_args.args[0]
        self.assertIn('RUST_LOG=info', command)

    async def test_nonzero_ssh_exit_status_is_failure(self):
        runner = CloudLabBench.__new__(CloudLabBench)
        connection = SimpleNamespace(run=AsyncMock(return_value=SimpleNamespace(
            exit_status=1, stdout='', stderr='build failed')))
        host, result = await runner._run_script_one('127.0.0.1', connection, 'false')
        self.assertEqual(host, '127.0.0.1')
        self.assertIsInstance(result, CloudLabExecutionError)
        self.assertIn('build failed', str(result))

    async def test_client_distribution_uses_each_endpoint_once(self):
        runner = CloudLabBench.__new__(CloudLabBench)
        runner._start_process = AsyncMock(return_value=('127.0.0.1', None))
        committee = Committee.from_address_list(
            OrderedDict((str(i), ['127.0.0.1'] * 2) for i in range(10)), 5000, 0)
        bench = BenchParameters(dict(faults=0, nodes=10, workers=1, rate=40000,
                                     tx_size=500, duration=120, runs=1, burst=50))
        await runner._run_clients(40000, 50, committee, bench, {'127.0.0.1': object()})
        self.assertEqual(runner._start_process.await_count, 10)
        for call in runner._start_process.await_args_list:
            command = shlex.split(call.args[2])
            self.assertEqual(command[command.index('--rate') + 1], '4000')
            self.assertEqual(command[command.index('--burst') + 1], '50')


if __name__ == '__main__':
    unittest.main()
