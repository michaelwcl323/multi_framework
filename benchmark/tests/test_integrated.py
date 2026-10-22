"""Checks for integrated-node topology and primary-only benchmark logs."""
import tempfile
import unittest
from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from benchmark.config import BenchParameters, Committee, ConfigError, NodeParameters
from benchmark.logs import LogParser
from benchmark.commands import CommandMaker
from benchmark.cloudlab_remote import CloudLabBench


class IntegratedBenchTests(unittest.TestCase):
    def test_segment_length_defaults_and_rejects_invalid_values(self):
        params = dict(gc_depth=50, sync_retry_delay=1000, sync_retry_nodes=3,
                      batch_size=2048000, max_batch_delay=200)
        self.assertEqual(NodeParameters(params).json['segment_length'], 25)
        self.assertEqual(NodeParameters(dict(params, segment_length=7)).json['segment_length'], 7)
        self.assertNotIn('epoch_length', NodeParameters(dict(params, epoch_length=1000)).json)
        for value in [0, -1, 2.5, '7', True]:
            with self.assertRaises(ConfigError):
                NodeParameters(dict(params, segment_length=value))

    def test_cloudlab_resolves_binaries_with_explicit_logging_environment(self):
        runner = CloudLabBench.__new__(CloudLabBench)
        runner.settings = SimpleNamespace(repo_name='iss')
        runner._get_connection_kwargs = Mock(return_value={})

        for command, log_file, expected in [
            (CommandMaker.run_primary('keys', 'committee', '.db-0', 'params'),
             'logs/primary-0.log', 'exec env RUST_LOG=info "${NODE_BIN}"'),
            (CommandMaker.run_client('127.0.0.1:3003', 512, 1000, []),
             'logs/client-0-0.log', 'exec env RUST_LOG=info "${CLIENT_BIN}"'),
        ]:
            calls = []

            def run(shell, **kwargs):
                calls.append(shell)
                if shell.startswith('setsid nohup'):
                    stdout = '12345'
                else:
                    stdout = 'Binaries found\nOK\nRunning'
                return SimpleNamespace(ok=True, stdout=stdout, stderr='')

            connection = Mock()
            connection.run.side_effect = run
            with patch('benchmark.cloudlab_remote.Connection', return_value=connection), \
                    patch('benchmark.cloudlab_remote.sleep'):
                runner._background_run({'hostname': 'node0'}, command, log_file)
            script = next(call for call in calls if call.startswith('cat >'))
            self.assertIn(expected, script)

    def test_rejects_separate_workers(self):
        params = dict(faults=0, nodes=4, workers=1, rate=1000, tx_size=512, duration=20)
        self.assertTrue(BenchParameters(params).collocate)
        for changes in [dict(workers=2), dict(collocate=False)]:
            with self.assertRaises(ConfigError):
                BenchParameters(dict(params, **changes))
        with self.assertRaises(ConfigError):
            Committee(OrderedDict([('node', ['127.0.0.1', '127.0.0.2'])]), 3000)

    def test_parses_batches_from_primary_log_without_worker_logs(self):
        def line(seconds, text):
            return f'[2026-10-21T00:00:{seconds}Z INFO test] {text}\n'

        client = ''.join([
            line('00.000', 'Transactions size: 512 B'),
            line('00.000', 'Transactions rate: 1000 tx/s'),
            line('00.000', 'Start sending transactions'),
            line('00.100', 'Sending sample transaction 0'),
        ])
        primary = ''.join(line('00.000', message) for message in [
            'Header size set to 1000 B', 'Max header delay set to 200 ms',
            'Garbage collection depth set to 50 rounds', 'Sync retry delay set to 1000 ms',
            'Sync retry nodes set to 3 nodes', 'Batch size set to 500000 B',
            'Max batch delay set to 200 ms', 'Primary key successfully booted on 127.0.0.1',
            'Integrated batch service listening to transactions',
        ])
        primary += line('00.200', 'Batch abc= contains sample tx 0')
        primary += line('00.200', 'Batch abc= contains 512 B')
        primary += line('00.200', 'Created B1(key) -> abc=')
        primary += line('00.600', 'Committed B1(key) -> abc=')
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, 'primary-0.log').write_text(primary)
            Path(directory, 'client-0-0.log').write_text(client)
            parser = LogParser.process(directory)
            self.assertTrue(parser.integrated)
            self.assertEqual(parser.workers, 0)
            self.assertEqual(parser.sizes, {'abc=': 512})
            self.assertAlmostEqual(parser._consensus_latency(), 0.4, places=5)
            self.assertAlmostEqual(parser._end_to_end_latency(), 0.5, places=5)
            self.assertIn('1 (integrated batching)', parser.result())
            direct = ''.join(row for row in primary.splitlines(keepends=True)
                             if 'Header size set' not in row and 'Max header delay set' not in row)
            direct = direct.replace('Integrated batch service listening to transactions',
                                    'Direct batch proposals enabled')
            Path(directory, 'primary-0.log').write_text(direct)
            parser = LogParser.process(directory)
            self.assertTrue(parser.direct_batches)
            self.assertEqual(parser.sizes, {'abc=': 512})
            self.assertIn('1 (direct batch proposals)', parser.result())
            self.assertNotIn('Header size:', parser.result())

    def test_pbft_matches_forwarded_samples_to_their_original_clients(self):
        def line(seconds, text):
            return f'[2026-10-21T00:00:{seconds}Z INFO test] {text}\n'

        def client(sample, sent_at):
            return ''.join([
                line('00.000', 'Transactions size: 512 B'),
                line('00.000', 'Transactions rate: 1000 tx/s'),
                line('00.000', 'Start sending transactions'),
                line(sent_at, f'Sending sample transaction {sample}'),
            ])

        common = ''.join(line('00.000', message) for message in [
            'Garbage collection depth set to 50 rounds', 'Sync retry delay set to 1000 ms',
            'Sync retry nodes set to 3 nodes', 'Batch size set to 500000 B',
            'Max batch delay set to 200 ms', 'Primary key successfully booted on 127.0.0.1',
            'Single-instance PBFT enabled: leader key, epoch 0, instance 0, view 0',
        ])
        leader = common + ''.join([
            line('00.200', 'Batch abc= contains sample tx 4294967296'),
            line('00.200', 'Batch abc= contains sample tx 8589934592'),
            line('00.200', 'Batch abc= contains 1024 B'),
            line('00.200', 'Created B1(key) -> abc='),
            line('00.300', 'Batch pending= contains 512 B'),
            line('00.300', 'Created B2(key) -> pending='),
            line('00.350', 'PBFT decided B2(key) -> pending= (instance 0)'),
            line('00.600', 'Committed B1(key) -> abc='),
        ])
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, 'primary-0.log').write_text(leader)
            Path(directory, 'primary-1.log').write_text(common)
            Path(directory, 'client-0-0.log').write_text(client(4294967296, '00.100'))
            Path(directory, 'client-1-0.log').write_text(client(8589934592, '00.150'))
            parser = LogParser.process(directory)
            self.assertTrue(parser.pbft)
            self.assertEqual(parser.sizes, {'abc=': 1024})
            self.assertAlmostEqual(parser._end_to_end_latency(), 0.475, places=5)
            self.assertIn('1 (single-instance PBFT)', parser.result())
            self.assertNotIn('Header size:', parser.result())
            # Each MultiBFT instance has its own sequence 1; proposal IDs retain
            # their instance identity even when local sequence numbers coincide.
            first = leader.replace('Single-instance PBFT enabled:', 'MultiBFT enabled:')
            second = common.replace('Single-instance PBFT enabled:', 'MultiBFT enabled:')
            second += ''.join([
                line('00.250', 'Batch xyz= contains 512 B'),
                line('00.250', 'Created B1(other-key) -> xyz='),
                line('00.650', 'Committed B1(other-key) -> xyz= (instance 1)'),
            ])
            Path(directory, 'primary-0.log').write_text(first)
            Path(directory, 'primary-1.log').write_text(second)
            parser = LogParser.process(directory)
            self.assertTrue(parser.multibft)
            self.assertEqual(parser.sizes, {'abc=': 1024, 'xyz=': 512})
            self.assertEqual(len(parser.proposals), 3)
            self.assertEqual(len(parser.commits), 2)
            self.assertIn('1 (MultiBFT)', parser.result())


if __name__ == '__main__':
    unittest.main()
