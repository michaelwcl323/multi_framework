"""Regression checks for fault-filtered log collection and offline reports."""
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
import subprocess
import sys
import tempfile
import unittest

from benchmark.logs import LogParser, ParseError
from benchmark.utils import PathMaker
from benchmark.cloudlab_remote import CloudLabBench
import download_logs


def line(seconds, text):
    return f'[2026-10-24T00:00:{seconds}Z INFO test] {text}\n'


def primary(digest='current=', created=True):
    log = ''.join(line('00.000', text) for text in [
        'Garbage collection depth set to 50 rounds',
        'Sync retry delay set to 1000 ms', 'Sync retry nodes set to 3 nodes',
        'Batch size set to 2048000 B', 'Max batch delay set to 200 ms',
        'Primary key successfully booted on 127.0.0.1',
        'MultiBFT enabled: 3 instances; local instance 0; epoch 0, view 0',
    ])
    log += line('00.200', f'Batch {digest} contains 512 B')
    log += line('00.200', f'Batch {digest} contains sample tx 0')
    if created:
        log += line('00.200', f'Created B1(key) -> {digest}')
    return log + line('00.600', f'Committed B1(key) -> {digest} (instance 0)')


def client(rate=1000):
    return ''.join(line(at, text) for at, text in [
        ('00.000', 'Transactions size: 512 B'),
        ('00.000', f'Transactions rate: {rate} tx/s'),
        ('00.000', 'Start sending transactions'),
        ('00.100', 'Sending sample transaction 0'),
    ])


def write_mixed_logs(directory):
    logs = Path(directory)
    logs.mkdir(parents=True, exist_ok=True)
    for i in [0, 1]:
        (logs / f'primary-{i}.log').write_text(primary())
        (logs / f'client-{i}-0.log').write_text(client())
    (logs / 'primary-2.log').write_text(primary('stale=', created=False))
    (logs / 'client-2-0.log').write_text(client(9000))


class LogReportingTests(unittest.TestCase):
    def test_faulty_nodes_old_logs_do_not_enter_current_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            write_mixed_logs(directory)
            result = LogParser.process(directory, faults=1, node_indices=[0, 1])
            self.assertEqual(result.committee_size, 3)
            self.assertEqual(sum(result.rate), 2000)
            self.assertEqual(set(result.commits), {'current='})
            self.assertAlmostEqual(result._consensus_latency(), .4, places=5)
            self.assertAlmostEqual(result._end_to_end_latency(), .5, places=5)
            (Path(directory) / 'primary-1.log').unlink()
            with self.assertRaisesRegex(ParseError, 'Missing primary logs'):
                LogParser.process(directory, faults=1, node_indices=[0, 1])

    def test_missing_proposal_is_a_diagnostic_error_not_a_partial_latency(self):
        with self.assertRaisesRegex(ParseError, 'no original proposal timestamp.*stale='):
            LogParser([client()], [primary('stale=', created=False)], [])

    def test_no_run_cli_saves_report_without_missing_pathmaker_interfaces(self):
        script = Path(__file__).resolve().parents[1] / 'run_cloudlab_benchmark.py'
        with tempfile.TemporaryDirectory() as directory:
            write_mixed_logs(Path(directory) / 'logs')
            result = subprocess.run(
                [sys.executable, str(script), '--no-run', '--faults', '1', '--nodes', '3'],
                cwd=directory, text=True, capture_output=True, timeout=30,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            summary = (Path(directory) / 'results/summary.txt').read_text()
            self.assertIn('Committee size: 3', summary)
            self.assertIn('Input rate: 2,000', summary)
            self.assertIn('Consensus latency: 400 ms', summary)

    def test_refresh_archives_previous_logs_and_starts_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            logs = root / 'logs'
            logs.mkdir()
            (logs / 'primary-9.log').write_text('old run')
            with patch.object(PathMaker, 'logs_path', return_value=str(logs)), \
                    patch.object(PathMaker, 'results_path', return_value=str(root / 'results')):
                self.assertEqual(PathMaker.reset_logs_path(), str(logs))
            self.assertEqual(list(logs.iterdir()), [])
            archived = list((root / 'results').glob('*/logs/primary-9.log'))
            self.assertEqual(len(archived), 1)
            self.assertEqual(archived[0].read_text(), 'old run')

    def test_full_downloader_selects_live_nodes_and_reports_missing_files(self):
        with tempfile.TemporaryDirectory() as directory, ExitStack() as patches:
            settings = SimpleNamespace(key_path='key', repo_name='repo')
            hosts = [{'hostname': f'node{i}', 'username': 'user'} for i in range(3)]
            manager = Mock()
            manager.get_host_info.return_value = hosts
            patches.enter_context(patch.object(download_logs.CloudLabSettings, 'load', return_value=settings))
            patches.enter_context(patch.object(download_logs, 'CloudLabInstanceManager', return_value=manager))
            patches.enter_context(patch.object(download_logs, 'get_connection_kwargs', return_value={'pkey': 'key'}))
            connection = patches.enter_context(patch.object(download_logs, 'Connection'))
            patches.enter_context(patch.object(PathMaker, 'logs_path', return_value=str(Path(directory) / 'logs')))
            patches.enter_context(patch.object(PathMaker, 'results_path', return_value=str(Path(directory) / 'results')))
            get = patches.enter_context(patch.object(download_logs, 'safe_get_file',
                side_effect=lambda conn, remote, local: Path(local).write_text('downloaded')))
            self.assertTrue(download_logs.download_logs(refresh=True, node_indices=[0, 1]))
            self.assertEqual({call.args[0] for call in connection.call_args_list}, {'node0', 'node1'})
            self.assertEqual(get.call_count, 4)
            get.side_effect = FileNotFoundError('missing current log')
            self.assertFalse(download_logs.download_logs(refresh=True, node_indices=[0, 1]))

    def test_fabric_collector_uses_current_committee_and_fault_count(self):
        runner = CloudLabBench.__new__(CloudLabBench)
        committee = Mock()
        committee.size.return_value = 3
        parsed = object()
        with patch('benchmark.cloudlab_remote.subprocess.run', return_value=SimpleNamespace(returncode=0)) as run, \
                patch.object(LogParser, 'process', return_value=parsed) as process:
            self.assertIs(runner._logs(committee, faults=1), parsed)
        download = run.call_args_list[0].args[0]
        report = run.call_args_list[1].args[0]
        self.assertIn('--refresh', download)
        self.assertEqual(download[download.index('--nodes') + 1], '0,1')
        self.assertEqual(report[report.index('--faults') + 1], '1')
        self.assertEqual(report[report.index('--nodes') + 1], '3')
        process.assert_called_once_with(PathMaker.logs_path(), faults=1, node_indices=[0, 1])


if __name__ == '__main__':
    unittest.main()
