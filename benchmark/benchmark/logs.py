# Copyright(C) Facebook, Inc. and its affiliates.
from datetime import datetime
from glob import glob
from multiprocessing import Pool
from os.path import join
from re import findall, search
from statistics import mean

from benchmark.utils import Print


class ParseError(Exception):
    pass


class LogParser:
    def __init__(self, clients, primaries, workers, faults=0):
        self.multibft = any('MultiBFT enabled' in log for log in primaries)
        self.pbft = self.multibft or any('Single-instance PBFT enabled' in log for log in primaries)
        self.direct_batches = any('Direct batch proposals enabled' in log for log in primaries)
        self.integrated = self.pbft or self.direct_batches or not workers or any('Integrated batch service' in log for log in primaries)
        if self.integrated:
            workers = primaries
        inputs = [clients, primaries, workers]
        assert all(isinstance(x, list) for x in inputs)
        assert all(isinstance(x, str) for y in inputs for x in y)
        assert all(x for x in inputs)

        # A recovered proposal has a new signed digest but retains the original
        # batch's start time, payload size and client samples.
        self.batch_origins = {}
        for log in primaries:
            for recovered, original in findall(
                    r'Recovered B\d+\([^ ]+\) -> (\S+) origin_proposal=(\S+)', log):
                if self.batch_origins.setdefault(recovered, original) != original:
                    raise ParseError('Conflicting recovered proposal origin')

        self.faults = faults
        if isinstance(faults, int):
            self.committee_size = len(primaries) + int(faults)
            self.workers = 0 if self.integrated else len(workers) // len(primaries)
        else:
            self.committee_size = '?'
            self.workers = '?'

        # Parse the clients logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_clients, clients)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse clients\' logs: {e}')
        self.size, self.rate, self.start, misses, self.sent_samples \
            = zip(*results)
        self.misses = sum(misses)

        # Parse the primaries logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_primaries, primaries)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse nodes\' logs: {e}')
        proposals, commits, self.configs, primary_ips = zip(*results)
        self.proposals = self._merge_results([
            [(self.batch_origins.get(d, d), t) for d, t in x.items()] for x in proposals])
        self.commits = self._merge_results([
            [(self.batch_origins.get(d, d), t) for d, t in x.items()] for x in commits])
        missing = self.commits.keys() - self.proposals.keys()
        if missing:
            example = sorted(missing)[0]
            raise ParseError(
                f'{len(missing)} committed batch(es) have no original proposal timestamp '
                f'(for example {example}). Check for missing nodes or mixed benchmark runs.'
            )

        # Parse the workers logs.
        try:
            with Pool() as p:
                results = p.map(self._parse_workers, workers)
        except (ValueError, IndexError, AttributeError) as e:
            raise ParseError(f'Failed to parse workers\' logs: {e}')
        sizes, self.received_samples, workers_ips = zip(*results)
        self.sizes = {
            k: v for x in sizes for k, v in x.items() if k in self.commits
        }

        # Determine whether the primary and the workers are collocated.
        self.collocate = set(primary_ips) == set(workers_ips)

        # Check whether clients missed their target rate.
        if self.misses != 0:
            Print.warn(
                f'Clients missed their target rate {self.misses:,} time(s)'
            )

    def _merge_results(self, input):
        # Keep the earliest timestamp.
        merged = {}
        for x in input:
            for k, v in x:
                if not k in merged or merged[k] > v:
                    merged[k] = v
        return merged

    def _parse_clients(self, log):
        if search(r'Error', log) is not None:
            raise ParseError('Client(s) panicked')

        size = int(search(r'Transactions size: (\d+)', log).group(1))
        rate = int(search(r'Transactions rate: (\d+)', log).group(1))

        tmp = search(r'\[(.*Z) .* Start ', log).group(1)
        start = self._to_posix(tmp)

        misses = len(findall(r'rate too high', log))

        tmp = findall(r'\[(.*Z) .* sample transaction (\d+)', log)
        samples = {int(s): self._to_posix(t) for t, s in tmp}

        return size, rate, start, misses, samples

    def _parse_primaries(self, log):
        if search(r'(?:panicked|Error)', log) is not None:
            raise ParseError('Primary(s) panicked')

        tmp = findall(r'\[(.*Z) .* Created B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        proposals = self._merge_results([tmp])

        tmp = findall(r'\[(.*Z) .* Committed B\d+\([^ ]+\) -> ([^ ]+=)', log)
        tmp = [(d, self._to_posix(t)) for t, d in tmp]
        commits = self._merge_results([tmp])

        configs = {
            'header_size': int(search(r'Header size .* (\d+)', log).group(1))
                if search(r'Header size .* (\d+)', log) else 0,
            'max_header_delay': int(search(r'Max header delay .* (\d+)', log).group(1))
                if search(r'Max header delay .* (\d+)', log) else 0,
            'gc_depth': int(
                search(r'Garbage collection depth .* (\d+)', log).group(1)
            ),
            'sync_retry_delay': int(
                search(r'Sync retry delay .* (\d+)', log).group(1)
            ),
            'sync_retry_nodes': int(
                search(r'Sync retry nodes .* (\d+)', log).group(1)
            ),
            'batch_size': int(
                search(r'Batch size .* (\d+)', log).group(1)
            ),
            'max_batch_delay': int(
                search(r'Max batch delay .* (\d+)', log).group(1)
            ),
        }

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)
        
        return proposals, commits, configs, ip

    def _parse_workers(self, log):
        if search(r'(?:panic|Error)', log) is not None:
            raise ParseError('Worker(s) panicked')

        tmp = findall(r'Batch ([^ ]+) contains (\d+) B', log)
        sizes = {self.batch_origins.get(d, d): int(s) for d, s in tmp}

        tmp = findall(r'Batch ([^ ]+) contains sample tx (\d+)', log)
        samples = {int(s): self.batch_origins.get(d, d) for d, s in tmp}

        ip = search(r'booted on (\d+.\d+.\d+.\d+)', log).group(1)

        return sizes, samples, ip

    def _to_posix(self, string):
        x = datetime.fromisoformat(string.replace('Z', '+00:00'))
        return datetime.timestamp(x)

    def _consensus_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.proposals.values()), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _consensus_latency(self):
        latency = [c - self.proposals[d] for d, c in self.commits.items()]
        return mean(latency) if latency else 0

    def _end_to_end_throughput(self):
        if not self.commits:
            return 0, 0, 0
        start, end = min(self.start), max(self.commits.values())
        duration = end - start
        bytes = sum(self.sizes.values())
        bps = bytes / duration
        tps = bps / self.size[0]
        return tps, bps, duration

    def _end_to_end_latency(self):
        if self.pbft:
            # Match samples globally, including historical forwarded-client logs.
            sent = {sample: timestamp for samples in self.sent_samples
                    for sample, timestamp in samples.items()}
            received = {sample: batch for samples in self.received_samples
                        for sample, batch in samples.items()}
            latency = [self.commits[batch] - sent[sample]
                       for sample, batch in received.items()
                       if sample in sent and batch in self.commits]
            return mean(latency) if latency else 0
        latency = []
        for sent, received in zip(self.sent_samples, self.received_samples):
            for tx_id, batch_id in received.items():
                if batch_id in self.commits:
                    assert tx_id in sent  # We receive txs that we sent.
                    start = sent[tx_id]
                    end = self.commits[batch_id]
                    latency += [end-start]
        return mean(latency) if latency else 0

    def result(self):
        header_size = self.configs[0]['header_size']
        max_header_delay = self.configs[0]['max_header_delay']
        gc_depth = self.configs[0]['gc_depth']
        sync_retry_delay = self.configs[0]['sync_retry_delay']
        sync_retry_nodes = self.configs[0]['sync_retry_nodes']
        batch_size = self.configs[0]['batch_size']
        max_batch_delay = self.configs[0]['max_batch_delay']

        consensus_latency = self._consensus_latency() * 1_000
        consensus_tps, consensus_bps, _ = self._consensus_throughput()
        end_to_end_tps, end_to_end_bps, duration = self._end_to_end_throughput()
        end_to_end_latency = self._end_to_end_latency() * 1_000

        topology = (
            ' Node processes per authority: 1 (MultiBFT)\n'
            if self.multibft else
            ' Node processes per authority: 1 (single-instance PBFT)\n'
            if self.pbft else
            ' Node processes per authority: 1 (direct batch proposals)\n'
            if self.direct_batches else
            ' Node processes per authority: 1 (integrated batching)\n'
            if self.integrated else
            f' Worker(s) per node: {self.workers} worker(s)\n'
            f' Collocate primary and workers: {self.collocate}\n'
        )

        header_config = '' if self.pbft or self.direct_batches else (
            f' Header size: {header_size:,} B\n'
            f' Max header delay: {max_header_delay:,} ms\n'
        )

        return (
            '\n'
            '-----------------------------------------\n'
            ' SUMMARY:\n'
            '-----------------------------------------\n'
            ' + CONFIG:\n'
            f' Faults: {self.faults} node(s)\n'
            f' Committee size: {self.committee_size} node(s)\n'
            f'{topology}'
            f' Input rate: {sum(self.rate):,} tx/s\n'
            f' Transaction size: {self.size[0]:,} B\n'
            f' Execution time: {round(duration):,} s\n'
            '\n'
            f'{header_config}'
            f' GC depth: {gc_depth:,} round(s)\n'
            f' Sync retry delay: {sync_retry_delay:,} ms\n'
            f' Sync retry nodes: {sync_retry_nodes:,} node(s)\n'
            f' batch size: {batch_size:,} B\n'
            f' Max batch delay: {max_batch_delay:,} ms\n'
            '\n'
            ' + RESULTS:\n'
            f' Consensus TPS: {round(consensus_tps):,} tx/s\n'
            f' Consensus BPS: {round(consensus_bps):,} B/s\n'
            f' Consensus latency: {round(consensus_latency):,} ms\n'
            '\n'
            f' End-to-end TPS: {round(end_to_end_tps):,} tx/s\n'
            f' End-to-end BPS: {round(end_to_end_bps):,} B/s\n'
            f' End-to-end latency: {round(end_to_end_latency):,} ms\n'
            '-----------------------------------------\n'
        )

    def print(self, filename):
        assert isinstance(filename, str)
        result = self.result()
        with open(filename, 'a') as f:
            f.write(result)

    @classmethod
    def process(cls, directory, faults=0, node_indices=None):
        assert isinstance(directory, str)

        if node_indices is not None:
            indices = sorted(set(node_indices))
            expected = {join(directory, f'primary-{i}.log') for i in indices}
            actual = set(glob(join(directory, 'primary-*.log')))
            if expected - actual:
                raise ParseError(f'Missing primary logs for this run: {sorted(expected - actual)}')
            if actual - expected:
                Print.warn(f'Ignoring primary logs outside this run\'s active nodes: {sorted(actual - expected)}')
            primary_files = sorted(expected)
            client_files = sorted(f for i in indices for f in glob(join(directory, f'client-{i}-*.log')))
            worker_files = sorted(f for i in indices for f in glob(join(directory, f'worker-{i}-*.log')))
        else:
            primary_files = sorted(glob(join(directory, 'primary-*.log')))
            client_files = sorted(glob(join(directory, 'client-*.log')))
            worker_files = sorted(glob(join(directory, 'worker-*.log')))

        clients = []
        for filename in client_files:
            with open(filename, 'r') as f:
                clients += [f.read()]
        primaries = []
        for filename in primary_files:
            with open(filename, 'r') as f:
                primaries += [f.read()]
        workers = []
        for filename in worker_files:
            with open(filename, 'r') as f:
                workers += [f.read()]

        return cls(clients, primaries, workers, faults=faults)
