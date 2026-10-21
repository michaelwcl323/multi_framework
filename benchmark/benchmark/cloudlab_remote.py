"""CloudLab runner for Narwhal HotStuff using the branch's config and log parser."""

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from io import StringIO
import json
from math import ceil
import os
from pathlib import Path
import shlex
import shutil
from time import sleep

from fabric import Connection
from paramiko import RSAKey

from benchmark.cloudlab_instance import CloudLabInstanceManager
from benchmark.commands import CommandMaker
from benchmark.config import BenchParameters, Committee, Key, NodeParameters
from benchmark.logs import LogParser
from benchmark.utils import PathMaker, Print


class CloudLabBench:
    def __init__(self, ctx):
        self.manager = CloudLabInstanceManager.make()
        self.settings = self.manager.settings
        settings_data = json.loads(Path('cloudlab_settings.json').read_text())
        password = os.environ.get('SSH_KEY_PASSWORD') or settings_data.get('ssh_key_password')
        self.connect_kwargs = dict(ctx.connect_kwargs)
        self.connect_kwargs['pkey'] = RSAKey.from_private_key_file(
            self.settings.key_path, password=password
        )
        for key in ('port', 'timeout', 'connect_timeout'):
            self.connect_kwargs.pop(key, None)
        self.revisions = {}

    def _connection(self, host):
        return Connection(
            host['hostname'], user=host.get('username', 'root'),
            port=host.get('port', 22), connect_kwargs=self.connect_kwargs,
            connect_timeout=30,
        )

    def _parallel(self, hosts, operation):
        failures = []
        with ThreadPoolExecutor(max_workers=len(hosts)) as pool:
            futures = {pool.submit(operation, host): host for host in hosts}
            for future in as_completed(futures):
                host = futures[future]
                try:
                    future.result()
                    Print.info(f"  OK {host['hostname']}")
                except Exception as error:
                    failures.append(f"{host['hostname']}: {error}")
        if failures:
            raise RuntimeError('\n'.join(failures))

    def test_connections(self):
        def check(host):
            with self._connection(host) as conn:
                conn.run('hostname', hide=True, timeout=30)
        self._parallel(self.settings.hosts, check)

    def install(self):
        Print.heading('Installing Narwhal HotStuff on CloudLab nodes')
        repo = shlex.quote(self.settings.repo_name)
        url = shlex.quote(self.settings.repo_url)
        branch = shlex.quote(self.settings.branch)
        command = f'''set -e
sudo -n apt-get update
sudo -n apt-get install -y build-essential cmake clang libclang-dev pkg-config curl git tmux iproute2 python3
if [ -f "$HOME/.cargo/env" ]; then . "$HOME/.cargo/env"; fi
if ! command -v rustup >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
    . "$HOME/.cargo/env"
fi
rustup default stable
if [ ! -d {repo} ]; then git clone --branch {branch} {url} {repo}; fi
test "$(git -C {repo} remote get-url origin | sed 's/\\.git$//')" = "$(printf %s {url} | sed 's/\\.git$//')"
git -C {repo} rev-parse --verify HEAD
'''
        def install_host(host):
            with self._connection(host) as conn:
                conn.run(command, hide=True, timeout=1800)
        self._parallel(self.settings.hosts, install_host)
        Print.heading(f'Initialized {len(self.settings.hosts)} CloudLab nodes')

    def _update(self, hosts):
        Print.info('Updating and compiling Narwhal HotStuff on all selected nodes...')
        repo = shlex.quote(self.settings.repo_name)
        branch = shlex.quote(self.settings.branch)
        command = f'''set -e
cd {repo}
test -z "$(git status --porcelain --untracked-files=no)" || {{ echo 'Tracked remote files have local changes'; exit 1; }}
git fetch origin
git checkout {branch}
git pull --ff-only origin {branch}
. "$HOME/.cargo/env"
cargo build --release --features benchmark > benchmark/cloudlab-build.log 2>&1 || {{ tail -n 100 benchmark/cloudlab-build.log; exit 1; }}
test -x target/release/node
test -x target/release/benchmark_client
git rev-parse HEAD
'''
        def build(host):
            with self._connection(host) as conn:
                result = conn.run(command, hide=True, timeout=3600)
                revision = result.stdout.strip().splitlines()[-1]
                self.revisions[host['hostname']] = revision
                Print.info(f"  {host['hostname']} revision {revision}")
        self._parallel(hosts, build)
        if len(set(self.revisions.values())) != 1:
            raise RuntimeError(f'Nodes built different revisions: {self.revisions}')

    def kill(self, hosts=None, delete_logs=False):
        """Stop only binaries belonging to this checkout, then optionally reset its state."""
        hosts = hosts or self.settings.hosts
        script = '''import os, signal, time, shutil
from pathlib import Path
root = Path.home() / REPO_NAME
binary_dir = root / 'target' / 'release'
def processes():
    found = []
    for p in Path('/proc').iterdir():
        if not p.name.isdigit():
            continue
        try:
            exe = (p / 'exe').resolve()
            if exe.parent == binary_dir and exe.name in ('node', 'benchmark_client'):
                found.append(int(p.name))
        except OSError:
            pass
    return found
for pid in processes():
    try: os.kill(pid, signal.SIGTERM)
    except ProcessLookupError: pass
time.sleep(0.2)
for pid in processes():
    try: os.kill(pid, signal.SIGKILL)
    except ProcessLookupError: pass
if DELETE_LOGS:
    shutil.rmtree(root / 'logs', ignore_errors=True)
    (root / 'logs').mkdir(parents=True, exist_ok=True)
    for db in root.glob('.db-*'):
        if db.is_dir(): shutil.rmtree(db)
'''.replace('REPO_NAME', repr(self.settings.repo_name)).replace('DELETE_LOGS', repr(delete_logs))
        def stop(host):
            with self._connection(host) as conn:
                conn.run('python3 -c ' + shlex.quote(script), hide=True, timeout=60)
        self._parallel(hosts, stop)

    def status(self):
        repo = shlex.quote(self.settings.repo_name)
        def inspect(host):
            with self._connection(host) as conn:
                result = conn.run(f'cd {repo} && ps -u "$(id -u)" -o pid,comm,args', hide=True)
                Print.info(host['hostname'] + '\n' + result.stdout)
        self._parallel(self.settings.hosts, inspect)

    def _config(self, hosts, parameters, bench):
        Print.info('Generating HotStuff committee (including consensus ports) and keys...')
        repo = self.settings.repo_name
        keys = []
        with self._connection(hosts[0]) as conn:
            for i in range(len(hosts)):
                filename = PathMaker.key_file(i)
                conn.run(
                    f'cd {shlex.quote(repo)} && ./target/release/node generate_keys --filename {filename}',
                    hide=True,
                )
                conn.get(f'{repo}/{filename}', filename)
                keys.append(Key.from_file(filename))
        addresses = OrderedDict(
            (key.name, [host['hostname']] * (1 + bench.workers))
            for key, host in zip(keys, hosts)
        )
        committee = Committee(addresses, self.settings.base_port)
        committee.print(PathMaker.committee_file())
        parameters.print(PathMaker.parameters_file())
        def upload(host):
            i = hosts.index(host)
            with self._connection(host) as conn:
                for filename in (PathMaker.committee_file(), PathMaker.parameters_file(), PathMaker.key_file(i)):
                    conn.put(filename, f'{repo}/{filename}')
        self._parallel(hosts, upload)
        return committee

    def _background_run(self, host, command, log_file):
        repo = shlex.quote(self.settings.repo_name)
        command = command.replace('./node ', './target/release/node ', 1)
        command = command.replace('./benchmark_client ', './target/release/benchmark_client ', 1)
        script = f'#!/bin/bash\nset -e\ncd {repo}\nexec {command} > {shlex.quote(log_file)} 2>&1\n'
        wrapper = f"{self.settings.repo_name}/.run-{Path(log_file).stem}.sh"
        with self._connection(host) as conn:
            conn.put(StringIO(script), wrapper)
            result = conn.run(
                f'setsid nohup bash {shlex.quote(wrapper)} </dev/null >/dev/null 2>&1 & echo $!',
                hide=True,
            )
            pid = int(result.stdout.strip())
            sleep(0.2)
            result = conn.run(f'kill -0 {pid}', hide=True, warn=True)
            if not result.ok:
                log = conn.run(f'tail -n 30 {repo}/{shlex.quote(log_file)}', hide=True, warn=True)
                raise RuntimeError(f"{host['hostname']} {log_file} failed to start:\n{log.stdout}")
        Print.info(f"Started {Path(log_file).stem} on {host['hostname']} (PID {pid})")

    def _run_single(self, hosts, committee, bench, rate, debug):
        self.kill(hosts, delete_logs=True)
        live_hosts = hosts[:len(hosts) - bench.faults]
        workers = committee.workers_addresses(bench.faults)
        all_addresses = [address for row in workers for _, address in row]
        rate_share = ceil(rate / committee.workers())
        # Clients wait for all workers, preserving the Tusk benchmark's start semantics.
        for i, row in enumerate(workers):
            for worker_id, address in row:
                self._background_run(hosts[i], CommandMaker.run_client(
                    address, bench.tx_size, rate_share, all_addresses
                ), PathMaker.client_log_file(i, worker_id))
        for i, host in enumerate(live_hosts):
            self._background_run(host, CommandMaker.run_primary(
                PathMaker.key_file(i), PathMaker.committee_file(), PathMaker.db_path(i),
                PathMaker.parameters_file(), debug=debug,
            ), PathMaker.primary_log_file(i))
        for i, row in enumerate(workers):
            for worker_id, _ in row:
                self._background_run(hosts[i], CommandMaker.run_worker(
                    PathMaker.key_file(i), PathMaker.committee_file(), PathMaker.db_path(i, worker_id),
                    PathMaker.parameters_file(), worker_id, debug=debug,
                ), PathMaker.worker_log_file(i, worker_id))
        Print.info(f'Running benchmark ({bench.duration} sec)...')
        for i in range(4):
            sleep(bench.duration / 4)
            Print.info(f'  Progress: {(i + 1) * 25}%')

    def _logs(self, hosts, bench):
        Print.info('Downloading all client, primary and worker logs...')
        logs_dir = Path(PathMaker.logs_path())
        logs_dir.mkdir(parents=True, exist_ok=True)
        def download(host):
            i = hosts.index(host)
            files = [PathMaker.primary_log_file(i)]
            for j in range(bench.workers):
                files += [PathMaker.worker_log_file(i, j), PathMaker.client_log_file(i, j)]
            with self._connection(host) as conn:
                for filename in files:
                    conn.get(f'{self.settings.repo_name}/{filename}', filename)
                    if not Path(filename).stat().st_size:
                        raise RuntimeError(f'Empty log: {filename}')
        self._parallel(hosts[:len(hosts) - bench.faults], download)
        # All SSH transports are closed before LogParser creates its worker pool.
        result = LogParser.process(str(logs_dir), faults=bench.faults)
        if not result.commits or not result.sizes:
            raise RuntimeError('No committed transaction batches found in benchmark logs')
        return result

    def run(self, bench_parameters, node_parameters, debug=False):
        bench = BenchParameters(bench_parameters)
        parameters = NodeParameters(node_parameters)
        if not bench.collocate:
            raise ValueError('CloudLab currently requires collocated primary and workers')
        count = max(bench.nodes)
        if count > len(self.settings.hosts):
            raise ValueError(f'Need {count} CloudLab hosts, have {len(self.settings.hosts)}')
        self._update(self.settings.hosts[:count])
        for count in bench.nodes:
            hosts = self.settings.hosts[:count]
            committee = self._config(hosts, parameters, bench)
            for rate in bench.rate:
                for run in range(bench.runs):
                    run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
                    archive = Path(PathMaker.results_path()) / run_id
                    archive.mkdir(parents=True)
                    logs = Path(PathMaker.logs_path())
                    if logs.exists():
                        shutil.move(str(logs), str(archive / 'previous-logs'))
                    Print.heading(f'Narwhal HotStuff: nodes={count}, rate={rate}, run={run + 1}/{bench.runs}')
                    try:
                        self._run_single(hosts, committee, bench, rate, debug)
                    finally:
                        Print.info('Stopping all benchmark processes...')
                        self.kill(hosts)
                    result = self._logs(hosts, bench)
                    summary = result.result()
                    print(summary)
                    result.print(PathMaker.result_file(
                        bench.faults, count, bench.workers, bench.collocate, rate, bench.tx_size,
                    ))
                    (archive / 'summary.txt').write_text(summary)
                    (archive / 'config.json').write_text(json.dumps({
                        'repo': self.settings.repo_url, 'branch': self.settings.branch,
                        'revisions': self.revisions,
                        'bench': {**bench_parameters, 'nodes': [count], 'rate': [rate]},
                        'parameters': node_parameters,
                    }, indent=4) + '\n')
                    shutil.copy2(PathMaker.committee_file(), archive / 'committee.json')
        Print.heading('All Narwhal HotStuff benchmarks completed')
