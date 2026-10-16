"""Preserve each CloudLab run and measure commit in its proposer's own log."""
import json
from pathlib import Path
import re
from statistics import fmean
import tarfile

from benchmark.logs import ParseError
from benchmark.utils import PathMaker, Print


def consensus_metrics(logger, primary_logs):
    samples = []
    proposed = 0
    all_commits = []
    for text in primary_logs:
        proposals, commits, _, _, _, _, _, _, sizes = logger._parse_primaries(text)
        all_commits.append(set(commits))
        for header, start in proposals.items():
            if sizes.get(header, 0) == 0:
                continue
            proposed += 1
            if header in commits:
                latency = (commits[header] - start) * 1000
                if latency < 0:
                    raise ParseError(f'Negative proposer-local latency for header {header}')
                samples.append(latency)
    if not samples:
        raise ParseError('No nonempty headers committed at their proposers')
    common = set.intersection(*all_commits) if all_commits else set()
    return dict(
        consensus_latency_ms=fmean(samples), consensus_samples=len(samples),
        consensus_sample_unit='nonempty header',
        consensus_latency_definition='propose -> proposer-local commit',
        proposed_nonempty_headers=proposed, uncommitted_nonempty_headers=proposed - len(samples),
        nodes=len(all_commits), all_node_common_commits=len(common),
        client_rate_misses=getattr(logger, 'misses', 0),
    )


def save_results(logger, rate, nodes, bench, params, burst, run, output_root, settings):
    logs = Path(PathMaker.logs_path())
    primaries = [path.read_text() for path in sorted(logs.glob('primary-*.log'))]
    metrics = consensus_metrics(logger, primaries)
    if metrics['nodes'] != nodes - bench.faults:
        raise ParseError('Missing primary logs for one or more active nodes')
    summary = logger.result()
    summary = re.sub(r'(?m)^ Consensus latency:.*$',
                     f" Consensus latency: {metrics['consensus_latency_ms']:.3f} ms", summary)
    summary += (f" Consensus latency definition: {metrics['consensus_latency_definition']}\n"
                f" Consensus samples: {metrics['consensus_samples']} nonempty headers\n"
                f" Uncommitted nonempty headers: {metrics['uncommitted_nonempty_headers']}\n")
    if logger.consensus_only:
        tps = logger._consensus_only_throughput()
        e2e_tps, e2e_latency = None, None
    else:
        tps = logger._consensus_throughput()[0]
        e2e_tps = logger._end_to_end_throughput()[0]
        e2e_latency = logger._end_to_end_latency() * 1000
    metrics.update(protocol='Sailfish', input_rate_tx_s=rate, run=run,
                   consensus_tps=tps, end_to_end_tps=e2e_tps,
                   end_to_end_latency_ms=e2e_latency, burst_ms=burst,
                   consensus_only=logger.consensus_only,
                   valid=metrics['all_node_common_commits'] > 0 and metrics['client_rate_misses'] == 0)
    root = Path(output_root)
    if len(bench.burst) > 1:
        root /= f'burst-{burst}'
    rate_dir = root / f'rate-{rate}'
    # Re-running the command must preserve preceding measurements.
    while (rate_dir / f'run-{run}').exists():
        run += 1
    metrics['run'] = run
    target = rate_dir / f'run-{run}'
    target.mkdir(parents=True)
    (target / 'summary.txt').write_text(summary)
    (target / 'metrics.json').write_text(json.dumps(metrics, indent=2) + '\n')
    (target / 'health.json').write_text(json.dumps({key: metrics[key] for key in (
        'valid', 'nodes', 'all_node_common_commits', 'client_rate_misses',
        'consensus_samples', 'uncommitted_nonempty_headers')}, indent=2) + '\n')
    (target / 'parameters.json').write_text(json.dumps(params.json, indent=2) + '\n')
    (target / 'committee.json').write_text(Path(PathMaker.committee_file()).read_text())
    (target / 'experiment.json').write_text(json.dumps(dict(
        protocol='Sailfish', nodes=nodes, faults=bench.faults, tx_size=bench.tx_size,
        duration_s=bench.duration, input_rate_tx_s=rate, burst_ms=burst,
        repository_url=settings.repo_url, branch=settings.branch,
    ), indent=2) + '\n')
    with tarfile.open(target / 'logs.tar.gz', 'w:gz') as archive:
        for path in sorted(logs.glob('*.log')):
            archive.add(path, arcname=path.name)
    Print.info(f'Results saved to {target}')
    print(summary)
    return target
