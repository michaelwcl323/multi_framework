#!/usr/bin/env python3
"""
Script to run CloudLab benchmark and process logs
This script runs 'fab cloudlab_remote', downloads logs, and processes them
"""

import sys
import os
import subprocess
import argparse
from pathlib import Path
from datetime import datetime

# Add benchmark directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from benchmark.logs import LogParser, ParseError
from benchmark.utils import PathMaker, Print, BenchError

def run_fab_command(task='cloudlab-remote', debug=False, fab_kwargs=None):
    """Run fab command"""
    fab_cmd = ['fab', task]
    if debug:
        fab_cmd.append('--debug')
    for key, value in (fab_kwargs or {}).items():
        fab_cmd.append(f'{key}={value}')
    
    Print.info(f'Running: {" ".join(fab_cmd)}')
    Print.info('=' * 60)
    
    try:
        result = subprocess.run(
            fab_cmd,
            cwd=os.path.dirname(os.path.abspath(__file__)),
            check=False  # Don't raise on error, we'll check return code
        )
        return result.returncode == 0
    except FileNotFoundError:
        Print.error('fab command not found. Please install fabric:')
        Print.error('  pip install fabric')
        return False
    except Exception as e:
        Print.error(f'Failed to run fab command: {e}')
        return False

def download_logs_if_needed(settings_file='cloudlab_settings.json', max_workers=1, force_download=False,
                            node_indices=None):
    """Download logs if they don't exist locally"""
    logs_dir = Path(PathMaker.logs_path())
    
    # Check if logs already exist
    primary_logs = list(logs_dir.glob('primary-*.log'))
    worker_logs = list(logs_dir.glob('worker-*.log'))
    client_logs = list(logs_dir.glob('client-*.log'))
    
    if not force_download and (primary_logs or worker_logs or client_logs):
        Print.info(f'Found existing logs: {len(primary_logs)} primary, {len(worker_logs)} worker, {len(client_logs)} client')
        return True
    
    # Try to download logs
    if force_download:
        Print.info('Refreshing shared local logs from remote nodes...')
    else:
        Print.info('No local logs found, attempting to download from remote nodes...')
    try:
        from download_logs import download_logs
        return download_logs(settings_file, max_workers, refresh=force_download, node_indices=node_indices)
    except ImportError:
        Print.warn('download_logs.py not found, skipping download')
        return False

def process_logs(faults=0, save_to_file=True, node_indices=None):
    """Process and display log results"""
    logs_dir = PathMaker.logs_path()
    
    if not os.path.exists(logs_dir):
        Print.warn(f'Logs directory not found: {logs_dir}')
        return False
    
    Print.info('=' * 60)
    Print.info('Processing logs...')
    Print.info('=' * 60)
    
    try:
        parser = LogParser.process(logs_dir, faults=faults, node_indices=node_indices)
        result = parser.result()
        
        # Print results
        print(result)
        
        # Save to file
        if save_to_file:
            result_file = Path(PathMaker.summary_file())
            result_file.parent.mkdir(parents=True, exist_ok=True)

            with open(result_file, 'w') as f:
                f.write(f'Benchmark Results - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
                f.write('=' * 60 + '\n')
                f.write(result)
            
            Print.info(f'\nResults saved to: {result_file}')
        
        return True
        
    except ParseError as e:
        Print.warn(f'Failed to parse logs: {e}')
        Print.warn('This may be because some log files are empty or incomplete.')
        return False
    except Exception as e:
        Print.warn(f'Error processing logs: {e}')
        return False

def main():
    parser = argparse.ArgumentParser(
        description='Run CloudLab benchmark and process logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Run benchmark and process logs
  python3 run_cloudlab_benchmark.py

  # Only process existing logs (skip running benchmark)
  python3 run_cloudlab_benchmark.py --no-run

  # Run benchmark with debug mode
  python3 run_cloudlab_benchmark.py --debug

  # Only download logs without running benchmark
  python3 run_cloudlab_benchmark.py --download-only
        '''
    )
    
    parser.add_argument('--no-run', action='store_true',
                       help='Skip running fab cloudlab_remote, only process existing logs')
    parser.add_argument('--download-only', action='store_true',
                       help='Only download logs from remote nodes, do not run benchmark or process')
    parser.add_argument('--debug', action='store_true',
                       help='Run benchmark in debug mode')
    parser.add_argument('--faults', type=int, default=0,
                       help='Number of faulty nodes (default: 0)')
    parser.add_argument('--no-save', action='store_true',
                       help='Do not save results to file')
    parser.add_argument('--max-workers', type=int, default=1,
                       help='Maximum number of workers per node for log download (default: 1)')
    parser.add_argument('--settings', default='cloudlab_settings.json',
                       help='Path to CloudLab settings file (default: cloudlab_settings.json)')
    parser.add_argument('--nodes', type=int, default=None,
                       help='Committee size; excludes the final --faults nodes from log collection')

    args = parser.parse_args()
    
    Print.heading('CloudLab Benchmark Runner')
    Print.info('=' * 60)
    
    success = True

    node_indices = None
    if args.nodes is not None:
        if args.faults < 0 or args.nodes <= args.faults:
            parser.error('--nodes must exceed --faults, which must be nonnegative')
        node_indices = list(range(args.nodes - args.faults))

    # Step 1: Run the existing Fabric task unless only processing/downloading.
    if not args.no_run and not args.download_only:
        success = run_fab_command(debug=args.debug)
        if not success:
            Print.warn('Benchmark run completed with errors, but continuing to process logs...')

    # Step 2: Download logs if needed (unless download-only)
    if not args.download_only:
        success = download_logs_if_needed(
            args.settings,
            args.max_workers,
            force_download=not args.no_run,
            node_indices=node_indices,
        ) and success
    else:
        # Download-only mode
        Print.info('Download-only mode: downloading logs from remote nodes...')
        downloaded = download_logs_if_needed(args.settings, args.max_workers,
                                             force_download=True, node_indices=node_indices)
        return 0 if downloaded else 1
    
    # Step 3: Process logs
    if not args.no_save:
        success = process_logs(faults=args.faults, save_to_file=True, node_indices=node_indices) and success
    else:
        success = process_logs(faults=args.faults, save_to_file=False, node_indices=node_indices) and success
    
    Print.info('=' * 60)
    if success:
        Print.info('✓ All operations completed successfully')
        return 0
    else:
        Print.warn('⚠ Some operations completed with errors')
        return 1

if __name__ == '__main__':
    sys.exit(main())

