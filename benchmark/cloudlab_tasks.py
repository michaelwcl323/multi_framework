"""CloudLab tasks; experiment values are edited in cloudlab_remote below."""
from fabric import task
from invoke import Exit

from benchmark.utils import BenchError, Print


def _bench(settings, output_dir='results/Sailfish'):
    from benchmark.cloudlab_remote import CloudLabBench
    return CloudLabBench(settings_file=settings, output_dir=output_dir)


@task
def cloudlab_info(ctx, settings='cloudlab_settings.json'):
    """Show the static CloudLab inventory."""
    from benchmark.cloudlab_instance import CloudLabInstanceManager
    try:
        CloudLabInstanceManager.make(settings).print_info()
    except BenchError as error:
        Print.error(error)
        raise Exit(code=1) from error


@task
def cloudlab_test(ctx, settings='cloudlab_settings.json'):
    """Test SSH to the configured CloudLab hosts."""
    try:
        _bench(settings).test_connections()
    except BenchError as error:
        Print.error(error)
        raise Exit(code=1) from error


@task
def cloudlab_install(ctx, settings='cloudlab_settings.json', max_workers=8):
    """Prepare the controller/hosts, then clone and build Sailfish."""
    from environment_setup import EnvironmentSetupError, setup_environment
    try:
        setup_environment(settings_file=settings, max_workers=int(max_workers))
        _bench(settings).install()
    except (BenchError, EnvironmentSetupError, ValueError) as error:
        Print.error(error if isinstance(error, BenchError) else BenchError('CloudLab setup failed', error))
        raise Exit(code=1) from error


@task
def cloudlab_remote(
    ctx, rates='40000,80000,120000,160000,200000', nodes=10,
    runs=2, duration=120, burst=50, debug=False, consensus_only=False,
    update=True, settings='cloudlab_settings.json', output_dir='results/Sailfish',
    batch_size=2_048_000, header_size=2_048_000,
    max_batch_delay=200, max_header_delay=200, tx_size=500,
):
    """Run Sailfish on CloudLab; sizes are bytes and delays are milliseconds."""
    # EDIT EXPERIMENT PARAMETERS HERE (also exposed as Fabric CLI options).
    # 2,048,000 B / 500 B = 4096 transactions per full batch/header.
    from environment_setup import EnvironmentSetupError, setup_local_environment
    try:
        rate_list = [int(x) for x in str(rates).replace(' ', ',').split(',') if x]
        nodes, runs, duration, burst = map(int, (nodes, runs, duration, burst))
        batch_size, header_size, tx_size = map(int, (batch_size, header_size, tx_size))
        max_batch_delay, max_header_delay = map(int, (max_batch_delay, max_header_delay))
        if nodes <= 1 or min(runs, duration, burst, max_batch_delay, max_header_delay) <= 0:
            raise ValueError('Nodes must exceed one; runs, durations and delays must be positive')
        if not rate_list or min(rate_list) <= 0 or tx_size < 9:
            raise ValueError('Input rates must be positive; transaction size must be at least 9 B')
        if batch_size < tx_size or header_size < tx_size:
            raise ValueError('Batch/header size must hold at least one transaction')
        if batch_size % tx_size or header_size % tx_size:
            raise ValueError('Batch/header sizes must be multiples of transaction size')
        setup_local_environment(check_only=True)
        bench = dict(faults=0, nodes=[nodes], workers=1, collocate=True,
                     rate=rate_list, tx_size=tx_size, duration=duration, runs=runs, burst=[burst])
        params = dict(consensus_only=bool(consensus_only), tx_size=tx_size,
                      header_size=header_size, max_header_delay=max_header_delay,
                      batch_size=batch_size, max_batch_delay=max_batch_delay,
                      gc_depth=50, sync_retry_delay=10_000,
                      sync_retry_nodes=min(7, nodes - 1),
                      client_rate=(rate_list[0] + nodes - 1) // nodes,
                      timeout_delay=max_header_delay)
        _bench(settings, output_dir).run(bench, params, debug=bool(debug),
                                       consensus_only=bool(consensus_only), update=bool(update))
    except (BenchError, EnvironmentSetupError, TypeError, ValueError) as error:
        Print.error(error if isinstance(error, BenchError) else BenchError('CloudLab benchmark failed', error))
        raise Exit(code=1) from error


@task
def cloudlab_kill(ctx, settings='cloudlab_settings.json'):
    """Stop Sailfish benchmark processes on configured hosts."""
    try:
        _bench(settings).kill()
    except BenchError as error:
        Print.error(error)
        raise Exit(code=1) from error
