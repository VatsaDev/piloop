"""``piloop worker list``: the workers of the ``workers`` executor, their host samples, and why each got no lease."""

import cyclopts
from rich.filesize import decimal
from rich.table import Table

from piloop.cli import common
from piloop.db.models import utcnow
from piloop.plugins.executor.contract import DEFAULT_EXECUTOR
from piloop.plugins.executor.workers import seen
from piloop.reports.overview import age

app = cyclopts.App(name='worker', help='The workers that run jobs on leases.')
WORKERS = 'workers'
"""The executor whose workers these are."""


@app.command(name='list')
def worker_list() -> None:
    """List each worker: its pool, when it was last heard, its host sample, its jobs, and why it got no lease."""
    with common.session() as s:
        now = utcnow()
        workers = seen(s)
        table = Table('worker', 'pool', 'seen', 'load', 'memory', 'disk', 'agents', 'evaluations', 'no lease', box=None)
        for w in workers:
            table.add_row(
                w.worker.id,
                w.worker.pool,
                f'{age(w.worker.seen_at, now)} ago',
                f'{w.host.load1:.1f}/{w.host.cpus}',
                decimal(w.host.memory_available),
                decimal(w.host.disk_free),
                str(w.running.agents),
                str(w.running.evaluations),
                w.worker.no_lease or '',
            )
    executor = common.settings.plugins.get('executor')
    if (selected := executor.provider if executor is not None else DEFAULT_EXECUTOR) != WORKERS:
        common.console.print(f'the executor is {selected}, not {WORKERS}: these workers take no job', markup=False)
    if workers:
        common.console.print(table)
    else:
        common.console.print('no worker yet: `piloop serve` starts one', markup=False)
