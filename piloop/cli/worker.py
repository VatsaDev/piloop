"""``piloop worker --server URL --pool NAME``, a worker on this machine, and ``piloop worker list``, the workers."""

import signal
import threading
from types import FrameType
from typing import Annotated

import cyclopts
from rich.filesize import decimal
from rich.table import Table

from piloop.cli import common, local
from piloop.db.models import utcnow
from piloop.otel import tracer
from piloop.plugins.cas.contract import CAS
from piloop.plugins.executor.contract import EXECUTORS
from piloop.plugins.executor.workers import Workers, instances, remote_worker, seen
from piloop.plugins.network.contract import NETWORKS, NoCredential
from piloop.proc import withhold
from piloop.text import age

app = cyclopts.App(name='worker', help='The workers that run jobs on leases.')


@app.default
def run(
    *,
    server: Annotated[str, cyclopts.Parameter(help='The URL of the server that `piloop serve --listen` runs.')],
    pool: Annotated[str, cyclopts.Parameter(help='The pool of the server whose slots this machine has.')],
    once: Annotated[bool, cyclopts.Parameter(help='Take one lease, run its job, and exit.')] = False,
    orphan_exit: Annotated[
        float | None,
        cyclopts.Parameter(help='Exit after this many seconds without an answer of the server, once no job runs.'),
    ] = None,
) -> None:
    """Take leases from a server, and run their jobs on this machine until it stops (Ctrl-C).

    The network (``[plugins.network]``) gives it its credential, and its name is the one that the server knows that
    credential by. It exits once the server drains it and its jobs reported. It exits non-zero when the server refuses
    it: a revoked credential, another piloop protocol, an unknown pool.
    """
    settings = common.settings
    network = NETWORKS.selected(settings.providers)
    try:
        client = network.client(server)
    except NoCredential as e:
        raise SystemExit(str(e)) from e
    settings.home.mkdir(parents=True, exist_ok=True)  # its jobs' files, its spool and its free disk are there
    stopping = threading.Event()
    with client as http:
        cas = CAS.selected(settings.providers).open(settings.home)
        withhold(network.CREDENTIALS)  # after its last client: no job gets the worker's credential
        reserve = settings.storage.reserve
        worker = remote_worker(
            http,
            stopping,
            tracer(),
            settings.home,
            cas,
            pool,
            reserve,
            settings.relay_file,
            once=once,
            orphan_exit=orphan_exit,
        )

        def stop(signum: int, frame: FrameType | None) -> None:
            stopping.set()
            worker.stop()

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        worker.start()
        worker.wait()
    if worker.dismissed is not None:
        raise SystemExit(str(worker.dismissed))


@app.command(name='list')
def worker_list() -> None:
    """List each worker: its pool, when it was last heard, its host sample, its jobs, and why it got no lease.

    Then each worker that a provisioner starts, until it ends: its pool, its state, and when it was asked for.
    """
    with local.session() as s:
        now = utcnow()
        workers = seen(s)
        started = instances(s)
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
    if not isinstance(executor := EXECUTORS.selected(common.settings.providers), Workers):
        named = EXECUTORS.name_of(executor)
        common.console.print(f'the executor is {named}, not workers: these workers take no job', markup=False)
    if workers:
        common.console.print(table)
    else:
        common.console.print('no worker yet: `piloop serve` starts one', markup=False)
    if started:
        provisioned = Table('instance', 'pool', 'state', 'asked', box=None)
        for row in started:
            provisioned.add_row(row.name, row.pool, row.state, f'{age(row.requested_at, now)} ago')
        common.console.print(provisioned)
