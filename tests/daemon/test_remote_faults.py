"""Fault injection on a worker that takes leases over HTTP, through a fault proxy (``tests/fixtures/netfault.py``).

Three processes on one machine, as on three hosts. Core runs in the test process: it submits evaluations and expires
leases. The lease routes run in a server process (``tests/fixtures/server_process.py``). The worker is ``piloop worker
--server``, with a home and clones of its own, and it reaches the server only through the proxy; it shares the server's
CAS, as a worker on another host shares an S3 bucket. The jobs are evaluations.
"""

import os
import subprocess
import sys
import threading
from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from sqlalchemy import func, select

from piloop.daemon.run_controller import RunController
from piloop.db.events import in_order
from piloop.db.models import Campaign, Lease, LeaseState, LeaseWorker, Measurement, TokenScope
from piloop.db.session import Database
from piloop.ids import CandidateId
from piloop.net import tokens
from piloop.otel import Tracer
from piloop.plugins.executor.kit.capacity import Pool
from piloop.plugins.executor.kit.leases import StaleReport
from piloop.plugins.executor.workers import Workers
from piloop.proc import alive
from piloop.settings import STORE_DIR
from tests.conftest import ScriptRuntime, baseline, factory, wait_until, workers_of
from tests.daemon.test_evaluation_queue import held_evaluate
from tests.daemon.test_run_controller import set_config
from tests.fixtures.netfault import Blackhole, CutResponse, Proxy

SERVER_PROCESS = Path(__file__).parent.parent / 'fixtures' / 'server_process.py'
WORKER = 'gpu1'
WORKERS = Workers(
    renew_seconds=0.2, grace_seconds=1.0, toleration_seconds=6.0, pools={'gpu': Pool(agents=0, evaluations=1)}
)
"""Short lease times with the default's two stages. The self-stop, 5.6 s, outlasts a restart of the server process."""


@dataclass
class Net:
    """The server process, the proxy in front of it, and the worker processes, which the test kills and starts."""

    db: Database
    tmp_path: Path
    token: str
    proxy: Proxy | None = None
    server: subprocess.Popen[str] | None = None
    workers: list[subprocess.Popen[str]] = field(default_factory=list)

    def start_server(self) -> None:
        """Start the server process, and point the proxy at it."""
        argv = [sys.executable, str(SERVER_PROCESS), f'sqlite:///{self.tmp_path / "piloop.db"}', str(self.tmp_path)]
        self.server = subprocess.Popen(
            [*argv, WORKERS.model_dump_json()],
            stdout=subprocess.PIPE,
            text=True,
        )
        assert self.server.stdout is not None
        ready, port = self.server.stdout.readline().split()
        assert ready == 'ready'
        if self.proxy is None:
            self.proxy = Proxy(('127.0.0.1', int(port)))
        self.proxy.upstream = ('127.0.0.1', int(port))

    def kill_server(self) -> None:
        assert self.server is not None and self.server.stdout is not None
        _kill(self.server)
        self.server.stdout.close()

    def start_worker(self) -> subprocess.Popen[str]:
        """Start ``piloop worker --server`` in the pool ``gpu``, and wait until it registered."""
        assert self.proxy is not None
        home = self.tmp_path / 'worker'
        home.mkdir(exist_ok=True)  # the server's CAS, which a worker reads and writes itself
        (home / 'config.toml').write_text(f'[plugins.cas]\nprovider = "disk"\npath = "{self.tmp_path / STORE_DIR}"\n')
        env = os.environ | {'PILOOP_HOME': str(home), 'PILOOP_HTTPS_TOKEN': self.token}
        url = f'http://127.0.0.1:{self.proxy.port}'
        argv = [sys.executable, '-m', 'piloop', 'worker', '--server', url, '--pool', 'gpu']
        before = _seen(self.db)
        proc = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        self.workers.append(proc)
        try:
            wait_until(lambda: _seen(self.db) != before, timeout=30)
        except AssertionError:
            proc.kill()
            assert proc.stdout is not None
            raise AssertionError(proc.stdout.read()) from None
        return proc

    @property
    def spool(self) -> Path:
        return self.tmp_path / 'worker' / 'spool' / WORKER


def _seen(db: Database) -> object:
    with db.session() as s:
        row = s.get(LeaseWorker, WORKER)
        return row.seen_at if row is not None else None


def _kill(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is None:
        proc.kill()
    proc.wait()


@pytest.fixture
def net(db: Database, tmp_path: Path) -> Generator[Net]:
    with db.session() as s:
        secret = tokens.create(s, WORKER, TokenScope.WORKER)
        s.commit()
    net = Net(db, tmp_path, secret)
    net.start_server()
    try:
        yield net
    finally:
        for proc in [*net.workers, *([net.server] if net.server else [])]:
            _kill(proc)
            if proc.stdout is not None:
                proc.stdout.close()
        if net.proxy is not None:
            net.proxy.close()


@pytest.fixture
def core(db: Database, runtime: ScriptRuntime, tmp_path: Path) -> RunController:
    """Core, with the ``workers`` executor, whose own worker never starts: the jobs go to the remote worker."""
    return RunController(db, Tracer(None), tmp_path, factory(runtime), executor=WORKERS)


@pytest.fixture
def hold(tmp_path: Path) -> Generator[Path]:
    """While this file exists, the evaluator waits."""
    path = tmp_path / 'hold'
    path.touch()
    yield path
    path.unlink(missing_ok=True)


@pytest.fixture
def held(db: Database, campaign: Campaign, tmp_path: Path, hold: Path) -> CandidateId:
    """The baseline, whose next evaluation waits while ``hold`` exists."""
    set_config(db, campaign, evaluate=held_evaluate(tmp_path))
    return baseline(db, campaign.id).id


def in_thread(target: Callable[..., object], *args: object) -> threading.Thread:
    """Run ``target`` in a thread that a failed test leaves behind, waiting for a measurement that never comes."""
    thread = threading.Thread(target=target, args=args, daemon=True)
    thread.start()
    return thread


def lease(db: Database, lease_id: int) -> Lease | None:
    with db.session() as s:
        return s.get(Lease, lease_id)


def state(db: Database, lease_id: int) -> LeaseState | None:
    return row.state if (row := lease(db, lease_id)) is not None else None


def evaluator(db: Database, lease_id: int) -> tuple[int, float]:
    """The process group of the lease's evaluator, and its start time."""
    row = lease(db, lease_id)
    assert row is not None
    pgid, start = row.handle['pgid'], row.handle['start_time']
    assert isinstance(pgid, int) and isinstance(start, float)
    return pgid, start


def measured(db: Database, candidate: CandidateId) -> int:
    with db.session() as s:
        return s.scalar(select(func.count()).select_from(Measurement).where(Measurement.candidate_id == candidate)) or 0


def stale_reports(db: Database) -> list[StaleReport]:
    with db.session() as s:
        return [payload for _, payload in in_order(s, StaleReport)]


def expire_until(core: RunController, db: Database, lease_id: int, wanted: LeaseState) -> None:
    def reached() -> bool:
        workers_of(core).leases.expire()
        return state(db, lease_id) is wanted

    wait_until(reached, timeout=30)


def test_a_remote_worker_measures_an_evaluation_once_and_its_logs_are_on_the_servers_side(
    db: Database, net: Net, core: RunController, held: CandidateId, hold: Path
) -> None:
    net.start_worker()
    hold.unlink()
    core.remeasure(held)
    assert measured(db, held) == 2, 'the baseline, then one measurement'
    row = lease(db, 1)
    assert row is not None and (row.state, row.worker) == (LeaseState.REPORTED, WORKER)
    place = f'{row.job["place"]}/lease-1'
    assert list((net.tmp_path / place).glob('*/eval.stdout')), "the evaluator's logs, back through the CAS"
    assert list((net.tmp_path / 'worker' / place).glob('*/eval.stdout')), 'where the worker wrote them'
    assert list((net.tmp_path / 'worker' / 'clones').iterdir()), 'its own clone, filled from a bundle in the CAS'
    assert state(db, 2) is None


def test_a_worker_killed_during_an_evaluation_is_measured_again_on_a_newer_lease_by_its_successor(
    db: Database, net: Net, core: RunController, held: CandidateId, hold: Path
) -> None:
    measuring = in_thread(core.remeasure, held)
    worker = net.start_worker()
    wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
    pgid, start = evaluator(db, 1)
    _kill(worker)
    net.start_worker()  # the successor kills the evaluator that the dead worker left, and reports it gone
    wait_until(lambda: not alive(pgid, start))
    expire_until(core, db, 1, LeaseState.EXPIRED)
    hold.unlink()
    measuring.join(30)
    assert not measuring.is_alive()
    assert state(db, 2) is LeaseState.REPORTED and measured(db, held) == 2


def test_a_partitioned_worker_stops_its_evaluation_before_its_lease_expires_and_its_late_report_is_refused(
    db: Database, net: Net, core: RunController, held: CandidateId, hold: Path
) -> None:
    measuring = in_thread(core.remeasure, held)
    net.start_worker()
    wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
    pgid, start = evaluator(db, 1)
    assert net.proxy is not None
    net.proxy.toxic = Blackhole()
    wait_until(lambda: not alive(pgid, start), timeout=30)
    row = lease(db, 1)
    assert row is not None and row.expires_at is not None
    assert state(db, 1) is LeaseState.RUNNING, 'the worker stopped its job before its lease expired'
    expire_until(core, db, 1, LeaseState.EXPIRED)
    net.proxy.toxic = None
    wait_until(lambda: stale_reports(db), timeout=30)
    [payload] = stale_reports(db)
    assert (payload.lease, payload.worker) == (1, WORKER)
    hold.unlink()
    measuring.join(30)
    assert not measuring.is_alive() and state(db, 2) is LeaseState.REPORTED, 'measured again, on a newer lease'


def test_a_report_whose_answer_was_cut_is_sent_again_with_its_request_id_and_applies_once(
    db: Database, net: Net, core: RunController, held: CandidateId, hold: Path
) -> None:
    measuring = in_thread(core.remeasure, held)
    net.start_worker()
    wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
    assert net.proxy is not None
    net.proxy.toxic = CutResponse(b'POST /v1/executor/report')
    hold.unlink()
    measuring.join(30)
    assert not measuring.is_alive() and net.proxy.cuts == 1
    assert state(db, 1) is LeaseState.REPORTED and measured(db, held) == 2
    assert stale_reports(db) == [], 'the resend got the receipt, not a refusal'


def test_an_acquire_whose_grant_was_cut_gets_the_same_lease_again_and_leaves_none_to_expire(
    db: Database, net: Net, core: RunController, held: CandidateId, hold: Path
) -> None:
    net.start_worker()
    assert net.proxy is not None
    net.proxy.toxic = CutResponse(b'POST /v1/executor/acquire', b'"lease":')
    hold.unlink()
    core.remeasure(held)
    assert net.proxy.cuts == 1
    row = lease(db, 1)
    assert row is not None and (row.state, row.worker) == (LeaseState.REPORTED, WORKER)
    assert state(db, 2) is None and measured(db, held) == 2, 'one lease, measured once'


def test_a_server_killed_during_an_evaluation_and_started_again_takes_its_report(
    db: Database, net: Net, core: RunController, held: CandidateId, hold: Path
) -> None:
    measuring = in_thread(core.remeasure, held)
    net.start_worker()
    wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
    net.kill_server()
    hold.unlink()
    net.start_server()
    measuring.join(30)
    assert not measuring.is_alive()
    assert state(db, 1) is LeaseState.REPORTED and state(db, 2) is None and measured(db, held) == 2


def test_a_worker_killed_with_a_report_in_its_spool_sends_it_when_it_starts_again(
    db: Database, net: Net, core: RunController, held: CandidateId, hold: Path
) -> None:
    measuring = in_thread(core.remeasure, held)
    worker = net.start_worker()
    wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
    assert net.proxy is not None
    net.proxy.toxic = Blackhole()
    hold.unlink()
    wait_until((net.spool / '1.json').exists)
    _kill(worker)
    net.proxy.toxic = None
    net.start_worker()
    measuring.join(30)
    assert not measuring.is_alive()
    assert state(db, 1) is LeaseState.REPORTED and state(db, 2) is None and measured(db, held) == 2
    wait_until(lambda: not (net.spool / '1.json').exists())


def test_a_worker_whose_token_is_revoked_stops_its_evaluation_and_exits_non_zero_keeping_its_report(
    db: Database, net: Net, core: RunController, held: CandidateId
) -> None:
    measuring = in_thread(core.remeasure, held)
    worker = net.start_worker()
    wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
    pgid, start = evaluator(db, 1)
    with db.session() as s:
        tokens.revoke(s, WORKER)
        s.commit()
    assert worker.wait(30) == 1
    assert worker.stdout is not None
    assert 'the server refused this worker (401): the token is unknown or revoked' in worker.stdout.read()
    assert not alive(pgid, start) and (net.spool / '1.json').exists()
    with db.session() as s:
        net.token = tokens.create(s, WORKER, TokenScope.WORKER)
        s.commit()
    net.start_worker()
    measuring.join(30)
    assert not measuring.is_alive() and state(db, 1) is LeaseState.REPORTED, 'a new token sends the kept report'
