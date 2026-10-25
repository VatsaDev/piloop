"""Fault injection on leases: a worker killed or frozen during a run and during an evaluation, a late report, a
duplicate report, a partition past the toleration, and a daemon killed and started again.

A worker in a process of its own (``tests/fixtures/worker_process.py``) takes the jobs of a control plane in the test
process, so that ``kill -9`` and ``SIGSTOP`` hit the worker and not the control plane. The leases use short times
(``FAST``), the same two stages as the defaults.
"""

import contextlib
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Generator, Iterator
from contextlib import closing, contextmanager
from pathlib import Path

import psutil
import pytest
from git import Repo
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pytest_mock import MockerFixture
from sqlalchemy import delete, func, select

from piloop.campaign.stop import attempts
from piloop.daemon.run_controller import RunController
from piloop.daemon.scheduler import Scheduler
from piloop.daemon.serve import SERVE_ARGV
from piloop.db.events import has, in_order
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Candidate,
    Evaluation,
    Event,
    Idea,
    IdeaStatus,
    Lease,
    LeaseState,
    Measurement,
    Run,
    RunEnd,
    RunStatus,
)
from piloop.db.session import Database
from piloop.ids import Revision, RunId, TargetKind
from piloop.otel import Tracer
from piloop.plugins.cas.contract import CAS
from piloop.plugins.executor.contract import AgentFinished, AgentReport, EvaluationReport, Source
from piloop.plugins.executor.kit.leases import NOT_TAKEN, SERVE, Leases, StaleReport, Timing
from piloop.plugins.executor.workers import Workers
from piloop.plugins.vcs.git import bundles as store_commits
from piloop.proc import alive, started
from piloop.settings import Settings
from tests.conftest import (
    WRITE_PID,
    ScriptRuntime,
    at_head,
    baseline,
    factory,
    register,
    serving,
    timed,
    wait_until,
    workers_of,
    write_toml,
)
from tests.daemon.test_evaluation_queue import held_evaluate
from tests.daemon.test_run_controller import execute, make_run, reload_campaign, run_row, set_config
from tests.daemon.test_scheduler import add_ideas, make_scheduler, request_watchdog
from tests.daemon.test_serve import env_for, reap

FAST = Timing(renew=0.1, grace=0.5, toleration=2.0)
SLACK = Timing(renew=0.5, grace=1.0, toleration=3.0)
"""For a deadline that the test checks: the worker stops its job at least one heartbeat before its lease expires,
which on a loaded host must be longer than a scheduling delay."""
WORKER_PROCESS = Path(__file__).parent.parent / 'fixtures' / 'worker_process.py'
COMMIT_THEN_HOLD = (
    f'#!/bin/sh\n{WRITE_PID}echo line >> solve.txt\ngit commit -qam "a line"\n'
    'while [ -f {hold} ]; do sleep 0.05; done\n'
)


def control_plane(db: Database, runtime: ScriptRuntime, tmp_path: Path) -> RunController:
    """The daemon's control plane, without its in-process worker, which never starts: the jobs go to the worker
    process."""
    return RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST))


def next_daemon(db: Database, tmp_path: Path) -> Scheduler:
    """The scheduler of a daemon that starts again: its ``reap`` starts its worker, which finds its leases again."""
    runtime = factory(ScriptRuntime())
    return Scheduler(
        db, RunController(db, Tracer(None), tmp_path, runtime, executor=Workers()), Tracer(None), tmp_path, runtime
    )


@contextmanager
def worker_process(tmp_path: Path, worker_id: str) -> Generator[subprocess.Popen[str]]:
    argv = [
        sys.executable,
        str(WORKER_PROCESS),
        f'sqlite:///{tmp_path / "piloop.db"}',
        str(tmp_path),
        worker_id,
        *(str(n) for n in (FAST.renew, FAST.grace, FAST.toleration)),
    ]
    proc = subprocess.Popen(argv, stdout=subprocess.PIPE, text=True)
    try:
        assert proc.stdout is not None and proc.stdout.readline() == 'ready\n'
        yield proc
    finally:
        if proc.poll() is None:
            os.kill(proc.pid, signal.SIGCONT)
            proc.kill()
        proc.wait()
        assert proc.stdout is not None
        proc.stdout.close()


def freeze(proc: subprocess.Popen[str], database: Path) -> None:
    """``SIGSTOP`` the worker process, at a moment when it holds no write lock on the database.

    A process frozen inside a write transaction would hold the lock, and every other writer would wait for it. A remote
    worker never opens the database; this one shares it only because it runs on the same machine.
    """
    while True:
        os.kill(proc.pid, signal.SIGSTOP)  # delivered asynchronously: wait until it took effect
        wait_until(lambda: psutil.Process(proc.pid).status() == psutil.STATUS_STOPPED)
        with contextlib.closing(sqlite3.connect(database, timeout=0)) as con:
            try:
                con.execute('BEGIN IMMEDIATE')
            except sqlite3.OperationalError:  # frozen mid-transaction: let it finish, and try again
                os.kill(proc.pid, signal.SIGCONT)
                time.sleep(0.01)
                continue
            con.rollback()
            return


def lease_of(db: Database, lease_id: int) -> Lease:
    with db.session() as s:
        return s.get_one(Lease, lease_id)


def state(db: Database, lease_id: int) -> LeaseState | None:
    """The lease's state, or None before it is queued."""
    with db.session() as s:
        lease = s.get(Lease, lease_id)
        return lease.state if lease is not None else None


def expire_until(ex: RunController, db: Database, lease_id: int, state: LeaseState) -> None:
    """Run the scheduler's expiry, as its tick does, until the lease is in ``state``."""

    def reached() -> bool:
        workers_of(ex).leases.expire()
        return lease_of(db, lease_id).state is state

    wait_until(reached)


def in_thread(target: Callable[..., object], *args: object) -> threading.Thread:
    thread = threading.Thread(target=target, args=args)
    thread.start()
    return thread


@pytest.fixture
def hold(tmp_path: Path) -> Iterator[Path]:
    """While this file exists, the agent waits. Its process outlives a killed worker: the test ends it."""
    path = tmp_path / 'hold'
    path.touch()
    yield path
    path.unlink(missing_ok=True)
    for pid_file in tmp_path.glob('runs/*/agent.pid'):
        with contextlib.suppress(ProcessLookupError):
            os.kill(int(pid_file.read_text()), signal.SIGKILL)


def test_a_worker_killed_during_a_run_loses_its_lease_and_the_run_is_no_attempt(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, hold: Path
) -> None:
    """``kill -9`` of a worker: its lease expires after the toleration, the run fails as ``worker lost`` and its idea
    goes back to ``ready``, with the attempt not counted."""
    write_toml(Path(campaign.path), COMMIT_THEN_HOLD.format(hold=hold))
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    ex = control_plane(db, runtime, tmp_path)
    with worker_process(tmp_path, 'w2') as proc:
        thread = in_thread(execute, ex, db, run_id)
        wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
    expire_until(ex, db, 1, LeaseState.EXPIRED)
    thread.join(30)
    run = run_row(db, run_id)
    assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.WORKER_LOST)
    with db.session() as s:
        idea = s.get_one(Idea, run.idea_id)
        assert (idea.status, attempts(idea)) == (IdeaStatus.READY, 0), 'a fault of the host, not an attempt'
        w = s.scalars(select(AgentSession)).one()
        assert w.state is AgentSessionState.KILLED and str(w.error).startswith('worker lost: w2 was silent for')
        assert s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all() == [], 'nothing came back'


def test_a_worker_killed_during_an_evaluation_is_measured_again_on_a_newer_lease_never_from_partial_output(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``kill -9`` of a worker while it evaluates: the evaluation is queued again with a larger fencing token, and is
    measured once, by a live worker."""
    (tmp_path / 'hold').touch()
    set_config(db, campaign, evaluate=held_evaluate(tmp_path))
    ex = control_plane(db, runtime, tmp_path)
    candidate = baseline(db, campaign.id).id
    measuring = in_thread(ex.remeasure, candidate)
    with worker_process(tmp_path, 'w2') as proc:
        wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
        pgid = lease_of(db, 1).handle['pgid']
        assert isinstance(pgid, int)
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
    os.killpg(pgid, signal.SIGKILL)  # the dead worker's evaluator; a restarted worker kills it (``Worker._recover``)
    expire_until(ex, db, 1, LeaseState.EXPIRED)
    (tmp_path / 'hold').unlink()
    # a live worker: its slots count, so core submits the evaluation again, with a larger token, and the worker takes it
    workers_of(ex).worker.start()
    measuring.join(30)
    assert not measuring.is_alive(), 'measured on its new lease'
    assert lease_of(db, 2).state is LeaseState.REPORTED
    with db.session() as s:
        measurements = s.scalars(select(Measurement).where(Measurement.candidate_id == candidate)).all()
        assert len(measurements) == 2 and measurements[-1].error is None, 'the baseline, then one new measurement'
        assert s.scalars(select(Evaluation)).all() == []


def test_a_late_report_of_a_frozen_worker_is_refused_and_kept_as_a_stale_report(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, hold: Path
) -> None:
    """A worker frozen past the toleration (``SIGSTOP``) wakes up, finds its lease gone and stops its job. Its report
    is refused: nothing of it applies, and the commit it brought back stays in the CAS, as a bundle that the kept
    report names, for a person to import."""
    write_toml(Path(campaign.path), COMMIT_THEN_HOLD.format(hold=hold))
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    ex = control_plane(db, runtime, tmp_path)
    with worker_process(tmp_path, 'w2') as proc:
        thread = in_thread(execute, ex, db, run_id)
        wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
        wait_until((tmp_path / 'runs' / str(run_id) / 'worktree' / 'solve.txt').exists)
        freeze(proc, tmp_path / 'piloop.db')
        expire_until(ex, db, 1, LeaseState.EXPIRED)
        thread.join(30)
        os.kill(proc.pid, signal.SIGCONT)

        def stale() -> list[StaleReport]:
            with db.session() as s:
                return [p for _, p in in_order(s, StaleReport)]

        wait_until(stale)
    [payload] = stale()
    assert (payload.lease, payload.worker) == (1, 'w2')
    report = payload.report
    assert isinstance(report, AgentFinished) and report.work is not None
    work = report.work
    run = run_row(db, run_id)
    assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.WORKER_LOST), 'the late report changed nothing'
    with db.session() as s:
        assert s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all() == []
    other = tmp_path / 'other'
    subprocess.run(['git', 'clone', '-q', campaign.path, str(other)], check=True)
    store_commits.unbundle(CAS.selected({}).open(tmp_path), Repo(other), work.blob, work.head)
    assert Repo(other).commit(work.head).summary == 'a line', 'its commit is kept, by the digest of its bundle'


def test_a_report_whose_answer_was_lost_is_sent_again_and_applies_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """A duplicate report: the control plane applied it but its answer was lost, so the worker sends it again with
    its request id. The second gets the first's receipt, and nothing applies twice."""
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST)))
    report = Leases.report
    sent: list[str] = []
    receipts: list[dict[str, object]] = []

    def answer_lost_once(
        self: Leases, lease_id: int, worker: str, request_id: str, result: AgentReport | EvaluationReport
    ) -> dict[str, object]:
        sent.append(request_id)
        receipt = report(self, lease_id, worker, request_id, result)
        receipts.append(receipt)
        if len(sent) == 1:
            raise ConnectionError('the answer was lost')
        return receipt

    mocker.patch.object(Leases, 'report', answer_lost_once)
    candidate = baseline(db, campaign.id).id
    ex.remeasure(candidate)
    wait_until(lambda: len(receipts) == 2)
    assert sent[0] == sent[1], 'sent again with its request id'
    assert receipts[1] == receipts[0] == {'reported': 'evaluation'}, "the first's receipt, not a refusal"
    with db.session() as s:
        assert s.scalars(select(Event).where(has(StaleReport))).all() == [], 'not refused as stale'
        count = s.scalar(select(func.count()).select_from(Measurement).where(Measurement.candidate_id == candidate))
        assert count == 2, 'the baseline and one measurement: applied once'
        recorded = s.scalar(select(func.count()).select_from(Event).where(Event.kind == 'measurement.recorded'))
        assert recorded == 1


def test_a_partitioned_worker_stops_its_job_before_its_lease_expires_and_its_report_is_refused(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    hold: Path,
    mocker: MockerFixture,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    """A partition past the toleration: the worker hears nothing back, so it stops its job before the lease expires,
    and keeps its report. Once it is heard again, the fence refuses the report: a job never runs twice. The agent
    would run for ten minutes: only the worker's own stop ends it."""
    write_toml(Path(campaign.path), COMMIT_THEN_HOLD.format(hold=hold), implementer_timeout=600)
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    ex = serving(RunController(db, tracer, tmp_path, factory(runtime), executor=timed(SLACK)))
    partitioned = threading.Event()
    heartbeat, report = Leases.heartbeat, Leases.report

    def cut(call: Callable[..., object]) -> Callable[..., object]:
        def through(self: Leases, *args: object) -> object:
            if partitioned.is_set():
                raise ConnectionError('no route to the control plane')
            return call(self, *args)

        return through

    mocker.patch.object(Leases, 'heartbeat', cut(heartbeat))
    mocker.patch.object(Leases, 'report', cut(report))
    thread = in_thread(execute, ex, db, run_id)
    wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
    pid_file = tmp_path / 'runs' / str(run_id) / 'agent.pid'
    wait_until(pid_file.exists)  # the lease runs once the agent's process started, before its script wrote the file
    agent_pid = int(pid_file.read_text())
    start = started(agent_pid)
    assert start is not None
    partitioned.set()
    wait_until(lambda: not alive(agent_pid, start), timeout=SLACK.toleration + 30)
    [stopped] = [s for s in spans.get_finished_spans() if s.name == 'worker.job_stopped']
    assert stopped.attributes is not None and stopped.attributes['why'] == 'silent'
    assert stopped.start_time is not None
    expires_at = lease_of(db, 1).expires_at
    assert expires_at is not None
    assert stopped.start_time / 1e9 < expires_at.timestamp(), 'the worker stopped its job before its lease expired'
    expire_until(ex, db, 1, LeaseState.EXPIRED)
    thread.join(30)
    partitioned.clear()

    def stale() -> int:
        with db.session() as s:
            return s.scalar(select(func.count()).select_from(Event).where(has(StaleReport))) or 0

    wait_until(stale)
    run = run_row(db, run_id)
    assert (run.status, run.end) == (RunStatus.FAILED, RunEnd.WORKER_LOST)


def test_a_daemon_killed_during_a_run_keeps_the_lease_and_the_next_daemon_finishes_the_run(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, hold: Path
) -> None:
    """A daemon restart after ``kill -9``: the next daemon renews the lease, its worker re-attaches to the agent, and
    the run ends as if nothing had happened. Nothing is queued again only because of the restart."""
    write_toml(Path(campaign.path), COMMIT_THEN_HOLD.format(hold=hold))
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    ex = control_plane(db, runtime, tmp_path)
    with worker_process(tmp_path, SERVE) as proc:  # the daemon's own worker, in the process that is killed
        thread = in_thread(execute, ex, db, run_id)
        wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
    ex.launcher.handover.set()  # the killed daemon's control plane: its waiting thread goes with it
    db.notify()
    thread.join(30)
    scheduler = next_daemon(db, tmp_path)
    scheduler.reap()
    hold.unlink()
    scheduler.wait()
    run = run_row(db, run_id)
    assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
    with db.session() as s:
        agents = select(Lease).where(Lease.agent_session_id.is_not(None))
        assert [(lease.id, lease.state) for lease in s.scalars(agents)] == [(1, LeaseState.REPORTED)], 'one lease'
        [candidate] = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all()
        assert candidate.summary == 'a line'
        assert (
            s.scalars(select(Event.kind).where(Event.target_kind == TargetKind.AGENT_SESSION))
            .all()
            .count('agent_session.finished')
            == 1
        )


def test_a_real_daemon_killed_during_a_run_is_followed_by_one_that_finishes_it_on_the_same_lease(
    campaign_path: Path, tmp_path: Path, schema: Path
) -> None:
    """``kill -9`` of ``piloop serve``: its worker dies with it, the agent does not. The next daemon renews the lease,
    re-attaches, and the run succeeds on lease 1."""
    cfg = Settings(home=tmp_path / 'home', tick_seconds=0.05)
    hold = tmp_path / 'hold'
    hold.touch()
    write_toml(campaign_path, COMMIT_THEN_HOLD.format(hold=hold), implementer_timeout=600)
    at_head(schema, cfg.home / 'piloop.db')
    db = Database(cfg.database_url, actor=cfg.actor)
    campaign = register(db, campaign_path)
    with db.session() as s:
        s.add(Idea(track='score', campaign_id=campaign.id, title='one line', hypothesis='h', created_by='t'))
        s.commit()
    env = env_for(cfg.home) | {'PILOOP_TICK_SECONDS': '0.05'}
    daemons: list[subprocess.Popen[bytes]] = []
    try:
        with (tmp_path / 'first.log').open('w') as log:
            daemons.append(subprocess.Popen(SERVE_ARGV, stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=env))
        wait_until(lambda: state(db, 1) is LeaseState.RUNNING, timeout=60)
        wait_until((cfg.home / 'runs' / '1' / 'agent.pid').is_file)
        daemons[0].send_signal(signal.SIGKILL)
        daemons[0].wait()
        with (tmp_path / 'second.log').open('w') as log:
            daemons.append(subprocess.Popen(SERVE_ARGV, stdin=subprocess.DEVNULL, stdout=log, stderr=log, env=env))
        wait_until(lambda: 'agent_session.reattached' in (tmp_path / 'second.log').read_text(), timeout=60)
        hold.unlink()
        wait_until(
            lambda: run_row(db, RunId(1)).status is RunStatus.SUCCEEDED and not run_row(db, RunId(1)).scoring,
            timeout=60,
        )
        with db.session() as s:
            agents = select(Lease).where(Lease.agent_session_id.is_not(None))
            assert [(lease.id, lease.state) for lease in s.scalars(agents)] == [(1, LeaseState.REPORTED)]
            assert s.scalars(select(AgentSession.state)).all() == [AgentSessionState.EXITED]
    finally:
        for daemon in daemons:
            daemon.kill()
            daemon.wait()
        reap(cfg.home)
        db.close()


@pytest.mark.parametrize('agent', ['run', 'sweep'])
def test_an_agent_that_a_daemon_before_leases_started_is_adopted_and_its_work_scored(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, agent: str, mocker: MockerFixture
) -> None:
    """An upgrade hands over to a daemon with leases: each running agent without one gets one, keeps running, and its
    run is scored. A run's implementer and a sweep."""
    hold = tmp_path / 'hold'
    hold.touch()
    write_toml(Path(campaign.path), COMMIT_THEN_HOLD.format(hold=hold).replace('../agent.pid', f'{tmp_path}/agent.pid'))
    reload_campaign(db, campaign)
    first = make_scheduler(db, runtime, tmp_path)
    if agent == 'run':
        add_ideas(db, campaign, 'one line')
    else:
        request_watchdog(db, campaign)
    assert first.tick() == [1]
    wait_until((tmp_path / 'agent.pid').exists)
    first.hand_over(timeout=5)
    with db.session() as s:  # what a daemon before leases leaves
        s.execute(delete(Lease))
        s.commit()
    second = next_daemon(db, tmp_path)
    sources = second._controller.sources
    snapshot = sources.source
    blocked: list[str] = []

    def source(campaign: Campaign, revision: Revision) -> Source:
        """Store the links, as the adoption does, while a command writes: a write lock held here would block it."""
        with closing(sqlite3.connect(db.engine.url.database or '', timeout=0)) as con:
            try:
                con.execute("UPDATE campaigns SET updated_by = 'human'")
                con.commit()
            except sqlite3.OperationalError as e:
                blocked.append(str(e))
        return snapshot(campaign, revision)

    mocker.patch.object(sources, 'source', source)
    second.reap()
    assert blocked == [], 'the reap stores the linked inputs of an adopted agent without holding the write lock'
    with db.session() as s:
        [adopted] = s.scalars(select(Lease)).all()
        assert (adopted.agent_session_id, adopted.state, adopted.worker) == (1, LeaseState.RUNNING, SERVE)
    hold.unlink()
    second.wait()
    with db.session() as s:
        assert s.get_one(AgentSession, 1).state is AgentSessionState.EXITED
        assert s.get_one(Lease, adopted.id).state is LeaseState.REPORTED
        if agent == 'run':
            [candidate] = s.scalars(select(Candidate).where(Candidate.run_id == 1)).all()
            assert candidate.summary == 'a line', 'its work is scored'


def test_a_stop_ends_an_agent_lease_that_no_worker_took_yet(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, campaign)
    ex = control_plane(db, runtime, tmp_path)
    thread = in_thread(execute, ex, db, run_id)
    wait_until(lambda: state(db, 1) is LeaseState.QUEUED)
    ex.stop(30)
    thread.join(30)
    assert (lease_of(db, 1).state, lease_of(db, 1).receipt) == (LeaseState.CANCELLED, {'cancelled': NOT_TAKEN})
    assert run_row(db, run_id).status is RunStatus.RUNNING, 'the stop writes the run, not its thread'


def test_a_running_agent_without_a_lease_or_a_worktree_is_not_adopted_and_fails_as_stale(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    run_id = make_run(db, campaign)
    with db.session() as s:
        w = s.scalars(select(AgentSession)).one()
        handle: dict[str, object] = {'pid': 4242}
        w.state, w.runtime, w.handle, w.log_dir = AgentSessionState.RUNNING, 'subprocess', handle, 'x'
        s.get_one(Run, run_id).status = RunStatus.RUNNING
        s.commit()
    make_scheduler(db, runtime, tmp_path).reap()
    with db.session() as s:
        assert s.scalars(select(Lease)).all() == [], 'its worktree is gone: nothing to adopt'
        assert (s.get_one(AgentSession, 1).state, s.get_one(Run, run_id).end) == (
            AgentSessionState.KILLED,
            RunEnd.STALE,
        )


def test_the_daemons_worker_killed_during_an_evaluation_kills_its_evaluator_when_it_starts_again(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """``kill -9`` of the daemon's worker while it evaluates leaves the evaluator running. The next one finds the lease,
    kills the evaluator by its process group, and the evaluation is queued again and measured once."""
    (tmp_path / 'hold').touch()
    set_config(db, campaign, evaluate=held_evaluate(tmp_path))
    ex = control_plane(db, runtime, tmp_path)
    candidate = baseline(db, campaign.id).id
    measuring = in_thread(ex.remeasure, candidate)
    with worker_process(tmp_path, SERVE) as proc:
        wait_until(lambda: state(db, 1) is LeaseState.RUNNING)
        handle = lease_of(db, 1).handle
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()
    pgid, start = handle['pgid'], handle['start_time']
    assert isinstance(pgid, int) and isinstance(start, float) and alive(pgid, start), 'the evaluator outlived it'
    workers_of(ex).worker.start()
    wait_until(lambda: not alive(pgid, start))
    (tmp_path / 'hold').unlink()
    measuring.join(30)
    assert not measuring.is_alive(), 'measured on its new lease'
    assert [lease_of(db, 1).state, lease_of(db, 2).state] == [LeaseState.EXPIRED, LeaseState.REPORTED]
    with db.session() as s:
        assert len(s.scalars(select(Measurement).where(Measurement.candidate_id == candidate)).all()) == 2
    assert list(tmp_path.glob('projects/*/measure/*/lease-*/worktree')) == [], 'each lease removed its worktree'
