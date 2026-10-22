"""The runtime contract behind the worker: every runtime runs an agent's job on a lease the same way.

Each case runs on ``subprocess`` (a real process) and on ``herdr`` (the real runtime against the fake ``herdr`` CLI):
an agent that ends, one that runs out of time, a start that fails, and a worker that restarts and re-attaches.
"""

import json
import shutil
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import psutil
import pytest
from git import Repo
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pytest_mock import MockerFixture
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError

from piloop import git
from piloop.agents.results import CampaignResults, DaemonStopped, Ended, Lost, NotStarted, Ran
from piloop.daemon.run_controller import RunController
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Candidate,
    Evaluation,
    Event,
    Lease,
    LeaseState,
    Measurement,
    Run,
    RunStatus,
    utcnow,
)
from piloop.db.session import Database
from piloop.ids import IMPLEMENTER, AgentSessionId, CommentId
from piloop.otel import Tracer
from piloop.plugins.executor.contract import (
    AgentJob,
    AgentKey,
    AgentReport,
    Checkout,
    Commits,
    EvaluationReport,
    GitTree,
    Prompt,
)
from piloop.plugins.executor.kit.capacity import HostSample, sample
from piloop.plugins.executor.kit.leases import (
    SERVE,
    STALE_REPORT,
    Grant,
    Leases,
    PromptMessage,
    StaleLease,
    Timing,
    cancel,
    enqueue,
    revoke,
)
from piloop.plugins.executor.workers.worker import WORKER_ANOMALY, Unsent, Worker
from piloop.plugins.runtime.contract import ExitStatus, Handle
from piloop.plugins.runtime.herdr import HerdrSettings
from piloop.runtime.factory import RuntimeFactory, make_runtime
from tests.conftest import Gate, ScriptRuntime, baseline, factory, serving, timed, wait_until, workers_of, write_toml
from tests.daemon.test_evaluation_queue import held_evaluate
from tests.daemon.test_run_controller import execute, make_run, reload_campaign, set_config
from tests.test_executors import Steerable
from tests.test_lease import agent_job, evaluation_job

NO_JOBS = {'error': {'code': 'pane_not_found', 'message': 'gone'}}
"""What the fake ``herdr`` answers for the agent's jobs: none, so an idle agent is finished."""
HOLD = '#!/bin/sh\nwhile [ -f {hold} ]; do sleep 0.05; done\n'


@dataclass(frozen=True)
class Contract:
    """One runtime under the contract: how to make it, and how to make it fail a start or time out."""

    kind: str
    runtime_factory: RuntimeFactory
    monkeypatch: pytest.MonkeyPatch

    def script(self, **responses: list[dict[str, object]]) -> None:
        """Answer the fake ``herdr`` CLI's calls with ``responses``. Nothing for the subprocess runtime."""
        if self.kind == 'herdr':
            scripts = {'pane process-info': [NO_JOBS], **{k.replace('_', ' '): v for k, v in responses.items()}}
            self.monkeypatch.setenv('FAKE_HERDR_SCRIPT', json.dumps(scripts))


@pytest.fixture(params=['subprocess', 'herdr'])
def contract(
    request: pytest.FixtureRequest, runtime: ScriptRuntime, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[Contract]:
    if request.param == 'herdr':
        request.getfixturevalue('herdr_log')
        monkeypatch.setattr('piloop.plugins.runtime.herdr.QUIET_SECONDS', 0)
        # the fake's agent is idle at once: past the grace, that is the end of its turn, not a slow start
        monkeypatch.setattr('piloop.plugins.runtime.herdr.herdr_settings', HerdrSettings(startup_grace_seconds=0.05))
        herdr = Contract('herdr', make_runtime, monkeypatch)
        herdr.script()
        yield herdr
    else:
        yield Contract('subprocess', factory(runtime), monkeypatch)


def use(db: Database, campaign: Campaign, contract: Contract, body: str, timeout: float = 20) -> None:
    write_toml(Path(campaign.path), body, implementer_timeout=timeout)
    reload_campaign(db, campaign)
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        row.config = {**row.config, 'runtime': contract.kind}
        s.commit()


def request_agent(db: Database, campaign: Campaign) -> AgentSessionId:
    with db.session() as s:
        w = AgentSession(kind=IMPLEMENTER, campaign_id=campaign.id, reason='contract')
        s.add(w)
        s.commit()
        return w.id


def launch(ex: RunController, agent_session_id: AgentSessionId, tmp_path: Path) -> Ended:
    cwd = tmp_path / 'cwd'
    cwd.mkdir(exist_ok=True)
    return ex.launcher.launch(
        agent_session_id, brief='b', cwd=cwd, log_dir=tmp_path / 'logs', artifact=Checkout(path=cwd)
    )


def lease_state(db: Database, agent_session_id: AgentSessionId) -> LeaseState | None:
    with db.session() as s:
        lease = s.scalars(select(Lease).where(Lease.agent_session_id == agent_session_id)).first()
        return lease.state if lease is not None else None


def lease_states(db: Database) -> list[LeaseState]:
    with db.session() as s:
        return list(s.scalars(select(Lease.state).order_by(Lease.id)))


def rows(db: Database, agent_session_id: AgentSessionId) -> tuple[AgentSession, Lease]:
    with db.session() as s:
        lease = s.scalars(select(Lease).where(Lease.agent_session_id == agent_session_id)).one()
        return s.get_one(AgentSession, agent_session_id), lease


def test_an_agent_that_ends_is_reported_on_its_lease(
    db: Database, campaign: Campaign, contract: Contract, tmp_path: Path
) -> None:
    use(db, campaign, contract, '#!/bin/sh\necho done\n')
    ex = serving(RunController(db, Tracer(None), tmp_path, contract.runtime_factory))
    agent_session_id = request_agent(db, campaign)
    outcome = launch(ex, agent_session_id, tmp_path)
    assert isinstance(outcome, Ran)
    assert (outcome.process.status, outcome.process.error) == (ExitStatus.EXITED, None)
    w, lease = rows(db, agent_session_id)
    assert (w.state, w.runtime, lease.state, lease.worker) == (
        AgentSessionState.EXITED,
        contract.kind,
        LeaseState.REPORTED,
        'serve',
    )
    assert lease.handle['runtime'] == contract.kind and w.handle == lease.handle['data'], 'steered by its handle'
    assert (tmp_path / 'logs' / 'transcript.txt').is_file() and (tmp_path / 'logs' / 'brief.md').read_text() == 'b'


def test_an_agent_that_runs_out_of_time_is_reported_timed_out(
    db: Database, campaign: Campaign, contract: Contract, tmp_path: Path
) -> None:
    use(db, campaign, contract, '#!/bin/sh\nsleep 30\n', timeout=0.5)
    contract.script(agent_wait=[{'error': {'code': 'timeout', 'message': 'timed out'}}])
    ex = serving(RunController(db, Tracer(None), tmp_path, contract.runtime_factory))
    agent_session_id = request_agent(db, campaign)
    outcome = launch(ex, agent_session_id, tmp_path)
    assert isinstance(outcome, Ran)
    assert (outcome.process.status, outcome.process.error) == (ExitStatus.TIMED_OUT, 'agent timeout after 0.5s')
    w, lease = rows(db, agent_session_id)
    assert (w.state, lease.state) == (AgentSessionState.KILLED, LeaseState.REPORTED)


def test_a_start_that_fails_is_reported_as_a_failed_start(
    db: Database, campaign: Campaign, contract: Contract, tmp_path: Path, runtime: ScriptRuntime, mocker: MockerFixture
) -> None:
    use(db, campaign, contract, '#!/bin/sh\necho done\n')
    contract.script(agent_start=[{'error': {'code': 'spawn_failed', 'message': 'no such harness'}}])
    if contract.kind == 'subprocess':
        mocker.patch.object(runtime, 'start', side_effect=RuntimeError('spawn_failed: no such harness'))
    ex = serving(RunController(db, Tracer(None), tmp_path, contract.runtime_factory))
    agent_session_id = request_agent(db, campaign)
    outcome = launch(ex, agent_session_id, tmp_path)
    assert isinstance(outcome, NotStarted) and 'spawn_failed' in str(outcome.reason)
    w, lease = rows(db, agent_session_id)
    assert (w.state, lease.state, w.handle) == (AgentSessionState.KILLED, LeaseState.REPORTED, {})


def test_a_restarted_worker_re_attaches_to_its_agent_and_reports_it(
    db: Database, campaign: Campaign, contract: Contract, tmp_path: Path
) -> None:
    """The worker of an in-place restart leaves its agent running. The next one finds it on its lease, by its handle."""
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, contract, HOLD.format(hold=hold))
    contract.script(agent_wait=[{'result': {'type': 'agent_info', 'agent': {'agent_status': 'working'}}}])
    first = serving(RunController(db, Tracer(None), tmp_path, contract.runtime_factory))
    agent_session_id = request_agent(db, campaign)
    launching = threading.Thread(target=launch, args=(first, agent_session_id, tmp_path))
    launching.start()
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.RUNNING)
    first.hand_over()
    launching.join(30)
    second = serving(RunController(db, Tracer(None), tmp_path, contract.runtime_factory))
    workers_of(second).leases.renew_all()
    workers_of(second).worker.start()
    contract.script()  # herdr's agent is now idle
    hold.unlink()
    outcome = second.launcher.reattach(agent_session_id)
    assert isinstance(outcome, Ran)
    assert (outcome.process.status, outcome.process.error) == (ExitStatus.EXITED, None)
    w, lease = rows(db, agent_session_id)
    assert (w.state, lease.id, lease.state) == (AgentSessionState.EXITED, 1, LeaseState.REPORTED), 'the same lease'


def test_a_revoked_agent_is_stopped_by_its_worker_and_still_reported(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A kill is a revoke: the worker stops the agent and reports it, so its run keeps what it made."""
    use(db, campaign, Contract('subprocess', factory(runtime), pytest.MonkeyPatch()), '#!/bin/sh\nsleep 30\n')
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    agent_session_id = request_agent(db, campaign)
    outcomes: list[Ended] = []
    launching = threading.Thread(target=lambda: outcomes.append(launch(ex, agent_session_id, tmp_path)))
    launching.start()
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.RUNNING)
    workers_of(ex).leases.revoke(AgentKey(agent_session_id))  # a ``Revoke`` message: the worker stops it at once
    launching.join(30)
    [outcome] = outcomes
    assert isinstance(outcome, Ran)
    assert runtime.killed and outcome.process.exit_code is not None and outcome.process.exit_code < 0, (
        'killed by the worker'
    )
    assert rows(db, agent_session_id)[1].state is LeaseState.REPORTED


def test_a_worker_whose_own_code_fails_loses_the_job_so_that_nothing_waits_for_ever(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    use(db, campaign, Contract('subprocess', factory(runtime), pytest.MonkeyPatch()), '#!/bin/sh\necho done\n')
    mocker.patch('piloop.plugins.executor.workers.worker.Worker._agent', side_effect=RuntimeError('a bug'))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    agent_session_id = request_agent(db, campaign)
    outcome = launch(ex, agent_session_id, tmp_path)
    assert outcome == Lost('worker lost: RuntimeError: a bug')
    assert rows(db, agent_session_id)[1].state is LeaseState.EXPIRED
    workers_of(ex).worker._lost(
        99, 'nothing'
    )  # a lease that is no longer the worker's: refused, and the worker goes on


def test_a_restarted_worker_reports_an_agent_whose_worktree_is_gone(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, Contract('subprocess', factory(runtime), pytest.MonkeyPatch()), HOLD.format(hold=hold))
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    agent_session_id = request_agent(db, campaign)
    launching = threading.Thread(target=launch, args=(first, agent_session_id, tmp_path))
    launching.start()
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.RUNNING)
    first.hand_over()
    launching.join(30)
    shutil.rmtree(tmp_path / 'cwd')
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    workers_of(second).worker.start()
    outcome = second.launcher.reattach(agent_session_id)
    assert outcome == Lost('worker lost: the worker restarted and could not find its agent')
    hold.unlink()


def test_a_hand_over_twice_leaves_a_re_attached_agent_to_the_third_daemon(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, Contract('subprocess', factory(runtime), pytest.MonkeyPatch()), HOLD.format(hold=hold))
    agent_session_id = request_agent(db, campaign)
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    threading.Thread(target=launch, args=(first, agent_session_id, tmp_path)).start()
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.RUNNING)
    first.hand_over()
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    workers_of(second).worker.start()
    wait_until(lambda: workers_of(second).worker._jobs)  # it re-attached
    second.hand_over()
    third = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    workers_of(third).worker.start()
    hold.unlink()
    outcome = third.launcher.reattach(agent_session_id)
    assert isinstance(outcome, Ran) and outcome.process.error is None
    assert rows(db, agent_session_id)[1].state is LeaseState.REPORTED


def test_an_agent_whose_lease_ends_while_it_starts_is_ended_and_not_reported(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A daemon stop ended the lease and closed the row while the runtime started the agent: nothing waits for it, so
    it is killed."""
    use(db, campaign, Contract('subprocess', factory(runtime), pytest.MonkeyPatch()), '#!/bin/sh\nsleep 30\n')
    gate = Gate()
    monkeypatch.setattr(runtime, 'start', gate.wrap(runtime.start))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    agent_session_id = request_agent(db, campaign)
    outcomes: list[Ended] = []
    launching = threading.Thread(target=lambda: outcomes.append(launch(ex, agent_session_id, tmp_path)))
    launching.start()
    assert gate.entered.wait(10)
    with db.session() as s:  # as a stop does for a job that did not report in time
        cancel(s, s.scalars(select(Lease)).one(), 'daemon stopped')
        w = s.get_one(AgentSession, agent_session_id)
        w.state, w.ended_at = AgentSessionState.KILLED, utcnow()
        s.commit()
    gate.release.set()
    launching.join(30)
    wait_until(lambda: runtime.killed)
    assert outcomes[0] == DaemonStopped() and rows(db, agent_session_id)[1].state is LeaseState.CANCELLED


def test_an_agent_that_removes_its_own_worktree_is_still_reported(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    use(db, campaign, Contract('subprocess', factory(runtime), pytest.MonkeyPatch()), '#!/bin/sh\nrm -rf "$PWD"\n')
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    agent_session_id = request_agent(db, campaign)
    with db.session() as s:
        base = s.get_one(Campaign, campaign.id)
        repo, commit = Path(base.path), Repo(base.path).head.commit.hexsha
    outcome = ex.launcher.launch(
        agent_session_id,
        brief='b',
        cwd=tmp_path / 'wt',
        log_dir=tmp_path / 'logs',
        artifact=GitTree(repo=repo, commit=commit),
        commits=Commits(message='leftovers', ref='refs/piloop/agents/1'),
    )
    assert isinstance(outcome, Ran) and outcome.process.error is not None and outcome.head is None, (
        'the report says what is known'
    )
    assert rows(db, agent_session_id)[1].state is LeaseState.REPORTED


def test_an_evaluation_at_a_hand_over_is_stopped_and_measured_again_by_the_next_worker(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The next worker cannot watch an evaluator again: it finds the lease, reports it gone, and it is queued again."""
    (tmp_path / 'hold').touch()
    set_config(db, campaign, evaluate=held_evaluate(tmp_path))
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    candidate = baseline(db, campaign.id).id
    threading.Thread(target=first.remeasure, args=(candidate,)).start()
    wait_until(lambda: lease_states(db) == [LeaseState.RUNNING])
    first.hand_over()
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    with db.session() as s:  # the next daemon's reap: the queue starts empty
        s.execute(delete(Evaluation))
        s.commit()
    (tmp_path / 'hold').unlink()
    workers_of(second).worker.start()
    measuring = threading.Thread(target=second.remeasure, args=(candidate,))  # the next daemon's resumed scoring
    measuring.start()
    measuring.join(30)
    assert not measuring.is_alive(), 'measured on its new lease'
    workers_of(second).worker.join(10)  # the first lease is reported gone by a thread of its own
    with db.session() as s:
        assert [lease.state for lease in s.scalars(select(Lease).order_by(Lease.id))] == [
            LeaseState.EXPIRED,
            LeaseState.REPORTED,
        ]
        assert len(s.get_one(Candidate, candidate).measurements) == 2


def test_an_evaluation_whose_lease_expired_before_its_evaluator_started_kills_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    gate = Gate()
    monkeypatch.setattr('piloop.plugins.executor.kit.jobs.git.add_worktree', gate.wrap(git.add_worktree))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    measuring = threading.Thread(target=ex.remeasure, args=(baseline(db, campaign.id).id,))
    measuring.start()
    assert gate.entered.wait(10)
    workers_of(ex).leases.expire(utcnow() + timedelta(seconds=workers_of(ex).leases.timing.toleration + 1))
    gate.release.set()
    measuring.join(30)
    assert not measuring.is_alive(), 'measured on its new lease'

    def stale() -> list[object]:
        with db.session() as s:
            return [e.payload['lease'] for e in s.scalars(select(Event).where(Event.kind == STALE_REPORT))]

    wait_until(stale)
    assert stale() == [1], 'its evaluator was killed at once; its report is refused'


def test_a_stale_evaluation_never_touches_the_files_of_the_lease_that_replaced_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Lease 1's job makes its worktree only after lease 2 replaced it and runs: each keeps a tree of its own."""
    (tmp_path / 'hold').touch()
    set_config(db, campaign, evaluate=held_evaluate(tmp_path))
    entered, release = threading.Event(), threading.Event()
    add_worktree = git.add_worktree

    def first_held(repo: Repo, path: Path, sha: str, links: dict[str, Path]) -> Repo:
        if not entered.is_set():
            entered.set()
            release.wait(10)
        return add_worktree(repo, path, sha, links)

    monkeypatch.setattr('piloop.plugins.executor.kit.jobs.git.add_worktree', first_held)
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    candidate = baseline(db, campaign.id).id
    measuring = threading.Thread(target=ex.remeasure, args=(candidate,))
    measuring.start()
    assert entered.wait(10)
    workers_of(ex).leases.expire(utcnow() + timedelta(seconds=workers_of(ex).leases.timing.toleration + 1))
    wait_until(lambda: lease_states(db) == [LeaseState.EXPIRED, LeaseState.RUNNING])
    with db.session() as s:
        pgid = s.get_one(Lease, 2).handle['pgid']
    assert isinstance(pgid, int)
    tree = Path(psutil.Process(pgid).cwd())
    release.set()

    def stale() -> list[object]:
        with db.session() as s:
            return [e.payload['lease'] for e in s.scalars(select(Event).where(Event.kind == STALE_REPORT))]

    wait_until(stale)
    assert tree.is_dir(), 'the stale job left the tree of the lease that replaced it'
    (tmp_path / 'hold').unlink()
    measuring.join(30)
    assert not measuring.is_alive()
    with db.session() as s:
        measurement = s.scalars(select(Measurement).order_by(Measurement.id.desc()).limit(1)).one()
        assert (measurement.candidate_id, measurement.error) == (candidate, None)


FAST = Timing(renew=0.1, grace=0.3, toleration=1.0)


def renewed_while(ex: RunController, db: Database, agent_session_id: AgentSessionId, seconds: float) -> None:
    """Tick the scheduler's expiry for ``seconds``, and check that the agent's lease stays live meanwhile."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        workers_of(ex).leases.expire()
        assert lease_state(db, agent_session_id) is LeaseState.RUNNING, 'its heartbeat renewed it'
        time.sleep(0.05)


@pytest.fixture
def launched(db: Database) -> Iterator[list[threading.Thread]]:
    """The threads that launch agents in the test: they end, as their agents do, before the database closes."""
    threads: list[threading.Thread] = []
    yield threads
    for thread in threads:
        thread.join(30)


def launching(
    db: Database, campaign: Campaign, ex: RunController, tmp_path: Path, name: str, threads: list[threading.Thread]
) -> AgentSessionId:
    """Launch an agent on a thread, and return its row."""
    agent_session_id = request_agent(db, campaign)
    cwd = tmp_path / name
    cwd.mkdir()
    thread = threading.Thread(
        target=ex.launcher.launch,
        args=(agent_session_id,),
        kwargs={'brief': 'b', 'cwd': cwd, 'log_dir': tmp_path / f'{name}-logs', 'artifact': Checkout(path=cwd)},
    )
    threads.append(thread)
    thread.start()
    return agent_session_id


def running_agent(
    db: Database, campaign: Campaign, ex: RunController, tmp_path: Path, name: str, threads: list[threading.Thread]
) -> AgentSessionId:
    """Launch an agent on a thread, and return its row once it runs."""
    agent_session_id = launching(db, campaign, ex, tmp_path, name, threads)
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.RUNNING)
    return agent_session_id


def test_a_slow_start_holds_back_no_heartbeat(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    launched: list[threading.Thread],
) -> None:
    """A Herdr start can take minutes; meanwhile the worker's other agents are renewed."""
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, Contract('subprocess', factory(runtime), monkeypatch), HOLD.format(hold=hold), timeout=600)
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST)))
    first = running_agent(db, campaign, ex, tmp_path, 'first', launched)
    start = runtime.start
    monkeypatch.setattr(runtime, 'start', lambda spec: time.sleep(2) or start(spec))
    launching(db, campaign, ex, tmp_path, 'second', launched)
    renewed_while(ex, db, first, 2.5)
    hold.unlink()


def test_a_failing_turn_never_ends_the_loop_and_a_failing_streak_is_an_anomaly(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    launched: list[threading.Thread],
) -> None:
    hold = tmp_path / 'hold'
    hold.touch()
    use(
        db,
        campaign,
        Contract('subprocess', factory(runtime), pytest.MonkeyPatch()),
        HOLD.format(hold=hold),
        timeout=600,
    )
    ex = serving(RunController(db, tracer, tmp_path, factory(runtime), executor=timed(FAST)))
    agent_session_id = running_agent(db, campaign, ex, tmp_path, 'held', launched)
    mocker.patch.object(
        workers_of(ex).leases, 'acquire', side_effect=OperationalError('SELECT', {}, Exception('database is locked'))
    )
    renewed_while(ex, db, agent_session_id, 2.5)
    names = [s.name for s in spans.get_finished_spans()]
    assert 'worker.turn_failed' in names and names.count(WORKER_ANOMALY) == 1, 'one anomaly for one failing streak'
    hold.unlink()


def test_a_kill_that_fails_is_tried_again_on_the_next_heartbeat(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, launched: list[threading.Thread]
) -> None:
    """A revoke whose kill fails (a Herdr server that does not answer) is not forgotten: the next answer asks again."""

    class Flaky(ScriptRuntime):
        def kill(self, handle: Handle) -> None:
            if not self.killed:
                self.killed.append(handle)
                raise RuntimeError('server_unavailable')
            super().kill(handle)

    runtime = Flaky()
    use(db, campaign, Contract('subprocess', factory(runtime), monkeypatch), '#!/bin/sh\nsleep 600\n', timeout=600)
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST)))
    agent_session_id = running_agent(db, campaign, ex, tmp_path, 'stubborn', launched)
    with db.session() as s:  # no message: each heartbeat's ``REVOKED`` asks again
        revoke(s, s.scalars(select(Lease)).one())
        s.commit()
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.REPORTED, timeout=20)
    assert len(runtime.killed) >= 2


def test_a_worker_killed_after_its_agent_ended_but_before_it_reported_reports_on_its_restart(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """``kill -9`` between the end of the agent and its report: the worktree and the pinned commits are still there,
    so the next worker reports them, and the run is scored."""
    write_toml(Path(campaign.path), '#!/bin/sh\necho line >> solve.txt\ngit commit -qam "a line"\n')
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    send = Worker._send

    def gone_before_the_report(self: Worker, lease_id: int, unsent: Unsent) -> None:
        if self is workers_of(first).worker:  # the process goes here: nothing after this line runs in it
            first.hand_over()
            return
        send(self, lease_id, unsent)

    mocker.patch.object(Worker, '_send', gone_before_the_report)
    thread = threading.Thread(target=execute, args=(first, db, run_id))
    thread.start()
    thread.join(30)
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    workers_of(second).leases.renew_all()
    workers_of(second).worker.start()
    with db.session() as s:
        agent_session_id = s.scalars(select(AgentSession.id)).one()
    second.reattach(run_id, agent_session_id)
    with db.session() as s:
        run = s.get_one(Run, run_id)
        assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
        [candidate] = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all()
        assert candidate.summary == 'a line'


def test_a_worker_killed_between_the_start_and_its_record_leaves_no_worktree(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The lease is acquired and the worktree made, but the start was never recorded: the next worker removes it."""
    agent_session_id = request_agent(db, campaign)
    with db.session() as s:
        base = s.get_one(Campaign, campaign.id)
        repo, commit = Path(base.path), Repo(base.path).head.commit.hexsha
    cwd = tmp_path / 'wt'
    job = AgentJob(
        agent_session=agent_session_id,
        runtime='subprocess',
        campaign=campaign.name,
        campaign_path=repo,
        artifact=GitTree(repo=repo, commit=commit),
        cwd=cwd,
        log_dir=tmp_path / 'logs',
        timeout=60,
        brief=None,
        spec=None,
    )
    git.add_worktree(git.open_repo(repo), cwd, commit, {})
    with db.session() as s:
        enqueue(s, job)
        s.commit()
    results = CampaignResults(db, Tracer(None), tmp_path / 'units', lambda: False)
    killed = Leases(db, results)  # the killed worker's lease, which no worker watches
    killed.register(SERVE, SERVE, '0.2.0', [], sample(tmp_path))
    killed.acquire(SERVE, sample(tmp_path))
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    workers_of(second).worker.start()
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.EXPIRED)
    workers_of(second).worker.join(10)
    assert not cwd.exists() and git.worktrees_under(git.open_repo(repo), cwd) == []


def test_a_worker_that_cannot_register_at_its_start_registers_on_its_next_turn(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    ex = RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST))
    register = mocker.patch.object(
        workers_of(ex).leases, 'register', side_effect=[OperationalError('SELECT', {}, Exception('locked')), []]
    )
    workers_of(ex).worker.start()
    wait_until(lambda: register.call_count == 2)
    assert workers_of(ex).worker.id == 'serve'


def test_an_evaluator_whose_start_is_refused_by_the_fence_is_killed_at_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """The fence refused the record of the evaluator's start: its lease is no longer the worker's, so it stops."""
    (tmp_path / 'hold').touch()
    set_config(db, campaign, evaluate=held_evaluate(tmp_path))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    mocker.patch.object(workers_of(ex).leases, 'started', side_effect=StaleLease('stale_lease: lease#1 was replaced'))
    measuring = threading.Thread(target=ex.remeasure, args=(baseline(db, campaign.id).id,))
    measuring.start()
    measuring.join(30)
    assert not measuring.is_alive(), 'its evaluator was killed, though the hold is still there'


def test_a_report_that_cannot_be_sent_keeps_the_worktree_and_a_restarted_worker_scores_the_run(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """The database stays locked past the busy timeout: the report waits in memory, so the worktree must outlive it.

    The worker sends the report again on each heartbeat. Killed meanwhile, its successor finds the agent ended in
    its worktree, and reports it: the run is scored, not lost.
    """
    write_toml(Path(campaign.path), '#!/bin/sh\necho line >> solve.txt\ngit commit -qam "a line"\n')
    reload_campaign(db, campaign)
    run_id = make_run(db, campaign)
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    report = Leases.report
    tried: list[int] = []

    def locked_for_the_first(
        self: Leases, lease_id: int, worker: str, request_id: str, result: AgentReport | EvaluationReport
    ) -> dict[str, object]:
        if self is workers_of(first).leases:
            tried.append(lease_id)
            raise OperationalError('BEGIN IMMEDIATE', {}, Exception('database is locked'))
        return report(self, lease_id, worker, request_id, result)

    mocker.patch.object(Leases, 'report', locked_for_the_first)
    thread = threading.Thread(target=execute, args=(first, db, run_id))
    thread.start()
    wait_until(lambda: len(tried) >= 2)  # sent again on a later turn
    worktree = tmp_path / 'runs' / str(run_id) / 'worktree'
    assert worktree.is_dir(), 'the worktree outlives a report that is not out'
    first.hand_over()  # the process goes
    thread.join(30)
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    workers_of(second).leases.renew_all()
    workers_of(second).worker.start()
    with db.session() as s:
        agent_session_id = s.scalars(select(AgentSession.id)).one()
    second.reattach(run_id, agent_session_id)
    with db.session() as s:
        run = s.get_one(Run, run_id)
        assert (run.status, run.error) == (RunStatus.SUCCEEDED, None)
        [candidate] = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all()
        assert candidate.summary == 'a line'
    workers_of(second).worker.join(10)
    assert not worktree.exists(), 'removed once the report is out'


def test_a_stop_ends_a_queued_agent_lease_whose_row_is_settled_and_a_worker_starts_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The stop ends the lease all the same: core refused the stopped result of a row that was closed already."""
    ex = RunController(db, Tracer(None), tmp_path, factory(runtime))  # not started: no turn may take the lease
    worker = workers_of(ex).worker
    worker.join(0)  # nothing started yet: no loop to wait for
    job = agent_job(db, campaign, tmp_path)  # its row is requested, not live: settled for core
    with db.session() as s:
        lease_id = enqueue(s, job).id
        s.commit()
    ex.stop(5)
    with db.session() as s:
        assert s.get_one(Lease, lease_id).state is LeaseState.CANCELLED
    worker.start()
    worker.start()  # a later call only wakes it
    worker.join(5)


def test_a_hand_over_of_a_worker_that_never_turned_has_no_loop_to_wait_for(
    db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    workers_of(RunController(db, Tracer(None), tmp_path, factory(runtime))).worker.hand_over()


def test_a_join_while_a_job_thread_starts_waits_for_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``stop`` joins the job threads while the loop may spawn one: it never finds a thread that is not started."""
    worker = workers_of(RunController(db, Tracer(None), tmp_path, factory(runtime))).worker
    errors: list[Exception] = []
    start = threading.Thread.start

    def join() -> None:
        try:
            worker.join(5)
        except Exception as e:
            errors.append(e)

    def joined_while_it_starts(thread: threading.Thread) -> None:
        if thread.name == 'agent-1':
            joining = threading.Thread(target=join)
            start(joining)
            joining.join(0.2)  # the join of a stop, at the moment the job's thread is registered and not started
        start(thread)

    monkeypatch.setattr(threading.Thread, 'start', joined_while_it_starts)
    worker._spawn(Grant(lease=1, job=agent_job(db, campaign, tmp_path)), lambda: None)
    worker.join(5)
    assert errors == []


def test_prompts_read_together_reach_the_agent_in_one_turn_and_one_sent_again_is_answered_without_a_second(
    db: Database, campaign: Campaign, tmp_path: Path, mocker: MockerFixture, launched: list[threading.Thread]
) -> None:
    """Two prompts typed into a busy pane join in its input queue: one ``Runtime.prompt`` carries both. A prompt that a
    restarted daemon sends again (no outcome was recorded) gets the first answer, and the agent never sees it twice."""
    runtime = Steerable()
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, Contract('subprocess', factory(runtime), pytest.MonkeyPatch()), HOLD.format(hold=hold))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    key = AgentKey(running_agent(db, campaign, ex, tmp_path, 'steered', launched))
    leases = workers_of(ex).leases
    answered = mocker.spy(leases, 'delivered')
    with leases._mail:  # both wait before the worker reads
        leases.prompt(key, Prompt(CommentId(1), 'one'))
        leases.prompt(key, Prompt(CommentId(2), 'two'))
    wait_until(lambda: answered.call_count == 2)
    leases.prompt(key, Prompt(CommentId(1), 'one'))
    wait_until(lambda: answered.call_count == 3)
    assert runtime.prompts == ['one\ntwo']
    assert [c.args[2] for c in answered.call_args_list] == [None, None, None]
    hold.unlink()


def test_a_prompt_that_cannot_reach_its_agent_is_answered_with_why_and_the_worker_goes_on(
    db: Database,
    campaign: Campaign,
    tmp_path: Path,
    tracer: Tracer,
    spans: InMemorySpanExporter,
    mocker: MockerFixture,
    launched: list[threading.Thread],
) -> None:
    """A runtime that refuses the prompt, a lease that the worker does not run, a lost answer, a lost long-poll."""
    runtime = ScriptRuntime()  # a subprocess agent cannot take a message
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, Contract('subprocess', factory(runtime), pytest.MonkeyPatch()), HOLD.format(hold=hold))
    ex = serving(RunController(db, tracer, tmp_path, factory(runtime), executor=timed(FAST)))
    key = AgentKey(running_agent(db, campaign, ex, tmp_path, 'headless', launched))
    leases = workers_of(ex).leases
    answered = mocker.spy(leases, 'delivered')
    leases.prompt(key, Prompt(CommentId(1), 'one'))
    wait_until(lambda: answered.call_count == 1)
    assert 'cannot take a message' in str(answered.call_args.args[2])
    leases._post('serve', PromptMessage(id=time.time_ns(), lease=999, prompt=Prompt(CommentId(2), 'two')))
    wait_until(lambda: answered.call_count == 2)
    assert answered.call_args.args[2] == 'the agent does not run on this worker'
    mocker.patch.object(leases, 'delivered', side_effect=OperationalError('INSERT', {}, Exception('locked')))
    leases._post('serve', PromptMessage(id=time.time_ns(), lease=999, prompt=Prompt(CommentId(3), 'three')))
    wait_until(lambda: 'worker.delivered_unsent' in span_names(spans))
    mocker.patch.object(leases, 'messages', side_effect=OperationalError('SELECT', {}, Exception('locked')))
    wait_until(lambda: 'worker.unheard' in span_names(spans))
    hold.unlink()


def span_names(spans: InMemorySpanExporter) -> list[str]:
    return [s.name for s in spans.get_finished_spans()]


def test_a_stop_that_comes_while_the_worker_registers_leaves_no_loop_behind(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """Else the loop would turn on a database that the daemon closes next."""
    executor = workers_of(RunController(db, Tracer(None), tmp_path, factory(runtime)))
    worker, register = executor.worker, executor.leases.register

    def stopped_meanwhile(name: str, pool: str, version: str, runners: list[str], host: HostSample) -> list[Grant]:
        worker.stop()
        return register(name, pool, version, runners, host)

    mocker.patch.object(executor.leases, 'register', side_effect=stopped_meanwhile)
    worker.start()
    assert worker._loop is None


def test_a_job_that_arrives_while_the_loop_ends_its_turn_is_taken_at_once(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The long-poll's ``Work`` that lands after a turn found nothing, before the loop waits, is not lost.

    The loop is held at the end of that turn, the job is submitted, and the loop goes on: it takes the job at once, not
    one heartbeat (``renew_seconds``, 10 s) later.
    """
    ex = RunController(db, Tracer(None), tmp_path, factory(runtime))
    worker, leases = workers_of(ex).worker, workers_of(ex).leases
    turned, release = threading.Event(), threading.Event()
    turn = worker._guarded_turn

    def held_after_its_turn() -> bool:
        went = turn()
        if not turned.is_set():
            turned.set()
            assert release.wait(10)
        return went

    monkeypatch.setattr(worker, '_guarded_turn', held_after_its_turn)
    serving(ex)  # its first turn finds nothing
    assert turned.wait(10)
    leases.submit(evaluation_job(db, campaign, tmp_path))
    wait_until(lambda: worker._wanted.is_set(), timeout=5)  # the long-poll delivered its Work
    release.set()
    wait_until(lambda: lease_states(db) != [LeaseState.QUEUED], timeout=leases.timing.renew / 2)
