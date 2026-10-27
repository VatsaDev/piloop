"""The worker runs an agent's job on a lease: an agent that ends, one that runs out of time, a start that fails, and a
worker that restarts and re-attaches."""

import os
import shutil
import threading
import time
from collections.abc import Iterator, Mapping, Sequence
from datetime import timedelta
from functools import partial
from http import HTTPStatus
from pathlib import Path
from uuid import uuid4

import psutil
import pytest
from covalence import Cid
from git import Repo
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import SecretStr
from pytest_mock import MockerFixture
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError

from piloop.actor import System
from piloop.agents.results import CampaignResults, DaemonStopped, Ended, Lost, NotStarted, PromptOutcome, Ran
from piloop.campaign.campaigns import checkout_of, record_of
from piloop.daemon.run_controller import RunController
from piloop.db import feed
from piloop.db.events import emit, in_order
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Candidate,
    Evaluation,
    Event,
    Lease,
    LeaseState,
    LeaseWorker,
    Measurement,
    Run,
    RunStatus,
    utcnow,
)
from piloop.db.session import Database
from piloop.ids import IMPLEMENTER, AgentSessionId, CommentId, RunId, TargetKind, TargetRef
from piloop.otel import Tracer
from piloop.plugins.executor.contract import (
    AgentJob,
    AgentKey,
    AgentReport,
    EvaluationJob,
    EvaluationReport,
    HarnessSession,
    JobSecrets,
    MissingBlobs,
    Prompt,
    Source,
)
from piloop.plugins.executor.kit.capacity import HostSample, sample
from piloop.plugins.executor.kit.client import Dismissed, Local, Registration, WorkerInfo
from piloop.plugins.executor.kit.door import AgentCall, Relayed
from piloop.plugins.executor.kit.jobs import STOPPED, local_site, remote_site, send_of_record
from piloop.plugins.executor.kit.leases import (
    SERVE,
    Answer,
    Drain,
    Grant,
    LeasePrompted,
    Leases,
    NotAddressed,
    PromptMessage,
    Revoke,
    StaleLease,
    StaleReport,
    Timing,
    Work,
    cancel,
    drain,
    enqueue,
    revoke,
)
from piloop.plugins.executor.kit.relay import Relay
from piloop.plugins.executor.workers import Workers
from piloop.plugins.executor.workers.worker import WORKER_ANOMALY, Unsent, Worker
from piloop.plugins.runtime.contract import QUEUE, ExitStatus, Handle, put, take
from piloop.plugins.vcs.git import commands as git
from piloop.settings import INPUTS_DIR, Settings
from piloop.store import tree as tree_module
from tests.conftest import (
    Gate,
    ScriptRuntime,
    baseline,
    disk,
    factory,
    home_cas,
    role_timeout,
    serving,
    timed,
    wait_until,
    workers_of,
    write_toml,
)
from tests.daemon.test_evaluation_queue import held_evaluate
from tests.daemon.test_run_controller import execute, make_run, reload_campaign, set_config
from tests.test_executors import Steerable
from tests.test_lease import PROBE, Recorder, agent_job, evaluation_job

HOLD = '#!/bin/sh\nwhile [ -f {hold} ]; do sleep 0.05; done\n'


def use(db: Database, campaign: Campaign, body: str, timeout: float = 20) -> None:
    write_toml(Path(campaign.path), body, implementer_timeout=timeout)
    reload_campaign(db, campaign)


def request_agent(db: Database, campaign: Campaign) -> AgentSessionId:
    with db.session() as s:
        w = AgentSession(kind=IMPLEMENTER, campaign_id=campaign.id, reason='contract')
        s.add(w)
        s.commit()
        return w.id


def head_of(ex: RunController, agent_session_id: AgentSessionId) -> Source:
    """The HEAD of the agent's campaign, as its artifact."""
    with ex.launcher.db.session() as s:
        campaign = s.get_one(Campaign, s.get_one(AgentSession, agent_session_id).campaign_id)
        return ex.sources.source(campaign, Repo(campaign.path).head.commit.hexsha)


def launch(ex: RunController, agent_session_id: AgentSessionId, tmp_path: Path) -> Ended:
    """Launch the agent in ``logs`` under the home ``tmp_path``, its worktree in ``logs/worktree``."""
    return ex.launcher.launch(
        agent_session_id,
        brief=ex.cas.put(b'b'),
        place='logs',
        artifact=head_of(ex, agent_session_id),
        timeout=role_timeout(ex.launcher.db, agent_session_id),
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
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    use(db, campaign, '#!/bin/sh\necho done\n')
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    agent_session_id = request_agent(db, campaign)
    outcome = launch(ex, agent_session_id, tmp_path)
    assert isinstance(outcome, Ran)
    assert (outcome.process.status, outcome.process.error) == (ExitStatus.EXITED, None)
    w, lease = rows(db, agent_session_id)
    assert (w.state, w.runtime, lease.state, lease.worker) == (
        AgentSessionState.EXITED,
        'subprocess',
        LeaseState.REPORTED,
        'serve',
    )
    assert lease.handle['runtime'] == 'subprocess' and w.handle == lease.handle['data'], 'steered by its handle'
    assert (tmp_path / 'logs' / 'transcript.txt').is_file() and (tmp_path / 'logs' / 'brief.md').read_text() == 'b'


def test_an_agent_that_runs_out_of_time_is_reported_timed_out(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    use(db, campaign, '#!/bin/sh\nsleep 30\n', timeout=0.5)
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    agent_session_id = request_agent(db, campaign)
    outcome = launch(ex, agent_session_id, tmp_path)
    assert isinstance(outcome, Ran)
    assert (outcome.process.status, outcome.process.error) == (ExitStatus.TIMED_OUT, 'agent timeout after 0.5s')
    w, lease = rows(db, agent_session_id)
    assert (w.state, lease.state) == (AgentSessionState.KILLED, LeaseState.REPORTED)


def test_a_start_that_fails_is_reported_as_a_failed_start(
    db: Database, campaign: Campaign, tmp_path: Path, runtime: ScriptRuntime, mocker: MockerFixture
) -> None:
    use(db, campaign, '#!/bin/sh\necho done\n')
    mocker.patch.object(runtime, 'start', side_effect=RuntimeError('spawn_failed: no such harness'))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    agent_session_id = request_agent(db, campaign)
    outcome = launch(ex, agent_session_id, tmp_path)
    assert isinstance(outcome, NotStarted) and 'spawn_failed' in str(outcome.reason)
    w, lease = rows(db, agent_session_id)
    assert (w.state, lease.state, w.handle) == (AgentSessionState.KILLED, LeaseState.REPORTED, {})


def test_a_worker_that_stops_while_its_agent_gets_its_secrets_starts_nothing(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mocker: MockerFixture,
) -> None:
    use(db, campaign, '#!/bin/sh\necho done\n')
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    worker = workers_of(ex).worker
    asked = Local.secrets

    def stopping(client: Local, lease: int) -> JobSecrets:
        worker.stop()
        return asked(client, lease)

    monkeypatch.setattr(Local, 'secrets', stopping)
    start = mocker.spy(runtime, 'start')
    agent_session_id = request_agent(db, campaign)
    outcome = launch(ex, agent_session_id, tmp_path)
    assert outcome == NotStarted(STOPPED) and start.call_count == 0
    assert rows(db, agent_session_id)[1].state is LeaseState.REPORTED


def test_a_restarted_worker_re_attaches_to_its_agent_and_reports_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The worker of an in-place restart leaves its agent running. The next one finds it on its lease, by its handle."""
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, HOLD.format(hold=hold))
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    agent_session_id = request_agent(db, campaign)
    launching = threading.Thread(target=launch, args=(first, agent_session_id, tmp_path))
    launching.start()
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.RUNNING)
    first.hand_over()
    launching.join(30)
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    workers_of(second).leases.renew_all()
    workers_of(second).worker.start()
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
    use(db, campaign, '#!/bin/sh\nsleep 30\n')
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
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


def test_a_worker_with_a_relay_relays_each_agent_while_its_job_runs(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """An agent's calls are relayed while its job runs, and no more once its report or its loss goes out."""
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, HOLD.format(hold=hold))
    ex = RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers())
    worker = workers_of(ex).worker
    relay = worker.relay = Relay(
        Settings(home=tmp_path / 'relay-home').relay_file, lambda lease, call: Relayed(exit_code=0)
    )
    ex.executor.adopt([])
    agent_session_id = request_agent(db, campaign)

    def relayed(agent: AgentSessionId) -> bool:
        return (
            relay.answer(AgentCall(agent=agent, argv=['sql'], token=SecretStr('plt_x')).model_dump_json().encode())[0]
            is HTTPStatus.OK
        )

    launching = threading.Thread(target=launch, args=(ex, agent_session_id, tmp_path))
    launching.start()
    wait_until(lambda: relayed(agent_session_id) and lease_state(db, agent_session_id) is LeaseState.RUNNING)
    ex.hand_over()  # as a worker that restarts: the next one finds the agent on its lease, and relays for it again
    launching.join(30)
    ex = RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers())
    relay = workers_of(ex).worker.relay = Relay(
        Settings(home=tmp_path / 'relay-home').relay_file, lambda lease, call: Relayed(exit_code=0)
    )
    workers_of(ex).leases.renew_all()
    ex.executor.adopt([])
    wait_until(lambda: relayed(agent_session_id))
    hold.unlink()
    assert isinstance(ex.launcher.reattach(agent_session_id), Ran)
    assert not relayed(agent_session_id), 'no more after its report'
    mocker.patch('piloop.plugins.executor.kit.jobs.run_agent', side_effect=RuntimeError('a bug'))
    lost = request_agent(db, campaign)
    assert isinstance(launch(ex, lost, tmp_path), Lost) and not relayed(lost), 'nor after its loss'
    ex.stop(5)


def test_a_worker_whose_own_code_fails_loses_the_job_so_that_nothing_waits_for_ever(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    use(db, campaign, '#!/bin/sh\necho done\n')
    mocker.patch('piloop.plugins.executor.workers.worker.Worker._agent', side_effect=RuntimeError('a bug'))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    agent_session_id = request_agent(db, campaign)
    outcome = launch(ex, agent_session_id, tmp_path)
    assert outcome == Lost('worker lost: RuntimeError: a bug')
    assert rows(db, agent_session_id)[1].state is LeaseState.EXPIRED
    workers_of(ex).worker._lost(
        99, 'nothing'
    )  # a lease that is no longer the worker's: refused, and the worker goes on


def test_a_report_that_names_blobs_the_cas_lacks_loses_the_job_with_why_and_is_not_sent_again(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    use(db, campaign, '#!/bin/sh\necho done\n')
    lacks = MissingBlobs('its report names blobs that the CAS lacks: bafk')
    finished = mocker.patch.object(CampaignResults, 'finished', side_effect=lacks)
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    agent_session_id = request_agent(db, campaign)
    outcome = launch(ex, agent_session_id, tmp_path)
    assert outcome == Lost('worker lost: its report names blobs that the CAS lacks: bafk')
    assert rows(db, agent_session_id)[1].state is LeaseState.EXPIRED and finished.call_count == 1
    wait_until(lambda: not (tmp_path / 'logs' / 'worktree').exists())  # removed once the job is lost
    assert workers_of(ex).worker.spool.leases() == set(), 'not sent again'


def test_a_restarted_worker_reports_an_agent_whose_worktree_is_gone(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, HOLD.format(hold=hold))
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    agent_session_id = request_agent(db, campaign)
    launching = threading.Thread(target=launch, args=(first, agent_session_id, tmp_path))
    launching.start()
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.RUNNING)
    first.hand_over()
    launching.join(30)
    shutil.rmtree(tmp_path / 'logs' / 'worktree')
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    workers_of(second).worker.start()
    outcome = second.launcher.reattach(agent_session_id)
    assert outcome == Lost('worker lost: the worker restarted and could not find its agent')
    hold.unlink()


def test_a_hand_over_twice_leaves_a_re_attached_agent_to_the_third_daemon(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, HOLD.format(hold=hold))
    agent_session_id = request_agent(db, campaign)
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    threading.Thread(target=launch, args=(first, agent_session_id, tmp_path)).start()
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.RUNNING)
    first.hand_over()
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    workers_of(second).worker.start()
    wait_until(lambda: workers_of(second).worker._jobs)  # it re-attached
    second.hand_over()
    third = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
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
    use(db, campaign, '#!/bin/sh\nsleep 30\n')
    gate = Gate()
    monkeypatch.setattr(runtime, 'start', gate.wrap(runtime.start))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
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
    use(db, campaign, '#!/bin/sh\nrm -rf "$PWD"\n')
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    agent_session_id = request_agent(db, campaign)
    outcome = ex.launcher.launch(
        agent_session_id,
        brief=ex.cas.put(b'b'),
        place='logs',
        artifact=head_of(ex, agent_session_id),
        timeout=60,
        capture=RunId(1),
    )
    assert isinstance(outcome, Ran) and outcome.process.error is not None and outcome.work is None, (
        'the report says what is known'
    )
    assert rows(db, agent_session_id)[1].state is LeaseState.REPORTED


def test_an_evaluation_at_a_hand_over_is_stopped_and_measured_again_by_the_next_worker(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The next worker cannot watch an evaluator again: it finds the lease, reports it gone, and it is queued again."""
    (tmp_path / 'hold').touch()
    set_config(db, campaign, evaluate=held_evaluate(tmp_path))
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    candidate = baseline(db, campaign.id).id
    threading.Thread(target=first.remeasure, args=(candidate,)).start()
    wait_until(lambda: lease_states(db) == [LeaseState.RUNNING])
    first.hand_over()
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
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
    monkeypatch.setattr('piloop.plugins.vcs.git.commands.add_worktree', gate.wrap(git.add_worktree))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    measuring = threading.Thread(target=ex.remeasure, args=(baseline(db, campaign.id).id,))
    measuring.start()
    assert gate.entered.wait(10)
    workers_of(ex).leases.expire(utcnow() + timedelta(seconds=workers_of(ex).leases.timing.toleration + 1))
    gate.release.set()
    measuring.join(30)
    assert not measuring.is_alive(), 'measured on its new lease'

    def stale() -> list[object]:
        with db.session() as s:
            return [stale.lease for _, stale in in_order(s, StaleReport)]

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

    monkeypatch.setattr('piloop.plugins.vcs.git.commands.add_worktree', first_held)
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
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
            return [stale.lease for _, stale in in_order(s, StaleReport)]

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
    thread = threading.Thread(
        target=ex.launcher.launch,
        args=(agent_session_id,),
        kwargs={
            'brief': ex.cas.put(b'b'),
            'place': name,
            'artifact': head_of(ex, agent_session_id),
            'timeout': role_timeout(db, agent_session_id),
        },
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
    """A start can take minutes (a checkout of a large repo); meanwhile the worker's other agents are renewed."""
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, HOLD.format(hold=hold), timeout=600)
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
        HOLD.format(hold=hold),
        timeout=600,
    )
    ex = serving(RunController(db, tracer, tmp_path, factory(runtime), executor=timed(FAST)))
    agent_session_id = running_agent(db, campaign, ex, tmp_path, 'held', launched)
    mocker.patch.object(
        workers_of(ex).leases, 'acquire', side_effect=OperationalError('SELECT', {}, Exception('database is locked'))
    )
    renewed_while(ex, db, agent_session_id, 2.5)
    deadline = time.monotonic() + 30  # a slow runner reaches the third failing turn after the window
    while WORKER_ANOMALY not in (names := [s.name for s in spans.get_finished_spans()]) and time.monotonic() < deadline:
        renewed_while(ex, db, agent_session_id, 0.1)
    assert 'worker.turn_failed' in names and names.count(WORKER_ANOMALY) == 1, 'one anomaly for one failing streak'
    hold.unlink()


def test_a_kill_that_fails_is_tried_again_on_the_next_heartbeat(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, launched: list[threading.Thread]
) -> None:
    """A revoke whose kill fails is not forgotten: the next answer asks again."""

    class Flaky(ScriptRuntime):
        def kill(self, handle: Handle) -> None:
            if not self.killed:
                self.killed.append(handle)
                raise RuntimeError('server_unavailable')
            super().kill(handle)

    runtime = Flaky()
    use(db, campaign, '#!/bin/sh\nsleep 600\n', timeout=600)
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST)))
    agent_session_id = running_agent(db, campaign, ex, tmp_path, 'stubborn', launched)
    with db.session() as s:  # no message: each heartbeat's ``REVOKED`` asks again
        revoke(s, home_cas(db), s.scalars(select(Lease)).one())
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
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
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
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
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
    repo, cwd = Path(campaign.path), tmp_path / 'logs' / 'worktree'
    second = RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers())
    source = head_of(second, agent_session_id)
    job = AgentJob(
        agent_session=agent_session_id,
        runtime='subprocess',
        artifact=source,
        place='logs',
        timeout=60,
        brief_cid=None,
        spec=None,
    )
    git.add_worktree(git.open_repo(repo), cwd, source.revision, {})
    with db.session() as s:
        enqueue(s, home_cas(db), job)
        s.commit()
    results = CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False)
    killed = Leases(db, results, disk(tmp_path))  # the killed worker's lease, which no worker watches
    killed.register(SERVE, SERVE, '0.2.0', [], sample(tmp_path))
    killed.acquire(SERVE, uuid4().hex, sample(tmp_path))
    serving(second)
    workers_of(second).worker.start()
    wait_until(lambda: lease_state(db, agent_session_id) is LeaseState.EXPIRED)
    workers_of(second).worker.join(10)
    assert not cwd.exists() and git.worktrees_under(git.open_repo(repo), cwd) == []


def test_a_worker_whose_heartbeat_hangs_stops_its_job_when_its_self_stop_passes(
    db: Database,
    campaign: Campaign,
    runtime: ScriptRuntime,
    tmp_path: Path,
    mocker: MockerFixture,
    tracer: Tracer,
    spans: InMemorySpanExporter,
) -> None:
    """A partition that drops packets holds a call until its own timeout, which can be longer than the lease has
    left. The worker stops its job on time all the same, on its own clock, not once the call gives up."""
    (tmp_path / 'hold').touch()
    set_config(db, campaign, evaluate=held_evaluate(tmp_path))
    ex = serving(RunController(db, tracer, tmp_path, factory(runtime), executor=timed(FAST)))
    measuring = threading.Thread(target=ex.remeasure, args=(baseline(db, campaign.id).id,))
    measuring.start()
    wait_until(lambda: lease_states(db) == [LeaseState.RUNNING])
    worker = workers_of(ex).worker
    healed = threading.Event()
    heartbeat = worker.client.heartbeat

    def unanswered(sessions: dict[int, HarnessSession], host: HostSample) -> dict[int, Answer]:
        healed.wait()  # no answer and no error, until the partition heals
        return heartbeat(sessions, host)

    mocker.patch.object(worker.client, 'heartbeat', side_effect=unanswered)
    stops = mocker.spy(worker, '_stop_jobs')

    def stopped() -> list[str]:
        return [str((s.attributes or {})['why']) for s in spans.get_finished_spans() if s.name == 'worker.job_stopped']

    try:
        wait_until(lambda: stops.call_count >= 2)  # and again a heartbeat later, while it stays silent
        assert stopped() == ['silent']
    finally:
        healed.set()
        (tmp_path / 'hold').unlink()
        measuring.join(30)


def test_a_worker_that_cannot_register_at_its_start_registers_on_its_next_turn(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    ex = RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST))
    register = mocker.patch.object(
        workers_of(ex).leases, 'register', side_effect=[OperationalError('SELECT', {}, Exception('locked')), ([], 0)]
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
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
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
    first = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
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
    second = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
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
    ex = RunController(
        db, Tracer(None), tmp_path, factory(runtime), executor=Workers()
    )  # not started: no turn may take the lease
    worker = workers_of(ex).worker
    worker.join(0)  # nothing started yet: no loop to wait for
    job = agent_job(db, campaign, tmp_path)  # its row is requested, not live: settled for core
    with db.session() as s:
        lease_id = enqueue(s, home_cas(db), job).id
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
    workers_of(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers())).worker.hand_over()


def test_a_join_while_a_job_thread_starts_waits_for_it(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``stop`` joins the job threads while the loop may spawn one: it never finds a thread that is not started."""
    worker = workers_of(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers())).worker
    errors: list[Exception] = []
    start = threading.Thread.start

    def join() -> None:
        try:
            worker.join(5)
        except Exception as e:
            errors.append(e)

    def joined_while_it_starts(thread: threading.Thread) -> None:
        if thread.name == 'lease-1':
            joining = threading.Thread(target=join)
            start(joining)
            joining.join(0.2)  # the join of a stop, at the moment the job's thread is registered and not started
        start(thread)

    monkeypatch.setattr(threading.Thread, 'start', joined_while_it_starts)
    worker._spawn(Grant(lease=1, job_cid=Cid.of(b'{}')), lambda: None)
    worker.join(5)
    assert errors == []


def held_reads(mocker: MockerFixture, leases: Leases) -> threading.Event:
    """Hold the worker's next read until the returned event is set, once its current long-poll has ended."""
    reading, release = threading.Event(), threading.Event()
    messages = leases.messages

    def held(worker: str, after: int, wait: float) -> list[Work | PromptMessage | Revoke | Drain]:
        reading.set()
        assert release.wait(10)
        return messages(worker, after, wait)

    mocker.patch.object(leases, 'messages', side_effect=held)
    assert reading.wait(10)
    return release


def outcomes(db: Database, agent_session_id: AgentSessionId) -> list[str]:
    on_agent = Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session_id)
    with db.session() as s:
        return [settled.kind for _, settled in in_order(s, PromptOutcome, on_agent)]


def prompts(db: Database) -> list[object]:
    with db.session() as s:
        return [prompted.comment for _, prompted in in_order(s, LeasePrompted)]


def test_prompts_read_together_reach_the_agent_in_one_turn_and_a_batch_read_again_is_not_applied_again(
    db: Database, campaign: Campaign, tmp_path: Path, mocker: MockerFixture, launched: list[threading.Thread]
) -> None:
    """Two prompts typed into a busy pane join in its input queue: one ``Runtime.prompt`` carries both. An answer that
    did not land keeps the cursor before its prompt: the next read gets it again, and it is answered, never typed
    again."""
    runtime = Steerable()
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, HOLD.format(hold=hold))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST)))
    key = AgentKey(running_agent(db, campaign, ex, tmp_path, 'steered', launched))
    leases = workers_of(ex).leases
    release = held_reads(mocker, leases)
    queued = leases.queued

    def lost_once(worker: str, message: int, error: str | None) -> None:
        if answered.call_count == 1:
            raise OperationalError('INSERT', {}, Exception('locked'))
        queued(worker, message, error)

    answered = mocker.patch.object(leases, 'queued', side_effect=lost_once)
    leases.prompt(key, Prompt(CommentId(1), 'one'))
    leases.prompt(key, Prompt(CommentId(2), 'two'))
    release.set()
    wait_until(lambda: outcomes(db, key.agent_session) == ['comment.queued', 'comment.queued'])
    assert runtime.prompts == ['one\ntwo'], 'typed once'
    assert answered.call_count == 3, 'the first answer did not land: it was answered again'
    hold.unlink()


class Queueing(ScriptRuntime):
    """The subprocess runtime with resume arguments: a prompt waits in the agent's queue until a hook takes it."""

    def prompt(self, handle: Handle, text: str) -> None:
        put(Path(str(handle.data['log_dir'])), text)


@pytest.mark.parametrize('read', [True, False], ids=['read', 'killed in a long tool call'])
def test_a_prompt_is_queued_and_delivered_only_once_the_agent_reads_it(
    db: Database, campaign: Campaign, tmp_path: Path, launched: list[threading.Thread], read: bool
) -> None:
    """A wrap-up that comes during a long tool call waits in the queue. The agent's hook reads it after the tool call,
    or the agent ends first, and the message never reached it."""
    runtime = Queueing()
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, HOLD.format(hold=hold))
    ex = serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST)))
    key = AgentKey(running_agent(db, campaign, ex, tmp_path, 'steered', launched))
    ex.executor.prompt(key, Prompt(CommentId(1), 'wrap up'))
    wait_until(lambda: outcomes(db, key.agent_session) == ['comment.queued'])
    if read:
        with db.session() as s:
            log_dir = Path(s.get_one(AgentSession, key.agent_session).log_dir or '')
        assert take(log_dir / QUEUE) == ['wrap up'], "the harness's hook, between two tool calls"
    hold.unlink()
    wait_until(lambda: lease_state(db, key.agent_session) is LeaseState.REPORTED)
    assert outcomes(db, key.agent_session) == ['comment.queued', 'comment.delivered' if read else 'comment.undelivered']


def test_a_prompt_survives_a_daemon_restart_and_reaches_its_agent_once(
    db: Database, campaign: Campaign, tmp_path: Path, mocker: MockerFixture, launched: list[threading.Thread]
) -> None:
    """The daemon stops before its worker reads the prompt. The next daemon's worker reads nothing until its agent is
    re-attached, then reads the prompt after its cursor, and core's second hand-over of the prompt writes nothing."""
    runtime = Steerable()
    hold = tmp_path / 'hold'
    hold.touch()
    use(db, campaign, HOLD.format(hold=hold))
    first = RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST))
    mocker.patch.object(workers_of(first).worker, '_listen')  # it reads no message: the daemon is gone first
    key = AgentKey(running_agent(db, campaign, serving(first), tmp_path, 'steered', launched))
    prompt = Prompt(CommentId(41), 'try lzma')
    first.executor.prompt(key, prompt)
    first.hand_over()
    second = RunController(db, Tracer(None), tmp_path, factory(runtime), executor=timed(FAST))
    worker, leases = workers_of(second).worker, workers_of(second).leases
    reattached, recover = threading.Event(), worker._recover

    def slow_to_reattach(grant: Grant) -> None:
        assert reattached.wait(10)
        recover(grant)

    mocker.patch.object(worker, '_recover', side_effect=slow_to_reattach)
    read = mocker.spy(leases, 'messages')
    serving(second)
    second.executor.prompt(key, prompt)  # core hands over an unanswered prompt again after a restart
    wait_until(lambda: worker._registered)
    time.sleep(0.5)  # a listener that did not wait for its agent would have read the prompt by now
    assert read.call_count == 0, 'it reads nothing before its agent is back'
    reattached.set()
    wait_until(lambda: outcomes(db, key.agent_session) == ['comment.queued'])
    assert (runtime.prompts, prompts(db)) == (['try lzma'], [41])
    with db.session() as s:
        [(event, _)] = in_order(s, LeasePrompted)
        message = event.id
    leases.queued(SERVE, message, None)
    assert outcomes(db, key.agent_session) == ['comment.queued'], 'an answer sent again changes nothing'
    hold.unlink()
    wait_until(lambda: lease_state(db, key.agent_session) is LeaseState.REPORTED)


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
    use(db, campaign, HOLD.format(hold=hold))
    ex = serving(RunController(db, tracer, tmp_path, factory(runtime), executor=timed(FAST)))
    key = AgentKey(running_agent(db, campaign, ex, tmp_path, 'headless', launched))
    leases = workers_of(ex).leases
    answered = mocker.spy(leases, 'queued')
    leases.prompt(key, Prompt(CommentId(1), 'one'))
    wait_until(lambda: answered.call_count == 1)
    assert 'cannot take a message' in str(answered.call_args.args[2])
    prompted(db, key, lease=999, comment=2)
    wait_until(lambda: answered.call_count == 2)
    assert answered.call_args.args[2] == 'the agent does not run on this worker'
    mocker.patch.object(leases, 'queued', side_effect=OperationalError('INSERT', {}, Exception('locked')))
    prompted(db, key, lease=999, comment=3)
    wait_until(lambda: 'worker.queued_unsent' in span_names(spans))
    mocker.patch.object(leases, 'queued', side_effect=NotAddressed('not_addressed: not for serve'))
    wait_until(lambda: 'worker.queued_refused' in span_names(spans))
    mocker.patch.object(leases, 'messages', side_effect=OperationalError('SELECT', {}, Exception('locked')))
    wait_until(lambda: 'worker.unheard' in span_names(spans))
    hold.unlink()


def prompted(db: Database, key: AgentKey, *, lease: int, comment: int) -> None:
    """A prompt for the worker ``serve`` on a lease that it does not run."""
    prompted = LeasePrompted(lease=lease, worker=SERVE, comment=CommentId(comment), text='x')
    with db.session() as s:
        emit(s, prompted, TargetRef(TargetKind.AGENT_SESSION, key.agent_session), System.DAEMON)
        s.commit()


def span_names(spans: InMemorySpanExporter) -> list[str]:
    return [s.name for s in spans.get_finished_spans()]


def test_a_stop_that_comes_while_the_worker_registers_leaves_no_loop_behind(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, mocker: MockerFixture
) -> None:
    """Else the loop would turn on a database that the daemon closes next."""
    executor = workers_of(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers()))
    worker, register = executor.worker, executor.leases.register

    def stopped_meanwhile(
        name: str, pool: str, version: str, runners: list[str], host: HostSample
    ) -> tuple[list[Grant], int]:
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
    ex = RunController(db, Tracer(None), tmp_path, factory(runtime), executor=Workers())
    worker, leases = workers_of(ex).worker, workers_of(ex).leases
    turned, release = threading.Event(), threading.Event()
    turn = worker._guarded_turn

    def held_after_its_turn() -> None:
        turn()
        if not turned.is_set():
            turned.set()
            assert release.wait(10)

    monkeypatch.setattr(worker, '_guarded_turn', held_after_its_turn)
    serving(ex)  # its first turn finds nothing
    assert turned.wait(10)
    leases.submit(evaluation_job(db, campaign, tmp_path))
    wait_until(lambda: worker._wanted.is_set(), timeout=5)  # the long-poll delivered its Work
    release.set()
    wait_until(lambda: lease_states(db) != [LeaseState.QUEUED], timeout=leases.timing.renew / 2)


FAST = Timing(renew=0.2, grace=1.0, toleration=5.0)


class Refusing(Local):
    """``Local``, whose control plane refuses the worker for good (``Dismissed``) on each call that ``refused`` names."""

    def __init__(self, leases: Leases, worker: str) -> None:
        super().__init__(leases, worker)
        self.refused: set[str] = set()

    def _check(self, call: str) -> None:
        if call in self.refused:
            raise Dismissed(f'{call}: the token is unknown or revoked')

    def heartbeat(self, leases: Mapping[int, HarnessSession], sample: HostSample) -> dict[int, Answer]:
        self._check('heartbeat')
        return super().heartbeat(leases, sample)

    def messages(self, after: int, wait: float) -> Sequence[Work | PromptMessage | Revoke | Drain]:
        self._check('messages')
        return super().messages(after, wait)

    def report(self, lease: int, request_id: str, report: AgentReport | EvaluationReport) -> dict[str, object]:
        self._check('report')
        return super().report(lease, request_id, report)

    def lost(self, lease: int, reason: str) -> None:
        self._check('lost')
        super().lost(lease, reason)


@pytest.mark.parametrize(
    ('refused', 'spooled'),
    [(('heartbeat', 'report'), {1}), (('messages',), set())],
    ids=['heartbeat', 'messages'],
)
def test_a_dismissed_worker_stops_its_job_keeps_an_unsent_report_in_its_spool_and_ends(
    db: Database, campaign: Campaign, tmp_path: Path, refused: tuple[str, ...], spooled: set[int]
) -> None:
    """Dismissed at once, by its first refused call: not after the self-stop, as a silent control plane would be."""
    leases = Leases(
        db, CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False), disk(tmp_path), FAST
    )
    hold = tmp_path / 'hold'
    hold.touch()
    job = evaluation_job(db, campaign, tmp_path)
    leases.submit(job.model_copy(update={'command': ['sh', '-c', f'while [ -f {hold} ]; do sleep 0.05; done']}))
    client = Refusing(leases, SERVE)
    worker = Worker(
        client,
        Tracer(None),
        local_site(tmp_path, disk(tmp_path), partial(checkout_of, db), partial(record_of, db), reserve=0),
        runners=('process',),
    )
    worker.start()
    wait_until(lambda: lease_states(db) == [LeaseState.RUNNING])
    client.refused |= set(refused)
    began = time.monotonic()
    worker.wait()
    assert str(worker.dismissed) == f'{refused[0]}: the token is unknown or revoked'
    assert time.monotonic() - began < FAST.self_stop
    assert worker.spool.leases() == spooled, 'a report that it could not send waits for a worker that is taken again'


def test_a_worker_elsewhere_short_of_disk_drops_the_input_copies_that_no_worktree_links_after_a_job(
    db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The eviction of ``make_room``, on a worker that has no archiver: its own home, its own clones, the shared CAS."""
    cas = disk(tmp_path)
    leases = Leases(db, CampaignResults(db, Tracer(None), tmp_path, cas, lambda: False), cas, FAST)
    home = tmp_path / 'worker'
    inputs, worktree = home / INPUTS_DIR, home / 'runs' / '1' / 'worktree'
    inputs.mkdir(parents=True)
    worktree.mkdir(parents=True)
    for name in ('used', 'old', 'new'):
        (inputs / name).write_bytes(os.urandom(1000))
    os.link(inputs / 'used', worktree / 'data')  # a job that still runs links it
    monkeypatch.setattr(tree_module, 'free', lambda _: 0)  # always short: only the linked copy may stay
    site = remote_site(home, cas, partial(send_of_record, cas, partial(record_of, db)), reserve=1)
    worker = Worker(Local(leases, 'w1'), Tracer(None), site, runners=('process',))
    job = evaluation_job(db, campaign, tmp_path)
    with db.session() as s:
        s.get_one(Evaluation, job.evaluation).started_at = utcnow()  # as the scheduler starts it
        s.commit()
    leases.submit(job)
    worker.start()
    wait_until(lambda: lease_states(db) == [LeaseState.REPORTED])
    wait_until(lambda: sorted(p.name for p in inputs.iterdir()) == ['used'])
    assert (worktree / 'data').samefile(inputs / 'used')
    worker.stop()
    worker.join(10)


class Unanswered(Local):
    """``Local``, whose first two ``register`` calls get no answer, as a server that is not up yet."""

    def __init__(self, leases: Leases, worker: str) -> None:
        super().__init__(leases, worker)
        self.registers = 0

    def register(self, info: WorkerInfo) -> Registration:
        self.registers += 1
        if self.registers <= 2:
            raise OSError('connection refused')
        return super().register(info)


def test_a_worker_whose_registration_failed_registers_on_a_later_turn_and_a_stop_ends_it(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    leases = Leases(
        db, CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False), disk(tmp_path), FAST
    )
    client = Unanswered(leases, SERVE)
    worker = Worker(
        client,
        Tracer(None),
        local_site(tmp_path, disk(tmp_path), partial(checkout_of, db), partial(record_of, db), reserve=0),
        runners=('process',),
    )
    worker.start()  # its own try, then the loop's first
    wait_until(lambda: client.registers == 2)
    worker._wake()  # the loop's next turn, without the default heartbeat's wait
    wait_until(lambda: worker.id == SERVE)
    worker.stop()
    worker.wait()
    assert client.registers == 3


def test_a_worker_dismissed_while_it_reports_a_job_it_cannot_find_again_ends(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    leases = Leases(
        db, CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False), disk(tmp_path), FAST
    )
    leases.submit(evaluation_job(db, campaign, tmp_path))
    client = Refusing(leases, SERVE)
    client.register(WorkerInfo(version='0', pool=SERVE, runners=['process'], sample=sample(tmp_path), cas_probe=PROBE))
    assert isinstance(client.acquire(uuid4().hex, sample(tmp_path)), Grant)  # an earlier process of it took the job
    client.refused.add('lost')
    worker = Worker(
        client,
        Tracer(None),
        local_site(tmp_path, disk(tmp_path), partial(checkout_of, db), partial(record_of, db), reserve=0),
        runners=('process',),
    )
    worker.start()
    worker.wait()
    assert str(worker.dismissed) == 'lost: the token is unknown or revoked'


def started_evaluation(db: Database, campaign: Campaign, tmp_path: Path) -> EvaluationJob:
    """An evaluation's job whose queue row has its slot, as core's dispatch gives it."""
    job = evaluation_job(db, campaign, tmp_path)
    with db.session() as s:
        s.get_one(Evaluation, job.evaluation).started_at = utcnow()
        s.commit()
    return job


def worker_on(leases: Leases, db: Database, tmp_path: Path, *, once: bool = False) -> Worker:
    site = local_site(tmp_path, disk(tmp_path), partial(checkout_of, db), partial(record_of, db), reserve=0)
    return Worker(Local(leases, SERVE), Tracer(None), site, runners=('process',), once=once)


def test_a_drained_worker_reports_its_job_takes_no_other_and_ends(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    leases = Leases(
        db, CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False), disk(tmp_path), FAST
    )
    hold = tmp_path / 'hold'
    hold.touch()
    job = started_evaluation(db, campaign, tmp_path)
    leases.submit(job.model_copy(update={'command': ['sh', '-c', f'while [ -f {hold} ]; do sleep 0.05; done']}))
    worker = worker_on(leases, db, tmp_path)
    worker.start()
    wait_until(lambda: lease_states(db) == [LeaseState.RUNNING])
    with db.session() as s:
        drain(s, SERVE, TargetRef(TargetKind.INSTANCE, 1))
        s.commit()
    leases.submit(job.model_copy(update={'evaluation': job.evaluation + 1000}))
    hold.unlink()
    worker.wait()
    assert lease_states(db) == [LeaseState.REPORTED, LeaseState.QUEUED]


def test_a_worker_once_runs_one_job_and_ends(db: Database, campaign: Campaign, tmp_path: Path) -> None:
    leases = Leases(
        db, CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False), disk(tmp_path), FAST
    )
    job = started_evaluation(db, campaign, tmp_path)
    leases.submit(job)
    leases.submit(job.model_copy(update={'evaluation': job.evaluation + 1000}))
    worker = worker_on(leases, db, tmp_path, once=True)
    worker.start()
    worker.wait()
    assert lease_states(db) == [LeaseState.REPORTED, LeaseState.QUEUED]


def test_a_worker_drained_before_it_starts_takes_no_job_and_ends(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    leases = Leases(
        db, CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False), disk(tmp_path), FAST
    )
    leases.submit(started_evaluation(db, campaign, tmp_path))
    leases.register(SERVE, SERVE, '0.2.0', ['process'], sample(tmp_path))
    on = TargetRef(TargetKind.INSTANCE, 1)
    with db.session() as s:
        drain(s, 'never-registered', on)
        drain(s, SERVE, on)
        drain(s, SERVE, on)
        s.commit()
        assert [e.target for e in s.scalars(select(Event).where(Event.kind == 'worker.drained'))] == [on]
        s.get_one(LeaseWorker, SERVE).cursor = feed.newest(s)  # past the message: its acquire alone tells it
        s.commit()
    worker = worker_on(leases, db, tmp_path)
    worker.start()
    worker.wait()
    assert lease_states(db) == [LeaseState.QUEUED]


class ServerGone(Local):
    """``Local``, whose server stops answering once the worker registered."""

    def heartbeat(self, leases: Mapping[int, HarnessSession], sample: HostSample) -> dict[int, Answer]:
        raise ConnectionError('connection refused')


def test_a_worker_with_an_orphan_exit_stays_while_its_server_answers_and_ends_once_it_does_not(
    db: Database, tmp_path: Path
) -> None:
    """Idle with its server there: only the server's drain ends it, so a warm worker of ``min`` stays."""
    leases = Leases(
        db, CampaignResults(db, Tracer(None), tmp_path, disk(tmp_path), lambda: False), disk(tmp_path), FAST
    )
    site = local_site(tmp_path, disk(tmp_path), partial(checkout_of, db), partial(record_of, db), reserve=0)
    answered = Worker(Local(leases, SERVE), Tracer(None), site, runners=('process',), orphan_exit=0.3)
    answered.start()
    assert answered._loop is not None
    answered._loop.join(1.5)
    assert answered._loop.is_alive(), 'idle past its orphan exit, with its server there'
    answered.stop()
    answered.wait()
    gone = Worker(ServerGone(leases, 'gone'), Tracer(None), site, runners=('process',), orphan_exit=0.3)
    began = time.monotonic()
    gone.start()
    gone.wait()
    assert 0.3 <= time.monotonic() - began < FAST.self_stop


def test_a_worker_whose_cas_lacks_the_job_loses_its_lease_with_why(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    leases = Leases(db, Recorder(db), home_cas(db), FAST)
    leases.submit(agent_job(db, campaign, tmp_path))
    elsewhere = tmp_path / 'elsewhere'
    site = local_site(elsewhere, disk(elsewhere), partial(checkout_of, db), partial(record_of, db), reserve=0)
    worker = Worker(Local(leases, SERVE), Tracer(None), site)
    worker.start()
    wait_until(lambda: lease_states(db) == [LeaseState.EXPIRED])
    with db.session() as s:
        lost = str(s.scalars(select(Lease)).one().receipt['lost'])
    assert lost.startswith('worker lost: MissingJob: the CAS lacks the job of lease#1, bafkr'), lost
    worker.stop()
    worker.join(5)
