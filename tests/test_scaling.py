"""Provisioned pools: queued leases start workers, idle ones drain and stop, and nothing outlives its row or its owner.

A server here is its database, its leases, its network and a ``Scaler`` over the ``memory`` provisioner
(``tests/fixtures/provisioner.py``). A worker that an instance would run is a registration with the instance's name.
Each pass gets its time, so a test moves the clock by passing a later one.
"""

import threading
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import count
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

from piloop.db.models import (
    Campaign,
    Candidate,
    Comment,
    CommentKind,
    Evaluation,
    Evaluator,
    InstanceEnd,
    LeaseWorker,
    NoLease,
    TokenScope,
    WorkerInstance,
    WorkerInstanceState,
    utcnow,
)
from piloop.db.session import Database
from piloop.ids import TargetKind
from piloop.net.tokens import Caller, Tokens
from piloop.otel import Tracer
from piloop.plugins import host as plugins
from piloop.plugins.executor.contract import Capacity, EvaluationJob, RepoId, Source
from piloop.plugins.executor.kit import scaling
from piloop.plugins.executor.kit.capacity import Pool
from piloop.plugins.executor.kit.client import Local
from piloop.plugins.executor.kit.leases import Drain, Grant, Leases, WorkerDrained
from piloop.plugins.executor.kit.scaling import InstanceEnded, InstanceRegistered, Scaler
from piloop.plugins.executor.workers import _worker_argv
from piloop.plugins.network.contract import NetworkHost
from piloop.plugins.network.https import TOKEN_ENV, Https
from piloop.plugins.provisioner.contract import InstanceState, Launch
from piloop.plugins.vcs.git import Git
from tests.conftest import ACTOR, at_head, baseline, home_cas, package
from tests.fixtures.provisioner import PLATFORMS, Machine, Platform
from tests.test_lease import HOST, MEASURED, TIMING, Recorder, events, join, queue

URL = 'https://piloop.test'
LAUNCH = Launch(name='gpu-ended', pool='gpu', argv=[], env={})
REGISTER = 600


@pytest.fixture
def platform(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """The name of a new account of the ``memory`` provisioner, which is installed and selectable."""
    package(
        tmp_path / 'site', 'piloop-memory', '1.0', '[piloop.provisioners]\nmemory = tests.fixtures.provisioner:Memory\n'
    )
    monkeypatch.syspath_prepend(tmp_path / 'site')
    plugins.close()
    name = uuid4().hex
    PLATFORMS[name] = Platform()
    yield name
    del PLATFORMS[name]
    plugins.close()


def pools(platform: str, **policy: object) -> dict[str, Pool]:
    """The pool ``gpu``, of one evaluation per worker, up to 2 workers unless ``policy`` says otherwise."""
    compute = {'provider': 'memory', 'platform': platform}
    gpu = {
        'agents': 0,
        'evaluations': 1,
        'runners': ['process'],
        'provisioner': {'max': 2, 'compute': compute} | policy,
    }
    return {'serve': Pool(agents=0, evaluations=0), 'gpu': Pool.model_validate(gpu)}


@dataclass
class Server:
    db: Database
    leases: Leases
    scaler: Scaler

    def rows(self) -> list[WorkerInstance]:
        with self.db.session() as s:
            return list(s.scalars(select(WorkerInstance).order_by(WorkerInstance.id)))

    def live(self) -> list[WorkerInstance]:
        return [row for row in self.rows() if row.state is not WorkerInstanceState.ENDED]

    def configure(self, found: Mapping[str, Pool]) -> None:
        self.leases.pools = found


def server(db: Database, home: Path, found: Mapping[str, Pool]) -> Server:
    network = Https(url=URL).open(NetworkHost(Tokens(db)))
    leases = Leases(db, Recorder(db), home_cas(db), TIMING, found)
    return Server(db, leases, Scaler.of(db, lambda: leases.pools, network, Tracer(None), TIMING, home, _worker_argv))


EVALUATIONS = count(1000)


def waiting(db: Database, campaign: Campaign, tmp_path: Path, n: int = 1, runner: str = 'process') -> None:
    """Queue ``n`` evaluations of ``runner`` on leases, each of a candidate of its own."""
    base = baseline(db, campaign.id)
    with db.session() as s:
        evaluator = s.scalar(select(Evaluator).where(Evaluator.campaign_id == campaign.id, Evaluator.runner == runner))
        if evaluator is None:
            evaluator = Evaluator(campaign_id=campaign.id, key=runner, definition={}, runner=runner)
            s.add(evaluator)
            s.flush()
        jobs = []
        for _ in range(n):
            tree = f't{next(EVALUATIONS)}'
            candidate = Candidate(campaign_id=campaign.id, tree_sha=tree, revision=base.revision, parent_id=base.id)
            s.add(candidate)
            s.flush()
            evaluation = Evaluation(candidate_id=candidate.id, evaluator_id=evaluator.id)
            s.add(evaluation)
            s.flush()
            jobs.append(
                EvaluationJob(
                    evaluation=evaluation.id,
                    candidate=candidate.id,
                    runner=runner,
                    artifact=Source(vcs=Git(), repo=RepoId(campaign.id), revision=base.revision),
                    place='eval',
                    command=['true'],
                    timeout=60,
                    required=['score'],
                )
            )
        s.commit()
    for job in jobs:
        queue(db, job)


def later(seconds: float) -> datetime:
    return utcnow() + timedelta(seconds=seconds)


def registered(s: Server, name: str) -> None:
    """The instance's worker registers, as ``piloop worker`` does on it."""
    join(s.leases, name, ['process'], 'gpu')


def test_a_provisioned_pool_counts_the_slots_of_its_max_workers_less_its_live_leases(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform))
    assert s.leases.capacity() == Capacity(0, 2), 'no worker yet, and two can start'
    waiting(db, campaign, tmp_path)
    assert s.leases.capacity() == Capacity(0, 1), 'a queued lease takes a slot'
    join(s.leases, 'gpu-1', ['process'], 'gpu')
    assert isinstance(Local(s.leases, 'gpu-1').acquire(uuid4().hex, HOST), Grant)
    assert s.leases.capacity() == Capacity(0, 1), 'a live lease too, whether its worker counts or not'
    s.configure(pools(platform, max=0))
    assert s.leases.capacity() == Capacity(0, 0)


def test_queued_leases_start_workers_up_to_max_each_with_a_credential_of_its_own(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform))
    s.scaler.reconcile(utcnow())
    assert PLATFORMS[platform].machines == {}, 'nothing waits, and min is 0'
    waiting(db, campaign, tmp_path, 3)
    s.scaler.reconcile(utcnow())
    machines = PLATFORMS[platform].machines
    assert [row.name for row in s.live()] == list(machines) and len(machines) == 2, 'max is 2'
    for name, machine in machines.items():
        assert name.startswith('gpu-')
        assert machine.launch.argv == ['piloop', 'worker', '--server', URL, '--pool', 'gpu', '--orphan-exit', '600']
        assert Tokens(db).verify(machine.launch.env[TOKEN_ENV].get_secret_value()) == Caller(name, TokenScope.WORKER)
    s.scaler.reconcile(utcnow())
    assert len(PLATFORMS[platform].starts) == 2, 'none more while they start'
    for name in machines:
        registered(s, name)
    s.scaler.reconcile(utcnow())
    assert [row.state for row in s.live()] == [WorkerInstanceState.RUNNING] * 2
    assert [e.name for _, _, e in events(db, InstanceRegistered)] == list(machines)


def test_a_free_slot_of_any_worker_covers_a_queued_lease_of_its_runner(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', {**pools(platform, max=4), 'cpu': Pool(agents=0, evaluations=1)})
    join(s.leases, 'cpu1', ['process'], 'cpu')
    join(s.leases, 'cuda1', ['cuda'], 'cpu')
    waiting(db, campaign, tmp_path)
    s.scaler.reconcile(utcnow())
    assert PLATFORMS[platform].machines == {}, "the static worker's free slot takes it"
    waiting(db, campaign, tmp_path, runner='gpu-only')
    s.scaler.reconcile(utcnow())
    assert PLATFORMS[platform].machines == {}, 'the pool has not the runner'
    waiting(db, campaign, tmp_path)
    s.scaler.reconcile(utcnow())
    assert len(PLATFORMS[platform].machines) == 1, 'two wait that it runs, and one slot is free'


def test_once_starts_a_worker_for_each_lease_and_drains_it_once_it_ran_one(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform, max=4, once=True, idle_seconds=3600, orphan_exit_seconds=None))
    waiting(db, campaign, tmp_path, 3)
    s.scaler.reconcile(utcnow())
    machines = PLATFORMS[platform].machines
    assert [m.launch.argv[-1] for m in machines.values()] == ['--once'] * 3
    name = next(iter(machines))
    registered(s, name)
    grant = Local(s.leases, name).acquire(uuid4().hex, HOST)
    assert isinstance(grant, Grant)
    Local(s.leases, name).report(grant.lease, uuid4().hex, MEASURED)
    s.scaler.reconcile(utcnow())
    s.scaler.reconcile(utcnow())
    assert name not in machines, 'it ran its lease: drained and stopped, long before idle_seconds'


def test_an_idle_worker_past_min_is_drained_then_stopped_and_its_credential_revoked(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform, min=1, idle_seconds=60))
    s.scaler.reconcile(utcnow())
    [first] = PLATFORMS[platform].machines
    first_secret = PLATFORMS[platform].machines[first].launch.env[TOKEN_ENV].get_secret_value()
    registered(s, first)
    waiting(db, campaign, tmp_path, 2)
    s.scaler.reconcile(utcnow())
    second = next(name for name in PLATFORMS[platform].machines if name != first)
    registered(s, second)
    for name in (first, second):
        grant = Local(s.leases, name).acquire(uuid4().hex, HOST)
        assert isinstance(grant, Grant)
        Local(s.leases, name).report(grant.lease, uuid4().hex, MEASURED)
    s.scaler.reconcile(later(30))
    assert [row.state for row in s.live()] == [WorkerInstanceState.RUNNING] * 2, 'not idle long enough yet'
    s.scaler.reconcile(later(61))
    [drained] = [row for row in s.live() if row.state is WorkerInstanceState.DRAINING]
    [kept] = [row for row in s.live() if row.state is WorkerInstanceState.RUNNING]
    assert drained.name == first, 'min is 1, and the longest idle goes first'
    assert Local(s.leases, drained.name).acquire(uuid4().hex, HOST) is NoLease.DRAINING
    assert [m.kind for m in Local(s.leases, drained.name).messages(0, 0)][-1] == 'drain'
    assert [m for m in Local(s.leases, kept.name).messages(0, 0) if isinstance(m, Drain)] == []
    assert len(events(db, WorkerDrained)) == 1
    s.scaler.reconcile(later(62))
    assert list(PLATFORMS[platform].machines) == [kept.name]
    assert [(r.name, r.end) for r in s.rows() if r.end is not None] == [(first, InstanceEnd.DRAINED)]
    assert Tokens(db).verify(first_secret) is None
    with db.session() as session:
        assert session.get(LeaseWorker, drained.name) is None, 'an ended instance leaves no worker to list'
    assert Local(s.leases, drained.name).messages(0, 0) == [], 'a long-poll that it left open ends'


def test_max_0_drains_a_busy_worker_which_stops_once_its_job_reported(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform))
    waiting(db, campaign, tmp_path)
    s.scaler.reconcile(utcnow())
    [name] = PLATFORMS[platform].machines
    registered(s, name)
    grant = Local(s.leases, name).acquire(uuid4().hex, HOST)
    assert isinstance(grant, Grant)
    s.configure(pools(platform, max=0))
    s.scaler.reconcile(utcnow())
    s.scaler.reconcile(utcnow())
    assert [row.state for row in s.live()] == [WorkerInstanceState.DRAINING] and name in PLATFORMS[platform].machines
    Local(s.leases, name).report(grant.lease, uuid4().hex, MEASURED)
    s.scaler.reconcile(utcnow())
    assert s.live() == [] and PLATFORMS[platform].machines == {}


def test_a_worker_that_never_registers_is_stopped_and_the_pool_backs_off(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform, max=1))
    waiting(db, campaign, tmp_path)
    s.scaler.reconcile(utcnow())
    [name] = PLATFORMS[platform].machines
    s.scaler.reconcile(later(REGISTER + 1))
    assert PLATFORMS[platform].machines == {}
    assert [(e.name, e.end) for _, _, e in events(db, InstanceEnded)] == [(name, InstanceEnd.REGISTER_TIMEOUT)]
    s.scaler.reconcile(later(REGISTER + 2))
    assert PLATFORMS[platform].machines == {}, 'the backoff holds the next start'
    s.scaler.reconcile(later(REGISTER + 1 + scaling.FIRST_BACKOFF.total_seconds()))
    assert len(PLATFORMS[platform].machines) == 1


def test_three_refused_starts_in_a_row_escalate_and_the_pool_backs_off_between_them(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    PLATFORMS[platform].refuse = 'no GPU in the region'
    s = server(db, tmp_path / 'home', pools(platform, max=1))
    waiting(db, campaign, tmp_path)
    at = 0.0
    for step in (0, 30, 60):
        at += step
        s.scaler.reconcile(later(at))
        s.scaler.reconcile(later(at + 1))
    assert len(PLATFORMS[platform].starts) == 3
    assert {row.end for row in s.rows()} == {InstanceEnd.START_REFUSED}
    with db.session() as session:
        [escalation] = session.scalars(select(Comment)).all()
    assert (escalation.kind, escalation.target_kind) == (CommentKind.ESCALATION, TargetKind.INSTANCE)
    assert 'no GPU in the region' in escalation.body and 'provisioner.max=0' in escalation.body


def test_a_restarted_server_stops_its_orphans_and_starts_what_it_asked_for_and_did_not_start(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    home = tmp_path / 'home'
    first = server(db, home, pools(platform))
    waiting(db, campaign, tmp_path, 2)
    first.scaler.reconcile(utcnow())
    started, orphan = list(PLATFORMS[platform].machines)
    with db.session() as s:
        s.delete(s.scalars(select(WorkerInstance).where(WorkerInstance.name == orphan)).one())  # a row lost
        s.add(WorkerInstance(name='gpu-unstarted', pool='gpu'))  # a row whose start never came
        s.commit()
    ended = Machine(next(iter(PLATFORMS[platform].machines.values())).owner, LAUNCH, InstanceState.ENDED)
    PLATFORMS[platform].machines['gpu-ended'] = ended
    again = server(db, home, pools(platform))
    again.scaler.reconcile(utcnow())
    assert set(PLATFORMS[platform].machines) == {started, 'gpu-unstarted', 'gpu-ended'}, 'an ended one is left be'
    assert PLATFORMS[platform].starts.count(started) == 1, 'the one that it lists runs: it is not started again'
    secret = PLATFORMS[platform].machines['gpu-unstarted'].launch.env[TOKEN_ENV].get_secret_value()
    assert Tokens(db).verify(secret) == Caller('gpu-unstarted', TokenScope.WORKER)


def test_two_servers_on_one_account_never_stop_each_others_instances(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path, schema: Path
) -> None:
    ours = server(db, tmp_path / 'ours', pools(platform))
    waiting(db, campaign, tmp_path)
    ours.scaler.reconcile(utcnow())
    other_db = Database(f'sqlite:///{at_head(schema, tmp_path / "other" / "piloop.db")}', actor=ACTOR)
    try:
        other = server(other_db, tmp_path / 'other', pools(platform))
        other.scaler.reconcile(utcnow())
        assert len(PLATFORMS[platform].machines) == 1, "the other's list has none of ours: no orphan"
    finally:
        other_db.close()


def busy(s: Server, name: str) -> None:
    """The worker of ``name`` registers and takes a lease."""
    registered(s, name)
    assert isinstance(Local(s.leases, name).acquire(uuid4().hex, HOST), Grant)


def test_an_instance_that_ends_with_a_lease_ends_gone_and_a_silent_one_lost(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform, min=2))
    s.scaler.reconcile(utcnow())
    preempted, silent = list(PLATFORMS[platform].machines)
    waiting(db, campaign, tmp_path, 2)
    busy(s, preempted)
    registered(s, silent)
    s.scaler.reconcile(utcnow())
    del PLATFORMS[platform].machines[preempted]
    s.scaler.reconcile(later(scaling.RESYNC_SECONDS))
    s.scaler.reconcile(later(scaling.RESYNC_SECONDS + TIMING.toleration + 1))
    ends = {e.name: e.end for _, _, e in events(db, InstanceEnded)}
    assert ends == {preempted: InstanceEnd.GONE, silent: InstanceEnd.LOST}
    assert len(s.live()) == 2, 'min is 2: two new ones start'
    exited, crashed = (row.name for row in s.live())
    busy(s, exited)
    lost = scaling.RESYNC_SECONDS + TIMING.toleration + 1
    s.scaler.reconcile(later(lost + scaling.RESYNC_SECONDS))
    for name in (exited, crashed):
        PLATFORMS[platform].machines[name].state = InstanceState.ENDED
    s.scaler.reconcile(later(lost + 2 * scaling.RESYNC_SECONDS))
    reasons = {e.name: e.reason for _, _, e in events(db, InstanceEnded)}
    assert (reasons[exited], reasons[crashed]) == (
        'its instance ended',
        'its instance ended before its worker registered',
    )


def test_a_worker_that_exits_by_itself_with_no_lease_ends_drained_and_runs_with_orphan_exit(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform, min=1, orphan_exit_seconds=900))
    s.scaler.reconcile(utcnow())
    [name] = PLATFORMS[platform].machines
    assert PLATFORMS[platform].machines[name].launch.argv[-2:] == ['--orphan-exit', '900']
    registered(s, name)
    s.scaler.reconcile(utcnow())
    PLATFORMS[platform].machines[name].state = InstanceState.ENDED  # it lost its server for 900 s, and exited
    s.scaler.reconcile(later(scaling.RESYNC_SECONDS))
    [ended] = [e for _, _, e in events(db, InstanceEnded)]
    assert (ended.name, ended.end, ended.reason) == (name, InstanceEnd.DRAINED, 'its worker exited with no lease')
    assert len(s.live()) == 1, 'min is 1: another starts'


def test_the_pool_starts_none_past_its_worker_hours_of_the_last_day(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform, max_worker_hours_per_day=1))
    with db.session() as session:
        ran = utcnow() - timedelta(hours=2)
        session.add(
            WorkerInstance(
                name='gpu-old',
                pool='gpu',
                state=WorkerInstanceState.ENDED,
                end=InstanceEnd.DRAINED,
                requested_at=ran,
                ended_at=ran + timedelta(minutes=61),
            )
        )
        session.commit()
    waiting(db, campaign, tmp_path)
    s.scaler.reconcile(utcnow())
    assert PLATFORMS[platform].machines == {}
    s.configure(pools(platform, max_worker_hours_per_day=2))
    s.scaler.reconcile(utcnow())
    assert len(PLATFORMS[platform].machines) == 1


def test_the_upkeep_runs_a_pass_on_a_queued_lease_or_at_its_deadline_and_one_at_a_time(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform))
    PLATFORMS[platform].down = True
    s.scaler.upkeep(utcnow())  # the first pass is due at once
    s.scaler.join(10)
    retry = s.scaler.upkeep(utcnow())
    assert retry is not None and retry <= later(TIMING.renew), 'a platform that does not answer fails the pass'
    PLATFORMS[platform].down = False
    s.scaler.upkeep(later(TIMING.renew))
    s.scaler.join(10)
    deadline = s.scaler.upkeep(utcnow())
    assert deadline is not None and deadline > later(scaling.RESYNC_SECONDS - 5), 'nothing to do until the next list'
    waiting(db, campaign, tmp_path)
    s.scaler.upkeep(utcnow())
    s.scaler.join(10)
    assert len(PLATFORMS[platform].machines) == 1, 'the queued lease woke it'
    queue_more = s.scaler.upkeep(utcnow())
    assert queue_more == s.scaler.upkeep(utcnow()), 'nothing new: no pass'
    s.scaler.join(10)
    held = threading.Event()
    s.scaler._pass = threading.Thread(target=held.wait, args=(10,))  # a pass that the platform holds
    s.scaler._pass.start()
    now = utcnow()
    assert s.scaler.upkeep(now) == now + scaling.PASS_POLL, 'a pass in flight: back soon, for the deadline that it sets'
    held.set()
    s.scaler.join(10)


def test_a_start_that_fails_on_the_way_is_started_again_after_a_list_with_a_new_credential(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    PLATFORMS[platform].flaky = 1
    s = server(db, tmp_path / 'home', pools(platform, max=1))
    waiting(db, campaign, tmp_path)
    s.scaler.reconcile(utcnow())
    [row] = s.live()
    assert (row.state, PLATFORMS[platform].machines) == (WorkerInstanceState.STARTING, {})
    s.scaler.reconcile(utcnow())
    assert PLATFORMS[platform].starts == [row.name, row.name] and len(s.live()) == 1
    secret = PLATFORMS[platform].machines[row.name].launch.env[TOKEN_ENV].get_secret_value()
    assert Tokens(db).verify(secret) == Caller(row.name, TokenScope.WORKER)


def test_a_pool_with_a_provisioner_names_its_runners_and_min_is_at_most_max(platform: str) -> None:
    compute = {'provider': 'memory', 'platform': platform}
    with pytest.raises(ValueError, match='runners'):
        Pool.model_validate({'evaluations': 1, 'provisioner': {'max': 1, 'compute': compute}})
    never = {'runners': ['process'], 'provisioner': {'max': 1, 'orphan_exit_seconds': None, 'compute': compute}}
    assert Pool.model_validate(never).provisioner is not None
    with pytest.raises(ValueError, match='min = 2 is more than max = 1'):
        Pool.model_validate({'runners': ['process'], 'provisioner': {'min': 2, 'max': 1, 'compute': compute}})
    with pytest.raises(ValueError, match='provider = .nope.'):
        Pool.model_validate({'runners': ['process'], 'provisioner': {'max': 1, 'compute': {'provider': 'nope'}}})
    pool = Pool.model_validate({'runners': ['process'], 'provisioner': {'max': 1, 'compute': compute}})
    assert pool.model_dump()['provisioner']['compute'] == compute, 'the nested table dumps with its provider'


def test_the_first_provisioned_pool_in_order_that_runs_a_lease_starts_its_worker(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    found = pools(platform)
    gpu = found['gpu']
    tpu = gpu.model_copy(update={'runners': ['jax']})
    big = gpu.model_copy(update={'evaluations': 2})
    s = server(db, tmp_path / 'home', {'serve': found['serve'], 'tpu': tpu, 'gpu': gpu, 'big': big})
    waiting(db, campaign, tmp_path)
    s.scaler.reconcile(utcnow())
    assert [m.launch.pool for m in PLATFORMS[platform].machines.values()] == ['gpu'], 'big comes later, tpu runs jax'


def test_a_restarted_daemon_gives_a_silent_worker_a_full_toleration_before_it_is_lost(
    db: Database, campaign: Campaign, platform: str, tmp_path: Path
) -> None:
    s = server(db, tmp_path / 'home', pools(platform, min=1))
    s.scaler.reconcile(utcnow())
    [name] = PLATFORMS[platform].machines
    registered(s, name)
    with db.session() as session:
        session.get_one(LeaseWorker, name).seen_at = utcnow() - timedelta(hours=3)  # the daemon was down
        session.commit()
    again = server(db, tmp_path / 'home', pools(platform, min=1))
    again.scaler.reconcile(utcnow())
    assert [row.state for row in again.live()] == [WorkerInstanceState.RUNNING]
