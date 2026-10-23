"""The lease operations: acquire, heartbeat, report, the fence, and the two stages of a silent worker."""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

import pytest
from pytest_mock import MockerFixture
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.db.models import (
    AgentSession,
    Campaign,
    Candidate,
    Evaluation,
    Event,
    Lease,
    LeaseState,
    LeaseWorker,
    NoLease,
    utcnow,
)
from piloop.db.session import Database, write_transaction
from piloop.ids import WATCHDOG, CommentId, TargetKind
from piloop.plugins.executor.contract import (
    AgentFinished,
    AgentJob,
    AgentKey,
    AgentReport,
    Capacity,
    Checkout,
    EvaluationJob,
    EvaluationKey,
    EvaluationReport,
    GitTree,
    Guard,
    JobKey,
    Prompt,
    Sample,
    Settled,
)
from piloop.plugins.executor.kit.capacity import GB, HostSample, Pool
from piloop.plugins.executor.kit.client import Local, Registration, WorkerInfo
from piloop.plugins.executor.kit.leases import (
    EXPIRED,
    NOT_TAKEN,
    STALE_REPORT,
    UNREACHABLE,
    AgentHandle,
    Answer,
    Grant,
    Leases,
    PromptMessage,
    Revoke,
    StaleLease,
    Timing,
    Work,
    cancel,
    enqueue,
    revoke,
)
from piloop.plugins.executor.workers import Workers
from piloop.plugins.runtime.contract import AgentExit, ExitStatus
from tests.conftest import baseline

HANDLE = AgentHandle(runtime='subprocess', data={})
TIMING = Timing(renew=10, grace=50, toleration=300)
POOLS = {'test': Pool(evaluations=99)}
"""A pool whose slots no test fills, unless it says so."""
HOST = HostSample(at=utcnow(), cpus=4, load1=0.5, memory_available=8 * GB, disk_free=100 * GB)


def join(leases: Leases, worker: str, runners: list[str], pool: str = 'test') -> Registration:
    """Register ``worker`` in ``pool`` with ``runners``, as its ``LeaseClient`` does."""
    return Local(leases, worker).register(WorkerInfo(version='0.2.0', pool=pool, runners=runners, sample=HOST))


def take(leases: Leases, worker: str, runners: list[str], pool: str = 'test') -> Grant | None:
    """Register ``worker`` in ``pool`` with ``runners``, then acquire: the grant, or None when it gets no lease."""
    join(leases, worker, runners, pool)
    grant = Local(leases, worker).acquire(HOST)
    return grant if isinstance(grant, Grant) else None


@dataclass
class Recorder:
    """A ``Results`` that records what the leases asked of core, after their guard, in a transaction as core's."""

    db: Database
    calls: list[tuple[str, int, object]] = field(default_factory=list)

    def _apply(self, guard: Guard | None, call: tuple[str, int, object], change: Callable[[Session], None]) -> None:
        with self.db.session() as s:
            write_transaction(s)
            if guard is not None:
                guard(s)
            change(s)
            s.commit()
        self.calls.append(call)

    def started(
        self, key: AgentKey, handle: dict[str, object], session_id: str | None, *, guard: Guard | None = None
    ) -> None:
        self._apply(guard, ('started', key.agent_session, handle), lambda s: None)

    def session(self, key: AgentKey, session_id: str, *, guard: Guard | None = None) -> None:
        self._apply(guard, ('session', key.agent_session, session_id), lambda s: None)

    def finished(
        self, job: AgentJob | EvaluationJob, report: AgentReport | EvaluationReport, *, guard: Guard | None = None
    ) -> None:
        target = job.agent_session if isinstance(job, AgentJob) else job.evaluation
        self._apply(guard, ('finished', target, report), lambda s: None)

    def lost(self, key: JobKey, reason: str, *, guard: Guard | None = None) -> None:
        def change(s: Session) -> None:
            if isinstance(key, EvaluationKey):  # as core does: the evaluation waits for a slot again
                s.get_one(Evaluation, key.evaluation).started_at = None

        target = key.evaluation if isinstance(key, EvaluationKey) else key.agent_session
        self._apply(guard, ('lost', target, reason), change)

    def delivered(self, key: AgentKey, prompt: CommentId, error: str | None) -> None:
        self.calls.append(('delivered', key.agent_session, (prompt, error)))


@pytest.fixture
def recorder(db: Database) -> Recorder:
    return Recorder(db)


@pytest.fixture
def leases(db: Database, recorder: Recorder) -> Leases:
    return Leases(db, recorder, TIMING, POOLS)


def agent_job(db: Database, campaign: Campaign, tmp_path: Path) -> AgentJob:
    with db.session() as s:
        w = AgentSession(kind=WATCHDOG, campaign_id=campaign.id)
        s.add(w)
        s.commit()
        agent_session_id = w.id
    return AgentJob(
        agent_session=agent_session_id,
        runtime='subprocess',
        campaign=campaign.name,
        campaign_path=Path(campaign.path),
        artifact=Checkout(path=tmp_path),
        cwd=tmp_path,
        log_dir=tmp_path / 'logs',
        timeout=60,
        brief='b',
        spec=None,
    )


def evaluation_job(db: Database, campaign: Campaign, tmp_path: Path, runner: str = 'process') -> EvaluationJob:
    base = baseline(db, campaign.id)
    with db.session() as s:
        evaluation = Evaluation(candidate_id=base.id, evaluator_id=s.get_one(Campaign, campaign.id).evaluator_id)
        s.add(evaluation)
        s.commit()
        evaluation_id = evaluation.id
    return EvaluationJob(
        evaluation=evaluation_id,
        candidate=base.id,
        runner=runner,
        artifact=GitTree(repo=Path(campaign.path), commit=base.commit_sha),
        cwd=tmp_path / 'wt',
        log_dir=tmp_path,
        command=['true'],
        timeout=60,
        required=['score'],
        units_file=tmp_path / 'units.pending',
    )


def queue(db: Database, job: AgentJob | EvaluationJob) -> int:
    with db.session() as s:
        lease = enqueue(s, job)
        s.commit()
        return lease.id


def worker_row(db: Database, worker: str) -> LeaseWorker:
    with db.session() as s:
        return s.get_one(LeaseWorker, worker)


def lease_row(db: Database, lease_id: int) -> Lease:
    with db.session() as s:
        return s.get_one(Lease, lease_id)


def events(db: Database, kind: str) -> list[tuple[TargetKind, int, dict[str, object]]]:
    with db.session() as s:
        return [(e.target_kind, e.target_id, e.payload) for e in s.scalars(select(Event).where(Event.kind == kind))]


DONE = AgentFinished(process=AgentExit(ExitStatus.EXITED, 1.0, 0))
RECEIPT = {'reported': 'agent_finished'}
MEASURED = EvaluationReport(samples=[Sample(metrics={'score': 2.0}, seconds=1.0)])


def test_the_timing_is_kubernetes_two_stages_and_the_parameters_name_it_and_refuse_a_disorder() -> None:
    """Renewed every 10 s (the kubelet's node Lease), unreachable after 50 s (``--node-monitor-grace-period`` since
    Kubernetes 1.32), expired after 300 s (the unreachable taint's ``tolerationSeconds``). The worker stops two
    heartbeats before."""
    defaults = Workers()
    assert (defaults.renew_seconds, defaults.grace_seconds, defaults.toleration_seconds) == (10, 50, 300)
    assert Timing() == Timing(renew=10, grace=50, toleration=300) and Timing().self_stop == 280
    with pytest.raises(ValueError, match='renew_seconds < grace_seconds'):
        Workers(grace_seconds=400)


def test_a_lease_is_acquired_started_renewed_and_reported_once_with_its_receipt(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases, recorder: Recorder
) -> None:
    job = agent_job(db, campaign, tmp_path)
    lease_id = queue(db, job)
    grant = take(leases, 'w1', ['process'])
    assert grant is not None and (grant.lease, grant.job) == (lease_id, job)
    assert take(leases, 'w2', ['process']) is None, 'a job goes to one worker'
    handle = AgentHandle(runtime='subprocess', data={'pid': 7})
    Local(leases, 'w1').started(lease_id, handle, None)
    assert Local(leases, 'w1').heartbeat({lease_id: 'sid'}, HOST) == {lease_id: Answer.RENEWED}
    assert Local(leases, 'w1').report(lease_id, 'r1', DONE) == RECEIPT
    assert Local(leases, 'w1').report(lease_id, 'r1', DONE) == RECEIPT, 'a resent report gets its receipt'
    assert recorder.calls == [
        ('started', job.agent_session, {'pid': 7}),
        ('session', job.agent_session, 'sid'),
        ('finished', job.agent_session, DONE),
    ], "it applied once, with the runtime's handle"
    row = lease_row(db, lease_id)
    assert (row.state, row.worker, row.handle, row.request_id) == (LeaseState.REPORTED, 'w1', handle.model_dump(), 'r1')
    with pytest.raises(StaleLease, match=f'lease#{lease_id} is reported'):
        Local(leases, 'w1').report(lease_id, 'r2', DONE)


def test_a_report_with_an_old_fencing_token_is_refused_and_kept_as_a_stale_report(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases, recorder: Recorder
) -> None:
    """A worker presumed dead comes back after its lease expired and its job went to another: its report is refused."""
    job = evaluation_job(db, campaign, tmp_path)
    first = queue(db, job)
    assert take(leases, 'w1', ['process']) is not None
    leases.expire(utcnow() + timedelta(seconds=301))
    assert lease_row(db, first).state is LeaseState.EXPIRED
    queue(db, job)  # core submits the lost evaluation again
    second = take(leases, 'w2', ['process'])
    assert second is not None and second.lease > first, 'the fencing token grows with each lease of the job'
    with pytest.raises(StaleLease, match=f'stale_lease: lease#{first} is expired'):
        Local(leases, 'w1').report(first, 'late', MEASURED)
    assert Local(leases, 'w1').heartbeat({first: None}, HOST) == {first: Answer.STALE}
    assert [c[0] for c in recorder.calls] == ['lost'], 'nothing of the late report applied'
    [(kind, target, payload)] = events(db, STALE_REPORT)
    assert (kind, target, payload['lease'], payload['worker']) == (TargetKind.CANDIDATE, job.candidate, first, 'w1')
    assert payload['report'] == MEASURED.model_dump(mode='json')
    assert Local(leases, 'w2').report(second.lease, 'r', MEASURED) == {'reported': 'evaluation'}


def test_only_the_newest_lease_of_a_job_is_live(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases
) -> None:
    """The fence checks the token, not only the state: two live leases of one job cannot both report."""
    job = agent_job(db, campaign, tmp_path)
    old = queue(db, job)
    take(leases, 'w1', [])
    new = queue(db, job)
    take(leases, 'w2', [])
    with pytest.raises(StaleLease, match=f'lease#{old} was replaced by lease#{new}'):
        Local(leases, 'w1').started(old, HANDLE, None)
    with pytest.raises(StaleLease, match='held by w2'):
        Local(leases, 'w1').started(new, HANDLE, None)


def test_a_silent_worker_is_unreachable_after_the_grace_period_and_its_leases_expire_after_the_toleration(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases, recorder: Recorder
) -> None:
    job = agent_job(db, campaign, tmp_path)
    lease_id = queue(db, job)
    take(leases, 'w1', [])
    now = utcnow()
    expiry = leases.expire(now + timedelta(seconds=49))
    assert expiry is not None and timedelta(seconds=299) < expiry - now < timedelta(seconds=301), 'the next deadline'
    assert events(db, UNREACHABLE) == []
    leases.expire(now + timedelta(seconds=51))
    leases.expire(now + timedelta(seconds=52))
    assert events(db, UNREACHABLE) == [
        (TargetKind.AGENT_SESSION, job.agent_session, {'lease': lease_id, 'worker': 'w1'})
    ], 'once per silence'
    assert worker_row(db, 'w1').no_lease == NoLease.UNREACHABLE
    Local(leases, 'w1').heartbeat({lease_id: None}, HOST)
    assert worker_row(db, 'w1').no_lease is None, 'heard again'
    heard = utcnow()
    for seconds in range(10, 301, 10):  # the scheduler's ticks: no gap that looks like its own absence
        leases.expire(heard + timedelta(seconds=seconds))
    assert leases.expire(heard + timedelta(seconds=301)) is None
    row = lease_row(db, lease_id)
    assert row.state is LeaseState.EXPIRED and str(row.receipt['lost']).startswith('worker lost: w1 was silent for')
    [(_, _, payload)] = events(db, EXPIRED)
    assert payload['lease'] == lease_id and recorder.calls[-1][:2] == ('lost', job.agent_session)


def test_a_restarted_daemon_gives_every_live_lease_a_full_period(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases
) -> None:
    lease_id = queue(db, agent_job(db, campaign, tmp_path))
    take(leases, 'w1', [])
    with db.session() as s:
        s.get_one(Lease, lease_id).expires_at = utcnow() - timedelta(seconds=1)
        s.commit()
    leases.renew_all()
    leases.expire()
    assert lease_row(db, lease_id).state is LeaseState.ACQUIRED, "nothing expires for the daemon's own absence"


def test_a_revoked_lease_is_answered_revoked_and_still_reports(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases, recorder: Recorder
) -> None:
    lease_id = queue(db, agent_job(db, campaign, tmp_path))
    take(leases, 'w1', [])
    with db.session() as s:
        revoke(s, s.get_one(Lease, lease_id))
        s.commit()
    assert Local(leases, 'w1').heartbeat({lease_id: None}, HOST) == {lease_id: Answer.REVOKED}
    assert Local(leases, 'w1').report(lease_id, 'r', DONE) == RECEIPT


def test_an_evaluation_goes_to_a_worker_with_its_runner_in_the_order_in_which_it_was_submitted(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases
) -> None:
    """Core submits an evaluation when it has a slot, in the queue's order: the leases keep that order."""
    other = evaluation_job(db, campaign, tmp_path, runner='gpu-runner')
    queue(db, other)
    assert take(leases, 'w1', ['process']) is None, 'no worker has the runner'
    job = evaluation_job_for_another_candidate(db, campaign, tmp_path)
    queue(db, job)
    grant = take(leases, 'w1', ['process'])
    assert grant is not None and isinstance(grant.job, EvaluationJob) and grant.job.evaluation == job.evaluation
    second = take(leases, 'w2', ['process', 'gpu-runner'])
    assert second is not None and isinstance(second.job, EvaluationJob) and second.job.evaluation == other.evaluation


def evaluation_job_for_another_candidate(db: Database, campaign: Campaign, tmp_path: Path) -> EvaluationJob:
    """An evaluation of a second candidate: one candidate and evaluator have one evaluation."""
    base = baseline(db, campaign.id)
    with db.session() as s:
        candidate = Candidate(campaign_id=campaign.id, tree_sha='t2', commit_sha=base.commit_sha, parent_id=base.id)
        s.add(candidate)
        s.flush()
        evaluation = Evaluation(candidate_id=candidate.id, evaluator_id=s.get_one(Campaign, campaign.id).evaluator_id)
        s.add(evaluation)
        s.commit()
        evaluation_id, candidate_id = evaluation.id, candidate.id
    return EvaluationJob(
        evaluation=evaluation_id,
        candidate=candidate_id,
        runner='process',
        artifact=GitTree(repo=Path(campaign.path), commit=base.commit_sha),
        cwd=tmp_path / 'wt2',
        log_dir=tmp_path,
        command=['true'],
        timeout=60,
        required=['score'],
        units_file=tmp_path / 'units2.pending',
    )


def test_a_worker_finds_its_live_leases_again_with_their_handles(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases
) -> None:
    job = agent_job(db, campaign, tmp_path)
    lease_id = queue(db, job)
    take(leases, 'w1', [])
    Local(leases, 'w1').started(lease_id, AgentHandle(runtime='subprocess', data={'pid': 1}), None)
    [grant] = join(leases, 'w1', []).held
    assert (grant.lease, grant.job, grant.handle) == (lease_id, job, AgentHandle(runtime='subprocess', data={'pid': 1}))
    assert grant.started_at is not None and join(leases, 'w2', []).held == []
    Local(leases, 'w1').lost(lease_id, 'gone')
    assert join(leases, 'w1', []).held == [] and lease_row(db, lease_id).receipt == {'lost': 'gone'}


def test_a_revoke_or_a_cancel_changes_only_a_lease_it_applies_to(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases
) -> None:
    """A queued lease has no worker to revoke; a reported one is over, and a cancel leaves it as it is."""
    queued = queue(db, agent_job(db, campaign, tmp_path))
    with db.session() as s:
        revoke(s, s.get_one(Lease, queued))
        s.commit()
    assert lease_row(db, queued).revoked_at is None
    take(leases, 'w1', [])
    Local(leases, 'w1').report(queued, 'r', DONE)
    with db.session() as s:
        cancel(s, s.get_one(Lease, queued), 'daemon stopped')
        s.commit()
    assert lease_row(db, queued).state is LeaseState.REPORTED


def test_a_report_on_a_lease_that_never_was_is_refused_and_kept_nowhere(db: Database, leases: Leases) -> None:
    with pytest.raises(StaleLease, match='there is no lease#9'):
        Local(leases, 'w1').report(9, 'r', DONE)
    assert events(db, STALE_REPORT) == []


def test_the_fence_and_the_write_are_one_transaction_so_an_expiry_cannot_slip_between_them(
    db: Database, campaign: Campaign, tmp_path: Path, recorder: Recorder
) -> None:
    """An expiry that comes while a report applies waits for it, then finds the lease reported: one of them applies."""
    other = Recorder(db)
    expiring = Leases(db, other, TIMING, POOLS)
    expiries: list[threading.Thread] = []

    class Slow(Recorder):
        def finished(
            self, job: AgentJob | EvaluationJob, report: AgentReport | EvaluationReport, *, guard: Guard | None = None
        ) -> None:
            def slow(s: Session) -> None:
                assert guard is not None
                guard(s)
                thread = threading.Thread(target=expiring.expire, args=(utcnow() + timedelta(seconds=301),))
                thread.start()
                expiries.append(thread)
                time.sleep(0.5)  # the expiry runs now, between the fence and the commit

            super().finished(job, report, guard=slow)

    leases = Leases(db, Slow(db), TIMING, POOLS)
    job = evaluation_job(db, campaign, tmp_path)
    lease_id = queue(db, job)
    take(leases, 'w1', ['process'])
    Local(leases, 'w1').report(lease_id, 'r', MEASURED)
    expiries[0].join(30)
    with db.session() as s:
        assert [lease.state for lease in s.scalars(select(Lease))] == [LeaseState.REPORTED], 'not queued again'
    assert other.calls == [] and events(db, EXPIRED) == []


def test_a_daemon_that_was_away_longer_than_the_grace_period_renews_the_leases_before_it_expires_any(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases, recorder: Recorder
) -> None:
    """A laptop asleep for an hour: no tick ran, and the daemon's own absence is no worker's silence."""
    lease_id = queue(db, agent_job(db, campaign, tmp_path))
    take(leases, 'w1', [])
    now = utcnow()
    leases.expire(now)
    leases.expire(now + timedelta(hours=1))  # the first tick after the resume
    assert (lease_row(db, lease_id).state, recorder.calls) == (LeaseState.ACQUIRED, [])
    assert events(db, UNREACHABLE) == [], 'and not unreachable either'


def test_an_unreachable_worker_counts_no_slot_until_it_is_heard_again(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases
) -> None:
    held = queue(db, agent_job(db, campaign, tmp_path))
    take(leases, 'w1', [])
    assert leases.capacity() == Capacity(None, 99)
    with db.session() as s:
        s.get_one(LeaseWorker, 'w1').seen_at = utcnow() - timedelta(seconds=51)
        s.commit()
    assert leases.capacity() == Capacity(0, 0), 'unreachable'
    Local(leases, 'w1').heartbeat({held: None}, HOST)
    assert leases.capacity() == Capacity(None, 99)


class Settles(Recorder):
    """Core that finds every job's row settled already: closed by a stop, measured, or gone."""

    def _apply(self, guard: Guard | None, call: tuple[str, int, object], change: Callable[[Session], None]) -> None:
        raise Settled('agent_session#1 is closed')


def test_a_job_whose_row_is_settled_is_refused_as_stale_and_its_lease_ends_all_the_same(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """Core refuses a result for a closed row (``Settled``): the fence treats it as stale, and nothing else changes."""
    leases = Leases(db, Settles(db), TIMING, POOLS)
    job = agent_job(db, campaign, tmp_path)
    lease_id = queue(db, job)
    take(leases, 'w1', [])
    with pytest.raises(StaleLease, match='stale_lease: agent_session#1 is closed'):
        Local(leases, 'w1').started(lease_id, AgentHandle(runtime='subprocess', data={'pid': 1}), None)
    assert Local(leases, 'w1').heartbeat({lease_id: 'sid'}, HOST) == {lease_id: Answer.RENEWED}, (
        'the session id waits for the end'
    )
    with pytest.raises(StaleLease, match='stale_lease: agent_session#1 is closed'):
        Local(leases, 'w1').report(lease_id, 'r', DONE)
    [(_, _, payload)] = events(db, STALE_REPORT)
    assert payload['reason'] == 'agent_session#1 is closed'
    Local(leases, 'w1').lost(lease_id, 'gone')
    assert lease_row(db, lease_id).receipt == {'lost': 'gone', 'settled': 'agent_session#1 is closed'}


def test_an_adoption_ends_the_evaluation_leases_that_no_worker_took(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases
) -> None:
    """The next daemon asks again for the measurements that it needs: the previous daemon's untaken ones would be lost
    work."""
    lease_id = queue(db, evaluation_job(db, campaign, tmp_path))
    assert leases.adopt([], 'local') == []
    assert (lease_row(db, lease_id).state, lease_row(db, lease_id).receipt) == (
        LeaseState.CANCELLED,
        {'cancelled': NOT_TAKEN},
    )


def test_an_expiry_that_a_report_overtakes_expires_nothing(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases, recorder: Recorder, mocker: MockerFixture
) -> None:
    """The report comes between the expiry's look for silent leases and its write: one of them applies, the report."""
    lease_id = queue(db, agent_job(db, campaign, tmp_path))
    take(leases, 'w1', [])
    lose = Leases._lose

    def heard_first(self: Leases, lease_id: int, reason: str, fence: Callable[[Session], Lease]) -> None:
        self.report(lease_id, 'w1', 'r', DONE)
        lose(self, lease_id, reason, fence)

    mocker.patch.object(Leases, '_lose', heard_first)
    leases.expire(utcnow() + timedelta(seconds=301))
    assert (lease_row(db, lease_id).state, [c[0] for c in recorder.calls]) == (LeaseState.REPORTED, ['finished'])


def test_a_stop_leaves_an_agent_lease_that_a_worker_took_meanwhile_to_its_report(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    class TakenFirst(Recorder):
        def finished(
            self, job: AgentJob | EvaluationJob, report: AgentReport | EvaluationReport, *, guard: Guard | None = None
        ) -> None:
            take(leases, 'w1', [])  # between the stop's look for untaken leases and its write
            super().finished(job, report, guard=guard)

    recorder = TakenFirst(db)
    leases = Leases(db, recorder, TIMING, POOLS)
    lease_id = queue(db, agent_job(db, campaign, tmp_path))
    leases.stop_untaken()
    assert (lease_row(db, lease_id).state, recorder.calls) == (LeaseState.ACQUIRED, [])


def test_a_worker_reads_its_messages_work_on_a_submit_a_revoke_and_a_prompt_that_it_answers(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases, recorder: Recorder
) -> None:
    """The GitHub Actions runner's long-poll: it returns at once when a message waits, and drops what was read."""
    w1, w2 = Local(leases, 'w1'), Local(leases, 'w2')
    join(leases, 'w1', [])
    join(leases, 'w2', [])
    assert w1.messages(0, 0.01) == [], 'nothing waits: it waits, then returns empty'
    job = agent_job(db, campaign, tmp_path)
    leases.submit(job)
    leases.submit(agent_job(db, campaign, tmp_path))
    [work] = w1.messages(0, 5)
    assert isinstance(work, Work) and [type(m) for m in w2.messages(0, 5)] == [Work], 'one Work for two jobs, to each'
    assert w1.messages(work.id, 0.01) == [], 'read: it goes'
    grant = w1.acquire(HOST)
    assert isinstance(grant, Grant)
    w1.started(grant.lease, HANDLE, None)
    leases.prompt(AgentKey(job.agent_session), Prompt(CommentId(7), 'try lzma'))
    leases.revoke(AgentKey(job.agent_session))
    prompt, stop = w1.messages(work.id, 5)
    assert isinstance(prompt, PromptMessage) and (prompt.lease, prompt.prompt) == (
        grant.lease,
        Prompt(CommentId(7), 'try lzma'),
    )
    assert isinstance(stop, Revoke) and stop.lease == grant.lease
    assert [type(m) for m in w2.messages(0, 0.01)] == [Work], "the other worker's messages are its own"
    w1.delivered(prompt.id, None)
    w1.delivered(prompt.id, None)
    assert [c for c in recorder.calls if c[0] == 'delivered'] == [('delivered', job.agent_session, (7, None))], 'once'


def test_a_prompt_for_an_agent_without_a_live_lease_is_undelivered_at_once(
    db: Database, campaign: Campaign, tmp_path: Path, leases: Leases, recorder: Recorder
) -> None:
    job = agent_job(db, campaign, tmp_path)
    queue(db, job)
    leases.prompt(AgentKey(job.agent_session), Prompt(CommentId(3), 'x'))
    assert recorder.calls == [('delivered', job.agent_session, (3, 'the agent has no live lease'))]
