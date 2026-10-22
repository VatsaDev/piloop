"""Leases, a building block for executors: how a job reaches a worker, wherever it runs, and how its report is fenced.

Core does not use this module: an executor opts in. The built-in ``workers`` executor runs every job on a lease, with
its worker in ``piloop serve``; remote workers take leases through the same operations. The words:

- A **lease** is a time-limited claim on one job by one worker. ``Leases.submit`` inserts it ``queued`` with the job's
  description. A worker acquires it, starts the job, renews the lease with a heartbeat, and reports.
- A **fencing token** is a number that grows with each new lease of a job: the lease id. A report with an old token
  is refused (``StaleLease``), so a worker that was presumed dead can't overwrite newer work. Its report stays as a
  ``lease.stale_report`` event on the job's row, with what it brought back.
- The **grace period** (``Timing.grace``, 50 s) is the silence after which a worker is unreachable: its leases are
  marked so, and it gets no new lease until it is heard again. After the **toleration** (``Timing.toleration``,
  300 s) its leases **expire**: a run fails as ``worker lost``, which is not an attempt, and an evaluation is queued
  again on a new lease. A worker that hears nothing for that long, less two heartbeats, stops its own jobs first, so a
  job never runs twice.

The timing is the two stages of Kubernetes: node leases (the kubelet renews every 10 s), the unreachable taint after
``--node-monitor-grace-period``, and eviction after the taint's ``tolerationSeconds`` of 300.

The fence is the executor's, not core's: core checks only the domain (``Settled``). Every operation that checks it
runs in one write transaction with the core operation that it guards (``Results``), so a report, a heartbeat and an
expiry never both apply. The ``leases`` table belongs here, but its model and migration stay in core's schema until
the plugin host gives a plugin tables of its own: one schema, one migration chain.
"""

import contextlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum

from sqlalchemy import ColumnElement, func, select, update
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.db.events import emit
from piloop.db.models import (
    LIVE_LEASES,
    OPEN_LEASES,
    Lease,
    LeaseKind,
    LeaseState,
    TargetKind,
    utcnow,
)
from piloop.db.session import Database, write_transaction
from piloop.plugins.executor.contract import (
    JOB,
    REPORT,
    AgentFinished,
    AgentJob,
    AgentKey,
    AgentNotStarted,
    AgentReport,
    EvaluationJob,
    EvaluationKey,
    EvaluationReport,
    Guard,
    JobKey,
    LeftJob,
    Results,
    Settled,
)

LOCAL_WORKER = 'local'
"""The id of the worker inside ``piloop serve``. It is stable, so the next daemon's worker finds its leases again."""
UNREPORTED = 'the daemon stopped, and the job did not report in time'
NOT_TAKEN = 'the daemon stopped before a worker took the job'
STALE_REPORT = 'lease.stale_report'
"""An event on the job's row (the agent session, or the evaluated candidate): a report that the fence refused."""
UNREACHABLE = 'lease.unreachable'
"""An event on the job's row: its worker was silent past ``grace_seconds``."""
EXPIRED = 'lease.expired'
"""An event on the job's row: the lease expired, or its worker reported the job gone. ``reason`` says which."""


@dataclass(frozen=True)
class Timing:
    """The three numbers of a lease, after Kubernetes. The ``workers`` executor takes them as ``renew_seconds``..."""

    renew: float = 10
    """How often a worker sends a heartbeat. The kubelet renews its node Lease every 10 s."""
    grace: float = 50
    """The silence after which a worker is unreachable. ``--node-monitor-grace-period``: 50 s since Kubernetes 1.32."""
    toleration: float = 300
    """The silence after which a lease expires and its job is queued again. The ``tolerationSeconds`` of the
    unreachable taint: 300 s."""

    @property
    def self_stop(self) -> float:
        """The silence after which a worker stops its own jobs: two heartbeats before its leases expire."""
        return self.toleration - 2 * self.renew


DEFAULT_TIMING = Timing()


class StaleLease(Exception):
    """A worker's call on a lease that is not its live, newest lease: refused, and nothing changed. ``stale_lease``."""


class Answer(StrEnum):
    """What a heartbeat answers for one lease."""

    RENEWED = 'renewed'
    REVOKED = 'revoked'
    """Stop the job, and still report it: a kill or a cancel."""
    STALE = 'stale'
    """The lease is no longer the worker's: stop the job. Its report will be refused."""


@dataclass(frozen=True)
class Grant:
    """A lease that a worker holds: its id, which is its fencing token, and its job."""

    lease: int
    job: AgentJob | EvaluationJob
    handle: dict[str, object]
    """What ``started`` recorded, for a worker that finds its job again after a restart. Empty before the start."""
    started_at: datetime | None


def enqueue(s: Session, job: AgentJob | EvaluationJob) -> Lease:
    """Insert the ``queued`` lease of ``job`` in the caller's transaction. The caller commits, then ``Leases.notify``."""
    lease = Lease(kind=LeaseKind(job.kind), job=JOB.dump_python(job, mode='json'))
    if isinstance(job, AgentJob):
        lease.agent_session_id = job.agent_session
    else:
        lease.evaluation_id = job.evaluation
    s.add(lease)
    s.flush()
    return lease


def of_agent(s: Session, agent_session_id: int) -> Lease | None:
    """Return the newest lease of an agent session, or None when it has none."""
    return s.scalars(
        select(Lease).where(Lease.agent_session_id == agent_session_id).order_by(Lease.id.desc()).limit(1)
    ).first()


def lease_of(s: Session, key: JobKey) -> Lease | None:
    """Return the newest lease of the job of ``key``, or None when it has none."""
    if isinstance(key, AgentKey):
        return of_agent(s, key.agent_session)
    return s.scalars(
        select(Lease).where(Lease.evaluation_id == key.evaluation).order_by(Lease.id.desc()).limit(1)
    ).first()


def revoke(s: Session, lease: Lease) -> None:
    """Ask the worker to stop a live lease's job, a kill or a cancel. It still reports. The caller commits."""
    if lease.state in LIVE_LEASES and lease.revoked_at is None:
        lease.revoked_at = utcnow()


def cancel(s: Session, lease: Lease, reason: str) -> None:
    """End a lease that no worker will report: a queued one, or every one at a daemon stop. The caller commits."""
    if lease.state in OPEN_LEASES:
        receipt: dict[str, object] = {'cancelled': reason}
        lease.state, lease.ended_at, lease.receipt = LeaseState.CANCELLED, utcnow(), receipt


def _target(lease: Lease) -> tuple[TargetKind, int]:
    """The row that a lease's events go on: its agent session, or the candidate that its evaluation measures."""
    if lease.agent_session_id is not None:
        return TargetKind.AGENT_SESSION, lease.agent_session_id
    job = JOB.validate_python(lease.job)
    assert isinstance(job, EvaluationJob)
    return TargetKind.CANDIDATE, job.candidate


def _job_of(lease: Lease) -> ColumnElement[bool]:
    """Select the leases of the job of ``lease``."""
    if lease.agent_session_id is not None:
        return Lease.agent_session_id == lease.agent_session_id
    return Lease.evaluation_id == lease.evaluation_id


class Leases:
    """The lease operations, and the signal that wakes a worker of this process on a change.

    Every operation that checks the fence runs in one write transaction (``write_transaction``) with the core
    operation that it guards (``results``), so a report, a heartbeat and an expiry never both apply to one lease. A
    result that core refuses (``Settled``) is refused as stale.
    """

    def __init__(self, db: Database, results: Results, timing: Timing = DEFAULT_TIMING) -> None:
        self._db = db
        self._results = results
        self.timing = timing
        self._expired_at: datetime | None = None
        """When ``expire`` last ran, to tell a gap in the scheduler's ticks (a suspend) from a silent worker."""
        self._listeners: list[Callable[[], None]] = []

    def listen(self, listener: Callable[[], None]) -> None:
        """Call ``listener`` after every change of a lease made in this process: the worker wakes on it."""
        self._listeners.append(listener)

    def notify(self) -> None:
        """Wake the listeners. Called after a commit that changed a lease."""
        for listener in self._listeners:
            listener()

    def acquire(self, worker: str, *, runners: Sequence[str]) -> Grant | None:
        """Take the oldest queued job that ``worker`` can run, or return None.

        Agent jobs go first. An evaluation job goes only to a worker with its runner. Core submits an evaluation only
        when it has a slot, in the order of its queue, so the order of the leases is that order. An unreachable worker
        gets nothing until a heartbeat of it is heard.
        """
        with self._db.session() as s:
            write_transaction(s)
            unreachable = select(Lease.id).where(
                Lease.worker == worker, Lease.state.in_(LIVE_LEASES), Lease.unreachable_at.is_not(None)
            )
            if s.scalar(unreachable.limit(1)) is not None:
                return None
            lease = s.scalars(
                select(Lease)
                .where(Lease.kind == LeaseKind.AGENT, Lease.state == LeaseState.QUEUED)
                .order_by(Lease.id)
                .limit(1)
            ).first()
            if lease is None:
                lease = s.scalars(
                    select(Lease)
                    .where(Lease.kind == LeaseKind.EVALUATION, Lease.state == LeaseState.QUEUED)
                    .where(Lease.job['runner'].as_string().in_(runners))
                    .order_by(Lease.id)
                    .limit(1)
                ).first()
            if lease is None:
                return None
            now = utcnow()
            lease.state, lease.worker, lease.acquired_at = LeaseState.ACQUIRED, worker, now
            lease.renewed_at, lease.expires_at = now, now + timedelta(seconds=self.timing.toleration)
            s.commit()
            grant = Grant(lease.id, JOB.validate_python(lease.job), {}, None)
        self.notify()
        return grant

    def held(self, worker: str) -> list[Grant]:
        """Return the live leases of ``worker``: what a restarted worker finds again."""
        with self._db.session() as s:
            rows = s.scalars(
                select(Lease).where(Lease.worker == worker, Lease.state.in_(LIVE_LEASES)).order_by(Lease.id)
            ).all()
            return [Grant(r.id, JOB.validate_python(r.job), dict(r.handle), r.started_at) for r in rows]

    def _live(self, s: Session, lease_id: int, worker: str) -> Lease:
        """Return the lease if it is ``worker``'s, live and the newest of its job: the fence. Else raise.

        The caller holds the write transaction (``write_transaction``) in which it then writes.
        """
        lease = s.get_one(Lease, lease_id)  # no lease is deleted: ``_job`` or ``held`` found it
        if lease.worker != worker or lease.state not in LIVE_LEASES:
            raise StaleLease(f'stale_lease: lease#{lease_id} is {lease.state}, held by {lease.worker}')
        newest = s.scalar(select(func.max(Lease.id)).where(_job_of(lease)))
        if newest != lease.id:
            raise StaleLease(f'stale_lease: lease#{lease_id} was replaced by lease#{newest}')
        return lease

    def _job(self, lease_id: int) -> Lease:
        """Read the lease, for what does not change on it (its job). Raises ``StaleLease`` when there is none."""
        with self._db.session() as s:
            lease = s.get(Lease, lease_id)
        if lease is None:
            raise StaleLease(f'stale_lease: there is no lease#{lease_id}')
        return lease

    def _fence(self, lease_id: int, worker: str) -> Guard:
        """A guard that refuses (``StaleLease``) unless the lease is ``worker``'s, live and the newest of its job."""

        def fence(s: Session) -> None:
            self._live(s, lease_id, worker)

        return fence

    def started(self, lease_id: int, worker: str, handle: dict[str, object], session_id: str | None = None) -> None:
        """Record that the job started, and where (``handle``). Raises ``StaleLease``: then stop the job."""

        def mark(s: Session) -> None:
            lease = self._live(s, lease_id, worker)
            lease.state, lease.handle = LeaseState.RUNNING, handle
            lease.started_at = lease.started_at or utcnow()

        agent_session = self._job(lease_id).agent_session_id
        if agent_session is None:  # an evaluator: core needs nothing of its start
            with self._db.session() as s:
                write_transaction(s)
                mark(s)
                s.commit()
        else:
            data = handle['data']
            assert isinstance(data, dict)
            try:
                self._results.started(AgentKey(agent_session), data, session_id, guard=mark)
            except Settled as e:
                raise StaleLease(f'stale_lease: {e}') from e
        self.notify()

    def heartbeat(self, worker: str, sessions: dict[int, str | None]) -> dict[int, Answer]:
        """Renew the leases of ``worker`` that it still runs, by id, with the session id each agent reported.

        The answer for each says go on, stop (``REVOKED``, and report), or the lease is no longer yours (``STALE``).
        """
        answers: dict[int, Answer] = {}
        reported: list[tuple[int, int, str]] = []
        now = utcnow()
        with self._db.session() as s:
            write_transaction(s)
            for lease_id, session_id in sessions.items():
                try:
                    lease = self._live(s, lease_id, worker)
                except StaleLease:
                    answers[lease_id] = Answer.STALE
                    continue
                lease.renewed_at, lease.unreachable_at = now, None
                lease.expires_at = now + timedelta(seconds=self.timing.toleration)
                if session_id is not None and lease.agent_session_id is not None:
                    reported.append((lease_id, lease.agent_session_id, session_id))
                answers[lease_id] = Answer.REVOKED if lease.revoked_at is not None else Answer.RENEWED
            s.commit()
        for lease_id, agent_session, session_id in reported:
            # a row that closed, or a lease that went, meanwhile: the next heartbeat's answer says so
            with contextlib.suppress(Settled, StaleLease):
                self._results.session(AgentKey(agent_session), session_id, guard=self._fence(lease_id, worker))
        return answers

    def report(
        self, lease_id: int, worker: str, request_id: str, report: AgentReport | EvaluationReport
    ) -> dict[str, object]:
        """Apply the job's result, and return the receipt. A report sent again with its request id gets the same receipt.

        Raises:
            StaleLease: the fence refused it. It is kept as a ``STALE_REPORT`` event, and nothing else changed.
        """
        lease = self._job(lease_id)
        if lease.state is LeaseState.REPORTED and lease.request_id == request_id:
            return dict(lease.receipt)
        receipt: dict[str, object] = {'reported': report.kind}

        def mark(s: Session) -> None:
            live = self._live(s, lease_id, worker)
            live.state, live.ended_at, live.request_id, live.receipt = (
                LeaseState.REPORTED,
                utcnow(),
                request_id,
                receipt,
            )

        try:
            self._results.finished(JOB.validate_python(lease.job), report, guard=mark)
        except (StaleLease, Settled) as e:
            self._keep_stale(lease, worker, report, str(e))
            if isinstance(e, Settled):
                raise StaleLease(f'stale_lease: {e}') from e
            raise
        self.notify()
        return receipt

    def _keep_stale(self, lease: Lease, worker: str, report: AgentReport | EvaluationReport, reason: str) -> None:
        """Keep a refused report as a ``STALE_REPORT`` event on its job's row, with the ref of its commits."""
        payload: dict[str, object] = {
            'lease': lease.id,
            'worker': worker,
            'reason': reason,
            'report': REPORT.dump_python(report, mode='json'),
        }
        job = JOB.validate_python(lease.job)
        if isinstance(job, AgentJob) and job.commits is not None and isinstance(report, AgentFinished):
            payload['ref'] = job.commits.ref if report.head is not None else None
        kind, target = _target(lease)
        with self._db.session() as s:
            emit(s, STALE_REPORT, kind, target, payload=payload)
            s.commit()

    def lost(self, lease_id: int, worker: str, reason: str) -> None:
        """End the lease as lost: its worker restarted and cannot find its job. Raises ``StaleLease``."""
        self._lose(lease_id, reason, lambda s: self._live(s, lease_id, worker))
        self.notify()

    def _lose(self, lease_id: int, reason: str, fence: Callable[[Session], Lease]) -> None:
        """End the lease as expired, and tell core that its job is lost, if ``fence`` still finds the lease its own.

        A job whose row closed elsewhere (a daemon stop) has nothing to lose: the lease ends all the same, and its
        receipt says so.
        """
        receipt: dict[str, object] = {'lost': reason}

        def mark(s: Session) -> None:
            lease = fence(s)
            lease.state, lease.ended_at, lease.receipt = LeaseState.EXPIRED, utcnow(), receipt
            kind, target = _target(lease)
            emit(s, EXPIRED, kind, target, payload={'lease': lease.id, 'worker': lease.worker, 'reason': reason})

        lease = self._job(lease_id)
        key: JobKey
        if lease.agent_session_id is not None:
            key = AgentKey(lease.agent_session_id)
        else:
            assert lease.evaluation_id is not None
            key = EvaluationKey(lease.evaluation_id)  # core submits it again
        try:
            self._results.lost(key, reason, guard=mark)
        except Settled as e:
            with self._db.session() as s:
                write_transaction(s)
                mark(s)
                s.get_one(Lease, lease_id).receipt = {**receipt, 'settled': str(e)}
                s.commit()

    def expire(self, now: datetime | None = None) -> datetime | None:
        """Mark the leases of silent workers unreachable, and expire those past their time. Return the next expiry.

        The scheduler runs this on every tick. The returned time is when it next has something to do, for a daemon
        that wakes on deadlines. A gap since the last tick longer than ``Timing.grace`` is the daemon's own absence (a
        laptop asleep, a stopped process), not a worker's silence: every live lease then gets a full period, as at a
        start (``renew_all``), and nothing expires.
        """
        now = now or utcnow()
        last, self._expired_at = self._expired_at, now
        if last is not None and now - last > timedelta(seconds=self.timing.grace):
            self.renew_all(now)
            return now + timedelta(seconds=self.timing.toleration)
        grace = timedelta(seconds=self.timing.grace)
        silent = (Lease.expires_at <= now) | (Lease.unreachable_at.is_(None) & (Lease.renewed_at <= now - grace))
        live = Lease.state.in_(LIVE_LEASES)
        expiring: list[tuple[int, str]] = []
        with self._db.session() as s:
            if s.scalar(select(Lease.id).where(live, silent).limit(1)) is not None:  # the write lock only when needed
                write_transaction(s)
                for lease in s.scalars(select(Lease).where(live, silent).order_by(Lease.id)).all():
                    assert lease.expires_at is not None and lease.renewed_at is not None  # set by acquire
                    if lease.expires_at <= now:
                        silence = round((now - lease.renewed_at).total_seconds())
                        expiring.append((lease.id, f'worker lost: {lease.worker} was silent for {silence} s'))
                    else:
                        lease.unreachable_at = now
                        kind, target = _target(lease)
                        emit(s, UNREACHABLE, kind, target, payload={'lease': lease.id, 'worker': lease.worker})
                s.commit()
        for lease_id, reason in expiring:
            with contextlib.suppress(StaleLease):  # a report or a heartbeat came first, in the meantime
                self._lose(lease_id, reason, lambda s, lease_id=lease_id: self._still_expired(s, lease_id, now))
        with self._db.session() as s:
            following = s.scalar(select(func.min(Lease.expires_at)).where(live))
        changed = bool(expiring)
        if changed:
            self.notify()
        return following

    def _still_expired(self, s: Session, lease_id: int, now: datetime) -> Lease:
        """Return the lease if it is still live and past its expiry at ``now``, in the transaction that expires it."""
        lease = s.get_one(Lease, lease_id)
        if lease.state not in LIVE_LEASES or lease.expires_at is None or lease.expires_at > now:
            raise StaleLease(f'stale_lease: lease#{lease_id} is {lease.state}, no longer silent')
        return lease

    def renew_all(self, now: datetime | None = None) -> None:
        """Give every live lease a full period: a daemon that starts again does not expire work for its own absence."""
        now = now or utcnow()
        with self._db.session() as s:
            write_transaction(s)
            s.execute(
                update(Lease)
                .where(Lease.state.in_(LIVE_LEASES))
                .values(renewed_at=now, unreachable_at=None, expires_at=now + timedelta(seconds=self.timing.toleration))
            )
            s.commit()

    def submit(self, job: AgentJob | EvaluationJob) -> None:
        """Queue ``job`` on a new lease, and wake the listeners."""
        with self._db.session() as s:
            enqueue(s, job)
            s.commit()
        self.notify()

    def revoke(self, key: JobKey) -> None:
        """Ask the worker of the job's live lease to stop it (``revoke``). Its job still reports."""
        with self._db.session() as s:
            lease = lease_of(s, key)
            if lease is not None:  # ``revoke`` changes a live lease only
                revoke(s, lease)
                s.commit()
        self.notify()

    def adopt(self, left: Sequence[LeftJob], worker: str) -> list[JobKey]:
        """Give every live lease a full period, and return the jobs of ``left`` that have no open lease.

        A job on an open lease goes on: its worker finds it again (``held``). A running agent that a daemon before
        leases started goes on a live lease of ``worker`` (``_adopt_running``).
        """
        self.renew_all()
        refused: list[JobKey] = []
        with self._db.session() as s:
            write_transaction(s)
            # an evaluation that no worker took is the previous daemon's: this one asks again for what it needs
            untaken = (Lease.kind == LeaseKind.EVALUATION) & (Lease.state == LeaseState.QUEUED)
            for lease in s.scalars(select(Lease).where(untaken)).all():
                cancel(s, lease, NOT_TAKEN)
            for job in left:
                lease = lease_of(s, job.key)
                if lease is None and _adopt_running(s, job, worker, self.timing):
                    continue
                if lease is None or lease.state not in OPEN_LEASES:
                    refused.append(job.key)
            s.commit()
        return refused

    def stop_untaken(self) -> None:
        """End the agent leases that no worker took, with a result that says so: what waits for each returns."""
        with self._db.session() as s:
            queued = (Lease.kind == LeaseKind.AGENT) & (Lease.state == LeaseState.QUEUED)
            untaken = s.scalars(select(Lease).where(queued)).all()
        for lease in untaken:

            def take(s: Session, lease_id: int = lease.id) -> None:
                found = s.get_one(Lease, lease_id)
                if found.state is not LeaseState.QUEUED:
                    raise StaleLease(f'stale_lease: lease#{lease_id} was taken')
                cancel(s, found, NOT_TAKEN)

            report = AgentNotStarted(reason=NOT_TAKEN)
            try:
                self._results.finished(AgentJob.model_validate(lease.job), report, guard=take)
            except StaleLease:  # a worker took it meanwhile: it reports
                continue
            except Settled:  # its row was closed already: nothing waits for it
                with self._db.session() as s:
                    write_transaction(s)
                    take(s)
                    s.commit()

    def end_unreported(self) -> None:
        """End the live agent leases: a report that comes later is refused, and no next worker takes them."""
        with self._db.session() as s:
            write_transaction(s)
            live = (Lease.kind == LeaseKind.AGENT) & Lease.state.in_(LIVE_LEASES)
            for lease in s.scalars(select(Lease).where(live)).all():
                cancel(s, lease, UNREPORTED)
            s.commit()


@deprecated.until('0.3.0', 'an agent that a daemon before leases started: delete _adopt_running')
def _adopt_running(s: Session, left: LeftJob, worker: str, timing: Timing) -> bool:
    """Put a running agent that a daemon before leases started on a live lease of ``worker``, and return True.

    It keeps running, and its work is scored. False for every other job without a lease.
    """
    if left.job is None or left.handle is None or not left.job.cwd.is_dir():
        return False
    lease = enqueue(s, left.job)
    now = utcnow()
    handle: dict[str, object] = {'runtime': left.job.runtime, 'data': left.handle}
    lease.state, lease.worker, lease.handle = LeaseState.RUNNING, worker, handle
    lease.acquired_at = lease.started_at = left.started_at or now
    lease.renewed_at, lease.expires_at = now, now + timedelta(seconds=timing.toleration)
    return True
