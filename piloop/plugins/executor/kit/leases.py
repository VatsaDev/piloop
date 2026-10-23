"""Leases, a building block for executors: how a job reaches a worker, wherever it runs, and how its report is fenced.

Core does not use this module: an executor opts in. The built-in ``workers`` executor runs every job on a lease, with
its worker in ``piloop serve``; remote workers take leases through the same operations. The words:

- A **lease** is a time-limited claim on one job by one worker. ``Leases.submit`` inserts it ``queued`` with the job's
  description. A worker acquires it, starts the job, renews the lease with a heartbeat, and reports.
- A **fencing token** is a number that grows with each new lease of a job: the lease id. A report with an old token
  is refused (``StaleLease``), so a worker that was presumed dead can't overwrite newer work. Its report stays as a
  ``lease.stale_report`` event on the job's row, with the addresses of what it brought back in the CAS.
- The **grace period** (``Timing.grace``, 50 s) is the silence after which a worker is unreachable: its slots count
  zero until it is heard again. After the **toleration** (``Timing.toleration``,
  300 s) its leases **expire**: a run ends ``worker_lost``, which is not an attempt, and an evaluation is queued
  again on a new lease. A worker that hears nothing for that long, less two heartbeats, stops its own jobs first, so a
  job never runs twice.

The timing is the two stages of Kubernetes: node leases (the kubelet renews every 10 s), the unreachable taint after
``--node-monitor-grace-period``, and eviction after the taint's ``tolerationSeconds`` of 300.

Each worker has a row in ``lease_workers``: its pool (``capacity.Pool``) and the host sample of its last call. A worker
takes a job only while ``capacity.admits`` it, and ``Leases.capacity`` sums the slots of the reachable workers that
are not under pressure.

A worker's messages are events on the job's row (``LeaseQueued``, ``LeasePrompted``, ``LeaseRevoked``), each written
in the transaction of the change that it announces: a transactional outbox. A worker reads those of its ``topics``
after its cursor (``piloop.db.feed``), which it moves past a prompt only once it answered ``queued``, as a NATS
JetStream consumer moves its ack floor. So a message survives a restart of the daemon or the worker.

The fence is the executor's, not core's: core checks only the domain (``Settled``). Every operation that checks it
runs in one write transaction with the core operation that it guards (``Results``), so a report, a heartbeat and an
expiry never both apply. The ``leases`` and ``lease_workers`` tables belong here, but their models and migrations stay in
core's schema until the plugin host gives a plugin tables of its own: one schema, one migration chain.
"""

import contextlib
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal, assert_never

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from sqlalchemy import ColumnElement, exists, func, select, update
from sqlalchemy.orm import Session, aliased

from piloop import deprecated
from piloop.actor import System
from piloop.agents.results import PromptOutcome
from piloop.db import feed
from piloop.db.events import emit, has
from piloop.db.feed import Topic
from piloop.db.models import (
    LIVE_LEASES,
    OPEN_LEASES,
    AgentSession,
    AgentSessionState,
    Evaluation,
    Evaluator,
    Event,
    EventModel,
    Lease,
    LeaseKind,
    LeaseState,
    LeaseWorker,
    NoLease,
    utcnow,
)
from piloop.db.session import Database, write_transaction
from piloop.ids import AgentSessionId, CommentId, TargetKind, TargetRef
from piloop.plugins.executor.contract import (
    JOB,
    AgentJob,
    AgentKey,
    AgentNotStarted,
    AgentReport,
    Capacity,
    EvaluationJob,
    EvaluationKey,
    EvaluationReport,
    Guard,
    Job,
    JobKey,
    LeftJob,
    Prompt,
    Report,
    Results,
    Settled,
)
from piloop.plugins.executor.kit.capacity import HostSample, Pool, admits, slots

SERVE = 'serve'
"""The id of the worker inside ``piloop serve``, and its pool. The id is stable, so the next daemon's worker finds its
leases again."""
DEFAULT_POOLS: Mapping[str, Pool] = {SERVE: Pool()}
UNREPORTED = 'the daemon stopped, and the job did not report in time'
NOT_TAKEN = 'the daemon stopped before a worker took the job'
KINDS = (LeaseKind.AGENT, LeaseKind.EVALUATION)
"""The order in which a worker takes jobs: agents first."""
PAGE = 100
"""How many messages one read returns at most. A worker reads again at once for the rest."""


class StaleReport(EventModel):
    """On the job's row (the agent session, or the evaluated candidate): a report that the fence refused."""

    kind: Literal['lease.stale_report'] = 'lease.stale_report'
    lease: int
    worker: str
    reason: str
    report: Report
    """What it brought back, by address: its commits and files stay in the CAS."""


class LeaseUnreachable(EventModel):
    """On the job's row: its worker was silent past ``grace_seconds``."""

    kind: Literal['lease.unreachable'] = 'lease.unreachable'
    lease: int
    worker: str | None


class LeaseExpired(EventModel):
    """On the job's row: the lease expired, or its worker reported the job gone. ``reason`` says which."""

    kind: Literal['lease.expired'] = 'lease.expired'
    lease: int
    worker: str | None
    reason: str


class LeaseQueued(EventModel):
    """On the job's row, a message for every worker (``Work``): the lease waits for one."""

    kind: Literal['lease.queued'] = 'lease.queued'
    lease: int
    lease_kind: LeaseKind


class LeasePrompted(EventModel):
    """On the agent's row, a message for its worker (``PromptMessage``): a prompt to deliver."""

    kind: Literal['lease.prompted'] = 'lease.prompted'
    lease: int
    worker: str
    comment: CommentId
    text: str


class LeaseRevoked(EventModel):
    """On the job's row, a message for its worker (``Revoke``): stop the job."""

    kind: Literal['lease.revoked'] = 'lease.revoked'
    lease: int
    worker: str


def topics(worker: str) -> tuple[Topic, ...]:
    """The events that are messages of ``worker``."""
    return Topic(LeaseQueued), Topic(LeasePrompted, to=worker), Topic(LeaseRevoked, to=worker)


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


class UnknownPool(ValueError):
    """A worker registers in a pool that ``[plugins.executor.pools]`` does not have."""


class StaleLease(Exception):
    """A worker's call on a lease that is not its live, newest lease: refused, and nothing changed. ``stale_lease``."""


class Answer(StrEnum):
    """What a heartbeat answers for one lease."""

    RENEWED = 'renewed'
    REVOKED = 'revoked'
    """Stop the job, and still report it: a kill or a cancel."""
    STALE = 'stale'
    """The lease is no longer the worker's: stop the job. Its report will be refused."""


class AgentHandle(BaseModel):
    """Where an agent runs: its runtime, and the runtime's handle (``Handle.data``)."""

    model_config = ConfigDict(frozen=True)

    runtime: str
    data: dict[str, object]


class ProcessHandle(BaseModel):
    """Where an evaluator runs: its process group, and the start time that tells it from a reused pid."""

    model_config = ConfigDict(frozen=True)

    pgid: int
    start_time: float | None
    """None when the evaluator was gone already at its start: its group is then never killed by its id alone."""


type JobHandle = AgentHandle | ProcessHandle
HANDLE: TypeAdapter[AgentHandle | ProcessHandle] = TypeAdapter(JobHandle)


class Grant(BaseModel):
    """A lease that a worker holds: its id, which is its fencing token, and its job."""

    model_config = ConfigDict(frozen=True)

    lease: int
    job: Job
    handle: JobHandle | None = None
    """What ``started`` recorded, for a worker that finds its job again after a restart. None before the start."""
    started_at: datetime | None = None

    @classmethod
    def of(cls, lease: Lease) -> Grant:
        """The grant of a lease row."""
        handle = HANDLE.validate_python(lease.handle) if lease.handle else None
        return cls(lease=lease.id, job=_granted(lease), handle=handle, started_at=lease.started_at)


class Work(BaseModel):
    """A message: a job waits. The worker asks for leases until it gets none.

    The id of each message is its event's.
    """

    kind: Literal['work'] = 'work'
    id: int


class PromptMessage(BaseModel):
    """A message: give ``prompt`` to the agent of ``lease``, then answer ``queued``."""

    kind: Literal['prompt'] = 'prompt'
    id: int
    lease: int
    prompt: Prompt


class Revoke(BaseModel):
    """A message: stop the job of ``lease`` now. It still reports. The heartbeat's ``REVOKED`` is the fallback."""

    kind: Literal['revoke'] = 'revoke'
    id: int
    lease: int


type Message = Annotated[Work | PromptMessage | Revoke, Field(discriminator='kind')]


class NotAddressed(Exception):
    """A worker's answer to a message that is not a prompt for it: refused, and nothing changed. ``not_addressed``."""


def _message(event: Event) -> Work | PromptMessage | Revoke:
    """The message that ``event``, of one of ``topics``, carries."""
    payload = event.payload
    assert isinstance(payload, LeaseQueued | LeasePrompted | LeaseRevoked)  # what ``topics`` select
    match payload:
        case LeaseQueued():
            return Work(id=event.id)
        case LeasePrompted(lease=lease, comment=comment, text=text):
            return PromptMessage(id=event.id, lease=lease, prompt=Prompt(comment, text))
        case LeaseRevoked(lease=lease):
            return Revoke(id=event.id, lease=lease)
        case never:
            assert_never(never)


def _granted(lease: Lease) -> AgentJob | EvaluationJob:
    """The lease's job, as its worker runs it. An evaluation gets a place of this lease's own, ``<place>/lease-<id>``.

    Its job is queued again on a new lease while the old one's may still run (a lease that expired before its
    evaluator started): with the same worktree and logs, the stale job would remove, overwrite or read the
    replacement's. An agent's job is never queued again: a re-attached agent goes on in its worktree.
    """
    job = JOB.validate_python(lease.job)
    if isinstance(job, AgentJob):
        return job
    return job.model_copy(update={'place': f'{job.place}/lease-{lease.id}'})


def enqueue(s: Session, job: AgentJob | EvaluationJob) -> Lease:
    """Insert the ``queued`` lease of ``job``, and its ``LeaseQueued``, in the caller's transaction.

    The caller commits.
    """
    lease = Lease(kind=LeaseKind(job.kind), job=JOB.dump_python(job, mode='json'))
    if isinstance(job, AgentJob):
        lease.agent_session_id = job.agent_session
    else:
        lease.evaluation_id = job.evaluation
    s.add(lease)
    s.flush()
    emit(s, LeaseQueued(lease=lease.id, lease_kind=lease.kind), _target(lease), System.DAEMON)
    return lease


def of_agent(s: Session, agent_session_id: AgentSessionId) -> Lease | None:
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
    """Ask the worker to stop a live lease's job, a kill or a cancel (``LeaseRevoked``). It still reports.

    The caller commits.
    """
    if lease.state in LIVE_LEASES and lease.revoked_at is None:
        lease.revoked_at = utcnow()
        assert lease.worker is not None  # a live lease has its worker
        emit(s, LeaseRevoked(lease=lease.id, worker=lease.worker), _target(lease), System.DAEMON)


def cancel(s: Session, lease: Lease, reason: str) -> None:
    """End a lease that no worker will report: a queued one, or every one at a daemon stop. The caller commits."""
    if lease.state in OPEN_LEASES:
        receipt: dict[str, object] = {'cancelled': reason}
        lease.state, lease.ended_at, lease.receipt = LeaseState.CANCELLED, utcnow(), receipt


def _target(lease: Lease) -> TargetRef:
    """The row that a lease's events go on: its agent session, or the candidate that its evaluation measures."""
    if lease.agent_session_id is not None:
        return TargetRef(TargetKind.AGENT_SESSION, lease.agent_session_id)
    job = JOB.validate_python(lease.job)
    assert isinstance(job, EvaluationJob)
    return TargetRef(TargetKind.CANDIDATE, job.candidate)


def _job_of(lease: Lease) -> ColumnElement[bool]:
    """Select the leases of the job of ``lease``."""
    if lease.agent_session_id is not None:
        return Lease.agent_session_id == lease.agent_session_id
    return Lease.evaluation_id == lease.evaluation_id


class Leases:
    """The lease operations, and the messages of each worker.

    Every operation that checks the fence runs in one write transaction (``write_transaction``) with the core
    operation that it guards (``results``), so a report, a heartbeat and an expiry never both apply to one lease. A
    result that core refuses (``Settled``) is refused as stale.
    """

    def __init__(
        self,
        db: Database,
        results: Results,
        timing: Timing = DEFAULT_TIMING,
        pools: Mapping[str, Pool] = DEFAULT_POOLS,
    ) -> None:
        self._db = db
        self._results = results
        self.timing = timing
        self.pools = pools
        self._expired_at: datetime | None = None
        """When ``expire`` last ran, to tell a gap in the scheduler's ticks (a suspend) from a silent worker."""

    def register(
        self, worker: str, pool: str, version: str, runners: Sequence[str], host: HostSample
    ) -> tuple[list[Grant], int]:
        """Record a worker that starts, and return its live leases and its cursor: what a restarted worker finds again.

        A new worker's cursor is the newest event, and so is one that is past it (a database restored from a backup):
        its leases are the truth then.

        Raises:
            UnknownPool: no ``[plugins.executor.pools.<pool>]``.
        """
        if pool not in self.pools:
            raise UnknownPool(f'no pool {pool!r}: the pools are {", ".join(sorted(self.pools))}')
        with self._db.session() as s:
            write_transaction(s)
            newest = feed.newest(s)
            row = s.get(LeaseWorker, worker) or LeaseWorker(id=worker, cursor=newest)
            row.pool, row.version, row.runners, row.no_lease = pool, version, list(runners), None
            row.cursor = min(row.cursor, newest)
            self._heard(row, host)
            s.add(row)
            rows = s.scalars(
                select(Lease).where(Lease.worker == worker, Lease.state.in_(LIVE_LEASES)).order_by(Lease.id)
            ).all()
            held, cursor = [Grant.of(r) for r in rows], row.cursor
            s.commit()
        return held, cursor

    @staticmethod
    def _heard(row: LeaseWorker, host: HostSample) -> None:
        row.sample, row.seen_at = host.model_dump(mode='json'), utcnow()
        if row.no_lease == NoLease.UNREACHABLE:
            row.no_lease = None

    def acquire(self, worker: str, request_id: str, host: HostSample) -> Grant | NoLease:
        """Take the oldest queued job that ``worker`` can run and that its pool admits, or say why there is none.

        Agent jobs go first. An evaluation job goes only to a worker with its runner. Core submits an evaluation only
        when it has a slot, in the order of its queue, so the order of the leases is that order. ``host`` is the
        worker's sample now, which the gates read (``capacity.admits``). An acquire sent again with its request id gets
        the lease that it took, so a lost answer leaves no lease to expire.
        """
        with self._db.session() as s:
            write_transaction(s)
            row = s.get_one(LeaseWorker, worker)  # ``register`` wrote it
            self._heard(row, host)
            taken = select(Lease).where(Lease.worker == worker, Lease.acquire_request == request_id)
            if (lease := s.scalars(taken).first()) is not None:
                s.commit()
                return Grant.of(lease)
            pool = self.pools[row.pool]
            counts = Counter(
                s.scalars(select(Lease.kind).where(Lease.worker == worker, Lease.state.in_(LIVE_LEASES))).all()
            )
            running = Capacity(counts[LeaseKind.AGENT], counts[LeaseKind.EVALUATION])
            refused: list[NoLease] = []
            for kind in KINDS:
                queued = select(Lease).where(Lease.kind == kind, Lease.state == LeaseState.QUEUED)
                if kind is LeaseKind.EVALUATION:
                    queued = queued.where(Lease.job['runner'].as_string().in_(row.runners))
                lease = s.scalars(queued.order_by(Lease.id).limit(1)).first()
                match lease, admits(pool, host, running, kind):
                    case None, None:
                        refused.append(NoLease.NOTHING)
                    case None, NoLease.SLOTS if not _held_back(s, kind, row.runners):
                        refused.append(NoLease.NOTHING)  # full slots hold back nothing when nothing waits
                    case _, NoLease() as refusal:
                        refused.append(refusal)
                    case _:
                        assert lease is not None  # the cases above take every other pair
                        now = utcnow()
                        lease.state, lease.worker, lease.acquired_at = LeaseState.ACQUIRED, worker, now
                        lease.acquire_request = request_id
                        lease.renewed_at, lease.expires_at = now, now + timedelta(seconds=self.timing.toleration)
                        row.no_lease = None
                        s.commit()
                        return Grant.of(lease)
            reason = row.no_lease = min(refused, key=list(NoLease).index)
            s.commit()
            return reason

    def capacity(self) -> Capacity:
        """The free slots of the reachable workers that their pools admit now, less the queued leases.

        A worker under pressure counts zero, and its live leases take nothing from another worker's slots.
        """
        since = utcnow() - timedelta(seconds=self.timing.grace)
        with self._db.session() as s:
            rows = s.scalars(select(LeaseWorker).where(LeaseWorker.seen_at > since)).all()
            leased = s.execute(select(Lease.worker, Lease.kind).where(Lease.state.in_(LIVE_LEASES))).all()
            live = Counter((worker, kind) for worker, kind in leased)
            queued = Counter(s.scalars(select(Lease.kind).where(Lease.state == LeaseState.QUEUED)))
        free: dict[LeaseKind, int | None] = dict.fromkeys(KINDS, 0)
        for row in rows:
            if (pool := self.pools.get(row.pool)) is None:
                continue
            running = Capacity(live[row.id, LeaseKind.AGENT], live[row.id, LeaseKind.EVALUATION])
            host = HostSample.model_validate(row.sample)
            for kind in KINDS:
                if admits(pool, host, running, kind) is None:
                    bound = slots(pool, kind)
                    left = None if bound is None else bound - (slots(running, kind) or 0)
                    free[kind] = _sum(free[kind], left)
        agent, evaluation = LeaseKind.AGENT, LeaseKind.EVALUATION
        return Capacity(_less(free[agent], queued[agent]), _less(free[evaluation], queued[evaluation]))

    def _live(self, s: Session, lease_id: int, worker: str) -> Lease:
        """Return the lease if it is ``worker``'s, live and the newest of its job: the fence. Else raise.

        The caller holds the write transaction (``write_transaction``) in which it then writes.
        """
        lease = s.get_one(Lease, lease_id)  # no lease is deleted: ``_job`` or ``register`` found it
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

    def started(self, lease_id: int, worker: str, handle: JobHandle, session_id: str | None = None) -> None:
        """Record that the job started, and where (``handle``). Raises ``StaleLease``: then stop the job."""

        def mark(s: Session) -> None:
            lease = self._live(s, lease_id, worker)
            lease.state, lease.handle = LeaseState.RUNNING, handle.model_dump(mode='json')
            lease.started_at = lease.started_at or utcnow()

        agent_session = self._job(lease_id).agent_session_id
        match handle:
            case AgentHandle() if agent_session is not None:
                try:
                    self._results.started(AgentKey(agent_session), handle.data, session_id, guard=mark)
                except Settled as e:
                    raise StaleLease(f'stale_lease: {e}') from e
            case _:  # an evaluator: core needs nothing of its start
                with self._db.session() as s:
                    write_transaction(s)
                    mark(s)
                    s.commit()

    def heartbeat(self, worker: str, sessions: dict[int, str | None], host: HostSample) -> dict[int, Answer]:
        """Renew the leases of ``worker`` that it still runs, by id, with the session id each agent reported.

        The answer for each says go on, stop (``REVOKED``, and report), or the lease is no longer yours (``STALE``).
        ``host`` is the worker's sample now: it goes on its row.
        """
        answers: dict[int, Answer] = {}
        reported: list[tuple[int, AgentSessionId, str]] = []
        now = utcnow()
        with self._db.session() as s:
            write_transaction(s)
            self._heard(s.get_one(LeaseWorker, worker), host)
            for lease_id, session_id in sessions.items():
                try:
                    lease = self._live(s, lease_id, worker)
                except StaleLease:
                    answers[lease_id] = Answer.STALE
                    continue
                lease.renewed_at = now
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
            StaleLease: the fence refused it. It is kept as a ``StaleReport`` event, and nothing else changed.
            MissingBlobs: the CAS lacks a blob that it names. Nothing changed.
        """
        lease = self._job(lease_id)
        if lease.state is LeaseState.REPORTED and lease.report_request == request_id:
            return dict(lease.receipt)
        receipt: dict[str, object] = {'reported': report.kind}

        def mark(s: Session) -> None:
            live = self._live(s, lease_id, worker)
            live.state, live.ended_at, live.report_request, live.receipt = (
                LeaseState.REPORTED,
                utcnow(),
                request_id,
                receipt,
            )

        try:
            self._results.finished(_granted(lease), report, guard=mark)
        except (StaleLease, Settled) as e:
            if (applied := self._job(lease_id)).state is LeaseState.REPORTED and applied.report_request == request_id:
                return dict(applied.receipt)  # the same report, sent again while the first was applied
            self._keep_stale(lease, worker, report, str(e))
            if isinstance(e, Settled):
                raise StaleLease(f'stale_lease: {e}') from e
            raise
        return receipt

    def _keep_stale(self, lease: Lease, worker: str, report: AgentReport | EvaluationReport, reason: str) -> None:
        """Keep a refused report as a ``StaleReport`` event on its job's row: what it made stays in the CAS."""
        stale = StaleReport(lease=lease.id, worker=worker, reason=reason, report=report)
        with self._db.session() as s:
            emit(s, stale, _target(lease), System.DAEMON)
            s.commit()

    def lost(self, lease_id: int, worker: str, reason: str) -> None:
        """End the lease as lost: its worker restarted and cannot find its job. Raises ``StaleLease``."""
        self._lose(lease_id, reason, lambda s: self._live(s, lease_id, worker))

    def _lose(self, lease_id: int, reason: str, fence: Callable[[Session], Lease]) -> None:
        """End the lease as expired, and tell core that its job is lost, if ``fence`` still finds the lease its own.

        A job whose row closed elsewhere (a daemon stop) has nothing to lose: the lease ends all the same, and its
        receipt says so.
        """
        receipt: dict[str, object] = {'lost': reason}

        def mark(s: Session) -> None:
            lease = fence(s)
            lease.state, lease.ended_at, lease.receipt = LeaseState.EXPIRED, utcnow(), receipt
            emit(s, LeaseExpired(lease=lease.id, worker=lease.worker, reason=reason), _target(lease), System.DAEMON)

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
        """Mark silent workers unreachable, and expire the leases past their time. Return the next expiry.

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
        live = Lease.state.in_(LIVE_LEASES)
        newly_silent = (LeaseWorker.seen_at <= now - timedelta(seconds=self.timing.grace)) & (
            LeaseWorker.no_lease.is_distinct_from(NoLease.UNREACHABLE)
        )
        expiring: list[tuple[int, str]] = []
        with self._db.session() as s:
            due = s.scalar(select(Lease.id).where(live, Lease.expires_at <= now).limit(1)) is not None
            if (
                due or s.scalar(select(LeaseWorker.id).where(newly_silent).limit(1)) is not None
            ):  # the write lock if needed
                write_transaction(s)
                for worker in s.scalars(select(LeaseWorker).where(newly_silent)).all():
                    worker.no_lease = NoLease.UNREACHABLE
                    for lease in s.scalars(select(Lease).where(live, Lease.worker == worker.id).order_by(Lease.id)):
                        unreachable = LeaseUnreachable(lease=lease.id, worker=lease.worker)
                        emit(s, unreachable, _target(lease), System.DAEMON)
                for lease in s.scalars(select(Lease).where(live, Lease.expires_at <= now).order_by(Lease.id)).all():
                    assert lease.renewed_at is not None  # set by acquire
                    silence = round((now - lease.renewed_at).total_seconds())
                    expiring.append((lease.id, f'worker lost: {lease.worker} was silent for {silence} s'))
                s.commit()
        for lease_id, reason in expiring:
            with contextlib.suppress(StaleLease):  # a report or a heartbeat came first, in the meantime
                self._lose(lease_id, reason, lambda s, lease_id=lease_id: self._still_expired(s, lease_id, now))
        with self._db.session() as s:
            return s.scalar(select(func.min(Lease.expires_at)).where(live))

    def _still_expired(self, s: Session, lease_id: int, now: datetime) -> Lease:
        """Return the lease if it is still live and past its expiry at ``now``, in the transaction that expires it."""
        lease = s.get_one(Lease, lease_id)
        if lease.state not in LIVE_LEASES or lease.expires_at is None or lease.expires_at > now:
            raise StaleLease(f'stale_lease: lease#{lease_id} is {lease.state}, no longer silent')
        return lease

    def renew_all(self, now: datetime | None = None) -> None:
        """Give every live lease, and its worker, a full period.

        A daemon that starts again does not expire work for its own absence.
        """
        now = now or utcnow()
        live = Lease.state.in_(LIVE_LEASES)
        with self._db.session() as s:
            write_transaction(s)
            s.execute(
                update(Lease)
                .where(live)
                .values(renewed_at=now, expires_at=now + timedelta(seconds=self.timing.toleration))
            )
            s.execute(
                update(LeaseWorker).where(LeaseWorker.id.in_(select(Lease.worker).where(live))).values(seen_at=now)
            )
            s.commit()

    def submit(self, job: AgentJob | EvaluationJob) -> None:
        """Queue ``job`` on a new lease, which tells every worker (``Work``)."""
        with self._db.session() as s:
            enqueue(s, job)
            s.commit()

    def revoke(self, key: JobKey) -> None:
        """Ask the worker of the job's live lease to stop it (``revoke``). Its job still reports."""
        with self._db.session() as s:
            lease = lease_of(s, key)
            if lease is not None:
                revoke(s, lease)
                s.commit()

    def prompt(self, key: AgentKey, prompt: Prompt) -> None:
        """Send ``prompt`` to the worker of the agent's live lease (``LeasePrompted``), unless it was sent.

        The worker answers ``queued``. An agent without a live lease never gets it: core records why at once.
        """
        with self._db.session() as s:
            write_transaction(s)
            lease = of_agent(s, key.agent_session)
            if lease is not None and lease.state in LIVE_LEASES and lease.worker is not None:
                if not _prompted(s, lease, prompt.id):
                    prompted = LeasePrompted(lease=lease.id, worker=lease.worker, comment=prompt.id, text=prompt.text)
                    emit(s, prompted, _target(lease), System.DAEMON)
                s.commit()
                return
        self._results.queued(key, prompt.id, 'the agent has no live lease')

    def messages(self, worker: str, after: int, wait: float) -> list[Work | PromptMessage | Revoke]:
        """Store ``after`` as the worker's cursor, and return its messages after it, waiting up to ``wait`` seconds.

        A long-poll, after the GitHub Actions runner: it returns at once when a message waits, and reads again on each
        commit (``Database.wait_until``). A prompt whose comment has its outcome is left out: the worker answered it.
        """
        found: list[Work | PromptMessage | Revoke] = []

        def read(s: Session) -> bool:
            row = s.get_one(LeaseWorker, worker)  # ``register`` wrote it
            if row.cursor != after:
                row.cursor = after
                s.commit()
            page = feed.after(s, after, topics(worker), limit=PAGE, where=[_unanswered()])
            found[:] = [_message(e) for e in s.scalars(select(Event).where(Event.id.in_(page.ids)).order_by(Event.id))]
            return bool(found)

        self._db.wait_until(read, timeout=wait)
        return found

    def queued(self, worker: str, message: int, error: str | None) -> None:
        """The worker's answer to the ``PromptMessage`` of id ``message``: core records the outcome once.

        Raises:
            NotAddressed: ``message`` is not a prompt for ``worker``.
        """
        with self._db.session() as s:
            event = s.get(Event, message)
        match event:
            case Event(payload=LeasePrompted(worker=to, comment=comment), target_id=agent) if to == worker:
                self._results.queued(AgentKey(AgentSessionId(agent)), comment, error)
            case _:
                raise NotAddressed(f'not_addressed: event#{message} is not a prompt for {worker}')

    def adopt(self, left: Sequence[LeftJob], worker: str) -> list[JobKey]:
        """Give every live lease a full period, and return the jobs of ``left`` that have no open lease.

        A job on an open lease goes on: its worker finds it again (``register``). A running agent that a daemon before
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


def _prompted(s: Session, lease: Lease, comment: CommentId) -> bool:
    """Whether ``lease`` has the ``LeasePrompted`` of ``comment``: core hands it over again after a restart."""
    found = select(Event.id).where(Event.target == _target(lease), has(LeasePrompted, lease=lease.id, comment=comment))
    return s.scalar(found.limit(1)) is not None


def _unanswered() -> ColumnElement[bool]:
    """Select the events but the prompts (``LeasePrompted``) whose comment has its outcome (``PromptOutcome``)."""
    outcome = aliased(Event)
    answered = exists().where(
        outcome.target_kind == Event.target_kind,
        outcome.target_id == Event.target_id,
        outcome.kind.in_(PromptOutcome.kinds()),
        func.json_extract(outcome.payload, '$.comment') == func.json_extract(Event.payload, '$.comment'),
    )
    return Event.kind.not_in(LeasePrompted.kinds()) | ~answered


def _sum(a: int | None, b: int | None) -> int | None:
    """Add two numbers of slots. None, no bound, absorbs."""
    return None if a is None or b is None else a + b


def _less(free: int | None, taken: int) -> int | None:
    """``free`` slots less ``taken``, never below zero. None, no bound, stays None."""
    return None if free is None else max(free - taken, 0)


@deprecated.until('0.3.0', 'an agent that a daemon before leases started: delete _adopt_running')
def _adopt_running(s: Session, left: LeftJob, worker: str, timing: Timing) -> bool:
    """Put a running agent that a daemon before leases started on a live lease of ``worker``, and return True.

    It keeps running, and its work is scored. False for every other job without a lease.
    """
    if left.job is None or left.handle is None:
        return False
    lease = enqueue(s, left.job)
    now = utcnow()
    handle = AgentHandle(runtime=left.job.runtime, data=left.handle)
    lease.state, lease.worker, lease.handle = LeaseState.RUNNING, worker, handle.model_dump(mode='json')
    lease.acquired_at = lease.started_at = left.started_at or now
    lease.renewed_at, lease.expires_at = now, now + timedelta(seconds=timing.toleration)
    return True


def _held_back(s: Session, kind: LeaseKind, runners: Sequence[str]) -> bool:
    """Whether core holds back a job of ``kind`` that a worker with ``runners`` could run.

    Core submits a job only when ``Executor.capacity`` has a free slot, so while every slot is full the job waits in
    core, as a requested agent or a waiting evaluation, and no lease is queued for it.
    """
    match kind:
        case LeaseKind.AGENT:
            query = select(AgentSession.id).where(AgentSession.state == AgentSessionState.REQUESTED)
        case LeaseKind.EVALUATION:
            query = (
                select(Evaluation.id)
                .join(Evaluator, Evaluator.id == Evaluation.evaluator_id)
                .where(Evaluation.started_at.is_(None), Evaluator.runner.in_(runners))
            )
        case _:
            assert_never(kind)
    return s.scalar(query.limit(1)) is not None
