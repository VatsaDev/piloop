"""Scaling, a building block for executors: start and stop the workers of each pool that has a provisioner.

A pool with ``[…provisioner]`` (``capacity.Provisioner``) has between ``min`` and ``max`` workers, which its
provisioner starts and stops (``piloop.plugins.provisioner``). The pool counts the slots of ``max`` workers
(``Leases.capacity``), so its jobs queue on leases, and the queued leases are the demand. One pass, for each provisioned
pool in config order:

| Step | Rule |
|---|---|
| waiting | The queued leases that the pool's workers can run: a kind of job that it has slots for, and an evaluation of one of its ``runners``. |
| covered | The free slots of the reachable workers of any pool that can run them, and the slots of the workers that still start, in any provisioned pool. |
| need | The workers for what waits and is not covered. With ``once``: one for each lease. |
| start | Up to ``min``, or the need, below ``max``. At most ``max_starting`` at once, and none during a backoff or past ``max_worker_hours_per_day``. |
| drain | The workers past ``max``; those past ``min`` that held no lease for ``idle_seconds``; a ``once`` worker that ran its lease. |

A drained worker takes no more leases (``NoLease.DRAINING``), and exits once each of its jobs reported. Once it holds
no lease, the pass stops its instance. The cluster autoscaler, Ray's autoscaler and Buildkite's elastic stack decide
the same way, and Slurm's power saving times out a node that never comes up (``register_seconds``).

The ``worker_instances`` row of an instance commits before its start, and its credential is minted then
(``Network.enroll``), so a daemon that restarts finds every instance that it asked for. The provisioner's ``list`` runs
at the first pass and then every ``RESYNC_SECONDS``: an instance that it lists with this server's owner and no row is
an orphan, and is stopped. Every change of a row is an ``instance.*`` event on it.

The pass runs on a thread of its own, at most one at a time, so a slow platform never holds the daemon's loop. The
executor's upkeep starts one when the feed has a change that the pass acts on (``WAKES``), or at the pass's next
deadline.
"""

import math
import threading
import uuid
from collections import Counter, defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import ClassVar, Literal

from sqlalchemy import func, select

from piloop.actor import System
from piloop.api.contract import Access
from piloop.db import feed
from piloop.db.events import emit
from piloop.db.feed import Topic
from piloop.db.models import (
    LIVE_LEASES,
    Comment,
    Evaluation,
    Evaluator,
    EventModel,
    InstanceEnd,
    Lease,
    LeaseKind,
    LeaseState,
    LeaseWorker,
    ReadBy,
    WorkerInstance,
    WorkerInstanceState,
    utcnow,
)
from piloop.db.session import Database, read_transaction, write_transaction
from piloop.ids import CommentKind, TargetKind, TargetRef
from piloop.otel import Tracer
from piloop.plugins.executor.contract import Capacity
from piloop.plugins.executor.kit import leases
from piloop.plugins.executor.kit.capacity import HostSample, Pool, Provisioner, add, admits, less, slots, times
from piloop.plugins.executor.kit.leases import KINDS, LeaseExpired, LeaseQueued, LeaseUnreachable, Timing, WorkerDrained
from piloop.plugins.network.contract import Network, NotEnrolled
from piloop.plugins.provisioner.contract import (
    Instance,
    InstanceState,
    Launch,
    ProvisionerHost,
    Provisioning,
    StartRefused,
)

RESYNC_SECONDS = 60.0
"""How often a pass lists the instances of each provisioner. Between two lists, it trusts its rows."""
FIRST_BACKOFF = timedelta(seconds=30)
MAX_BACKOFF = timedelta(minutes=10)
"""After a start that the provisioner refused, or a worker that did not register, the pool starts none for this long,
doubling up to the most. The cluster autoscaler backs a node group off the same way."""
REFUSALS = 3
"""The refused starts in a row after which the pool escalates to the inbox."""
ESCALATION_PRIORITY = 1
OWNER_FILE = 'owner'
"""The file under the home that holds this server's owner id (``ProvisionerHost.owner``), made once."""
DAY = timedelta(hours=24)


class InstanceRequested(EventModel):
    """On an instance: the pool needs it, and its row and credential exist. Its start comes next."""

    READ_BY: ClassVar[ReadBy] = ReadBy.READERS
    kind: Literal['instance.requested'] = 'instance.requested'
    name: str
    pool: str


class InstanceRegistered(EventModel):
    """On an instance: its worker registered."""

    READ_BY: ClassVar[ReadBy] = ReadBy.READERS
    kind: Literal['instance.registered'] = 'instance.registered'
    name: str


class InstanceEnded(EventModel):
    """On an instance: it ended, its credential is revoked, and its provisioner was asked to stop it."""

    READ_BY: ClassVar[ReadBy] = ReadBy.READERS
    kind: Literal['instance.ended'] = 'instance.ended'
    name: str
    end: InstanceEnd
    reason: str | None = None


PASS_POLL = timedelta(seconds=1)
"""How soon the upkeep looks again while a pass runs, for the deadline that the pass sets."""
WAKES = (Topic(LeaseQueued), Topic(LeaseExpired), Topic(LeaseUnreachable), Topic(WorkerDrained))
"""The events that start a pass: a job waits, a worker went silent or lost a lease, a worker was drained."""


def owner(home: Path) -> str:
    """This server's owner id, from ``<home>/owner``: made the first time."""
    path = home / OWNER_FILE
    if not path.is_file():
        home.mkdir(parents=True, exist_ok=True)
        path.write_text(uuid.uuid4().hex)
    return path.read_text().strip()


@dataclass(frozen=True)
class Seen:
    """What one pass reads, in one snapshot."""

    instances: list[WorkerInstance]
    """The rows that have not ended, oldest first."""
    workers: dict[str, LeaseWorker]
    live: Counter[tuple[str, LeaseKind]]
    """The live leases of each worker, by kind."""
    held: set[str]
    """The workers that held a lease once."""
    last_end: dict[str, datetime]
    """When each worker's last lease ended."""
    queued: list[tuple[LeaseKind, str | None]]
    """The queued leases: their kind, and an evaluation's runner."""
    hours: dict[str, float]
    """The hours that each pool's workers ran in the last 24."""

    def busy(self, worker: str) -> bool:
        return any(self.live[worker, kind] for kind in KINDS)


@dataclass
class Backoff:
    """A pool's backoff after a start that failed: none before ``until``."""

    until: datetime | None = None
    step: timedelta = FIRST_BACKOFF
    refused: int = 0
    """The refused starts in a row."""

    def fail(self, now: datetime) -> None:
        self.until, self.step = now + self.step, min(self.step * 2, MAX_BACKOFF)

    def ok(self) -> None:
        self.until, self.step, self.refused = None, FIRST_BACKOFF, 0


@dataclass
class Scaler:
    """The passes over the provisioned pools of ``pools()``, each with the provisioning that ``opened`` has for it."""

    db: Database
    pools: Callable[[], Mapping[str, Pool]]
    """The pools with the live limits applied, as the executor has them now."""
    opened: Mapping[str, Provisioning]
    network: Network
    tracer: Tracer
    timing: Timing
    argv: Callable[[str, str, Provisioner], list[str]]
    """What an instance runs, for a server URL, a pool and its policy."""
    _started: dict[str, datetime] = field(default_factory=dict)
    """When this process started each instance, or found it started: a ``starting`` row that is not here is started
    again after a list. Its ``register_seconds`` count from then."""
    _backoff: defaultdict[str, Backoff] = field(default_factory=lambda: defaultdict(Backoff))
    _listed: datetime | None = None
    _next: datetime | None = None
    _cursor: int = 0
    _pass: threading.Thread | None = None
    _since: datetime = field(default_factory=utcnow)
    """When this daemon began: a worker's silence counts from then at the earliest, not from before a restart."""

    @classmethod
    def of(
        cls,
        db: Database,
        pools: Callable[[], Mapping[str, Pool]],
        network: Network,
        tracer: Tracer,
        timing: Timing,
        home: Path,
        argv: Callable[[str, str, Provisioner], list[str]],
    ) -> Scaler:
        """Open the provisioner of each pool that has one, for this server's owner (``owner``)."""
        host = ProvisionerHost(owner(home), tracer)
        opened = {
            name: pool.provisioner.compute.open(host) for name, pool in pools().items() if pool.provisioner is not None
        }
        with db.session() as s:
            cursor = feed.newest(s)
        return cls(db, pools, opened, network, tracer, timing, argv, _cursor=cursor)

    def upkeep(self, now: datetime) -> datetime | None:
        """Start a pass when the feed has a change that it acts on, or at its deadline, and return the next deadline.

        While a pass runs, the next deadline is soon: the one that the pass sets is known only once it ends.
        """
        if not self.opened:
            return None
        if self._pass is not None and self._pass.is_alive():
            return now + PASS_POLL
        with self.db.session() as s:
            page = feed.after(s, self._cursor, WAKES, limit=1)
        woken, self._cursor = bool(page.ids), page.next
        if woken or self._next is None or now >= self._next:
            self._pass = threading.Thread(target=self._guarded, args=(now,), name='scaling', daemon=True)
            self._pass.start()
            return now + PASS_POLL
        return self._next

    def join(self, timeout: float) -> None:
        """Wait up to ``timeout`` seconds for the pass in flight."""
        if self._pass is not None:
            self._pass.join(timeout)

    def _guarded(self, now: datetime) -> None:
        try:
            self._next = self.reconcile(now)
        except Exception as e:  # the database is locked, a platform that fails: the next pass tries again
            self.tracer.event('scaling.failed', error=f'{type(e).__name__}: {e}')
            self._next = now + timedelta(seconds=self.timing.renew)

    def reconcile(self, now: datetime) -> datetime:
        """One pass over every provisioned pool, and when the next one is due."""
        pools = self.pools()
        seen = _read(self.db, now)
        unstarted = any(
            row.state is WorkerInstanceState.STARTING and row.name not in self._started for row in seen.instances
        )
        if unstarted or self._listed is None or now - self._listed >= timedelta(seconds=RESYNC_SECONDS):
            listed = {name: {i.name: i for i in p.list() if i.pool == name} for name, p in self.opened.items()}
            self._listed = now
        else:
            listed = {}
        deadlines = [self._listed + timedelta(seconds=RESYNC_SECONDS)]
        provisioned = [
            (name, pool, pool.provisioner, provisioning)
            for name, pool in pools.items()
            if pool.provisioner is not None and (provisioning := self.opened.get(name)) is not None
        ]
        live: dict[str, list[WorkerInstance]] = {}
        for name, _, policy, provisioning in provisioned:
            rows = [row for row in seen.instances if row.pool == name]
            if name in listed:
                self._resync(provisioning, name, rows, listed[name], seen, now)
            for row in rows:
                self._settle(provisioning, policy, row, seen, now, deadlines)
            live[name] = [row for row in rows if row.state is not WorkerInstanceState.ENDED]
        starting = Counter(
            row.pool for rows in live.values() for row in rows if row.state is WorkerInstanceState.STARTING
        )
        for name, pool, policy, provisioning in provisioned:
            starting[name] += self._scale(provisioning, name, pool, policy, live[name], seen, starting, now, deadlines)
        if self._listed is None:  # a start failed on the way: list again soon
            return now + timedelta(seconds=self.timing.renew)
        return min(deadlines)

    def _resync(
        self,
        provisioning: Provisioning,
        pool: str,
        rows: list[WorkerInstance],
        found: dict[str, Instance],
        seen: Seen,
        now: datetime,
    ) -> None:
        """Stop the orphans that the provisioner lists, end the rows that it no longer does, and restart the unstarted."""
        known = {row.name for row in rows}
        for orphan in sorted(set(found) - known):
            if found[orphan].state is not InstanceState.ENDED:
                self.tracer.event('instance.orphan', instance=orphan, pool=pool)
                provisioning.stop(orphan)
                self.network.revoke(orphan)
        for row in rows:
            match row.state, found.get(row.name):
                case WorkerInstanceState.STARTING, None if row.name not in self._started:
                    self._launch(provisioning, row, now)
                case WorkerInstanceState.STARTING, Instance(state=InstanceState.ENDED):
                    self._backoff[pool].fail(now)  # it fails as it boots: starting it again would fail again
                    self._end(provisioning, row, InstanceEnd.GONE, 'its instance ended before its worker registered')
                case WorkerInstanceState.STARTING, Instance():
                    self._started.setdefault(row.name, now)
                case WorkerInstanceState.RUNNING | WorkerInstanceState.DRAINING, (
                    None | Instance(state=InstanceState.ENDED)
                ) if not seen.busy(row.name):
                    self._end(provisioning, row, InstanceEnd.DRAINED, 'its worker exited with no lease')
                case WorkerInstanceState.RUNNING | WorkerInstanceState.DRAINING, None:
                    self._end(provisioning, row, InstanceEnd.GONE, 'the provisioner no longer lists it')
                case WorkerInstanceState.RUNNING | WorkerInstanceState.DRAINING, Instance(state=InstanceState.ENDED):
                    self._end(provisioning, row, InstanceEnd.GONE, 'its instance ended')
                case _:
                    pass

    def _settle(
        self,
        provisioning: Provisioning,
        policy: Provisioner,
        row: WorkerInstance,
        seen: Seen,
        now: datetime,
        deadlines: list[datetime],
    ) -> None:
        """Move a row on from what its worker did: register it, or end it."""
        worker = seen.workers.get(row.name)
        heard = max(worker.seen_at, self._since) if worker is not None else now
        silent = now - heard > timedelta(seconds=self.timing.toleration)
        timeout = max(row.requested_at, self._started.get(row.name, now)) + timedelta(seconds=policy.register_seconds)
        match row.state:
            case WorkerInstanceState.ENDED:  # a resync ended it
                pass
            case WorkerInstanceState.STARTING if worker is not None:
                self._registered(row)
            case WorkerInstanceState.STARTING if now >= timeout:
                self._backoff[row.pool].fail(now)
                self._end(
                    provisioning, row, InstanceEnd.REGISTER_TIMEOUT, f'no worker in {policy.register_seconds:g} s'
                )
            case WorkerInstanceState.STARTING:
                deadlines.append(timeout)
            case WorkerInstanceState.RUNNING | WorkerInstanceState.DRAINING if silent:
                self._end(
                    provisioning, row, InstanceEnd.LOST, f'its worker was silent past {self.timing.toleration:g} s'
                )
            case WorkerInstanceState.DRAINING if not seen.busy(row.name):
                self._end(provisioning, row, InstanceEnd.DRAINED)
            case _:
                pass

    def _scale(
        self,
        provisioning: Provisioning,
        name: str,
        pool: Pool,
        policy: Provisioner,
        rows: list[WorkerInstance],
        seen: Seen,
        starting: Counter[str],
        now: datetime,
        deadlines: list[datetime],
    ) -> int:
        """Drain what the pool no longer needs, start what it needs, and return how many started."""
        up = [row for row in rows if row.state is not WorkerInstanceState.DRAINING]
        for row in self._surplus(policy, up, seen, now, deadlines):
            self._drain(row)
            up.remove(row)
        backoff = self._backoff[name]
        if backoff.until is not None and now < backoff.until:
            deadlines.append(backoff.until)
            return 0
        if policy.max_worker_hours_per_day is not None and seen.hours[name] >= policy.max_worker_hours_per_day:
            return 0
        since = now - timedelta(seconds=self.timing.grace)
        need = _need(pool, policy, seen, self.pools(), starting, since)
        now_starting = sum(1 for row in up if row.state is WorkerInstanceState.STARTING)
        desired = min(max(len(up) + need, policy.min), policy.max)
        started = 0
        for _ in range(max(min(desired - len(up), policy.max_starting - now_starting), 0)):
            if self._launch(provisioning, self._request(name), now) is not None:
                break
            started += 1
        return started

    def _surplus(
        self, policy: Provisioner, up: list[WorkerInstance], seen: Seen, now: datetime, deadlines: list[datetime]
    ) -> list[WorkerInstance]:
        """The running workers to drain, and the times when idle ones come due.

        A ``once`` worker that ran its lease, those past ``max`` (idle first), and those past ``min`` that held no
        lease for ``idle_seconds`` (the longest idle first).
        """
        running = [row for row in up if row.state is WorkerInstanceState.RUNNING]

        def idle_since(row: WorkerInstance) -> datetime:
            return seen.last_end.get(row.name) or row.registered_at or row.requested_at

        idle = sorted((row for row in running if not seen.busy(row.name)), key=idle_since)
        drained = [row for row in idle if policy.once and row.name in seen.held]
        rest = [row for row in [*idle, *(row for row in running if seen.busy(row.name))] if row not in drained]
        drained += rest[: max(len(up) - len(drained) - policy.max, 0)]
        for row in [row for row in idle if row not in drained]:
            if len(up) - len(drained) <= policy.min:
                break
            due = idle_since(row) + timedelta(seconds=policy.idle_seconds)
            if now >= due:
                drained.append(row)
            else:
                deadlines.append(due)
        return drained

    def _request(self, pool: str) -> WorkerInstance:
        """Commit the row of a new instance of ``pool``, with a name of its own."""
        with self.db.session() as s:
            write_transaction(s)
            row = WorkerInstance(name=f'{pool}-{uuid.uuid4().hex[:8]}', pool=pool)
            s.add(row)
            s.flush()
            emit(s, InstanceRequested(name=row.name, pool=pool), _on(row), System.DAEMON)
            s.commit()
        return row

    def _launch(self, provisioning: Provisioning, row: WorkerInstance, now: datetime) -> str | None:
        """Mint the instance's credential, in place of any that it had, and start it. Return why it did not start.

        A start that the provisioner refuses ends the row and backs the pool off. Another failure leaves the row
        ``starting``: the next pass lists the instances, and starts it again unless the provisioner has it.
        """
        backoff = self._backoff[row.pool]
        try:
            enrolled = self.network.enroll(row.name, Access.WORKER)
            argv = self.argv(enrolled.server, row.pool, self._policy(row.pool))
            provisioning.start(Launch(name=row.name, pool=row.pool, argv=argv, env=enrolled.env))
        except (StartRefused, NotEnrolled) as e:
            backoff.fail(now)
            backoff.refused += 1
            self._end(provisioning, row, InstanceEnd.START_REFUSED, str(e))
            if backoff.refused == REFUSALS:
                self._escalate(row, str(e))
            return str(e)
        except Exception as e:  # a platform that does not answer: the next pass lists, and starts it again
            error = f'{type(e).__name__}: {e}'
            self.tracer.event('instance.start_failed', instance=row.name, error=error)
            self._listed = None
            return error
        self._started[row.name] = now
        return None

    def _policy(self, pool: str) -> Provisioner:
        found = self.pools()[pool].provisioner
        assert found is not None  # only a provisioned pool has instances
        return found

    def _registered(self, row: WorkerInstance) -> None:
        with self.db.session() as s:
            write_transaction(s)
            found = s.get_one(WorkerInstance, row.id)
            found.state, found.registered_at = WorkerInstanceState.RUNNING, utcnow()
            emit(s, InstanceRegistered(name=row.name), _on(row), System.DAEMON)
            s.commit()
        row.state, row.registered_at = found.state, found.registered_at
        self._backoff[row.pool].ok()

    def _drain(self, row: WorkerInstance) -> None:
        """Drain the worker of ``row``: it takes no more leases (``leases.drain``). The next pass stops it, once idle."""
        with self.db.session() as s:
            write_transaction(s)
            s.get_one(WorkerInstance, row.id).state = WorkerInstanceState.DRAINING
            leases.drain(s, row.name, _on(row))
            s.commit()
        row.state = WorkerInstanceState.DRAINING

    def _end(
        self, provisioning: Provisioning, row: WorkerInstance, end: InstanceEnd, reason: str | None = None
    ) -> None:
        """Stop the instance, revoke its credential, forget its worker, and end its row."""
        provisioning.stop(row.name)
        self.network.revoke(row.name)
        with self.db.session() as s:
            write_transaction(s)
            found = s.get_one(WorkerInstance, row.id)
            found.state, found.end, found.ended_at = WorkerInstanceState.ENDED, end, utcnow()
            worker = s.get(LeaseWorker, row.name)
            if worker is not None:
                s.delete(worker)
            emit(s, InstanceEnded(name=row.name, end=end, reason=reason), _on(row), System.DAEMON)
            s.commit()
        row.state = WorkerInstanceState.ENDED
        self._started.pop(row.name, None)

    def _escalate(self, row: WorkerInstance, reason: str) -> None:
        body = (
            f'The provisioner of the pool {row.pool} refused {REFUSALS} starts in a row, the last one with: {reason}. '
            'The pool backs off and tries again. Fix the provisioner, or set its max to 0: piloop limits set '
            f'executor.pools.{row.pool}.provisioner.max=0 --reason "…"'
        )
        with self.db.session() as s:
            s.add(
                Comment(
                    target=_on(row),
                    author=System.DAEMON.signature,
                    kind=CommentKind.ESCALATION,
                    priority=ESCALATION_PRIORITY,
                    body=body,
                )
            )
            s.commit()


def _on(row: WorkerInstance) -> TargetRef:
    return TargetRef(TargetKind.INSTANCE, row.id)


def _read(db: Database, now: datetime) -> Seen:
    since = now - DAY
    with db.session() as s, read_transaction(s):
        instances = list(
            s.scalars(
                select(WorkerInstance)
                .where(WorkerInstance.state != WorkerInstanceState.ENDED)
                .order_by(WorkerInstance.id)
            )
        )
        workers = {w.id: w for w in s.scalars(select(LeaseWorker))}
        live = Counter(
            (worker, kind)
            for worker, kind in s.execute(select(Lease.worker, Lease.kind).where(Lease.state.in_(LIVE_LEASES)))
            if worker is not None
        )
        ended = s.execute(
            select(Lease.worker, func.max(Lease.ended_at)).where(Lease.worker.is_not(None)).group_by(Lease.worker)
        ).all()
        queued: list[tuple[LeaseKind, str | None]] = [  # an agent's lease has no evaluator: None
            (kind, runner)
            for kind, runner in s.execute(
                select(Lease.kind, Evaluator.runner)
                .outerjoin(Evaluation, Evaluation.id == Lease.evaluation_id)
                .outerjoin(Evaluator, Evaluator.id == Evaluation.evaluator_id)
                .where(Lease.state == LeaseState.QUEUED)
            )
        ]
        recent = s.scalars(
            select(WorkerInstance).where(WorkerInstance.ended_at.is_(None) | (WorkerInstance.ended_at > since))
        ).all()
        s.expunge_all()
    hours: defaultdict[str, float] = defaultdict(float)
    for row in recent:
        ran = (row.ended_at or now) - max(row.requested_at, since)
        hours[row.pool] += max(ran.total_seconds(), 0) / 3600
    last_end = {worker: at for worker, at in ended if worker is not None and at is not None}
    held = {worker for worker, _ in ended if worker is not None}
    return Seen(instances, workers, live, held, last_end, queued, hours)


def _need(
    pool: Pool, policy: Provisioner, seen: Seen, pools: Mapping[str, Pool], starting: Counter[str], since: datetime
) -> int:
    """The workers that ``pool`` needs for the jobs that wait and that no free or starting slot covers."""
    need = 0
    for kind in KINDS:
        per = slots(pool, kind)
        if per == 0:
            continue
        waiting = sum(1 for k, runner in seen.queued if k is kind and _runs(pool, kind, runner))
        covered = _covered(kind, pool, seen, pools, starting, since)
        short = waiting - covered if covered is not None else 0
        if short > 0:
            need = max(need, short if policy.once or per is None else math.ceil(short / per))
    return need


def _runs(pool: Pool, kind: LeaseKind, runner: str | None) -> bool:
    """Whether a worker of ``pool`` can run a job of ``kind``, of ``runner`` for an evaluation."""
    return kind is LeaseKind.AGENT or runner in pool.runners


def _covered(
    kind: LeaseKind, pool: Pool, seen: Seen, pools: Mapping[str, Pool], starting: Counter[str], since: datetime
) -> int | None:
    """The slots of ``kind`` that can take the jobs that ``pool`` would run. None: no bound.

    The free ones of the workers heard ``since`` and not drained, and those of the workers that start.
    """
    total: int | None = 0
    for worker in seen.workers.values():
        found = pools.get(worker.pool)
        if found is None or worker.drained_at is not None or worker.seen_at <= since:
            continue
        if kind is LeaseKind.EVALUATION and not set(worker.runners) & set(pool.runners):
            continue
        running = Capacity(seen.live[worker.id, LeaseKind.AGENT], seen.live[worker.id, LeaseKind.EVALUATION])
        if admits(found, HostSample.model_validate(worker.sample), running, kind) is None:
            total = add(total, less(slots(found, kind), slots(running, kind) or 0))
    for name, count in starting.items():  # each a provisioned pool of the pass
        found = pools[name]
        if kind is LeaseKind.AGENT or set(found.runners) & set(pool.runners):
            total = add(total, times(slots(found, kind), count))
    return total
