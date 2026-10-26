"""``piloop.daemon.loop``: what wakes each consumer, the dues, and that routing by kind misses no change."""

import importlib
import pkgutil
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from inline_snapshot import snapshot
from sqlalchemy import select, text

import piloop
from piloop.actor import System
from piloop.daemon.loop import Consumer, Loop, Phase, Turn, routes
from piloop.daemon.run_controller import RunController
from piloop.daemon.scheduler import Scheduler
from piloop.db.events import emit
from piloop.db.models import AgentSession, Campaign, EventModel, Idea, ReadBy, Run, utcnow
from piloop.db.session import Database
from piloop.db.triggers.changelog import IdeaCreated, LimitSet
from piloop.ids import TargetKind, TargetRef
from piloop.otel import Tracer
from piloop.plugins.archive.contract import ArchiveHost
from piloop.plugins.archive.local import LocalArchive
from piloop.plugins.executor.workers import Workers
from tests.conftest import ScriptRuntime, at_head, disk, factory, register, run_all, trace


@dataclass
class Clock:
    """A clock that a test moves."""

    at: datetime = field(default_factory=utcnow)

    def __call__(self) -> datetime:
        return self.at

    def advance(self, seconds: float) -> None:
        self.at += timedelta(seconds=seconds)


@dataclass
class Recorder:
    """A consumer that records each turn it runs in, and returns ``due`` seconds after it."""

    name: str
    wakes_on: frozenset[type[EventModel]]
    due: float | None = None
    phase: Phase = Phase.PLAN
    turns: list[Turn] = field(default_factory=list)

    def consumer(self) -> Consumer:
        return Consumer(self.name, self.wakes_on, self.run, self.phase)

    def run(self, turn: Turn) -> datetime | None:
        self.turns.append(turn)
        return None if self.due is None else turn.now + timedelta(seconds=self.due)


def add(db: Database, event: EventModel) -> None:
    with db.session() as s:
        emit(s, event, TargetRef(TargetKind.CAMPAIGN, 1), System.HUMAN)
        s.commit()


@pytest.fixture
def clock() -> Clock:
    return Clock()


def nothing() -> None:
    """The archiver's wake: the test only reads the consumers."""


@pytest.fixture
def consumers(db: Database, runtime: ScriptRuntime, tmp_path: Path) -> tuple[Consumer, ...]:
    """The consumers of a daemon with the workers executor and the local archive: those of piloop's own providers."""
    tracer = Tracer(None)
    controller = RunController(db, tracer, tmp_path, factory(runtime), executor=Workers())
    archiver = LocalArchive().open(ArchiveHost(db, disk(tmp_path), tmp_path, 0, tracer, nothing))
    return Scheduler(db, controller, tracer, tmp_path, factory(runtime), archiver=archiver).consumers()


def test_every_kind_wakes_a_consumer_or_says_that_only_readers_read_it(consumers: tuple[Consumer, ...]) -> None:
    """A new kind forces a decision where it is defined: route it to the consumers that read what it says, or declare
    ``READ_BY = ReadBy.READERS``. A provider's kinds wake what its capability says: the workers executor's capacity
    events wake the launches."""
    models = every_kind()
    routed = routes(consumers, models)
    readers = {kind for kind, model in models.items() if model.READ_BY is ReadBy.READERS}
    assert [kind for kind, names in routed.items() if not names and kind not in readers] == [], 'neither'
    assert [kind for kind, names in routed.items() if names and kind in readers] == [], 'both'
    capacity = {k: routed[k] for k in ('lease_worker.joined', 'lease.queued')}
    assert capacity == {'lease_worker.joined': ['evaluations', 'campaigns'], 'lease.queued': ['executor']}, (
        'a job that waits wakes the scaler of the provisioned pools, through the executor upkeep'
    )
    assert routed['agent_session.running'] == ['prompts', 'wind_down', 'campaigns'], (
        'an agent that runs gets its comments, and its wrap-up lead is a due from then: a starting row has none'
    )


def every_kind() -> dict[str, type[EventModel]]:
    """Every kind of piloop, its plugins' included: each module that defines one is imported first."""
    for found in pkgutil.walk_packages(piloop.__path__, 'piloop.'):
        if not found.name.endswith('__main__') and '.migrations.' not in found.name:
            importlib.import_module(found.name)
    return {k: m for k, m in sorted(EventModel.registered().items()) if m.__module__.startswith('piloop.')}


def test_each_kind_wakes_the_consumers_that_it_woke_before(consumers: tuple[Consumer, ...]) -> None:
    """What each kind of piloop wakes, by consumer name. A change here changes what the daemon reacts to."""
    assert routes(consumers, every_kind()) == snapshot(
        {
            'agent_session.cancel_requested': ['wind_down'],
            'agent_session.finished': ['archiver', 'campaigns'],
            'agent_session.identified': [],
            'agent_session.job_ended': [],
            'agent_session.news_taken': [],
            'agent_session.not_resumed': [],
            'agent_session.requested': ['campaigns'],
            'agent_session.running': ['prompts', 'wind_down', 'campaigns'],
            'agent_session.session_saved': [],
            'agent_session.started': ['campaigns'],
            'agent_session.stop_ended': [],
            'agent_session.workspace_conflict': [],
            'agent_session.wrap_up': [],
            'anomaly.disk': [],
            'anomaly.reported': ['campaigns'],
            'anomaly.startup': ['campaigns'],
            'campaign.added': ['facts', 'campaigns'],
            'campaign.closed': [],
            'campaign.config': ['facts', 'campaigns'],
            'campaign.parent_id': [],
            'campaign.paused': ['campaigns'],
            'campaign.status': ['archiver', 'wind_down', 'facts', 'campaigns'],
            'campaign.stop_requested': ['wind_down', 'campaigns'],
            'campaign.type': [],
            'candidate.deleted': ['campaigns'],
            'candidate.imported': ['campaigns'],
            'candidate.judged': ['campaigns'],
            'candidate.origin': ['campaigns'],
            'candidate.rebased': ['campaigns'],
            'candidate.rejudge_failed': [],
            'candidate.rejudge_requested': ['campaigns'],
            'candidate.remeasure_requested': ['campaigns'],
            'candidate.verdict': ['campaigns'],
            'comment.acked_at': [],
            'comment.decision': ['prompts', 'wind_down', 'campaigns'],
            'comment.deleted': [],
            'comment.delivered': [],
            'comment.escalation': ['prompts', 'wind_down', 'campaigns'],
            'comment.finding': ['prompts', 'wind_down', 'campaigns'],
            'comment.message': ['prompts', 'wind_down', 'campaigns'],
            'comment.note': ['prompts', 'wind_down', 'campaigns'],
            'comment.queued': [],
            'comment.undelivered': [],
            'evaluation.cancelled': ['evaluations'],
            'evaluator.metric_lines': [],
            'fact.read': ['campaigns'],
            'idea.base_candidate_id': ['campaigns'],
            'idea.created': ['campaigns'],
            'idea.deleted': ['campaigns'],
            'idea.linked': ['campaigns'],
            'idea.priority': ['campaigns'],
            'idea.recovery_requeued': [],
            'idea.status': ['campaigns'],
            'idea.track': ['campaigns'],
            'idea.unlinked': ['campaigns'],
            'instance.ended': [],
            'instance.registered': [],
            'instance.requested': [],
            'launch.paused': [],
            'launch.resumed': [],
            'lease.expired': ['executor'],
            'lease.prompted': [],
            'lease.queued': ['executor'],
            'lease.revoked': [],
            'lease.stale_report': [],
            'lease.unreachable': ['executor'],
            'lease_worker.joined': ['evaluations', 'campaigns'],
            'lease_worker.relieved': ['evaluations', 'campaigns'],
            'limit.set': ['limits', 'evaluations', 'campaigns'],
            'limit.unset': ['limits', 'evaluations', 'campaigns'],
            'measurement.recorded': ['evaluations', 'campaigns'],
            'request.answered': [],
            'run.brief_cid': [],
            'run.cancel_requested': ['wind_down'],
            'run.finished': ['campaigns'],
            'run.queued': [],
            'run.received': [],
            'run.relaunched': [],
            'run.rerouted': [],
            'run.started': [],
            'secret.deleted': [],
            'secret.set': [],
            'subject.settled': ['archiver'],
            'worker.drained': ['executor'],
        }
    )


def test_a_turn_runs_each_consumer_that_an_event_routes_to_or_whose_due_came_once(db: Database, clock: Clock) -> None:
    ideas = Recorder('ideas', frozenset({IdeaCreated}))
    timer = Recorder('timer', frozenset(), due=60)
    everything = Recorder('everything', frozenset({EventModel}))
    loop = Loop(db, [ideas.consumer(), timer.consumer(), everything.consumer()], clock)
    loop.start()
    assert [len(r.turns) for r in (ideas, timer, everything)] == [1, 1, 1], 'the start runs each one'
    add(db, IdeaCreated())
    add(db, IdeaCreated())
    add(db, LimitSet(key='k', value=1))
    loop.turn()
    assert [len(r.turns) for r in (ideas, timer, everything)] == [2, 1, 2], 'once for both ideas; a base model is all'
    loop.turn()
    assert [len(r.turns) for r in (ideas, timer, everything)] == [2, 1, 2], 'nothing new: nothing runs'
    clock.advance(59)
    loop.turn()
    assert len(timer.turns) == 1
    clock.advance(1)
    assert loop.next_due() == clock.at
    loop.turn()
    assert len(timer.turns) == 2 and loop.next_due() == clock.at + timedelta(seconds=60), 'its due came, and moves on'
    loop.wake('ideas')
    loop.turn()
    assert len(ideas.turns) == 3, 'woken without an event: a thread of the process ended'


def test_a_turn_runs_the_phases_in_order_and_the_consumers_of_one_in_the_order_given(
    db: Database, clock: Clock
) -> None:
    ran: list[str] = []
    given = [
        Recorder('plan', frozenset(), phase=Phase.PLAN),
        Recorder('upkeep', frozenset(), phase=Phase.UPKEEP),
        Recorder('configure', frozenset(), phase=Phase.CONFIGURE),
        Recorder('upkeep too', frozenset(), phase=Phase.UPKEEP),
    ]
    Loop(db, [replace(c, run=recorded(c, ran)) for c in (r.consumer() for r in given)], clock).start()
    assert ran == ['configure', 'upkeep', 'upkeep too', 'plan']


@pytest.fixture
def served(db: Database) -> Iterator[Callable[[Loop], None]]:
    stop = threading.Event()
    threads: list[threading.Thread] = []

    def serve(loop: Loop) -> None:
        thread = threading.Thread(target=loop.serve, args=(stop.is_set,))
        thread.start()
        threads.append(thread)

    yield serve
    stop.set()
    db.notify()
    for thread in threads:
        thread.join(10)
        assert not thread.is_alive(), 'a stop that notifies ends the wait'


def test_the_loop_sleeps_until_another_process_writes_an_event_or_a_due_comes(
    db: Database, served: Callable[[Loop], None]
) -> None:
    ideas = Recorder('ideas', frozenset({IdeaCreated}))
    timer = Recorder('timer', frozenset(), due=0.5)
    served(Loop(db, [ideas.consumer(), timer.consumer()]))
    deadline = time.monotonic() + 10
    while len(timer.turns) < 3 and time.monotonic() < deadline:
        time.sleep(0.05)
    assert len(timer.turns) >= 3, 'its due wakes it'
    assert len(ideas.turns) == 1, 'only the start: no event came'
    other = Database(str(db.engine.url), actor=System.HUMAN)
    add(other, IdeaCreated())
    other.close()
    deadline = time.monotonic() + 10
    while len(ideas.turns) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert len(ideas.turns) == 2, "another process's event rang the doorbell"


def test_a_turn_late_past_its_due_says_how_long_the_loop_was_away(db: Database, clock: Clock) -> None:
    timer = Recorder('timer', frozenset(), due=10)
    loop = Loop(db, [timer.consumer()], clock)
    loop.start()
    clock.advance(3600)
    loop.turn(absent=timedelta(seconds=3590))
    assert timer.turns[-1].absent == timedelta(seconds=3590), 'the executor does not expire work for a suspend'


@dataclass(frozen=True)
class Home:
    """One database and a scheduler without slots: the passes decide, and nothing launches."""

    db: Database
    scheduler: Scheduler


def home(db: Database, runtime: ScriptRuntime, tmp_path: Path, clock: Clock) -> Home:
    tracer = Tracer(None)
    controller = RunController(db, tracer, tmp_path, factory(runtime), executor=Workers())
    return Home(db, Scheduler(db, controller, tracer, tmp_path, factory(runtime), now=clock))


def state(db: Database) -> list[object]:
    """What the daemon decides: the agents it requests, the runs it queues, the ideas and campaigns it moves."""
    with db.session() as s:
        return [
            [(a.kind, a.state, a.reason) for a in s.scalars(select(AgentSession).order_by(AgentSession.id))],
            [(r.idea_id, r.status) for r in s.scalars(select(Run).order_by(Run.id))],
            [(i.title, i.status) for i in s.scalars(select(Idea).order_by(Idea.id))],
            [(c.status, c.paused) for c in s.scalars(select(Campaign).order_by(Campaign.id))],
        ]


WRITES = (
    "INSERT INTO ideas (track, campaign_id, title, hypothesis, created_by) VALUES ('score', 1, 'a', 'h', 'human')",
    "INSERT INTO ideas (track, campaign_id, title, hypothesis, created_by, status) VALUES ('score', 1, 's', 'h', 'scout#9',"
    " 'seed')",
    "UPDATE ideas SET priority = 0 WHERE title = 'a'",
    "INSERT INTO agent_sessions (kind, campaign_id, state, reason) VALUES ('watchdog', 1, 'requested', 'by hand')",
    "UPDATE campaigns SET paused = 'launches', paused_reason = 'night' WHERE id = 1",
    'UPDATE campaigns SET paused = NULL, paused_reason = NULL WHERE id = 1',
    "UPDATE ideas SET status = 'parked' WHERE title = 'a'",
    "INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('campaign', 1, 'human', 'note', 'n')",
    "INSERT INTO limits (key, value, reason, actor) VALUES ('executor.unknown', '1', 'test', 'human')",
    "UPDATE campaigns SET status = 'stopped', stopped_reason = 'by hand' WHERE id = 1",
)
"""What a person or an agent writes, each through another connection, as the CLI or ``piloop sql`` does."""


def test_routing_by_kind_decides_what_a_pass_on_every_change_decides(
    db: Database, campaign: Campaign, campaign_path: Path, schema: Path, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The missed-wake check that a periodic pass would otherwise be: after each write, the routed loop and a loop that
    runs every consumer leave the same state. A write whose event no consumer reads would make them differ."""
    clock = Clock()
    every_db = Database(f'sqlite:///{at_head(schema, tmp_path / "every" / "piloop.db")}', actor=System.DAEMON)
    register(every_db, campaign_path)
    routed, every = home(db, runtime, tmp_path / 'routed', clock), home(every_db, runtime, tmp_path / 'every', clock)
    ran: list[str] = []
    loop = Loop(db, [replace(c, run=recorded(c, ran)) for c in routed.scheduler.consumers()], clock)
    loop.start()
    run_all(every.scheduler)
    writers = [Database(str(h.db.engine.url), actor=System.HUMAN) for h in (routed, every)]
    turns: list[list[str]] = []
    try:
        for write in WRITES:
            for writer in writers:
                with writer.engine.begin() as conn:
                    conn.execute(text(write))
            clock.advance(1)
            ran.clear()
            while loop.turn():
                pass
            turns.append(list(ran))
            run_all(every.scheduler)
            assert state(routed.db) == state(every.db), write
    finally:
        for writer in writers:
            writer.close()
        every_db.close()
    assert turns == snapshot(
        [
            ['campaigns'],
            ['campaigns'],
            ['campaigns'],
            ['campaigns'],
            ['campaigns'],
            ['campaigns'],
            ['campaigns'],
            ['prompts', 'wind_down', 'campaigns'],
            ['limits', 'evaluations', 'campaigns'],
            ['wind_down', 'facts', 'campaigns'],
        ]
    ), 'the consumers that each write ran, in the order of the turn'
    assert trace(db) == snapshot(
        [
            'idea.created idea',
            'idea.status idea',
            'agent_session.requested agent_session',
            'run.queued run',
            'idea.created idea',
            'agent_session.requested agent_session',
            'idea.priority idea',
            'agent_session.requested agent_session',
            'campaign.paused campaign',
            'campaign.paused campaign',
            'idea.status idea',
            'comment.note comment',
            'limit.set home',
            'campaign.status campaign',
        ]
    )


def recorded(consumer: Consumer, ran: list[str]) -> Callable[[Turn], datetime | None]:
    """``consumer``'s job, which also appends its name to ``ran``."""

    def run(turn: Turn) -> datetime | None:
        ran.append(consumer.name)
        return consumer.run(turn)

    return run


def test_a_served_loop_reads_a_long_feed_in_pages_and_runs_an_overdue_consumer_without_waiting(
    db: Database, served: Callable[[Loop], None]
) -> None:
    late: list[Turn] = []

    def overdue(turn: Turn) -> datetime | None:
        """Due a second ago on its first run, then never: the loop runs it again before it waits."""
        late.append(turn)
        return turn.now - timedelta(seconds=1) if len(late) == 1 else None

    ideas = Recorder('ideas', frozenset({IdeaCreated}))
    gate = threading.Event()
    other = Database(str(db.engine.url), actor=System.HUMAN)

    def held(turn: Turn) -> None:
        if not gate.is_set():  # the first turns wait here, so that both ideas land in one gap
            gate.set()
            add(other, IdeaCreated())
            add(other, IdeaCreated())

    consumers = [
        Consumer('held', frozenset(), held, Phase.PLAN),
        Consumer('late', frozenset(), overdue, Phase.PLAN),
        ideas.consumer(),
    ]
    served(Loop(db, consumers, page=1))
    deadline = time.monotonic() + 10
    while len(ideas.turns) < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    other.close()
    assert len(late) == 2, 'its due had passed: no wait'
    assert len(ideas.turns) == 3, 'the start, then one page for each idea'
