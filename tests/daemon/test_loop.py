"""``piloop.daemon.loop``: what wakes each consumer, the dues, and that routing by kind misses no change."""

import re
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, text

from piloop.actor import System
from piloop.daemon.loop import Consumer, Loop, Turn, routes
from piloop.daemon.run_controller import RunController
from piloop.daemon.scheduler import IGNORED, Scheduler
from piloop.db.events import emit
from piloop.db.models import AgentSession, Campaign, EventModel, Idea, Run, utcnow
from piloop.db.session import Database
from piloop.db.triggers.changelog import IdeaCreated, LimitSet
from piloop.ids import TargetKind, TargetRef
from piloop.otel import Tracer
from piloop.plugins.executor.workers import Workers
from tests.conftest import ScriptRuntime, at_head, factory, register


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
    turns: list[Turn] = field(default_factory=list)

    def consumer(self) -> Consumer:
        return Consumer(self.name, self.wakes_on, self.run)

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


def test_every_kind_of_core_wakes_a_consumer_of_the_scheduler_or_is_ignored_on_purpose(
    db: Database, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """A new kind forces a decision: route it to the jobs that read what it says, or name it in ``IGNORED``. A
    provider's kinds wake what its capability says: the workers executor's capacity events wake the launches."""
    tracer = Tracer(None)
    controller = RunController(db, tracer, tmp_path, factory(runtime), executor=Workers())
    consumers = Scheduler(db, controller, tracer, tmp_path, factory(runtime)).consumers()
    ignored = {kind for model in IGNORED for kind in model.kinds()}
    core = {k: m for k, m in EventModel.registered().items() if re.match(r'piloop\.(?!plugins\.)', m.__module__)}
    routed = routes(consumers, core)
    assert [kind for kind, names in routed.items() if not names and kind not in ignored] == [], 'neither'
    assert [kind for kind, names in routed.items() if names and kind in ignored] == [], 'both'
    capacity = routes(consumers, {k: EventModel.registered()[k] for k in ('lease_worker.joined', 'lease.queued')})
    assert capacity == {'lease_worker.joined': ['evaluations', 'campaigns'], 'lease.queued': ['executor']}, (
        'a job that waits wakes the scaler of the provisioned pools, through the executor upkeep'
    )
    assert routed['agent_session.running'] == ['prompts', 'wind_down', 'campaigns'], (
        'an agent that runs gets its comments, and its wrap-up lead is a due from then: a starting row has none'
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
    routed.scheduler.loop.start()
    every.scheduler.run_all()
    writers = [Database(str(h.db.engine.url), actor=System.HUMAN) for h in (routed, every)]
    try:
        for write in WRITES:
            for writer in writers:
                with writer.engine.begin() as conn:
                    conn.execute(text(write))
            clock.advance(1)
            while routed.scheduler.loop.turn():
                pass
            every.scheduler.run_all()
            assert state(routed.db) == state(every.db), write
    finally:
        for writer in writers:
            writer.close()
        every_db.close()


def test_a_served_loop_reads_a_long_feed_in_pages_and_runs_an_overdue_consumer_without_waiting(
    db: Database, served: Callable[[Loop], None], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr('piloop.daemon.loop.PAGE', 1)
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

    served(Loop(db, [Consumer('held', frozenset(), held), Consumer('late', frozenset(), overdue), ideas.consumer()]))
    deadline = time.monotonic() + 10
    while len(ideas.turns) < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    other.close()
    assert len(late) == 2, 'its due had passed: no wait'
    assert len(ideas.turns) == 3, 'the start, then one page for each idea'
