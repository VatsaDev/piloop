"""``Database.wait_until``: a waiter wakes on a commit of this process, on an event of another, and gives up."""

import asyncio
import sqlite3
import threading
import time
from collections.abc import Callable

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.actor import System
from piloop.db.models import Campaign, Pause
from piloop.db.session import Database, DatabaseClosed


def rename(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        s.get_one(Campaign, campaign.id).name = 'renamed'
        s.commit()


def renamed(campaign: Campaign) -> Callable[[Session], bool]:
    return lambda s: s.get_one(Campaign, campaign.id).name == 'renamed'


def pause(db: Database, campaign: Campaign) -> None:
    """A change that adds an event: ``campaign.paused``."""
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        row.paused, row.paused_reason = Pause.ALL, 'a test'
        s.commit()


def paused(campaign: Campaign) -> Callable[[Session], bool]:
    return lambda s: s.get_one(Campaign, campaign.id).paused is Pause.ALL


def test_a_waiter_wakes_on_a_commit_of_this_process(db: Database, campaign: Campaign) -> None:
    seen: list[bool] = []
    waiting = threading.Thread(target=lambda: seen.append(db.wait_until(renamed(campaign))))
    waiting.start()
    time.sleep(0.1)
    rename(db, campaign)
    waiting.join(10)
    assert seen == [True]


def test_a_waiter_wakes_on_an_event_of_another_process(db: Database, campaign: Campaign) -> None:
    other = Database(str(db.engine.url), actor=System.HUMAN)  # its commits notify its own waiters, not this one's
    seen: list[bool] = []
    waiting = threading.Thread(target=lambda: seen.append(db.wait_until(paused(campaign))))
    waiting.start()
    time.sleep(0.1)
    began = time.monotonic()
    pause(other, campaign)
    other.close()
    waiting.join(10)
    assert seen == [True]
    assert time.monotonic() - began < 5, 'the doorbell rang'


def test_a_commit_of_another_process_without_an_event_wakes_no_waiter(db: Database, campaign: Campaign) -> None:
    """A heartbeat or a renewal of another process must not wake every waiter: only the feed's head rings."""
    other = Database(str(db.engine.url), actor=System.HUMAN)
    waiting = threading.Thread(target=lambda: db.wait_until(renamed(campaign), timeout=1))
    began = time.monotonic()
    waiting.start()
    time.sleep(0.1)
    rename(other, campaign)
    other.close()
    waiting.join(10)
    assert time.monotonic() - began >= 0.9, 'it read again only at its timeout'


def test_notify_wakes_a_waiter_to_check_its_give_up(db: Database) -> None:
    asked = threading.Event()
    seen: list[bool] = []
    waiting = threading.Thread(target=lambda: seen.append(db.wait_until(lambda _s: False, give_up=asked.is_set)))
    waiting.start()
    time.sleep(0.1)
    asked.set()
    db.notify()
    waiting.join(5)
    assert seen == [False]


def test_a_closed_database_makes_its_waiters_give_up(db: Database) -> None:
    seen: list[bool] = []
    waiting = threading.Thread(target=lambda: seen.append(db.wait_until(lambda _s: False)))
    waiting.start()
    time.sleep(0.1)
    db.close()
    waiting.join(10)
    assert not waiting.is_alive() and seen == [False]
    assert db.wait_until(lambda _s: True) is False, 'and a new waiter gives up at once'
    assert db._doorbell is not None
    db._doorbell.join(5)
    assert not db._doorbell.is_alive(), 'the doorbell stops, and its connection went back before the pool closed'


def test_a_waiter_gives_up_when_asked(db: Database) -> None:
    asked = threading.Event()
    seen: list[bool] = []
    waiting = threading.Thread(target=lambda: seen.append(db.wait_until(lambda _s: False, give_up=asked.is_set)))
    waiting.start()
    asked.set()
    waiting.join(10)
    assert seen == [False]


def test_close_lets_a_read_in_flight_end_before_it_closes_the_connections(db: Database) -> None:
    """A connection that a read holds while the pool goes is never closed, and Python warns when it collects it."""
    reading, release = threading.Event(), threading.Event()

    def slow(_s: Session) -> bool:
        reading.set()
        release.wait(10)
        return False

    waiting = threading.Thread(target=lambda: db.wait_until(slow))
    waiting.start()
    assert reading.wait(10)
    closing = threading.Thread(target=db.close)
    closing.start()
    closing.join(0.2)
    assert closing.is_alive(), 'it waits for the read'
    release.set()
    closing.join(10)
    waiting.join(10)
    assert not closing.is_alive() and not waiting.is_alive()


def test_close_lets_a_session_in_flight_end_before_it_closes_the_connections(db: Database) -> None:
    """Any session, not only a waiter's read: a thread that the daemon did not join can still be in one."""
    holding, release = threading.Event(), threading.Event()
    held: list[sqlite3.Connection] = []

    def hold() -> None:
        with db.session() as s:
            s.execute(select(1))
            raw = s.connection().connection.dbapi_connection
            assert isinstance(raw, sqlite3.Connection)
            held.append(raw)
            holding.set()
            release.wait(10)

    holder = threading.Thread(target=hold)
    holder.start()
    assert holding.wait(10)
    closing = threading.Thread(target=db.close)
    closing.start()
    closing.join(0.2)
    assert closing.is_alive(), 'it waits for the session'
    release.set()
    closing.join(10)
    holder.join(10)
    with pytest.raises(sqlite3.ProgrammingError, match='closed database'):
        held[0].execute('SELECT 1')


def test_a_session_opened_before_the_close_gets_no_connection_after_it(db: Database) -> None:
    """Its first statement fails loudly, and opens no connection that nothing would close."""
    with db.session() as s:
        db.close()
        with pytest.raises(DatabaseClosed, match='the database is closed'):
            s.execute(select(1))


def test_a_closed_database_refuses_a_new_session(db: Database) -> None:
    """A late caller fails loudly: a session after the close would open a connection that nothing closes."""
    db.close()
    with pytest.raises(DatabaseClosed, match='the database is closed'):
        db.session()


def test_a_waiter_gives_up_at_its_timeout(db: Database) -> None:
    began = time.monotonic()
    assert db.wait_until(lambda _s: False, timeout=0.1) is False
    assert time.monotonic() - began < 5


def test_changed_returns_on_a_commit_of_this_process_and_holds_no_thread(db: Database, campaign: Campaign) -> None:
    async def waits() -> None:
        seen = await db.changed(0, 0)
        assert await db.changed(seen, 0.01) == seen, 'nothing committed: it returns at its timeout'
        threading.Timer(0.1, rename, [db, campaign]).start()  # another thread, as the daemon's writers are
        began = time.monotonic()
        assert await db.changed(seen, 30) == seen + 1
        assert time.monotonic() - began < 10

    asyncio.run(waits())


def test_changed_waits_to_its_timeout_and_returns_at_once_once_closed(db: Database) -> None:
    async def waits() -> None:
        seen = await db.changed(0, 0)
        began = time.monotonic()
        assert await db.changed(seen, 0.5) == seen
        assert time.monotonic() - began >= 0.4, 'nothing committed: the doorbell does not wake it'
        threading.Timer(0.1, db.close).start()
        began = time.monotonic()
        await db.changed(seen, 30)
        assert time.monotonic() - began < 10, 'the close resolves it'
        assert await db.changed(seen, 30) == seen, 'once closed, at once'

    asyncio.run(waits())
