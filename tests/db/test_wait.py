"""``Database.wait_until``: a waiter wakes on a commit of this process, polls for one of another, and gives up."""

import threading
import time
from collections.abc import Callable

import pytest
from sqlalchemy.orm import Session

from piloop.actor import System
from piloop.db import session as db_session
from piloop.db.models import Campaign
from piloop.db.session import Database, DatabaseClosed


def rename(db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        s.get_one(Campaign, campaign.id).name = 'renamed'
        s.commit()


def renamed(campaign: Campaign) -> Callable[[Session], bool]:
    return lambda s: s.get_one(Campaign, campaign.id).name == 'renamed'


def test_a_waiter_wakes_on_a_commit_of_this_process_before_its_poll(
    db: Database, campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(db_session, 'WAIT_POLL_SECONDS', 60)
    seen: list[bool] = []
    waiting = threading.Thread(target=lambda: seen.append(db.wait_until(renamed(campaign))))
    waiting.start()
    time.sleep(0.1)
    rename(db, campaign)
    waiting.join(10)
    assert seen == [True]


def test_a_waiter_polls_for_a_commit_of_another_process(
    db: Database, campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(db_session, 'WAIT_POLL_SECONDS', 0.05)
    other = Database(str(db.engine.url), actor=System.HUMAN)  # its commits notify its own waiters, not this one's
    seen: list[bool] = []
    waiting = threading.Thread(target=lambda: seen.append(db.wait_until(renamed(campaign))))
    waiting.start()
    time.sleep(0.1)
    rename(other, campaign)
    other.close()
    waiting.join(10)
    assert seen == [True]


def test_a_closed_database_makes_its_waiters_give_up(db: Database) -> None:
    seen: list[bool] = []
    waiting = threading.Thread(target=lambda: seen.append(db.wait_until(lambda _s: False)))
    waiting.start()
    time.sleep(0.1)
    db.close()
    waiting.join(10)
    assert not waiting.is_alive() and seen == [False]
    assert db.wait_until(lambda _s: True) is False, 'and a new waiter gives up at once'


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


def test_a_closed_database_refuses_a_new_session(db: Database) -> None:
    """A late caller fails loudly: a session after the close would open a connection that nothing closes."""
    db.close()
    with pytest.raises(DatabaseClosed, match='the database is closed'):
        db.session()
