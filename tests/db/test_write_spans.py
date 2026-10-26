import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from datetime import timedelta

import pytest
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import event, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from piloop.daemon.main import ACTOR
from piloop.db.models import Campaign
from piloop.db.session import WRITE_SPAN, Database, WriteSpans, write_transaction
from piloop.otel import Tracer
from piloop.settings import TracingSettings

RELEASE_SECONDS = 0.05
"""How long the lock is held: well over the default ``slow_write``."""


@pytest.fixture
def measured(db: Database, tracer: Tracer) -> Iterator[Database]:
    """A second ``Database`` on the file of ``db`` that measures its writes into ``tracer``."""
    assert tracer.measures is not None
    database = Database(
        str(db.engine.url), actor=ACTOR, writes=WriteSpans(tracer.measures, TracingSettings().slow_write)
    )
    yield database
    database.close()


@pytest.fixture
def holder(measured: Database) -> Iterator[sqlite3.Connection]:
    """Another connection that holds the write lock until it commits. ``measured`` connects first: its connect-time
    pragmas need the lock."""
    measured.engine.connect().close()
    con = sqlite3.connect(measured.engine.url.database or '', isolation_level=None, check_same_thread=False)
    con.execute('BEGIN IMMEDIATE')
    yield con
    con.close()


def writes(spans: InMemorySpanExporter) -> list[ReadableSpan]:
    return [s for s in spans.get_finished_spans() if s.name == WRITE_SPAN]


def ms(span: ReadableSpan, key: str) -> float:
    value = (span.attributes or {})[key]
    assert isinstance(value, float)
    return value


def explicit(s: Session, campaign: Campaign) -> None:
    write_transaction(s)
    s.get(Campaign, campaign.id)


def implicit(s: Session, campaign: Campaign) -> None:
    s.execute(update(Campaign).where(Campaign.id == campaign.id).values(name='renamed'))


def many(s: Session, campaign: Campaign) -> None:
    s.connection().exec_driver_sql(
        'UPDATE campaigns SET name = ? WHERE id = ?', [('a', campaign.id), ('b', campaign.id)]
    )


@pytest.mark.parametrize('write', [explicit, implicit, many])
def test_a_contended_write_records_its_wait_for_the_lock(
    measured: Database,
    campaign: Campaign,
    holder: sqlite3.Connection,
    spans: InMemorySpanExporter,
    write: Callable[[Session, Campaign], None],
) -> None:
    def release_once_asked(*_: object) -> None:
        threading.Timer(RELEASE_SECONDS, holder.commit).start()

    with measured.session() as s:
        s.get(Campaign, campaign.id)  # a read asks for no lock
        event.listen(measured.engine, 'before_cursor_execute', release_once_asked, once=True)
        write(s, campaign)
        s.commit()

    [span] = writes(spans)
    assert dict(span.attributes or {}).keys() == {'wait_ms', 'hold_ms'}
    assert ms(span, 'wait_ms') >= RELEASE_SECONDS * 1000


def test_a_write_that_gives_up_on_the_lock_records_its_wait(
    measured: Database, holder: sqlite3.Connection, spans: InMemorySpanExporter
) -> None:
    with measured.session() as s:
        s.connection().exec_driver_sql('PRAGMA busy_timeout = 20')
        with pytest.raises(OperationalError, match='database is locked'):
            write_transaction(s)

    [span] = writes(spans)
    assert ms(span, 'wait_ms') >= 20


def test_a_long_write_records_its_hold_and_prints_nothing(
    measured: Database, spans: InMemorySpanExporter, capsys: pytest.CaptureFixture[str]
) -> None:
    with measured.session() as s:
        write_transaction(s)
        time.sleep(RELEASE_SECONDS)
        s.commit()

    [span] = writes(spans)
    assert ms(span, 'hold_ms') >= RELEASE_SECONDS * 1000
    assert WRITE_SPAN not in capsys.readouterr().err  # exported, never printed


@pytest.mark.parametrize(('slow', 'recorded'), [(timedelta(0), 1), (timedelta(days=1), 0)])
def test_only_a_slow_write_records_a_span_and_a_read_never_does(
    db: Database, tracer: Tracer, campaign: Campaign, spans: InMemorySpanExporter, slow: timedelta, recorded: int
) -> None:
    assert tracer.measures is not None
    measured = Database(str(db.engine.url), actor=ACTOR, writes=WriteSpans(tracer.measures, slow))
    with measured.session() as s:
        s.scalars(select(Campaign)).all()
    with measured.session() as s:
        implicit(s, campaign)
        s.commit()
    measured.close()

    assert len(writes(spans)) == recorded
