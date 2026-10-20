"""Rows that never change: a ``journal`` line or a lesson is never rewritten, and events are only ever appended.

The authorizer of ``piloop sql`` (``piloop.db.sql``) denies an UPDATE of these tables, and a DELETE of the journal and
of events. A lesson can be deleted, for example with its campaign. The authorizer cannot see an ``INSERT OR REPLACE``:
SQLite deletes the old row to resolve the conflict without asking it. So a persistent trigger refuses an insert whose
id is taken, whatever its conflict clause. For events, TEMP triggers (``EVENTS``) refuse any id that is not the next.
"""

from piloop.db.triggers.ddl import Trigger

IMMUTABLE = ('journal', 'lessons')
"""Tables whose rows are never updated or replaced: a new line, or a new conclusion, is a new row."""
UNDELETABLE = ('journal',)
"""Tables whose rows are never deleted either."""
APPEND_ONLY = ('events',)
"""Tables that a client only appends to: no UPDATE, no DELETE, and each new row takes the next id (``EVENTS``).

An event id is a reader's cursor (``piloop.reports.feed``). SQLite reuses the ids of deleted newest rows, an UPDATE can
move an id, and an explicit id can land below a cursor: a reader past it would miss the event."""


def no_replace(table: str) -> Trigger:
    """Persistent: refuse an INSERT into ``table`` whose id is taken, so that ``INSERT OR REPLACE`` cannot rewrite a row."""
    return Trigger(
        f'{table}_no_replace',
        table,
        'BEFORE INSERT',
        when=f'EXISTS (SELECT 1 FROM {table} WHERE id = NEW.id)',
        body=f"SELECT RAISE(ABORT, '{table} rows are never replaced: insert a new row')",
    )


NO_REPLACE = tuple(no_replace(table) for table in IMMUTABLE)


NEXT_ID = "SELECT RAISE(ABORT, 'events take the next id: leave id out')"
EVENTS = (
    # An id left out reads -1 in a BEFORE trigger, like an explicit -1, which the AFTER trigger refuses
    Trigger(
        'events_checked',
        'events',
        'BEFORE INSERT',
        body=';\n'.join(
            [
                f'{NEXT_ID} WHERE NEW.id != -1',
                "SELECT RAISE(ABORT, 'events.kind and events.actor are text')"
                " WHERE typeof(NEW.kind) != 'text' OR typeof(NEW.actor) != 'text'",
                "SELECT RAISE(ABORT, 'events.target_id is an integer') WHERE typeof(NEW.target_id) != 'integer'",
                "SELECT RAISE(ABORT, 'events.at is a time in text, like 2026-10-20 08:00:00')"
                " WHERE typeof(NEW.at) != 'text' OR julianday(NEW.at) IS NULL",
                "SELECT RAISE(ABORT, 'events.payload is JSON text, not a blob') WHERE typeof(NEW.payload) = 'blob'",
            ]
        ),
        temp=True,
    ),
    Trigger('events_next_id', 'events', 'AFTER INSERT', when='NEW.id < 1', body=NEXT_ID, temp=True),
)
"""TEMP: an event takes the next id, and its columns have the types that a reader of the feed reads.

Every piloop connection has them, so they also hold for ``piloop sql``. The payload may still be any text: the daemon
writes what ``json.dumps`` gives, ``NaN`` included, and the reader of the feed shows a payload that is not JSON as
stored."""
