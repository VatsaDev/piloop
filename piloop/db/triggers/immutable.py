"""Rows that never change: a ``journal`` line or a lesson is never rewritten, and a journal line is never deleted.

The authorizer of ``piloop sql`` (``piloop.db.sql``) denies an UPDATE of these tables, and a DELETE of the journal. A
lesson can be deleted, for example with its project. The authorizer cannot see an ``INSERT OR REPLACE``: SQLite
deletes the old row to resolve the conflict without asking it. So a persistent trigger refuses an insert whose id is
taken, whatever its conflict clause.
"""

from piloop.db.triggers.ddl import Trigger

IMMUTABLE = ('journal', 'lessons')
"""Tables whose rows are never updated or replaced: a new line, or a new conclusion, is a new row."""
UNDELETABLE = ('journal',)
"""Tables whose rows are never deleted either."""


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
