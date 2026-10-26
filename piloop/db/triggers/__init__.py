"""piloop's own triggers: what the migrations install (``PERSISTENT``) and what every connection installs (``TEMP``).

The change log must be TEMP: only a trigger of the connection can read the connection's actor
(``temp.session_actor``), and an event without an actor is of no use. ``updated_at`` and ``comment.escalation`` need
nothing from the connection, so they are persistent. ``updated_by`` and every event of the change log are TEMP and
belong to the connection. Core and each plugin declare the triggers on their tables (``piloop.db.schema.Schema``), and
each of piloop's schemas takes its share of these (``on``). ``piloop.db.sql.install_actor`` creates every TEMP one at
connect time (``temp_sql``). The migrations run the DDL of the persistent ones that Python renders.

To add a trigger, add one call to one tuple: ``changed('ideas', 'plan')`` in
``changelog.CHANGES`` for a decision, ``tombstone(...)`` in ``TOMBSTONES`` for a delete, a table in
``provenance.TRACKED_TABLES`` for both stamps, or a table in ``fts.FTS_COLUMNS`` for an index and its sync triggers.
A new persistent trigger also needs a migration that calls ``recreate(op, table)``. SQLite drops the triggers of a
table when Alembic's batch mode rebuilds it, so a batch migration also ends with ``recreate(op, table)``.
"""

from collections.abc import Iterable

from alembic.operations import Operations
from sqlalchemy import text

from piloop.db.models import Base
from piloop.db.triggers import campaigns, changelog, ended, fts, immutable, legacy, links, provenance, roles
from piloop.db.triggers.ddl import Event, Trigger
from piloop.db.triggers.provenance import DAEMON_ACTOR, TRACKED_TABLES

__all__ = [
    'DAEMON_ACTOR',
    'PERSISTENT',
    'TEMP',
    'TRACKED_TABLES',
    'Event',
    'Trigger',
    'install',
    'on',
    'recreate',
    'temp_sql',
]

PERSISTENT: tuple[Trigger, ...] = (
    *provenance.UPDATED_AT,
    changelog.escalation(),
    links.same_campaign(),
    roles.known_role(),
    *campaigns.acyclic(),
    *immutable.NO_REPLACE,
    *fts.SYNC,
)
"""The triggers in ``sqlite_master`` after ``migrate()``. ``tests/db/test_migrations.py`` checks that the two agree."""
TEMP: tuple[Trigger, ...] = (
    *provenance.UPDATED_BY,
    *changelog.CHANGES,
    *changelog.TOMBSTONES,
    *changelog.COMMENTS,
    *changelog.OUTBOX,
    *ended.ENDED_AGENTS,
    *immutable.EVENTS,
)
"""The triggers in ``sqlite_temp_master`` of every connection."""


def on(tables: Iterable[type[Base]]) -> tuple[Trigger, ...]:
    """Piloop's own triggers on ``tables``, persistent and TEMP: the share of one of piloop's schemas."""
    names = {table.__tablename__ for table in tables}
    return tuple(t for t in (*PERSISTENT, *TEMP) if t.table in names)


def recreate(op: Operations, table: str, triggers: Iterable[Trigger] = PERSISTENT) -> None:
    """Create the persistent ``triggers`` of ``table`` again, for example after a batch rebuild.

    A package's revision passes its schema's triggers; piloop's revisions create piloop's own. A trigger that
    ``requires`` a table is created only when that table exists at this point of the migration chain, so a batch
    migration does not need to know which indexes exist yet. Before a renaming of the schema, a trigger has the names
    of that schema (``legacy``).
    """
    old = legacy.stages(op)
    for trigger in (t for t in triggers if not t.temp):
        _, on, requires, sql = legacy.rendered(trigger, old)
        if on == table and (requires is None or _exists(op, requires)):
            op.execute(sql)


def install(op: Operations, names: Iterable[str], triggers: Iterable[Trigger] = PERSISTENT) -> None:
    """Create the persistent ones of ``triggers`` that ``names`` lists, in the names of the schema at hand (``legacy``)."""
    wanted, old = set(names), legacy.stages(op)
    for trigger in (t for t in triggers if not t.temp):
        name, _, _, sql = legacy.rendered(trigger, old)
        if name in wanted:
            op.execute(sql)


def _exists(op: Operations, table: str) -> bool:
    query = text("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = :name")
    return op.get_bind().execute(query, {'name': table}).scalar() is not None


def temp_sql(triggers: Iterable[Trigger]) -> str:
    """What every connection runs before its authorizer is on: the actor table, then the TEMP ones of ``triggers``."""
    return '\n'.join([provenance.SESSION_ACTOR_TABLE, *(t.sql() for t in triggers if t.temp)]) + '\n'
