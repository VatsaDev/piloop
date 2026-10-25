"""Trigger DDL: what the migrations install (``PERSISTENT``) and what every connection installs for itself (``TEMP``).

The change log must be TEMP: only a trigger of the connection can read the connection's actor
(``temp.session_actor``), and an event without an actor is of no use. ``updated_at`` and ``comment.escalation`` need
nothing from the connection, so they are persistent. ``updated_by`` and every event of the change log are TEMP and
belong to the connection. ``temp_sql`` renders them into ``piloop/db/actor_triggers.sql``, which
``piloop.db.sql.install_actor`` runs at connect time. ``uv run -m scripts.generate_all`` writes that file, and
``make docs-check`` fails when it is out of date. The migrations run the DDL of ``PERSISTENT`` that Python renders.

To add a trigger, add one call to one tuple, then run ``make docs``: ``changed('ideas', 'plan')`` in
``changelog.CHANGES`` for a decision, ``tombstone(...)`` in ``TOMBSTONES`` for a delete, a table in
``provenance.TRACKED_TABLES`` for both stamps, or a table in ``fts.FTS_COLUMNS`` for an index and its sync triggers.
A new persistent trigger also needs a migration that calls ``recreate(op, table)``. SQLite drops the triggers of a
table when Alembic's batch mode rebuilds it, so a batch migration also ends with ``recreate(op, table)``.
"""

from collections.abc import Iterable

from alembic.operations import Operations
from sqlalchemy import text

from piloop.db.triggers import campaigns, changelog, ended, fts, immutable, legacy, links, provenance, roles
from piloop.db.triggers.ddl import Event, Trigger
from piloop.db.triggers.provenance import DAEMON_ACTOR, TRACKED_TABLES

__all__ = [
    'DAEMON_ACTOR',
    'EVENT_TRIGGER_NAMES',
    'PERSISTENT',
    'TEMP',
    'TRACKED_TABLES',
    'Event',
    'Trigger',
    'install',
    'recreate',
    'temp_sql',
]

GENERATED = (
    '-- WARNING: This file is auto-generated (uv run -m scripts.generate_all). Update the triggers in'
    " 'piloop/db/triggers/' if you want to change anything here.\n"
)

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
"""The triggers in ``sqlite_temp_master`` of every connection, in the order that ``actor_triggers.sql`` creates them."""
EVENT_TRIGGER_NAMES = frozenset(t.name for t in (*PERSISTENT, *TEMP) if t.body.startswith('INSERT INTO events'))
"""The triggers that write events: the only statements of ``piloop sql`` that the authorizer lets insert into them."""


def recreate(op: Operations, table: str) -> None:
    """Create the persistent triggers of ``table`` again, for example after a batch rebuild.

    A trigger that ``requires`` a table is created only when that table exists at this point of the migration chain,
    so a batch migration does not need to know which indexes exist yet. Before a renaming of the schema, a trigger has
    the names of that schema (``legacy``).
    """
    old = legacy.stages(op)
    for trigger in PERSISTENT:
        _, on, requires, sql = legacy.rendered(trigger, old)
        if on == table and (requires is None or _exists(op, requires)):
            op.execute(sql)


def install(op: Operations, names: Iterable[str]) -> None:
    """Create the persistent triggers that ``names`` lists, in the names of the schema at hand (``legacy``)."""
    wanted, old = set(names), legacy.stages(op)
    for trigger in PERSISTENT:
        name, _, _, sql = legacy.rendered(trigger, old)
        if name in wanted:
            op.execute(sql)


def _exists(op: Operations, table: str) -> bool:
    query = text("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = :name")
    return op.get_bind().execute(query, {'name': table}).scalar() is not None


CHANGE_LOG = """
-- The change log: decisions leave a record in the append-only events table, signed by the same actor. Every client
-- loads this file, so a raw UPDATE from the CLI or an implementer is logged like an ORM write of the daemon.
-- `AFTER UPDATE OF <col>` fires only when a statement names that column, so the updated_at and updated_by triggers
-- above (which SET their own column) never fire these. The WHEN clause ignores a SET to the value already there.
-- Event kinds are `<table singular>.<column>`. Payloads have the old and new value, never a large body."""
TOMBSTONES = """
-- Tombstones: the key columns of a deleted row, so the feed can still say what was there. A cascade (DELETE FROM
-- campaigns) fires them for every child row that it deletes."""
COMMENTS = """
-- Every comment is an event, with the same shape as the event of the persistent escalation trigger. That trigger
-- stays persistent, because `comment.escalation` must be logged whatever connection wrote the comment. The rest of
-- the forum (`comment.note`, `comment.finding`, `comment.decision`) is what the daemon pushes to a running implementer."""
OUTBOX = """
-- The outbox: the writes that the daemon reacts to and that no decision above logs (a requested agent, a new idea, a
-- campaign's config, a cancelled evaluation, a limit), so that it wakes on their events. A limit is on the home."""
ENDED_AGENTS = """
-- An agent whose session has ended leaves no finding. session_actor holds the agent session of an agent's connection,
-- and the daemon sets the row ending in a commit before it kills the agent."""
APPEND_ONLY = """
-- The feed is append-only: an event takes the next id, because a reader's cursor is an event id. Its columns have the
-- types that a reader reads. The authorizer of `piloop sql` denies an UPDATE or a DELETE of events."""


def temp_sql() -> str:
    """The text of ``piloop/db/actor_triggers.sql``: the actor table, then each tuple of ``TEMP`` under its comment."""
    sections = (
        (provenance.SESSION_ACTOR_TABLE, provenance.UPDATED_BY),
        (CHANGE_LOG, changelog.CHANGES),
        (TOMBSTONES, changelog.TOMBSTONES),
        (COMMENTS, changelog.COMMENTS),
        (OUTBOX, changelog.OUTBOX),
        (ENDED_AGENTS, ended.ENDED_AGENTS),
        (APPEND_ONLY, immutable.EVENTS),
    )
    return GENERATED + '\n'.join('\n'.join([lead, *(t.sql() for t in triggers)]) for lead, triggers in sections) + '\n'
