"""Who changed a row and when: ``updated_at`` (persistent) and ``updated_by`` (TEMP, from the connection's actor)."""

from piloop.db.triggers.ddl import Trigger

TRACKED_TABLES = ('projects', 'ideas', 'runs', 'candidates', 'comments', 'workers')
"""Every table with ``updated_at`` and ``updated_by`` (``models.Tracked``). Each one gets both triggers."""
DAEMON_ACTOR = 'daemon'
"""What the daemon's connections record in ``updated_by``. ``piloop.daemon.main.ACTOR`` has this value."""
SESSION_ACTOR = '(SELECT actor FROM temp.session_actor)'
"""The actor of the connection, as a SQL expression that a TEMP trigger can read."""

SESSION_ACTOR_TABLE = """\
-- Who changed each row, for this connection only. piloop.db.sql.install_actor runs this file, then inserts the
-- connection's actor into session_actor. A TEMP trigger can update main tables and read temp ones, so every UPDATE
-- through this connection sets updated_by, and the caller does not need to name itself. The WHEN clause stops the
-- trigger's own UPDATE from firing it again, and replaces any actor that a statement tries to set.
-- WITHOUT ROWID keeps the actor row out of last_insert_rowid, which clients report for the caller's statement.
CREATE TEMP TABLE session_actor (actor TEXT NOT NULL PRIMARY KEY) WITHOUT ROWID;"""


def updated_at(table: str) -> Trigger:
    """Persistent: set ``updated_at``, with milliseconds, on every UPDATE that does not set it.

    strftime('%f') keeps the milliseconds. CURRENT_TIMESTAMP cuts to the second, so a row could sort before another row
    that changed earlier in the same second. The WHEN clause lets a statement set updated_at itself, stops the
    trigger's own UPDATE from firing it again, and skips the UPDATE of the ``updated_by`` trigger, which would
    otherwise change a timestamp that the statement set.
    """
    return Trigger(
        f'{table}_updated_at',
        table,
        'AFTER UPDATE',
        when='NEW.updated_at IS OLD.updated_at AND NEW.updated_by IS OLD.updated_by',
        body=f"UPDATE {table} SET updated_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = NEW.id",
    )


def updated_by(table: str) -> Trigger:
    """TEMP: set ``updated_by`` to the connection's actor on every UPDATE, whatever value the statement gave it."""
    return Trigger(
        f'{table}_updated_by',
        table,
        'AFTER UPDATE',
        when=f'NEW.updated_by IS NOT {SESSION_ACTOR}',
        body=f'UPDATE {table} SET updated_by = {SESSION_ACTOR} WHERE id = NEW.id',
        temp=True,
    )


UPDATED_AT = tuple(updated_at(t) for t in TRACKED_TABLES)
UPDATED_BY = tuple(updated_by(t) for t in TRACKED_TABLES)
