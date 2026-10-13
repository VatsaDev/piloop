"""Import the database of piloop's predecessor (hiloopai/piloop-archive) into a new piloop home.

Revision 0001 here is the squash of the predecessor's whole chain: its last schema (its revision 0019) has the same
tables and columns. So an import creates the new database at 0001, copies every row with its id, and leaves the rest to
the migrations, like any upgrade: a later revision fills what it adds from the rows that are there.

The source is opened read-only. The copy is one transaction on a file next to the target, renamed into place once it
is complete, so a failed import leaves nothing behind. No trigger fires while the rows are copied: the events table is
copied with the rest, and an escalation or a change must not be logged twice. The full-text indexes are rebuilt at the
end. A project that was active is stopped, so that a daemon on the new home does not run a campaign that the source's
daemon may still run on the same repo; ``UPDATE projects SET status = 'active', stopped_reason = NULL`` resumes it.
Every open inbox item is acknowledged by ``ACKED_BY``: it is the source's history, and otherwise hundreds of old
escalations fill ``piloop inbox`` and ``piloop wake`` before the first item of a campaign run on the new home.
``piloop inbox --all`` still lists them.
"""

import os
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import create_engine

from piloop.db.session import migrate
from piloop.db.triggers.fts import is_fts_object

SCHEMA = '0001'
"""The revision whose tables and columns are the predecessor's last schema."""
OPEN_RUNS = "status IN ('queued', 'running')"
OPEN_WORKERS = "state IN ('requested', 'starting', 'running')"
ACKED_BY = 'import'
"""Who acknowledged the inbox items that the import copied open (``comments.acked_by``)."""


class NotImportable(RuntimeError):
    """The source is not a database that this import can read, or the target exists already."""


@dataclass(frozen=True)
class Imported:
    """What ``import_database`` did: the rows copied, by table, and the open inbox items it acknowledged."""

    copied: dict[str, int]
    acked: int


def import_database(source: Path, target: Path) -> Imported:
    """Copy every row of the predecessor's database ``source`` into a new database at ``target``, at revision 0001.

    Migrate ``target`` to head afterwards, as for any database.

    Returns:
        The rows copied, by table, and how many open inbox items were acknowledged.

    Raises:
        NotImportable: ``target`` exists, ``source`` is not a piloop database, its tables differ from the schema at
            0001, or it has open runs or workers.
    """
    if target.exists():
        raise NotImportable(f'{target} exists: import into a new PILOOP_HOME')
    if not source.is_file():
        raise NotImportable(f'{source} is not a file')
    partial = target.with_name(f'{target.name}.importing')
    partial.unlink(missing_ok=True)
    try:
        engine = create_engine(f'sqlite:///{partial}')
        try:
            migrate(engine, to=SCHEMA)
        finally:
            engine.dispose()
        with closing(sqlite3.connect(partial.as_uri(), uri=True, isolation_level=None)) as con:
            con.execute('ATTACH DATABASE ? AS old', (f'{source.resolve().as_uri()}?mode=ro',))
            imported = _copy(con, source)
            con.execute('DETACH DATABASE old')
        os.replace(partial, target)
    finally:
        partial.unlink(missing_ok=True)
        Path(f'{partial}.migrate.lock').unlink(missing_ok=True)
    return imported


def _copy(con: sqlite3.Connection, source: Path) -> Imported:
    tables = [
        name
        for (name,) in con.execute(
            "SELECT name FROM main.sqlite_master WHERE type = 'table' AND name != 'alembic_version' ORDER BY name"
        )
        if not is_fts_object(name)
    ]
    if con.execute("SELECT 1 FROM old.sqlite_master WHERE name = 'alembic_version'").fetchone() is None:
        raise NotImportable(f'{source} is not a piloop database: it has no alembic_version table')
    for table in tables:
        new, old = _columns(con, 'main', table), _columns(con, 'old', table)
        if new != old:
            raise NotImportable(
                f'{source}: table {table} has columns {sorted(old) or "none"}, not {sorted(new)}. Upgrade it to the '
                "predecessor's last schema first: run its `piloop migrate`"
            )
    open_rows = {
        'runs': con.execute(f'SELECT count(*) FROM old.runs WHERE {OPEN_RUNS}').fetchone()[0],
        'workers': con.execute(f'SELECT count(*) FROM old.workers WHERE {OPEN_WORKERS}').fetchone()[0],
    }
    if any(open_rows.values()):
        counts = ' and '.join(f'{n} {table}' for table, n in open_rows.items() if n)
        raise NotImportable(f'{source} has {counts} still open: stop its daemon (`piloop stop`), then import again')
    triggers = con.execute("SELECT name, sql FROM main.sqlite_master WHERE type = 'trigger' ORDER BY name").fetchall()
    copied: dict[str, int] = {}
    con.execute('BEGIN IMMEDIATE')
    for name, _ in triggers:
        con.execute(f'DROP TRIGGER main.{name}')
    for table in tables:
        columns = ', '.join(sorted(_columns(con, 'main', table)))
        copied[table] = con.execute(f'INSERT INTO main.{table} ({columns}) SELECT {columns} FROM old.{table}').rowcount
    acked = con.execute(
        'UPDATE main.comments SET acked_at = CURRENT_TIMESTAMP, acked_by = ? '
        'WHERE priority IS NOT NULL AND acked_at IS NULL',
        (ACKED_BY,),
    ).rowcount
    for _, sql in triggers:
        con.execute(sql)
    for (index,) in con.execute("SELECT name FROM main.sqlite_master WHERE sql LIKE 'CREATE VIRTUAL TABLE%'"):
        con.execute(f"INSERT INTO main.{index} ({index}) VALUES ('rebuild')")
    con.execute(
        "UPDATE main.projects SET status = 'stopped', stopped_reason = ? WHERE status = 'active'",
        (f'imported from {source}',),
    )
    if violations := con.execute('PRAGMA main.foreign_key_check').fetchall():
        raise NotImportable(f'{source} has rows that point to missing rows: {violations[:5]}')
    con.execute('COMMIT')
    return Imported(copied, acked)


def _columns(con: sqlite3.Connection, schema: str, table: str) -> set[str]:
    return {name for (_, name, *_) in con.execute(f'PRAGMA {schema}.table_info({table})')}
