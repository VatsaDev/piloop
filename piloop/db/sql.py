"""Raw SQL access for agents: any statement except a schema change.

The same rules apply to every client (``piloop sql``, the watchdog, any agent). SQLite's authorizer denies DDL,
ATTACH, transactions, writes to ``alembic_version`` and ``session_actor``, and all PRAGMAs except the read-only ones
that describe the schema. The connection is in autocommit mode, because a forgotten BEGIN would lock the database.
Everything else is allowed, DELETE included. The schema enforces the rules on the data.

Anyone can read the full-text indexes (``ideas_fts``, ``comments_fts``) with ``MATCH``, ``bm25()`` and
``snippet()``, but only their sync triggers write them. The authorizer sees the name of the trigger behind each
statement of a trigger body and lets those statements through. So an agent's own ``INSERT INTO ideas_fts`` (or a
``'rebuild'``) is denied, while an insert into ``ideas`` still indexes the row. The authorizer cannot tell the shadow
tables that FTS5 writes for itself (``ideas_fts_data`` and others) from a statement of an agent. SQLite's own guard
protects them instead: ``SQLITE_DBCONFIG_DEFENSIVE`` makes shadow tables read-only for normal SQL, while FTS5 still
writes them. To repair a damaged index, run ``INSERT INTO ideas_fts (ideas_fts) VALUES ('rebuild')`` from a
connection without these limits.

Every connection, here and in the daemon's engine, has an actor. ``install_actor`` creates the TEMP objects of
``piloop/db/actor_triggers.sql`` before the authorizer is turned on, because after that ``SQLITE_CREATE_TEMP_TRIGGER``
is denied. From then on, every UPDATE sets ``updated_by``, and the tracked changes, deletes and comments go into
``events``. The authorizer does not protect ``events``, so the INSERTs of the trigger bodies pass.
"""

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import MetaData
from sqlalchemy.dialects import sqlite
from sqlalchemy.schema import CreateIndex, CreateTable

from piloop.db.models import Base
from piloop.db.triggers.fts import FTS_COLUMNS, FTS_TABLES, FTS_TRIGGER_NAMES, fts_table
from piloop.proc import command_line

DENIED = {
    sqlite3.SQLITE_CREATE_INDEX,
    sqlite3.SQLITE_CREATE_TABLE,
    sqlite3.SQLITE_CREATE_TEMP_INDEX,
    sqlite3.SQLITE_CREATE_TEMP_TABLE,
    sqlite3.SQLITE_CREATE_TEMP_TRIGGER,
    sqlite3.SQLITE_CREATE_TEMP_VIEW,
    sqlite3.SQLITE_CREATE_TRIGGER,
    sqlite3.SQLITE_CREATE_VIEW,
    sqlite3.SQLITE_DROP_INDEX,
    sqlite3.SQLITE_DROP_TABLE,
    sqlite3.SQLITE_DROP_TEMP_INDEX,
    sqlite3.SQLITE_DROP_TEMP_TABLE,
    sqlite3.SQLITE_DROP_TEMP_TRIGGER,
    sqlite3.SQLITE_DROP_TEMP_VIEW,
    sqlite3.SQLITE_DROP_TRIGGER,
    sqlite3.SQLITE_DROP_VIEW,
    sqlite3.SQLITE_CREATE_VTABLE,
    sqlite3.SQLITE_DROP_VTABLE,
    sqlite3.SQLITE_ALTER_TABLE,
    sqlite3.SQLITE_ATTACH,
    sqlite3.SQLITE_DETACH,
    sqlite3.SQLITE_TRANSACTION,
    sqlite3.SQLITE_SAVEPOINT,
}
# data_version is not for agents, but FTS5 reads it on every write to an index. Denying it would make the sync
# triggers fail, and with them every insert into ideas and comments.
READ_ONLY_PRAGMAS = {
    'table_info',
    'table_xinfo',
    'index_list',
    'index_info',
    'foreign_key_list',
    'table_list',
    'data_version',
}
MIGRATION_TABLE = 'alembic_version'
ACTOR_TABLE = 'session_actor'
PROTECTED_TABLES = {MIGRATION_TABLE, ACTOR_TABLE}
ACTOR_TRIGGERS = (Path(__file__).parent / 'actor_triggers.sql').read_text()
WRITES = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
BUSY_TIMEOUT_MS = 5000
CONNECTION_PRAGMAS = (
    f'busy_timeout={BUSY_TIMEOUT_MS}',
    'journal_mode=WAL',
    'foreign_keys=ON',
    'synchronous=NORMAL',
    'temp_store=MEMORY',
)
"""The connection settings. Every client uses the same ones (``piloop.db.session``, and ``connect`` here).

``journal_mode=WAL`` is a property of the file: readers never block the daemon's writes. Every client sets it,
because the client that connects first creates the file, and Alembic's own engine creates it in rollback mode. When
the file is in WAL mode, the pragma does nothing. ``synchronous=NORMAL`` is the setting that SQLite recommends for
WAL. A commit survives a process crash, and only a power loss can lose the last transactions. That is acceptable for
a coordination database whose state can be rebuilt from git, and it saves the fsync on each commit that FULL adds to
every insert of an agent. ``temp_store=MEMORY`` keeps the sorts for the ``ORDER BY`` of the status and the tick out
of temporary files. ``cache_size`` stays at SQLite's default: the tables that a tick reads fit in it, and each pooled
connection pays for its own cache.
"""
PROC_LOCKS = Path('/proc/locks')
WAL_WRITE_LOCK = 120
"""The byte of the ``-shm`` file that a connection locks with an exclusive ``fcntl`` lock while its write transaction
is open: SQLite's ``UNIX_SHM_BASE``, WAL lock slot 0."""
OPTIMIZE = 'PRAGMA optimize'
"""Runs just before a connection closes, as SQLite recommends. It runs ``ANALYZE`` for the tables that this
connection queried and whose statistics are missing or out of date, limited by ``analysis_limit`` (SQLite 3.46 and
later set it). Otherwise it does nothing."""
ROW_LIMIT = 500


def _authorize(action: int, arg1: str | None, arg2: str | None, _db: str | None, source: str | None) -> int:
    """SQLite's authorizer callback. ``source`` names the trigger (or view) whose body has the statement, or is None."""
    if action in DENIED:
        return sqlite3.SQLITE_DENY
    if action == sqlite3.SQLITE_PRAGMA:
        return sqlite3.SQLITE_OK if arg1 in READ_ONLY_PRAGMAS else sqlite3.SQLITE_DENY
    if action in WRITES and arg1 in PROTECTED_TABLES:
        return sqlite3.SQLITE_DENY
    if action in WRITES and arg1 in FTS_TABLES and source not in FTS_TRIGGER_NAMES:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def install_actor(con: sqlite3.Connection, actor: str) -> None:
    """Make every UPDATE through ``con`` record ``actor`` in ``updated_by``, and log tracked changes in ``events``.

    It creates a TEMP table and TEMP triggers, for this connection only.
    """
    con.executescript(ACTOR_TRIGGERS)
    con.execute(f'INSERT INTO {ACTOR_TABLE} (actor) VALUES (?)', (actor,))
    con.commit()


def optimize(con: sqlite3.Connection) -> None:
    """Run ``PRAGMA optimize`` before a close. Skip it while another client holds the write lock.

    The ANALYZE that it may run writes to the database. A close must not wait for a writer, fail a read that
    succeeded, or hide the error that ended it. With the busy timeout, a read-only SELECT of ``piloop status`` or
    ``piloop sql`` would wait 5 s for the lock and then fail with ``database is locked``. The next close runs it.
    """
    con.execute('PRAGMA busy_timeout=0')
    try:
        con.execute(OPTIMIZE)
    except sqlite3.OperationalError as e:
        if not _busy(e):
            raise


def _busy(e: BaseException | None) -> bool:
    """Whether ``e`` is ``database is locked``: SQLITE_BUSY or one of its extended codes."""
    return isinstance(e, sqlite3.OperationalError) and e.sqlite_errorcode & 0xFF == sqlite3.SQLITE_BUSY


def lock_holder(e: BaseException | None, database: Path) -> str | None:
    """For a ``database is locked`` error, the process that holds the write lock and its command line. Else None.

    SQLite only says that some process holds the lock. piloop reads the holder from ``/proc/locks``, so this works on
    Linux only. It never opens the ``-shm`` file to find out: closing any descriptor of a file drops every ``fcntl``
    lock that this process holds on the file, SQLite's own locks included. None on other systems, and when the holder
    is gone.
    """
    if not _busy(e) or (pid := _write_lock_pid(Path(f'{database}-shm'))) is None:
        return None
    command = command_line(pid) or '(exited)'  # the holder can commit or end in the meantime
    return f'pid {pid} holds the write lock: {command}'


def _write_lock_pid(shm: Path) -> int | None:
    """The pid that holds ``WAL_WRITE_LOCK`` on ``shm``, from ``/proc/locks``. None without /proc, the file or a holder.

    A line is ``<id>: POSIX ADVISORY WRITE <pid> <major>:<minor>:<inode> <start> <end>``, with the device numbers in
    hex. A process that waits for the lock has ``->`` after the id, and is not the holder.
    """
    try:
        st = shm.stat()
        lines = PROC_LOCKS.read_text().splitlines()
    except FileNotFoundError:
        return None
    file = f'{os.major(st.st_dev):02x}:{os.minor(st.st_dev):02x}:{st.st_ino}'
    for line in lines:
        fields = line.split()
        if fields[3:4] == ['WRITE'] and fields[5:7] == [file, str(WAL_WRITE_LOCK)]:
            return int(fields[4])
    return None


class Connection(sqlite3.Connection):
    """A connection whose ``close`` runs ``optimize`` first, with the authorizer turned off for it.

    The authorizer denies agents every PRAGMA except the ones that describe the schema, for the life of the
    connection. The optimize comes from piloop, not from the agent, so it skips the authorizer, as the pragmas at
    connect time do.
    """

    def close(self) -> None:
        self.set_authorizer(None)
        optimize(self)
        super().close()


def connect(path: Path, *, actor: str) -> sqlite3.Connection:
    """A connection for agents to an existing database: no DDL, and the same pragmas as the daemon's engine.

    The pragmas and the actor's TEMP objects are set before the authorizer is turned on. After that, every statement
    is the agent's and goes through the authorizer.

    Args:
        path: The database file. It must exist (``piloop serve`` creates it).
        actor: The value that every UPDATE on this connection records in ``updated_by``.
    """
    if not path.is_file():
        raise FileNotFoundError(f'no piloop database at {path}; run `piloop serve` first')
    con = sqlite3.connect(
        f'file:{path}?mode=rw', uri=True, timeout=BUSY_TIMEOUT_MS / 1000, isolation_level=None, factory=Connection
    )
    try:
        for pragma in CONNECTION_PRAGMAS:
            con.execute(f'PRAGMA {pragma}')
        install_actor(con, actor)
        con.setconfig(sqlite3.SQLITE_DBCONFIG_DEFENSIVE, True)  # only FTS5 itself may write the shadow tables
        con.set_authorizer(_authorize)
    except BaseException:  # a locked database fails the setup: close, or the connection leaks
        con.close()
        raise
    con.row_factory = sqlite3.Row
    return con


@dataclass(frozen=True)
class Result:
    """The rows of a query, or the number of changed rows of a statement."""

    columns: list[str]
    rows: list[tuple[object, ...]]
    changes: int
    last_insert_rowid: int
    truncated: bool


def run(con: sqlite3.Connection, sql: str, limit: int = ROW_LIMIT) -> Result:
    """Run one statement. Errors, including denials of the authorizer, are raised as ``sqlite3.Error``."""
    cur = con.execute(sql)
    columns = [d[0] for d in cur.description or []]
    rows = [tuple(r) for r in cur.fetchmany(limit + 1)] if columns else []
    truncated = len(rows) > limit
    return Result(columns, rows[:limit], cur.rowcount if not columns else 0, cur.lastrowid or 0, truncated)


def schema_ddl(metadata: MetaData = Base.metadata) -> str:
    """The schema as SQLite DDL: the tables with their indexes, and the FTS indexes that agents query. No triggers.

    For briefs and docs. The indexes are part of the contract: they tell an agent which filters are fast.
    """
    statements = []
    for t in metadata.sorted_tables:
        statements.append(str(CreateTable(t).compile(dialect=sqlite.dialect())).strip() + ';')
        for index in sorted(t.indexes, key=lambda i: i.name or ''):
            statements.append(str(CreateIndex(index).compile(dialect=sqlite.dialect())).strip() + ';')
    statements += [fts_table(t) for t in FTS_COLUMNS]
    return '\n'.join(line.rstrip() for statement in statements for line in statement.splitlines())
