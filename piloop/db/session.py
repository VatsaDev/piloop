"""Engine, sessions and migrations for the database.

Migrations are append-only. Every schema change is a new revision
(``uv run alembic revision --autogenerate -m "..."``), never an edit of an existing one. A new version of piloop
migrates an existing database when it first opens it.
"""

import fcntl
import os
import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from pathlib import Path

from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from alembic.script.revision import ResolutionError
from alembic.util.exc import CommandError
from sqlalchemy import Engine, create_engine, event, make_url
from sqlalchemy.engine import ExceptionContext
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import ConnectionPoolEntry

from piloop.db.models import utcnow
from piloop.db.sql import CONNECTION_PRAGMAS, install_actor, install_functions, lock_holder, optimize
from piloop.version import installed

MIGRATIONS = Path(__file__).parent / 'migrations'
BACKUP_INFIX = '.bak-'


def _pragmas(conn: sqlite3.Connection) -> None:
    """Apply the settings that every client uses (``piloop.db.sql.CONNECTION_PRAGMAS``)."""
    cur = conn.cursor()
    for pragma in CONNECTION_PRAGMAS:
        cur.execute(f'PRAGMA {pragma}')
    cur.close()


def make_engine(url: str, actor: str) -> Engine:
    """An engine whose connections apply the SQLite pragmas and record ``actor`` in ``updated_by``.

    ``PRAGMA optimize`` runs when a pooled connection is closed (``Database.close`` closes them), as SQLite
    recommends. It updates the query planner's statistics for the tables that the connection queried, at a low cost.

    A ``database is locked`` error gets an extra line that names the process that holds the write lock
    (``lock_holder``).
    """
    database = Path(make_url(url).database or '')

    def on_connect(conn: sqlite3.Connection, _record: ConnectionPoolEntry) -> None:
        _pragmas(conn)
        install_actor(conn, actor)
        install_functions(conn)

    def on_close(conn: sqlite3.Connection, _record: ConnectionPoolEntry) -> None:
        optimize(conn)

    def on_error(context: ExceptionContext) -> None:
        e = context.sqlalchemy_exception
        if e is not None and (holder := lock_holder(e.orig, database)) is not None:
            e.add_detail(holder)

    engine = create_engine(url, connect_args={'check_same_thread': False})
    event.listen(engine, 'connect', on_connect)
    event.listen(engine, 'close', on_close)
    event.listen(engine, 'handle_error', on_error)
    return engine


class SchemaAhead(RuntimeError):
    """A newer version of piloop migrated the database. Downgrades are not supported."""


class BackupFailed(RuntimeError):
    """The copy of the database before a migration could not be written, so nothing was migrated."""


def migrate(engine: Engine, *, backups: int) -> Path | None:
    """Migrate the database to the latest Alembic revision. Create it if it does not exist.

    Before it applies a revision to a database that has one, it writes a consistent copy of the database next to it,
    ``<database>.bak-<revision>-<UTC time>`` (``VACUUM INTO``), then removes the oldest copies beyond the newest
    ``backups``. ``backups=0`` writes none. A copy that fails stops the migration: nothing is applied.

    Returns:
        The copy, or None when nothing was applied or no copy was asked for.

    Raises:
        SchemaAhead: The database is at a revision that this piloop does not know: an older CLI on a newer database.
        BackupFailed: The copy could not be written, for example on a full disk.
    """
    path = Path(engine.url.database or '')
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    cfg = AlembicConfig()
    cfg.set_main_option('script_location', str(MIGRATIONS))
    cfg.set_main_option('sqlalchemy.url', engine.url.render_as_string(hide_password=False))
    copy = None
    try:
        with _migration_lock(path):
            at = behind(engine)
            if backups and at not in {None, 'none'}:
                copy = _backup(path, at, backups)
            command.upgrade(cfg, 'head')
    except CommandError as e:
        if not isinstance(e.__cause__, ResolutionError):  # pragma: no cover - alembic fails here for no other reason
            raise
        raise ahead(path, revision(engine)) from e
    return copy


def backups(database: Path) -> list[Path]:
    """The copies that ``migrate`` wrote of ``database``, oldest first."""
    return sorted(database.parent.glob(f'{database.name}{BACKUP_INFIX}*'), key=lambda p: p.name.rpartition('-')[2])


def _backup(database: Path, at: str, keep: int) -> Path:
    """Copy ``database`` at revision ``at`` with ``VACUUM INTO``, then keep only the newest ``keep`` copies.

    ``VACUUM INTO`` reads one snapshot, with what the WAL holds, so the copy is consistent while others write.
    """
    copy = database.with_name(f'{database.name}{BACKUP_INFIX}{at}-{utcnow():%Y%m%dT%H%M%SZ}')
    try:
        with closing(sqlite3.connect(database)) as con:
            con.execute('VACUUM INTO ?', (str(copy),))
    except sqlite3.Error as e:
        copy.unlink(missing_ok=True)  # a partial copy, for example on a full disk
        raise BackupFailed(
            f'could not back up {database} to {copy} before migrating it ({e}), so it was not migrated: free some'
            ' space there, or set migration_backups = 0 in config.toml to migrate without a backup'
        ) from e
    for old in backups(database)[:-keep]:
        old.unlink()
    return copy


def head() -> str | None:
    """The newest schema revision that this piloop knows."""
    return ScriptDirectory(str(MIGRATIONS)).get_current_head()


def behind(engine: Engine) -> str | None:
    """Return the revision of a database that this piloop would migrate, or None when it is at head or ahead of it.

    A database that was never migrated is at revision ``none``.
    """
    at, script = revision(engine), ScriptDirectory(str(MIGRATIONS))
    if at == script.get_current_head() or (at is not None and at not in {r.revision for r in script.walk_revisions()}):
        return None
    return at or 'none'


def ahead(database: Path, at: str | None) -> SchemaAhead:
    """Return the error for ``database`` at revision ``at``, which a newer piloop wrote and this one does not know.

    Only the newer piloop has the script of ``at``, so this one cannot name the version: it names its own and the
    newest revision it knows.
    """
    return SchemaAhead(
        f'{database} is at schema revision {at}, which this {installed()} does not know (it knows up to {head()}): '
        'a newer piloop migrated it. Install a newer piloop (`piloop upgrade`), or set PILOOP_HOME to another directory.'
    )


@contextmanager
def _migration_lock(database: Path) -> Generator[None]:
    """Hold an exclusive lock on ``<database>.migrate.lock`` while Alembic upgrades.

    An upgrade is several transactions: read the current revision, then apply each missing script. Two clients that
    start on the same new home directory (``piloop serve --detach`` twice, or a daemon and ``piloop status``) can both
    read "no revision" and both run ``0001``, and then the second fails on ``CREATE TABLE``. SQLite puts statements in
    order, but not this read and the write after it, so the lock does. The second client waits, then finds the
    database at head and applies nothing. The kernel releases a ``flock`` when the descriptor closes or the process
    ends, so a crash during an upgrade never leaves a stale lock. The empty lock file stays next to the database.
    """
    fd = os.open(f'{database}.migrate.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        os.close(fd)


def revision(engine: Engine) -> str | None:
    """The Alembic revision of the database. None when it was never migrated or does not exist yet.

    It reads on a plain engine. The connections of a piloop engine install the actor triggers, and those name tables
    that a database behind this piloop's schema does not have yet.
    """
    if not Path(engine.url.database or '').is_file():
        return None
    plain = create_engine(engine.url)
    try:
        with plain.connect() as conn:
            return MigrationContext.configure(conn).get_current_revision()
    finally:
        plain.dispose()


@contextmanager
def read_transaction(s: Session) -> Generator[None]:
    """Hold one read transaction on ``s``: every read inside it sees the same committed state of the database.

    pysqlite opens no transaction for a SELECT, so each statement of a session otherwise sees the latest commit, and
    two reads of one view can disagree. A deferred ``BEGIN`` takes its WAL snapshot at the first read. The rollback at
    the end releases the snapshot at once: a reader that stays open blocks checkpoints. The session must have nothing
    to flush, and no open transaction (``BEGIN`` inside one fails).

    Raises:
        RuntimeError: The session has changes that are not flushed. They would be lost.
    """
    if s.new or s.dirty or s.deleted:
        raise RuntimeError('a read transaction on a session with uncommitted changes: commit or roll back first')
    s.expire_all()  # an object loaded before this transaction must be read again inside it
    s.connection().exec_driver_sql('BEGIN')
    try:
        yield
    finally:
        s.rollback()


class Database:
    """One engine and a session factory. Modules get the database only through this class.

    Args:
        url: The SQLAlchemy URL of the database.
        actor: The value that every UPDATE through this database records in ``updated_by``.
    """

    def __init__(self, url: str, *, actor: str) -> None:
        self.engine = make_engine(url, actor)
        self._sessions = sessionmaker(self.engine, expire_on_commit=False)

    def session(self) -> Session:
        """A new session. Use it as a context manager."""
        return self._sessions()

    def close(self) -> None:
        """Close the pooled connections."""
        self.engine.dispose()
