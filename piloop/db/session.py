"""Engine, sessions and migrations for the database.

Migrations are append-only. Every schema change is a new revision
(``uv run alembic revision --autogenerate -m "..."``), never an edit of an existing one. Only ``migrate`` changes the
schema, and only ``piloop serve`` (when it starts) and ``piloop migrate`` (which ``piloop upgrade`` runs) call it. Every
other command refuses a database at another revision (``schema_mismatch``) and changes nothing.
"""

import fcntl
import os
import sqlite3
from collections.abc import Generator
from contextlib import closing, contextmanager
from pathlib import Path

from alembic import command
from alembic.config import Config as AlembicConfig
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


class SchemaMismatch(RuntimeError):
    """The database is missing, or at a schema revision other than this piloop's. Nothing was changed.

    Args:
        found: What is wrong, with the revision found and the one that this piloop knows.
        fix: What to run.
    """

    def __init__(self, found: str, fix: str) -> None:
        super().__init__(f'{found}: {fix}')
        self.found, self.fix = found, fix


class SchemaAhead(SchemaMismatch):
    """A newer version of piloop migrated the database. Downgrades are not supported."""


class OlderDaemon(RuntimeError):
    """A daemon of an older piloop runs, so this piloop must not migrate the database. The message says what to run."""


class BackupFailed(RuntimeError):
    """The copy of the database before a migration could not be written, so nothing was migrated."""


def migrate(engine: Engine, *, backups: int, daemon: int | None) -> Path | None:
    """Migrate the database to the latest Alembic revision. Create it if it does not exist.

    This is the only function that changes the schema. ``daemon`` is the pid that holds the daemon's lock
    (``piloop.daemon.serve.running_pid``), whoever started it: a shell, systemd or launchd. When it is another process
    and the database is behind this piloop, that daemon runs an older piloop (a daemon migrates when it starts), and
    the new schema would break it, so this refuses. An in-place restart holds the lock itself, and migrates.

    Before it applies a revision to a database that has one, it writes a consistent copy of the database next to it,
    ``<database>.bak-<revision>-<UTC time>`` (``VACUUM INTO``), then removes the oldest copies beyond the newest
    ``backups``. ``backups=0`` writes none. A copy that fails stops the migration: nothing is applied.

    Returns:
        The copy, or None when nothing was applied or no copy was asked for.

    Raises:
        OlderDaemon: Another process holds the daemon's lock, and the database is behind this piloop.
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
            at = behind(path)
            if at is not None and daemon not in {None, os.getpid()}:
                raise OlderDaemon(
                    f'the daemon (pid {daemon}) runs an older piloop: the database is at schema revision {at}, and'
                    f' this {installed()} would migrate it to {head()} under the daemon. Run `piloop upgrade`, which'
                    ' restarts the daemon on the new code, or `piloop restart` once its piloop is this one, or stop it'
                    ' with `piloop stop`.'
                )
            if backups and at not in {None, 'none'}:
                copy = _backup(path, at, backups)
            command.upgrade(cfg, 'head')
    except CommandError as e:
        if not isinstance(e.__cause__, ResolutionError):  # pragma: no cover - alembic fails here for no other reason
            raise
        raise ahead(path, revision(path)) from e
    return copy


def schema_mismatch(database: Path, daemon: int | None) -> SchemaMismatch | None:
    """Return why a command other than ``piloop serve`` and ``piloop migrate`` must not open ``database``, else None.

    It reads the database read-only, and never creates it. ``daemon`` is the pid that holds the daemon's lock: a
    daemon behind this piloop is an older one, and its restart on this piloop migrates.
    """
    if not database.is_file():
        return SchemaMismatch(f'{database} does not exist', '`piloop init <repo>` creates it with the first campaign')
    at, script = revision(database), ScriptDirectory(str(MIGRATIONS))
    if at == script.get_current_head():
        return None
    if at is not None and at not in {r.revision for r in script.walk_revisions()}:
        return ahead(database, at)
    found = f'{database} is at schema revision {at or "none"}, and this {installed()} knows {head()}'
    if daemon is None:
        return SchemaMismatch(found, 'run `piloop migrate`, which backs it up first')
    return SchemaMismatch(
        found,
        f'the daemon (pid {daemon}) runs an older piloop. `piloop restart` restarts it on this piloop, which migrates it',
    )


def revision(database: Path) -> str | None:
    """The Alembic revision of ``database``, read read-only. None when it does not exist or was never migrated."""
    if not database.is_file():
        return None
    with closing(sqlite3.connect(f'file:{database}?mode=ro', uri=True)) as con:
        try:
            row = con.execute('SELECT version_num FROM alembic_version').fetchone()
        except sqlite3.OperationalError:  # never migrated, so there is no alembic_version table
            return None
    return row[0] if row else None


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


def behind(database: Path) -> str | None:
    """Return the revision of a database that this piloop would migrate, or None when it is at head or ahead of it.

    A database that was never migrated is at revision ``none``.
    """
    at, script = revision(database), ScriptDirectory(str(MIGRATIONS))
    if at == script.get_current_head() or (at is not None and at not in {r.revision for r in script.walk_revisions()}):
        return None
    return at or 'none'


def ahead(database: Path, at: str | None) -> SchemaAhead:
    """Return the error for ``database`` at revision ``at``, which a newer piloop wrote and this one does not know.

    Only the newer piloop has the script of ``at``, so this one cannot name the version: it names its own and the
    newest revision it knows.
    """
    return SchemaAhead(
        f'{database} is at schema revision {at}, and this {installed()} knows up to {head()}',
        'this database was migrated by a newer piloop: install it (`piloop upgrade` installs the latest), or set'
        ' PILOOP_HOME to another directory',
    )


@contextmanager
def _migration_lock(database: Path) -> Generator[None]:
    """Hold an exclusive lock on ``<database>.migrate.lock`` while Alembic upgrades.

    An upgrade is several transactions: read the current revision, then apply each missing script. Two clients that
    start on the same new home directory (``piloop serve --detach`` twice, or a daemon and ``piloop migrate``) can both
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
