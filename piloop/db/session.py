"""Engine, sessions and migrations for the database.

Migrations are append-only. Every schema change is a new revision
(``uv run alembic revision --autogenerate -m "..."``), never an edit of an existing one. Only ``migrate`` changes the
schema, and only ``piloop serve`` (when it starts) and ``piloop migrate`` (which ``piloop upgrade`` runs) call it. Every
other command refuses a database at another revision (``schema_mismatch``) and changes nothing.

Revision ids are bare numbers, so a branch's ``0006`` and ``main``'s share one. Each migration also records, in
``PRAGMA user_version``, a fingerprint of the revision file names up to the one it stamps (``fingerprints``), and
``unmigratable`` refuses a database whose stamp or fingerprint is not in this build's history (``foreign``). 0 marks a
database that a build before fingerprints migrated: only its stamp is checked.
"""

import asyncio
import fcntl
import hashlib
import math
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Generator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import NewType, assert_never

from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, Engine, create_engine, event, make_url
from sqlalchemy.engine import ExceptionContext
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import ConnectionPoolEntry

from piloop.actor import Actor, Agent, System
from piloop.db.models import utcnow
from piloop.db.sql import CONNECTION_PRAGMAS, install_actor, install_functions, lock_holder, optimize
from piloop.version import Version, installed

MIGRATIONS = Path(__file__).parent / 'migrations'
BACKUP_INFIX = '.bak-'
Revision = NewType('Revision', str)
"""An Alembic revision id of ``MIGRATIONS``, such as ``0001``."""


@dataclass(frozen=True)
class Release:
    """A minor release at its ``.0``, and the head revision that it shipped."""

    version: Version
    head: Revision


RELEASES = (Release(Version(0, 1, 0), Revision('0001')),)
"""Every minor release, oldest first. The newest is the floor: this piloop migrates a database from its head or from a
newer revision, never from an older one, so a database upgrades one minor release at a time.

The release of ``X.Y.0`` adds ``X.(Y-1).0`` (CONTRIBUTING.md, Release); ``tests/db/test_migrations.py`` fails at that
version bump until it does, and checks that each head is in the chain, in order. Each minor release deletes the compat
code that the one before it marked (``piloop.deprecated``), and a database that skipped that release still needs it.
"""


def _pragmas(conn: sqlite3.Connection) -> None:
    """Apply the settings that every client uses (``piloop.db.sql.CONNECTION_PRAGMAS``)."""
    cur = conn.cursor()
    for pragma in CONNECTION_PRAGMAS:
        cur.execute(f'PRAGMA {pragma}')
    cur.close()


def make_engine(url: str, actor: Actor) -> Engine:
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
    """Another piloop build, newer or of another branch, migrated the database. Downgrades are not supported."""


class SchemaTooOld(SchemaMismatch):
    """The database is older than the head of the previous minor release (``RELEASES``): upgrade through it first."""


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
        SchemaMismatch: This piloop cannot migrate the database (``unmigratable``): it is ahead of this piloop, or
            older than the previous minor release.
        BackupFailed: The copy could not be written, for example on a full disk.
    """
    path = Path(engine.url.database or '')
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    cfg = AlembicConfig()
    cfg.set_main_option('script_location', str(MIGRATIONS))
    cfg.set_main_option('sqlalchemy.url', engine.url.render_as_string(hide_password=False))
    copy = None
    with _migration_lock(path):
        if (refused := unmigratable(path)) is not None:
            raise refused
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
    return copy


def unmigratable(database: Path) -> SchemaMismatch | None:
    """Return why ``migrate`` would refuse ``database``, read read-only, else None.

    Another piloop build migrated it (``foreign``), or it is older than the floor, the newest of ``RELEASES``
    (``SchemaTooOld``). A database that does not exist, or that was never migrated, can be.
    """
    stamp, chain = revision(database), revisions()
    if stamp is None:
        return None
    if (refused := foreign(database, stamp)) is not None:
        return refused
    at = Revision(stamp)
    position, floor = chain.index, RELEASES[-1]
    if position(at) >= position(floor.head):
        return None
    before = [r.version.line for r in RELEASES if position(r.head) <= position(at)]
    stops = [r.version.line for r in RELEASES if position(r.head) > position(at)]
    origin = f'piloop {before[-1]}' if before else f'a piloop before {stops[0]}'
    steps = ', then '.join(
        f"install piloop {m} (`uv tool install 'piloop=={m}.*'`) and run `piloop upgrade`" for m in stops
    )
    return SchemaTooOld(
        f'{database} is from {origin} (schema revision {at}), and this {installed()} migrates from piloop'
        f' {floor.version.line} (schema revision {floor.head}) or newer',
        f'upgrade one release at a time: {steps}, then install this piloop again and run `piloop upgrade`',
    )


def revisions() -> list[Revision]:
    """Every schema revision that this piloop knows, oldest first."""
    return [Revision(r.revision) for r in reversed(list(ScriptDirectory(str(MIGRATIONS)).walk_revisions()))]


def schema_mismatch(database: Path, daemon: int | None, actor: Actor) -> SchemaMismatch | None:
    """Return why a command other than ``piloop serve`` and ``piloop migrate`` must not open ``database``, else None.

    It reads the database read-only, and never creates it. ``daemon`` is the pid that holds the daemon's lock: a
    daemon behind this piloop is an older one, and its restart on this piloop migrates. The fix is for ``actor``, the
    caller: an agent runs what the fix names, so it is told to wait for the upgrade instead.
    """
    if not database.is_file():
        return SchemaMismatch(f'{database} does not exist', '`piloop init <repo>` creates it with the first campaign')
    if (refused := unmigratable(database)) is not None:
        return refused
    at = revision(database)
    if at == head():
        return None
    found = f'{database} is at schema revision {at or "none"}, and this {installed()} knows {head()}'
    match actor:
        case Agent():
            fix = 'the daemon is being upgraded; retry in a minute'
        case System() if daemon is None:
            fix = 'run `piloop migrate`, which backs it up first'
        case System():
            fix = (
                f'the daemon (pid {daemon}) runs an older piloop. `piloop restart` restarts it on this piloop,'
                ' which migrates it'
            )
        case _:
            assert_never(actor)
    return SchemaMismatch(found, fix)


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
    """Return the revision of a database that this piloop would migrate, or None when it is at head.

    A database that was never migrated is at revision ``none``. Check ``unmigratable`` first.
    """
    at = revision(database)
    return None if at == head() else at or 'none'


def fingerprints(script: ScriptDirectory) -> dict[str, int]:
    """For each revision of ``script``, a hash of the file names of the revisions up to it: a positive int32, never 0.

    Names, not contents: a refactor of a released revision's code must not refuse every database.
    """
    history, found = hashlib.sha256(), {}
    for rev in reversed(list(script.walk_revisions())):
        history.update(f'{Path(rev.path).stem}\n'.encode())
        found[rev.revision] = int.from_bytes(history.digest()[:4]) >> 1 or 1
    return found


def record(connection: Connection, script: ScriptDirectory) -> None:
    """Set ``PRAGMA user_version`` to the fingerprint of the database's revision, if it is not already. ``env.py``."""
    at = connection.exec_driver_sql('SELECT version_num FROM alembic_version').scalar()
    mark = fingerprints(script)[at] if at is not None else 0
    if connection.exec_driver_sql('PRAGMA user_version').scalar() != mark:
        connection.exec_driver_sql(f'PRAGMA user_version = {mark:d}')


def foreign(database: Path, at: str) -> SchemaAhead | None:
    """Return the refusal of ``database``, stamped ``at``, if another piloop build migrated it. Reads it read-only."""
    with closing(sqlite3.connect(f'file:{database}?mode=ro', uri=True)) as con:
        (mark,) = con.execute('PRAGMA user_version').fetchone()
    known = fingerprints(ScriptDirectory(str(MIGRATIONS)))
    if at in known and mark in {0, known[at]}:
        return None
    why = f', which knows up to {head()}' if at not in known else f': this build has a different {at} or one before it'
    return SchemaAhead(
        f'{database} was migrated by a different piloop build (revision {at} is not in the history of this'
        f' {installed()}{why})',
        'install the piloop that migrated it (`piloop upgrade` for a newer release), or set PILOOP_HOME to another'
        ' directory',
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


WAIT_POLL_SECONDS = 1.0
"""How often ``Database.wait_until`` reads again for a commit of another process (the CLI, an agent's ``piloop sql``).

It goes when the daemon wakes on the commits of every process: those then notify it, as this process's do."""


CLOSE_WAIT_SECONDS = 5.0
"""How long ``Database.close`` waits for the reads of its waiters, which are short."""


def write_transaction(s: Session) -> None:
    """Begin the write transaction of ``s`` now (``BEGIN IMMEDIATE``): it holds the write lock until the commit.

    So a check that a later write depends on, a lease's fence say, reads the state that the write changes: no other
    writer commits between them. pysqlite would otherwise begin the transaction only at the first write. The session
    must have no open transaction, and the caller commits or rolls back.
    """
    s.connection().exec_driver_sql('BEGIN IMMEDIATE')


class DatabaseClosed(RuntimeError):
    """A session asked of a ``Database`` after its ``close``: the caller outlived the daemon's database."""


class Database:
    """One engine and a session factory. Modules get the database only through this class.

    Args:
        url: The SQLAlchemy URL of the database.
        actor: The value that every UPDATE through this database records in ``updated_by``.
    """

    def __init__(self, url: str, *, actor: Actor) -> None:
        self.engine = make_engine(url, actor)
        self._sessions = sessionmaker(self.engine, expire_on_commit=False)
        self._committed = threading.Condition()
        self._commits = 0
        """How many sessions of this process committed: a waiter that saw this number waits only for a later one."""
        self._closed = False
        self._reading = 0
        """How many waiters read now: ``close`` lets them end before it closes the connections that they hold."""
        self._changed: set[asyncio.Future[None]] = set()
        """The futures of ``changed``'s waiters, each on its own event loop: the next commit resolves them."""
        event.listen(self._sessions, 'after_commit', lambda _s: self._wake())

    def session(self) -> Session:
        """A new session. Use it as a context manager.

        Raises:
            DatabaseClosed: ``close`` came first. A session then would open a connection that nothing closes.
        """
        if self._closed:
            raise DatabaseClosed('the database is closed')
        return self._sessions()

    def _wake(self) -> None:
        with self._committed:
            self._commits += 1
            self._committed.notify_all()
            self._resolve()

    def _resolve(self) -> None:
        """Resolve every waiter of ``changed`` on its own loop. The caller holds ``_committed``."""
        for future in self._changed:
            future.get_loop().call_soon_threadsafe(future.set_result, None)
        self._changed.clear()

    def wait_until(
        self, done: Callable[[Session], bool], give_up: Callable[[], bool] = lambda: False, timeout: float = math.inf
    ) -> bool:
        """Block until ``done`` holds in a new session, and return True. False at ``timeout``, ``give_up`` or ``close``.

        It reads again after every commit of this process, and every ``WAIT_POLL_SECONDS`` for one of another. The
        read happens outside the condition, so no commit waits for it.
        """
        deadline = time.monotonic() + timeout
        while True:
            with self._committed:
                if self._closed:
                    return False
                seen, self._reading = self._commits, self._reading + 1
            try:
                with self._sessions() as s:  # registered in _reading: close waits for it
                    found = done(s)
            finally:
                with self._committed:
                    self._reading -= 1
                    self._committed.notify_all()
            if found:
                return True
            left = deadline - time.monotonic()
            if left <= 0 or give_up():
                return False
            with self._committed:
                self._committed.wait_for(
                    lambda seen=seen: self._commits != seen or self._closed, min(left, WAIT_POLL_SECONDS)
                )

    async def changed(self, seen: int, timeout: float) -> int:
        """The count of this process's commits once it is not ``seen``, or at ``timeout``.

        ``wait_until`` for a coroutine: it holds no thread while it waits, so a server can hold many long-polls. It
        sees only this process's commits: wait on it for what only this process writes, as a worker's messages. A
        caller that saw nothing yet passes 0, and reads again at once if anything committed.
        """
        future = asyncio.get_running_loop().create_future()
        with self._committed:
            if self._commits != seen or self._closed:
                return self._commits
            self._changed.add(future)
        try:
            await asyncio.wait([future], timeout=timeout)
        finally:
            with self._committed:
                self._changed.discard(future)
        return self._commits

    def close(self) -> None:
        """Make every waiter give up, let the reads in flight end, then close the pooled connections.

        A connection that a read holds while the pool goes is never closed: Python warns when it collects it.
        """
        with self._committed:
            self._closed = True
            self._committed.notify_all()
            self._resolve()
            self._committed.wait_for(lambda: self._reading == 0, CLOSE_WAIT_SECONDS)
        self.engine.dispose()
