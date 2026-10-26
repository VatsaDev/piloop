"""Engine, sessions and migrations for the database.

Migrations are append-only. Every schema change is a new revision
(``uv run alembic revision --autogenerate -m "..."``), never an edit of an existing one. Only ``migrate`` changes the
schema, and only ``piloop serve`` (when it starts) and ``piloop migrate`` (which ``piloop upgrade`` runs) call it. Every
other command refuses a database at another revision (``schema_mismatch``) and changes nothing.

Core and each plugin with tables (``piloop.db.schema``) have their own chain of revisions and their own version
table, Alembic's recipe for several environments in one database: core's is ``alembic_version``, a plugin's
``alembic_version_<name>`` (``memory/local``: ``alembic_version_memory__local``). ``migrate`` runs core's chain first,
then each installed plugin's, in one transaction. A plugin that is not installed keeps its tables and its version
table, and nothing reads them.

Revision ids are bare numbers, so a branch's ``0006`` and ``main``'s share one. So each migration also records, in
``migrations``, a fingerprint of the chain's revision file names up to the one it stamps (``fingerprints``), and
``unmigratable`` refuses a database whose stamp or fingerprint is not in this build's history (``foreign``). 0 marks a
database that a build before fingerprints migrated: only its stamp is checked.
"""

import asyncio
import fcntl
import hashlib
import math
import os
import re
import sqlite3
import threading
import time
from collections.abc import Callable, Generator, Iterable
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, NewType, Self

from alembic import command, context
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import Connection, Engine, create_engine, event, make_url
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.engine import ExceptionContext
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import ConnectionPoolEntry, NullPool, PoolProxiedConnection

from piloop import deprecated
from piloop.actor import Actor, Agent
from piloop.db.models import Migration, utcnow
from piloop.db.schema import CORE, SCHEMAS, schemas
from piloop.db.sql import (
    CONNECTION_PRAGMAS,
    MIGRATION_TABLE,
    install_actor,
    install_functions,
    lock_holder,
    optimize,
    sign,
)
from piloop.otel import Measures
from piloop.plugins.cas.contract import Cas
from piloop.plugins.host import BUILT_IN
from piloop.version import Version, installed

if TYPE_CHECKING:
    from sqlite3 import _Parameters

MIGRATIONS = Path(__file__).parent / 'migrations'
CAS_ATTRIBUTE = 'cas'
"""The key of the home's CAS in the Alembic config's ``attributes``, for ``migration_cas``."""
SCHEMA_ATTRIBUTE = 'schema'
"""The key of the schema whose chain runs, by name, in the Alembic config's ``attributes``. ``env.py`` reads it."""
CONNECTION_ATTRIBUTE = 'connection'
"""The key of the connection, in its transaction, that every chain runs on (``migrate``)."""
BACKUP_INFIX = '.bak-'
BACKUP_TIME = '%Y%m%dT%H%M%SZ'
MigrationRevision = NewType('MigrationRevision', str)
"""An Alembic revision id of a chain, core's or a plugin's, such as ``0001``."""
type Stamps = dict[str, str]
"""A revision of core and of each plugin, by name: of a database (``stamps``), or the newest that this piloop has
(``heads``)."""


@dataclass(frozen=True)
class Release:
    """A minor release at its ``.0``, and the head revision that it shipped."""

    version: Version
    head: MigrationRevision


RELEASES = (
    Release(Version(0, 1, 0), MigrationRevision('0001')),
    Release(Version(0, 2, 0), MigrationRevision('0018')),
)
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


WRITE_SPAN = 'piloop.db.write'
_WRITE = re.compile(r'\s*(INSERT|UPDATE|DELETE|REPLACE|BEGIN\s+IMMEDIATE)\b', re.IGNORECASE)


@dataclass(frozen=True)
class WriteSpans:
    """Where each write transaction that lasts ``slow`` or longer records a ``WRITE_SPAN`` (``MeasuredConnection``)."""

    measures: Measures
    slow: timedelta
    """``[tracing] slow_write`` of the settings."""


class MeasuredConnection(sqlite3.Connection):
    """A connection that records each slow write transaction (``WriteSpans``) as a ``WRITE_SPAN``.

    A transaction asks for the write lock at its first write statement, or at ``BEGIN IMMEDIATE``, and waits for it up
    to the busy timeout. ``wait_ms`` lasts until that statement ends, with the lock or with ``database is locked``, and
    ``hold_ms`` from then until the commit or the rollback ends. The span carries no SQL and no values.

    It hooks the driver, not SQLAlchemy's events: one listener for each statement makes SQLAlchemy dispatch all of its
    connection events, which costs about 25 µs a transaction.
    """

    def __init__(self, database: str, *, check_same_thread: bool, factory: object, writes: WriteSpans) -> None:
        # sqlite3.connect passes on its own arguments: make_engine's connect_args, this factory among them
        super().__init__(database, check_same_thread=check_same_thread)
        self._measures = writes.measures
        self._slow_ns = writes.slow // timedelta(microseconds=1) * 1000
        self._write: tuple[int, int] | None = None
        """When the open write transaction asked for the lock, and when it got it, in ns since the epoch."""

    def cursor(self, factory: Callable[[sqlite3.Connection], sqlite3.Cursor] | None = None) -> sqlite3.Cursor:
        return _MeasuredCursor(self) if factory is None else super().cursor(factory)

    def run[T](self, sql: str, statement: Callable[[], T]) -> T:
        """Run ``statement``, and time it when it asks for the write lock."""
        if self._write is not None or not _WRITE.match(sql):
            return statement()
        start = time.time_ns()
        try:
            return statement()
        finally:
            self._write = (start, time.time_ns())

    def commit(self) -> None:
        try:
            super().commit()
        finally:
            self._release()

    def rollback(self) -> None:
        try:
            super().rollback()
        finally:
            self._release()

    def _release(self) -> None:
        if self._write is None:
            return
        (start, held), self._write = self._write, None
        end = time.time_ns()
        if end - start >= self._slow_ns:
            self._measures.record(WRITE_SPAN, start, end, wait_ms=(held - start) / 1e6, hold_ms=(end - held) / 1e6)


class _MeasuredCursor(sqlite3.Cursor):
    def __init__(self, con: MeasuredConnection) -> None:
        super().__init__(con)
        self._con = con

    def execute(self, sql: str, parameters: _Parameters = (), /) -> Self:
        return self._con.run(sql, partial(super().execute, sql, parameters))

    def executemany(self, sql: str, seq_of_parameters: Iterable[_Parameters], /) -> Self:
        return self._con.run(sql, partial(super().executemany, sql, seq_of_parameters))


def make_engine(url: str, actor: Actor, writes: WriteSpans | None = None) -> Engine:
    """An engine whose connections apply the SQLite pragmas and record ``actor`` in ``updated_by``.

    With ``writes``, each connection records its slow write transactions (``MeasuredConnection``).

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

    connect_args: dict[str, object] = {'check_same_thread': False}
    if writes is not None:
        connect_args['factory'] = partial(MeasuredConnection, writes=writes)
    engine = create_engine(url, connect_args=connect_args)
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


def migrate(engine: Engine, *, cas: Cas, backups: int, daemon: int | None) -> Path | None:
    """Migrate the database to the newest revision of each chain. Create it if it does not exist.

    This is the only function that changes the schema. It runs core's chain first, then each installed plugin's, in
    the order of ``schemas``, all in one transaction: an upgrade that fails or is killed changes nothing. ``cas`` is
    the home's CAS, for a revision that moves data into it or out of it (``migration_cas``). ``daemon`` is the pid that
    holds the daemon's lock (``piloop.daemon.serve.running_pid``), whoever started it: a shell, systemd or launchd. When
    it is another process and the database is behind this piloop, that daemon runs an older piloop (a daemon migrates
    when it starts), and the new schema would break it, so this refuses. An in-place restart holds the lock itself,
    and migrates.

    Before it applies a revision to a database that has one, it writes a consistent copy of the database next to it,
    ``<database>.bak-<core revision>-<UTC time>`` (``VACUUM INTO``), then removes the oldest copies beyond the newest
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
    copy = None
    with _migration_lock(path):
        if (refused := unmigratable(path)) is not None:
            raise refused
        late = behind(path)
        if late and daemon not in {None, os.getpid()}:
            raise OlderDaemon(
                f'the daemon (pid {daemon}) runs an older piloop: the database is at schema {described(late)}, and'
                f' this {installed()} would migrate it to {described(heads())} under the daemon. Run `piloop upgrade`,'
                ' which restarts the daemon on the new code, or `piloop restart` once its piloop is this one, or stop'
                ' it with `piloop stop`.'
            )
        if not late:
            return None
        if backups and (at := revision(path)) is not None:
            copy = _backup(path, at, backups)
        _upgrade(engine.url.render_as_string(hide_password=False), cas)
    return copy


def _upgrade(url: str, cas: Cas) -> None:
    """Run each chain to its head on one connection, in one transaction, and record each one's fingerprint.

    pysqlite commits before each DDL statement by itself, so the driver's own transaction handling is turned off and
    the transaction is begun by hand.
    """
    engine = create_engine(url, poolclass=NullPool)
    event.listen(engine, 'connect', lambda dbapi, _: setattr(dbapi, 'isolation_level', None))
    event.listen(engine, 'begin', lambda connection: connection.exec_driver_sql('BEGIN'))
    packages = {name: source.package or source.entry_point for name, source in SCHEMAS.sources().items()}
    try:
        with engine.connect() as connection, connection.begin():
            for name in schemas():
                cfg = alembic_config(url, name, cas)
                cfg.attributes[CONNECTION_ATTRIBUTE] = connection
                command.upgrade(cfg, 'head')
                _record(connection, name, packages.get(name, BUILT_IN))
    finally:
        engine.dispose()


def alembic_config(url: str, name: str, cas: Cas) -> AlembicConfig:
    """The Alembic config of the chain of the schema ``name`` on the database at ``url``, with the home's ``cas``."""
    cfg = AlembicConfig()
    cfg.set_main_option('script_location', str(MIGRATIONS))
    cfg.set_main_option('path_separator', 'os')
    cfg.set_main_option('version_locations', str(schemas()[name].versions))
    cfg.set_main_option('sqlalchemy.url', url)
    cfg.attributes[CAS_ATTRIBUTE] = cas
    cfg.attributes[SCHEMA_ATTRIBUTE] = name
    return cfg


def version_table(name: str) -> str:
    """The version table of the schema ``name``: ``alembic_version`` for core, else ``alembic_version_<name>``.

    ``/`` in a name is ``__`` in the table's: a schema name has no ``__``.
    """
    return MIGRATION_TABLE if name == CORE else f'{MIGRATION_TABLE}_{name.replace("/", "__")}'


def _named(table: str) -> str:
    """The schema whose version table is ``table``."""
    return CORE if table == MIGRATION_TABLE else table.removeprefix(f'{MIGRATION_TABLE}_').replace('__', '/')


def scripts() -> dict[str, ScriptDirectory]:
    """The revisions that this piloop has of each chain, by schema name, in the order of ``schemas``."""
    return {
        name: ScriptDirectory(str(MIGRATIONS), version_locations=[str(schema.versions)])
        for name, schema in schemas().items()
    }


def migration_cas() -> Cas:
    """The home's CAS, in a revision that ``migrate`` runs."""
    cas: Cas = context.config.attributes[CAS_ATTRIBUTE]
    return cas


def unmigratable(database: Path) -> SchemaMismatch | None:
    """Return why ``migrate`` would refuse ``database``, read read-only, else None.

    Another piloop build migrated an installed chain (``foreign``), or core's is older than the floor,
    the newest of ``RELEASES`` (``SchemaTooOld``). A database that does not exist, or that was never migrated, can be.
    """
    found, chain = stamps(database), revisions()
    if (stamp := found.get(CORE)) is None:
        return None
    for name in schemas():
        if name in found and (refused := foreign(database, name, found[name])) is not None:
            return refused
    at = MigrationRevision(stamp)
    position, floor = chain.index, RELEASES[-1]
    if position(at) < position(HERDR_REMOVED) and (refused := on_herdr(database)) is not None:
        return refused
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


HERDR_REMOVED = MigrationRevision('0034')
"""The revision that moves the stopped campaigns off the herdr runtime."""


@deprecated.until('0.4.0', 'a 0.2 database with work on the herdr runtime: delete on_herdr, HERDR_REMOVED and its call')
def on_herdr(database: Path) -> SchemaMismatch | None:
    """Return why ``database`` cannot be migrated yet: an active campaign or a live agent is on the herdr runtime.

    ``HERDR_REMOVED`` moves only the stopped campaigns, which run nothing. The others move with the piloop that runs
    them, before the upgrade. A database from before ``agent_sessions`` (0.1) is not checked.
    """
    with closing(sqlite3.connect(f'file:{database}?mode=ro', uri=True)) as con:
        try:
            campaigns = con.execute(
                "SELECT DISTINCT 'campaign ' || name FROM campaigns"
                " LEFT JOIN json_each(campaigns.config, '$.roles') AS role ON 1 WHERE status = 'active'"
                " AND :herdr IN (json_extract(config, '$.runtime'), json_extract(role.value, '$.runtime'))",
                {'herdr': deprecated.HERDR},
            ).fetchall()
            agents = con.execute(
                "SELECT 'agent_session#' || id FROM agent_sessions"
                " WHERE runtime = :herdr AND state IN ('starting', 'running', 'ending')",
                {'herdr': deprecated.HERDR},
            ).fetchall()
        except sqlite3.OperationalError:  # a 0.1 schema, with projects and workers
            return None
    if not (found := [row[0] for row in (*campaigns, *agents)]):
        return None
    return SchemaMismatch(
        f'{database} has work on the herdr runtime, which piloop 0.3.0 removes: {", ".join(found)}',
        'with piloop 0.2 (`uv tool install \'piloop==0.2.*\'`), set runtime = "subprocess" in the piloop.toml of each'
        ' such campaign and run `piloop init <repo>`, let its herdr agents end or `piloop agent cancel` them, then'
        f' install this piloop again and run `piloop upgrade` ({deprecated.HERDR_UPGRADE})',
    )


def revisions() -> list[MigrationRevision]:
    """Every revision of core's chain that this piloop knows, oldest first."""
    return [MigrationRevision(r.revision) for r in reversed(list(ScriptDirectory(str(MIGRATIONS)).walk_revisions()))]


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
    if not (late := behind(database)):
        return None
    found = f'{database} is at schema {described(late)}, and this {installed()} knows {described(heads())}'
    match actor.launched:
        case Agent():
            fix = 'the daemon is being upgraded; retry in a minute'
        case None if daemon is None:
            fix = 'run `piloop migrate`, which backs it up first'
        case _:
            fix = (
                f'the daemon (pid {daemon}) runs an older piloop. `piloop restart` restarts it on this piloop,'
                ' which migrates it'
            )
    return SchemaMismatch(found, fix)


def revision(database: Path) -> str | None:
    """The revision of core's chain in ``database``, read read-only. None when it was never migrated."""
    return stamps(database).get(CORE)


def stamps(database: Path) -> Stamps:
    """The revision of core and of each plugin that migrated ``database``, installed or not. Empty when none did.

    It reads the database read-only.
    """
    if not database.is_file():
        return {}
    with closing(sqlite3.connect(f'file:{database}?mode=ro', uri=True)) as con:
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND (name = ? OR name GLOB ?) ORDER BY name",
            (MIGRATION_TABLE, f'{MIGRATION_TABLE}_*'),
        ).fetchall()
        rows = {table: con.execute(f'SELECT version_num FROM "{table}"').fetchone() for (table,) in tables}
    return {_named(table): row[0] for table, row in rows.items() if row is not None}


def described(found: Stamps) -> str:
    """``found`` for a person: each revision after its schema's name, ``core 0060, memory/local 0001``.

    ``none`` alone for a database that was never migrated.
    """
    if found.get(CORE) == 'none':
        return 'none'
    return ', '.join(f'{name} {at}' for name, at in found.items())


def backups(database: Path) -> list[Path]:
    """The copies that ``migrate`` wrote of ``database``, ``<database>.bak-<revision>-<UTC time>``, oldest first.

    No other name is one: not SQLite's ``-shm`` and ``-wal`` next to a copy that something opened, and not a copy that
    a person named ``<database>.bak-<anything>``.
    """
    ours = re.compile(rf'{re.escape(database.name)}{re.escape(BACKUP_INFIX)}[0-9]+-(?P<at>.+)')
    written: dict[Path, datetime] = {}
    for path in database.parent.glob(f'{database.name}{BACKUP_INFIX}*'):
        if (found := ours.fullmatch(path.name)) is not None and (at := _written_at(found['at'])) is not None:
            written[path] = at
    return sorted(written, key=written.__getitem__)


def _written_at(stamp: str) -> datetime | None:
    """The time in the name of a copy, or None for a text that is no such time: no name that ``migrate`` wrote."""
    try:
        return datetime.strptime(stamp, BACKUP_TIME)
    except ValueError:
        return None


def _backup(database: Path, at: str, keep: int) -> Path:
    """Copy ``database`` at revision ``at`` with ``VACUUM INTO``, then keep only the newest ``keep`` copies.

    ``VACUUM INTO`` reads one snapshot, with what the WAL holds, so the copy is consistent while others write.
    """
    copy = database.with_name(f'{database.name}{BACKUP_INFIX}{at}-{utcnow():{BACKUP_TIME}}')
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


def heads() -> Stamps:
    """The newest revision of each installed chain that this piloop knows, by schema name, in ``schemas`` order."""
    return {name: head for name, script in scripts().items() if (head := script.get_current_head()) is not None}


def behind(database: Path) -> Stamps:
    """The revision of each installed chain that this piloop would migrate (``none``: never migrated). Empty at head.

    Check ``unmigratable`` first.
    """
    found = stamps(database)
    return {name: found.get(name, 'none') for name, head in heads().items() if found.get(name) != head}


def fingerprints(script: ScriptDirectory) -> dict[str, int]:
    """For each revision of ``script``, a hash of the file names of the revisions up to it: a positive int32, never 0.

    Names, not contents: a refactor of a released revision's code must not refuse every database.
    """
    history, found = hashlib.sha256(), {}
    for rev in reversed(list(script.walk_revisions())):
        history.update(f'{Path(rev.path).stem}\n'.encode())
        found[rev.revision] = int.from_bytes(history.digest()[:4]) >> 1 or 1
    return found


def _record(connection: Connection, name: str, package: str) -> None:
    """Record the ``package`` of the schema ``name`` and the fingerprint of its revision in ``migrations``."""
    at = connection.exec_driver_sql(f'SELECT version_num FROM {version_table(name)}').scalar_one()
    mark = fingerprints(scripts()[name])[at]
    statement = insert(Migration).values(name=name, package=package, fingerprint=mark)
    connection.execute(
        statement.on_conflict_do_update(index_elements=[Migration.name], set_=dict(package=package, fingerprint=mark))
    )


def foreign(database: Path, name: str, at: str) -> SchemaAhead | None:
    """Return the refusal of ``database`` if another piloop build migrated the chain of ``name`` to ``at``. Read-only."""
    known = fingerprints(scripts()[name])
    if at in known and _fingerprint(database, name) in {0, known[at]}:
        return None
    head = heads().get(name)
    why = f', which knows up to {head}' if at not in known else f': this build has a different {at} or one before it'
    return SchemaAhead(
        f'{database} was migrated by a different piloop build ({name} revision {at} is not in the history of this'
        f' {installed()}{why})',
        'install the piloop that migrated it (`piloop upgrade` for a newer release), or set PILOOP_HOME to another'
        ' directory',
    )


def _fingerprint(database: Path, name: str) -> int:
    """The fingerprint that the last migration of the chain of ``name`` recorded in ``database``. 0 when none did."""
    with closing(sqlite3.connect(f'file:{database}?mode=ro', uri=True)) as con:
        try:
            row = con.execute('SELECT fingerprint FROM migrations WHERE name = ?', (name,)).fetchone()
        except sqlite3.OperationalError:  # before 0060, which adds migrations
            row = None
        if row is not None:
            return row[0]
        return _user_version(con) if name == CORE else 0


@deprecated.until(
    '0.5.0', 'a home before revision 0060 has the fingerprint in PRAGMA user_version: delete _user_version'
)
def _user_version(con: sqlite3.Connection) -> int:
    """Core's fingerprint as a database before ``migrations`` kept it."""
    (mark,) = con.execute('PRAGMA user_version').fetchone()
    return mark


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


DOORBELL_SECONDS = 0.05
"""How often a process that waits reads the newest event id, to wake on the events of other processes.

SQLite tells no process of another's commits. The read costs about 10 µs, so a waiting process spends about 5 CPU
seconds an hour on it. A commit that adds no event (a heartbeat, a lease renewal) rings nothing.

Retirement: deleted when the feed moves to a server database that notifies its listeners (Postgres ``LISTEN``)."""


CLOSE_WAIT_SECONDS = 5.0
"""How long ``Database.close`` waits for the connections in use to come back: the reads of its waiters, and every
other session in flight."""


def write_transaction(s: Session) -> None:
    """Begin the write transaction of ``s`` now (``BEGIN IMMEDIATE``): it holds the write lock until the commit.

    So a check that a later write depends on, a lease's fence say, reads the state that the write changes: no other
    writer commits between them. pysqlite would otherwise begin the transaction only at the first write. The session
    must have no open transaction, and the caller commits or rolls back.
    """
    s.connection().exec_driver_sql('BEGIN IMMEDIATE')


def _newest_event(conn: PoolProxiedConnection) -> int:
    """The id of the newest event, read on a raw connection. 0 when there is none."""
    cur = conn.cursor()
    try:
        cur.execute('SELECT max(id) FROM events')
        row = cur.fetchone()
    finally:
        cur.close()
    return (row[0] if row else None) or 0


class DatabaseClosed(RuntimeError):
    """A session asked of a ``Database`` after its ``close``: the caller outlived the daemon's database."""


class Database:
    """One engine and a session factory. Modules get the database only through this class.

    Args:
        url: The SQLAlchemy URL of the database.
        actor: The value that every UPDATE through this database records in ``updated_by``.
        writes: Where each slow write transaction records how long it waited for the write lock and held it
            (``MeasuredConnection``). None records nothing.
    """

    def __init__(self, url: str, *, actor: Actor, writes: WriteSpans | None = None) -> None:
        self.engine = make_engine(url, actor, writes)
        self.actor = actor
        self._sessions = sessionmaker(self.engine, expire_on_commit=False)
        self._committed = threading.Condition()
        self._wakes = 0
        """How many wakes came: a commit of this process, a new event of another (the doorbell), or ``notify``. A
        waiter that saw this number waits only for a later one."""
        self._doorbell: threading.Thread | None = None
        """Reads the newest event id every ``DOORBELL_SECONDS`` once something waits, until ``close``."""
        self._closed = False
        self._reading = 0
        """How many waiters read now: ``close`` lets them end before it closes the connections that they hold."""
        self._changed: set[asyncio.Future[None]] = set()
        """The futures of ``changed``'s waiters, each on its own event loop: the next commit resolves them."""
        event.listen(self._sessions, 'after_commit', lambda _s: self._wake())
        self._checked_out = 0
        """How many connections the pool has handed out now: ``close`` lets them come back before it closes them."""
        event.listen(self.engine, 'checkout', lambda *_: self._count_checkout(1))
        event.listen(self.engine, 'checkin', lambda *_: self._count_checkout(-1))
        event.listen(self.engine, 'do_connect', self._refuse_once_closed)

    def session(self) -> Session:
        """A new session. Use it as a context manager.

        Raises:
            DatabaseClosed: ``close`` came first. A session then would open a connection that nothing closes.
        """
        if self._closed:
            raise DatabaseClosed('the database is closed')
        return self._sessions()

    @contextmanager
    def acting(self, actor: Actor) -> Generator[Session]:
        """A session whose writes sign as ``actor``, as a session of a client that ``actor`` runs would: the API's.

        Its connection signs as ``actor`` until the block ends, then as this database's own actor again, before it goes
        back to the pool.

        Raises:
            DatabaseClosed: ``close`` came first.
        """
        if self._closed:
            raise DatabaseClosed('the database is closed')
        with self.engine.connect() as conn:
            driver = conn.connection.driver_connection
            assert isinstance(driver, sqlite3.Connection)  # make_engine opens SQLite only
            sign(driver, actor)
            try:
                with self._sessions(bind=conn) as s:
                    yield s
            finally:
                sign(driver, self.actor)

    def _count_checkout(self, change: int) -> None:
        with self._committed:
            self._checked_out += change
            self._committed.notify_all()

    def _refuse_once_closed(self, *_: object) -> None:
        """Open no connection after ``close``: a session made before it would leave one that nothing closes."""
        if self._closed:
            raise DatabaseClosed('the database is closed')

    def _wake(self) -> None:
        with self._committed:
            self._wakes += 1
            self._committed.notify_all()
            self._resolve()

    def notify(self) -> None:
        """Wake every waiter of this process, as a commit does, so that each checks its ``give_up`` again."""
        self._wake()

    def _ring_on_events(self) -> None:
        """Start the doorbell, once: the first waiter of the process starts it. The caller holds ``_committed``.

        The doorbell takes its connection itself, without the lock: a connection that comes back to the pool takes the
        lock (``_count_checkout``), so a waiter that waited for one under it could wait for the pool's whole timeout.
        It counts as a read until it has it, so a ``close`` meanwhile waits for it. One that gets none wakes the waiters,
        whose next pass starts it again.
        """
        if self._doorbell is None and not self._closed:
            self._reading += 1
            self._doorbell = threading.Thread(target=self._ring, name='doorbell', daemon=True)
            self._doorbell.start()

    def _ring(self) -> None:
        """Wake the waiters when the newest event id moves, until ``close``. Then give the connection back to the pool."""
        conn: PoolProxiedConnection | None = None
        try:
            conn = self.engine.raw_connection()
        except DatabaseClosed:  # a close came first: there is nothing to ring for
            return
        finally:
            with self._committed:
                self._reading -= 1
                if conn is None:  # wake the waiters: each one's next pass starts it again
                    self._doorbell, self._wakes = None, self._wakes + 1
                    self._resolve()
                self._committed.notify_all()
        with closing(conn):
            head = _newest_event(conn)
            while True:
                with self._committed:
                    if self._committed.wait_for(lambda: self._closed, DOORBELL_SECONDS):
                        return
                if (newest := _newest_event(conn)) != head:
                    head = newest
                    self._wake()

    def _resolve(self) -> None:
        """Resolve every waiter of ``changed`` on its own loop. The caller holds ``_committed``."""
        for future in self._changed:
            future.get_loop().call_soon_threadsafe(future.set_result, None)
        self._changed.clear()

    def wait_until(
        self, done: Callable[[Session], bool], give_up: Callable[[], bool] = lambda: False, timeout: float = math.inf
    ) -> bool:
        """Block until ``done`` holds in a new session, and return True. False at ``timeout``, ``give_up`` or ``close``.

        It reads again after every commit of this process, every new event of another, and every ``notify``: whoever
        sets what ``give_up`` reads calls ``notify``. A commit of another process that adds no event wakes nothing. The
        read happens outside the condition, so no commit waits for it.
        """
        deadline = time.monotonic() + timeout
        while True:
            with self._committed:
                if self._closed:
                    return False
                self._ring_on_events()
                seen, self._reading = self._wakes, self._reading + 1
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
                    lambda seen=seen: self._wakes != seen or self._closed, None if math.isinf(left) else left
                )

    async def changed(self, seen: int, timeout: float) -> int:
        """The count of wakes once it is not ``seen``, or at ``timeout``.

        ``wait_until`` for a coroutine: it holds no thread while it waits, so a server can hold many long-polls. It
        wakes as ``wait_until`` does. A caller that saw nothing yet passes 0, and reads again at once if anything woke.
        """
        future = asyncio.get_running_loop().create_future()
        with self._committed:
            if self._wakes != seen or self._closed:
                return self._wakes
            self._ring_on_events()
            self._changed.add(future)
        try:
            await asyncio.wait([future], timeout=timeout)
        finally:
            with self._committed:
                self._changed.discard(future)
        return self._wakes

    def close(self) -> None:
        """Make every waiter give up, stop the doorbell, let the sessions in flight end, then close the connections.

        A connection that is checked out while the pool goes is never closed: Python warns when it collects it. So it
        waits for every one to come back, a waiter's read, the doorbell's or any other session's, and opens no new one
        meanwhile.
        """
        with self._committed:
            self._closed = True
            self._committed.notify_all()
            self._resolve()
            self._committed.wait_for(lambda: self._reading == self._checked_out == 0, CLOSE_WAIT_SECONDS)
        self.engine.dispose()
