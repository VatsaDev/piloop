"""Engine, sessions and migrations for the database.

Migrations are append-only. Every schema change is a new revision
(``uv run alembic revision --autogenerate -m "..."``), never an edit of an existing one. A new version of piloop
migrates an existing database when it first opens it.
"""

import fcntl
import os
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.runtime.migration import MigrationContext
from alembic.script.revision import ResolutionError
from alembic.util.exc import CommandError
from sqlalchemy import Engine, create_engine, event, make_url
from sqlalchemy.engine import ExceptionContext
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import ConnectionPoolEntry

from piloop.db.sql import CONNECTION_PRAGMAS, install_actor, install_functions, lock_holder, optimize

MIGRATIONS = Path(__file__).parent / 'migrations'


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


def migrate(engine: Engine) -> None:
    """Migrate the database to the latest Alembic revision. Create it if it does not exist.

    Raises:
        SchemaAhead: The database is at a revision that this piloop does not know: an older CLI on a newer database.
    """
    path = Path(engine.url.database or '')
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    cfg = AlembicConfig()
    cfg.set_main_option('script_location', str(MIGRATIONS))
    cfg.set_main_option('sqlalchemy.url', engine.url.render_as_string(hide_password=False))
    try:
        with _migration_lock(path):
            command.upgrade(cfg, 'head')
    except CommandError as e:
        if not isinstance(e.__cause__, ResolutionError):  # pragma: no cover - alembic fails here for no other reason
            raise
        raise SchemaAhead(
            f'{engine.url.database} is at schema revision {revision(engine)}, which this piloop does not know: '
            'it was migrated by a newer piloop. Run `piloop upgrade`.'
        ) from e


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
