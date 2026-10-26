"""A package that owns tables: installed, uninstalled with its tables present, and installed again."""

import sqlite3
from contextlib import closing
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy import create_engine

from piloop.actor import System
from piloop.cli.doctor import _database as database_check
from piloop.db import sql
from piloop.db.schema import CORE, SCHEMAS
from piloop.db.session import SchemaAhead, alembic_config, migrate, schema_mismatch, stamps
from tests.conftest import disk
from tests.daemon.test_roles import installed

NOTES = 'tests.fixtures.notes.tables'


def install(monkeypatch: pytest.MonkeyPatch, schema: str | None) -> None:
    """Install the ``notes`` package with its schema ``schema`` (``SCHEMA``, ``LATER``), or uninstall it (None)."""
    entries = [] if schema is None else [EntryPoint('notes', f'{NOTES}:{schema}', SCHEMAS.group)]
    installed(monkeypatch, entries)
    sql.declared.cache_clear()


def migrated(path: Path) -> None:
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine, cas=disk(path.parent), backups=0, daemon=None)
    engine.dispose()


def rows(path: Path, query: str) -> list[tuple[object, ...]]:
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute('PRAGMA foreign_keys = ON')
        return con.execute(query).fetchall()


def test_an_uninstalled_package_leaves_its_tables_and_a_home_that_migrates_and_opens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / 'piloop.db'
    install(monkeypatch, 'SCHEMA')
    migrated(path)
    assert stamps(path)['notes'] == '0001'
    with closing(sqlite3.connect(path)) as con:
        con.execute("INSERT INTO campaigns (name, path, config) VALUES ('kept', '/k', '{}'), ('gone', '/g', '{}')")
        con.execute("INSERT INTO notes (campaign_id, body) VALUES (1, 'stays'), (2, 'goes with its campaign')")
        con.commit()

    install(monkeypatch, None)
    assert schema_mismatch(path, None, System.HUMAN) is None, 'its version table is not read'
    found = database_check(path, None, System.HUMAN).found
    assert found.endswith(
        '; warning: the plugin notes (notes = tests.fixtures.notes.tables:SCHEMA) is not installed: its tables stay'
    )
    command.downgrade(alembic_config(f'sqlite:///{path}', CORE, disk(tmp_path)), '0059')
    found = database_check(path, None, System.HUMAN)
    assert found.fix is not None, 'core is behind: migrate'
    migrated(path)
    assert database_check(path, None, System.HUMAN).found.endswith(
        '; warning: the plugin notes (an unknown package) is not installed: its tables stay'
    ), 'core migrated past it: the migrations table lost its row, and its tables stayed'
    assert rows(path, 'SELECT body FROM notes ORDER BY id') == [('stays',), ('goes with its campaign',)]
    rows(path, "DELETE FROM campaigns WHERE name = 'gone'")
    assert rows(path, 'SELECT body FROM notes') == [('stays',)], 'SQLite cascades without the package'

    install(monkeypatch, 'LATER')
    migrated(path)
    assert stamps(path)['notes'] == '0002'
    assert rows(path, 'SELECT body, pinned FROM notes') == [('stays', None)], 'it resumes from its own revision'
    assert rows(path, "SELECT package FROM migrations WHERE name = 'notes'") == [
        ('notes = tests.fixtures.notes.tables:LATER',)
    ]

    install(monkeypatch, 'SCHEMA')
    with pytest.raises(SchemaAhead, match='notes revision 0002 is not in the history of this'):
        migrated(path)
