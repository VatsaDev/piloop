"""The hot paths stay on indexes: EXPLAIN QUERY PLAN of every SELECT a tick, the widget, the status and the history run.

The statements are recorded from the real functions by ``scripts/db_volume.py`` on a small populated database, so a
query someone rewrites onto an unindexed column fails here; the script at full volume is where a new index is
justified before it is added.
"""

import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from piloop.db.models import Base
from piloop.db.session import Database, migrate
from scripts.db_volume import (
    HOT_INDEXES,
    SMALL,
    Statement,
    documented_queries,
    drop_new_indexes,
    explain,
    hot_statements,
    populate,
    scans,
)

HOT_PATHS = {
    'scheduler.tick',
    'stop.stop_reason',
    'overview.overview',
    'history.renderables',
    'sql_procedures.md #1',
    'inbox.news',
}


def allowed(st: Statement, table: str) -> bool:
    """A scan the plan may keep: ``events`` or ``comments`` walked backwards by rowid for their newest rows.

    The overview's last events and latest comments read newest first and stop after a handful of rows; they filter
    by project with a CASE over the target columns, which no index can serve until those rows carry a project column.
    """
    return table in ('events', 'comments') and f'ORDER BY {table}.id DESC' in st.sql


@pytest.fixture(scope='module')
def populated(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, list[Statement]]:
    home = tmp_path_factory.mktemp('plans')
    path = home / 'piloop.db'
    db = Database(f'sqlite:///{path}', actor='daemon')
    migrate(db.engine)
    populate(path, SMALL)
    try:
        return path, hot_statements(db, home)
    finally:
        db.close()


def violations(path: Path, statements: list[Statement]) -> list[str]:
    found = []
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        for st in statements:
            plan = explain(con, st)
            scanned = [t for t in scans(st, plan) if not allowed(st, t)]
            if scanned:
                found.append(f'{st.origin} scans {", ".join(scanned)}:\n  {st.sql}\n  ' + '\n  '.join(plan))
    return found


def test_no_hot_statement_scans_a_table_that_grows(populated: tuple[Path, list[Statement]]) -> None:
    path, statements = populated
    assert violations(path, statements) == []


def test_the_recording_covers_every_hot_path(populated: tuple[Path, list[Statement]]) -> None:
    """The assertion above is only as good as what was recorded: every hot path issued at least one statement."""
    _, statements = populated
    assert {st.origin for st in statements} >= HOT_PATHS
    assert len(documented_queries()) >= 6, 'the sql fences of sql_procedures.md were not parsed'
    assert len(statements) > 30


def test_the_check_fails_without_the_hot_indexes(populated: tuple[Path, list[Statement]], tmp_path: Path) -> None:
    """Proof the test has teeth: without the hot indexes the same statements scan workers, candidates, comments, runs."""
    path, statements = populated
    copy = tmp_path / 'before.db'
    shutil.copy(path, copy)
    drop_new_indexes(copy)
    scanned = ' '.join(violations(copy, statements))
    assert all(table in scanned for table in ('workers', 'candidates', 'comments', 'runs'))


def test_the_models_declare_what_the_script_drops() -> None:
    declared = {i.name for t in Base.metadata.tables.values() for i in t.indexes}
    assert set(HOT_INDEXES) <= declared


def test_scans_resolves_aliases_and_ignores_small_tables() -> None:
    st = Statement('t', 'SELECT c.id FROM candidates c JOIN projects p ON p.id = c.project_id JOIN runs AS r ON 1', ())
    plan = ['SCAN p', 'SCAN c', 'SCAN r', 'SEARCH ideas USING INTEGER PRIMARY KEY (rowid=?)', 'USE TEMP B-TREE']
    assert scans(st, plan) == ['candidates', 'runs']
