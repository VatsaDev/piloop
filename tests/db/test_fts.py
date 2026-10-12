"""Full-text search over ideas and comments: the index follows every write, ranks, and is nobody's to write by hand."""

import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from sqlalchemy import text

from piloop.db import sql
from piloop.db.session import Database
from piloop.db.triggers import PERSISTENT, fts, recreate

IDEAS = (
    "INSERT INTO ideas (project_id, title, hypothesis, plan, created_by, track) VALUES (1, 'Soft voting ensemble', "
    "'Averaging the probabilities of the trees and a logistic regression beats either alone', NULL, 'research#1', 'a')",
    "INSERT INTO ideas (project_id, title, hypothesis, plan, created_by, track) VALUES (1, 'Target encoding of cabin', "
    "'The cabin deck carries survival signal the one-hot loses', "
    "'Encode the deck letter; naïve mean target encoding with smoothing', 'research#1', 'a')",
    "INSERT INTO ideas (project_id, title, hypothesis, plan, created_by, track) VALUES (1, 'Weighted vote by validation', "
    "'Weighting each model by its fold accuracy before voting sharpens the ensemble vote', NULL, 'research#2', 'a')",
)
COMMENT = (
    "INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('idea', 1, 'worker#3', 'finding', "
    "'Voting gained nothing: the trees agree on every row, so target encoding is the direction left')"
)


def integrity(path: Path) -> None:
    """FTS5's own check that each index matches its content table, from a connection nothing restricts."""
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        for table in ('ideas_fts', 'comments_fts'):
            con.execute(f"INSERT INTO {table} ({table}) VALUES ('integrity-check')")


def hits(con: sqlite3.Connection, table: str, query: str) -> list[object]:
    """The matching rowids, best match first."""
    fts = f'{table}_fts'
    return [
        r[0] for r in sql.run(con, f"SELECT rowid FROM {fts} WHERE {fts} MATCH '{query}' ORDER BY bm25({fts})").rows
    ]


def test_the_index_follows_every_write_through_the_agent_connection(db: Database, tmp_path: Path) -> None:
    """The sync triggers fire under the authorizer (their bodies are authorized too) and under DEFENSIVE mode."""
    path = tmp_path / 'piloop.db'
    with closing(sql.connect(path, actor='research#1')) as con:
        sql.run(con, "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        for statement in IDEAS:
            sql.run(con, statement)
        assert hits(con, 'ideas', 'votes') == [3, 1], (
            'porter: votes, vote and voting are one term; idea#3 says it twice'
        )
        assert hits(con, 'ideas', 'naive') == [2], 'remove_diacritics: naïve is naive'
        assert hits(con, 'ideas', '"target encoding"') == [2]
        assert hits(con, 'ideas', 'ensem*') == [1, 3], 'prefix queries work (against stems)'

        sql.run(con, "UPDATE ideas SET status = 'parked', priority = 0 WHERE id = 1")
        assert hits(con, 'ideas', 'soft') == [1], 'a status change leaves the index alone (and correct)'
        sql.run(con, "UPDATE ideas SET title = 'Hard voting ensemble', plan = 'majority rule' WHERE id = 1")
        assert (hits(con, 'ideas', 'soft'), hits(con, 'ideas', 'majority')) == ([], [1])
        sql.run(con, 'DELETE FROM ideas WHERE id = 2')
        assert hits(con, 'ideas', 'cabin') == []

        sql.run(con, COMMENT)
        assert hits(con, 'comments', 'trees agree') == [1]
        sql.run(con, "UPDATE comments SET body = 'retracted' WHERE id = 1")
        assert (hits(con, 'comments', 'trees'), hits(con, 'comments', 'retracted')) == ([], [1])
        sql.run(con, 'DELETE FROM comments WHERE id = 1')
        assert hits(con, 'comments', 'retracted') == []
    integrity(path)


def test_the_documented_search_queries_rank_and_mark_the_hits(db: Database, tmp_path: Path) -> None:
    """The two examples in sql_procedures.md, verbatim but for the project id."""
    with closing(sql.connect(tmp_path / 'piloop.db', actor='human')) as con:
        sql.run(con, "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        for statement in IDEAS:
            sql.run(con, statement)
        sql.run(con, COMMENT)
        ideas = sql.run(
            con,
            "SELECT i.id, i.status, snippet(ideas_fts, -1, '[', ']', '…', 8) AS hit, bm25(ideas_fts) AS score "
            'FROM ideas_fts JOIN ideas i ON i.id = ideas_fts.rowid '
            "WHERE ideas_fts MATCH 'voting OR ensemble' AND i.project_id = 1 ORDER BY score LIMIT 10",
        )
        assert [(r[0], r[1], r[2]) for r in ideas.rows] == [
            (3, 'ready', '…fold accuracy before [voting] sharpens the [ensemble] [vote]'),
            (1, 'ready', 'Soft [voting] [ensemble]'),
        ], 'idea#3 matches the stem three times; column -1 lets the snippet pick the column with the most hits'
        best, second = (r[3] for r in ideas.rows)
        assert isinstance(best, float) and isinstance(second, float)
        assert best < second < 0, 'bm25: more negative is a better match'
        comments = sql.run(
            con,
            "SELECT c.target_kind, c.target_id, c.author, c.kind, snippet(comments_fts, 0, '[', ']', '…', 8) AS hit "
            'FROM comments_fts JOIN comments c ON c.id = comments_fts.rowid '
            'WHERE comments_fts MATCH \'"target encoding"\' ORDER BY c.id DESC LIMIT 10',
        )
        assert comments.rows == [('idea', 1, 'worker#3', 'finding', '…the trees agree on every row, so [target]…')]
        with pytest.raises(sqlite3.OperationalError, match='no such column: hot'):
            sql.run(con, "SELECT rowid FROM ideas_fts WHERE ideas_fts MATCH 'one-hot'")
        assert hits(con, 'ideas', '"one-hot"') == [2], (
            'the pitfall the procedures warn about: punctuation goes in quotes'
        )


def test_agents_cannot_write_the_indexes_or_their_shadow_tables(db: Database, tmp_path: Path) -> None:
    with closing(sql.connect(tmp_path / 'piloop.db', actor='pi')) as con:
        for statement in (
            "INSERT INTO ideas_fts (rowid, title, hypothesis, plan) VALUES (1, 'x', 'y', NULL)",
            "INSERT INTO ideas_fts (ideas_fts) VALUES ('rebuild')",
            "UPDATE comments_fts SET body = 'x'",
            'DELETE FROM comments_fts',
        ):
            with pytest.raises(sqlite3.DatabaseError, match='not authorized'):
                sql.run(con, statement)
        for statement in ('DELETE FROM ideas_fts_data', "INSERT INTO comments_fts_config (k, v) VALUES ('k', 1)"):
            with pytest.raises(sqlite3.OperationalError, match='may not be modified'):
                sql.run(con, statement)
        assert sql.run(con, 'SELECT count(*) > 0 FROM ideas_fts_data').rows == [(1,)], 'reading them is allowed'


def test_fts_objects_are_the_indexes_their_shadows_and_their_sync_triggers() -> None:
    names = (*fts.FTS_TABLES, *fts.FTS_TRIGGER_NAMES, 'ideas_fts_data')
    assert {t.name for t in PERSISTENT if t.requires is not None} == fts.FTS_TRIGGER_NAMES
    assert all(fts.is_fts_object(name) for name in names)
    assert not fts.is_fts_object('ideas')


def test_recreate_restores_the_sync_triggers_once_the_index_exists(db: Database) -> None:
    """A batch rebuild of ideas in a migration drops its triggers; recreate() puts all four back."""
    with db.engine.begin() as conn:
        for name in ('ideas_updated_at', 'ideas_fts_insert', 'ideas_fts_delete', 'ideas_fts_update'):
            conn.execute(text(f'DROP TRIGGER {name}'))
        recreate(Operations(MigrationContext.configure(conn)), 'ideas')
        query = text("SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'ideas' ORDER BY name")
        assert conn.execute(query).scalars().all() == [
            'ideas_fts_delete',
            'ideas_fts_insert',
            'ideas_fts_update',
            'ideas_updated_at',
        ]
