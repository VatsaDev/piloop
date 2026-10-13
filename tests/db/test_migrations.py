"""The schema at head: what the migrations create, that models and migrations agree, and that 0001 upgrades to head."""

import sqlite3
import threading
import time
from contextlib import closing
from json import dumps, loads
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from git import Repo
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

from piloop.db.models import Idea, Project
from piloop.db.session import MIGRATIONS, Database, SchemaAhead, migrate, revision
from piloop.db.triggers import PERSISTENT

INBOX = 'ix_comments_inbox'
INBOX_WHERE = 'priority IS NOT NULL AND acked_at IS NULL'


def alembic(path: Path) -> AlembicConfig:
    cfg = AlembicConfig()
    cfg.set_main_option('script_location', str(MIGRATIONS))
    cfg.set_main_option('sqlalchemy.url', f'sqlite:///{path}')
    return cfg


def test_head_has_every_table_and_trigger_and_matches_the_models(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, 'head')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        tables = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")]
        assert tables == [
            'alembic_version',
            'candidates',
            'comments',
            'comments_fts',
            'comments_fts_config',
            'comments_fts_data',
            'comments_fts_docsize',
            'comments_fts_idx',
            'documents',
            'documents_fts',
            'documents_fts_config',
            'documents_fts_data',
            'documents_fts_docsize',
            'documents_fts_idx',
            'evaluators',
            'events',
            'idea_links',
            'ideas',
            'ideas_fts',
            'ideas_fts_config',
            'ideas_fts_data',
            'ideas_fts_docsize',
            'ideas_fts_idx',
            'journal',
            'measurements',
            'projects',
            'runs',
            'techniques',
            'workers',
        ], 'an external-content FTS5 index has no _content shadow: the text stays in the base table only'
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == sorted(t.name for t in PERSISTENT), 'the package lists what the migrations install'
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        con.execute("INSERT INTO candidates (project_id, tree_sha, commit_sha) VALUES (1, 't', 'c')")
        with pytest.raises(
            sqlite3.IntegrityError, match='UNIQUE constraint failed: candidates.project_id, candidates.tree_sha'
        ):
            con.execute("INSERT INTO candidates (project_id, tree_sha, commit_sha) VALUES (1, 't', 'other commit')")
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed'):
            con.execute("UPDATE candidates SET verdict = 'maybe' WHERE id = 1")
        assert con.execute('SELECT status, stopped_reason FROM projects').fetchall() == [('active', None)]
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed'):
            con.execute("UPDATE projects SET status = 'paused' WHERE id = 1")
    command.check(cfg)
    command.downgrade(cfg, 'base')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert (
            con.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name != 'alembic_version'").fetchall()
            == []
        )


def test_an_older_piloop_refuses_a_database_a_newer_one_migrated(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine)
    with closing(sqlite3.connect(path)) as con:
        con.execute("UPDATE alembic_version SET version_num = '9999_from_the_future'")
        con.commit()
    with pytest.raises(SchemaAhead, match='revision 9999_from_the_future.*Run `piloop upgrade`'):
        migrate(engine)
    engine.dispose()


def test_idea_links_stay_inside_the_project_never_point_at_the_idea_itself_and_go_with_either_idea(
    tmp_path: Path,
) -> None:
    """CHECKs refuse a self-link and an unknown kind, the trigger a link into another project (or to no idea)."""
    path = tmp_path / 'piloop.db'
    command.upgrade(alembic(path), 'head')
    link = 'INSERT INTO idea_links (idea_id, target_id, kind) VALUES '
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute('PRAGMA foreign_keys=ON')
        con.executescript(
            "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}'), ('q', '/q', '{}');"
            'INSERT INTO ideas (project_id, title, hypothesis, created_by, track)'
            " VALUES (1, 'first', 'h', 't', 'a'), (2, 'other', 'h', 't', 'a'), (1, 'then', 'h', 't', 'a');"
        )
        refused = {
            "(1, 1, 'after')": 'CHECK constraint failed: idea_links_not_self',
            "(1, 3, 'blocks')": 'CHECK constraint failed: linkkind',
            "(1, 2, 'after')": 'idea_links: idea_id and target_id must be ideas of the same project',
            "(1, 99, 'supersedes')": 'idea_links: idea_id and target_id must be ideas of the same project',
        }
        for values, error in refused.items():
            with pytest.raises(sqlite3.IntegrityError, match=error):
                con.execute(link + values)
        con.execute(link + "(3, 1, 'after'), (3, 1, 'supersedes')")  # one pair may carry both kinds
        with pytest.raises(sqlite3.IntegrityError, match='UNIQUE constraint failed: idea_links.idea_id'):
            con.execute(link + "(3, 1, 'after')")
        con.execute('DELETE FROM ideas WHERE id = 1')
        assert con.execute('SELECT count(*) FROM idea_links').fetchone() == (0,), 'links go with the target'
        con.execute(
            "INSERT INTO ideas (project_id, title, hypothesis, created_by, track) VALUES (1, 'again', 'h', 't', 'a')"
        )
        con.execute(link + "(4, 3, 'after')")
        con.execute('DELETE FROM ideas WHERE id = 4')
        assert con.execute('SELECT count(*) FROM idea_links').fetchone() == (0,), 'and with the idea'


def test_idea_titles_are_unique_per_project(db: Database) -> None:
    """Concurrent research sweeps that land on the same title collide at insert; another project may reuse it."""
    with db.session() as s:
        s.add(Project(name='a', path='/a', config={}))
        s.add(Project(name='b', path='/b', config={}))
        s.commit()
        s.add(Idea(track='score', project_id=1, title='target encoding', hypothesis='x', created_by='research#1'))
        s.add(Idea(track='score', project_id=2, title='target encoding', hypothesis='x', created_by='research#2'))
        s.commit()
        s.add(Idea(track='score', project_id=1, title='target encoding', hypothesis='y', created_by='research#3'))
        with pytest.raises(IntegrityError, match='UNIQUE constraint failed: ideas.project_id, ideas.title'):
            s.commit()


def test_concurrent_migrations_of_a_fresh_database_run_one_at_a_time(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two daemons starting on a fresh home both call ``migrate``: unserialised, both read "no revision" and the second
    dies on ``CREATE TABLE comments`` (the ``piloop serve --detach`` race). A second ``migrate`` must wait until the
    first is out of Alembic, then find the database at head and do nothing.
    """
    path = tmp_path / 'piloop.db'
    real = command.upgrade
    inside = threading.Event()  # the first upgrade has started and is parked ...
    release = threading.Event()  # ... until the test lets it finish
    entered: list[str] = []

    def gated(cfg: AlembicConfig, rev: str) -> None:
        entered.append(threading.current_thread().name)
        if threading.current_thread().name == 'first':
            inside.set()
            assert release.wait(5)
        real(cfg, rev)

    monkeypatch.setattr(command, 'upgrade', gated)
    engines = [create_engine(f'sqlite:///{path}') for _ in range(2)]
    first = threading.Thread(target=migrate, args=(engines[0],), name='first')
    second = threading.Thread(target=migrate, args=(engines[1],), name='second')
    first.start()
    assert inside.wait(5)
    second.start()
    time.sleep(0.3)
    assert entered == ['first'], 'the second migrate entered Alembic while the first was still upgrading'
    release.set()
    first.join(5)
    second.join(5)
    head = ScriptDirectory.from_config(alembic(path)).get_current_head()
    assert entered == ['first', 'second'] and revision(engines[0]) == head
    for engine in engines:
        engine.dispose()


def test_0002_admits_lessons_names_each_projects_repo_adds_documents_and_the_journal_and_downgrades(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0001')
    clone = Repo.init(tmp_path / 'clone')
    clone.create_remote('origin', 'git@github.com:hiloopai/mudbench.git')
    comment = "INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('project', 1, 'w', ?, ?)"
    lesson = 'family: voting | status: dead | best: +0.002 acc (evaluator#1)\nidea#1'
    config = {'name': 'p', 'roles': {'worker': {'wake_budget': 0}}, 'memory': {'documents': ['docs/*.md']}}
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', ?, '{}')", (str(clone.working_dir),))
        con.execute("INSERT INTO projects (name, path, config) VALUES ('q', '/gone', '{}')")
        con.execute(comment, ('finding', 'voting gained nothing'))
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: commentkind'):
            con.execute(comment, ('lesson', lesson))
    command.upgrade(cfg, 'head')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT repo FROM projects ORDER BY id').fetchall() == [
            ('github.com/hiloopai/mudbench',),
            ('/gone',),
        ], 'filled from each checkout: its remote, else its path'
        con.execute(comment, ('lesson', lesson))
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: a lesson starts family'):
            con.execute(comment, ('lesson', 'voting is dead'))
        assert [r for (r,) in con.execute("SELECT rowid FROM comments_fts WHERE comments_fts MATCH 'voting'")] == [1, 2]
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == sorted(t.name for t in PERSISTENT), 'the rebuilt comments table has its triggers back'
        con.execute(
            "INSERT INTO documents (project_id, path, hash, title, body, created_by) VALUES (1, 'r.md', ?, 'R', 'clock', 'me')",
            ('0' * 64,),
        )
        assert con.execute("SELECT rowid FROM documents_fts WHERE documents_fts MATCH 'clock'").fetchall() == [(1,)]
        con.execute("INSERT INTO journal (author, line) VALUES ('orchestrator', 'started')")
        con.execute('UPDATE projects SET config = ? WHERE id = 1', (dumps(config),))
    command.downgrade(cfg, '0001')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT kind, body FROM comments ORDER BY id').fetchall() == [
            ('finding', 'voting gained nothing'),
            ('finding', lesson),
        ], "0001's code has no lessons; the text stays, as a finding"
        names = "SELECT name FROM sqlite_master WHERE name LIKE 'documents%' OR name = 'journal'"
        assert con.execute(names).fetchall() == []
        assert 'repo' not in [c for (_, c, *_) in con.execute('PRAGMA table_info(projects)')]
        assert [loads(c) for (c,) in con.execute('SELECT config FROM projects ORDER BY id')] == [
            {'name': 'p', 'roles': {'worker': {}}},
            {},
        ], "0001's code refuses the memory and wake_budget keys"
