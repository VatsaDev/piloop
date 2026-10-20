"""The schema at head: what the migrations create, that models and migrations agree, and that 0001 upgrades to head."""

import json
import re
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

from piloop.config import ProjectConfig
from piloop.db.models import Idea, Project
from piloop.db.session import MIGRATIONS, Database, SchemaAhead, migrate, revision
from piloop.db.triggers import PERSISTENT, projects
from piloop.db.triggers.roles import known_role
from piloop.version import installed

INBOX = 'ix_comments_inbox'
SINCE_0015 = {t.name for t in projects.acyclic()}
"""The persistent triggers that revision 0015 adds: a test of an older revision expects every other one."""
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
            'archives',
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
            'evaluations',
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
            'lessons',
            'measurements',
            'projects',
            'runs',
            'sqlite_sequence',
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
            con.execute(  # SQLite keeps sqlite_sequence, which AUTOINCREMENT made, and never drops it
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT IN ('alembic_version', 'sqlite_sequence')"
            ).fetchall()
            == []
        )


def test_an_older_piloop_refuses_a_database_a_newer_one_migrated(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine)
    head = ScriptDirectory.from_config(alembic(path)).get_current_head()
    with closing(sqlite3.connect(path)) as con:
        con.execute("UPDATE alembic_version SET version_num = '9999_from_the_future'")
        con.commit()
    this = re.escape(installed())
    with pytest.raises(
        SchemaAhead,
        match=rf'revision 9999_from_the_future, which this {this} does not know \(it knows up to {head}\): a newer '
        r'piloop migrated it\. Install a newer piloop \(`piloop upgrade`\), or set PILOOP_HOME to another directory\.$',
    ):
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


def test_0003_turns_the_daemons_skip_discards_into_unmeasured_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0002')
    candidate = 'INSERT INTO candidates (project_id, tree_sha, commit_sha, verdict, verdict_by, verdict_reason) VALUES'
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute('PRAGMA foreign_keys = ON')
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        con.execute(f"{candidate} (1, 'a', 'a', 'discard', 'daemon', 'not evaluated: Piloop-Eval: skip')")
        con.execute(f"{candidate} (1, 'b', 'b', 'discard', 'worker#1', 'not evaluated: Piloop-Eval: skip')")
        con.execute(f"{candidate} (1, 'c', 'c', 'discard', 'daemon', 'against candidate#1: no gain')")
        con.execute("INSERT INTO evaluators (project_id, key, definition) VALUES (1, 'k', '{}')")
        con.execute('INSERT INTO measurements (candidate_id, evaluator_id) VALUES (3, 1)')
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: verdict'):
            con.execute("UPDATE candidates SET verdict = 'unmeasured' WHERE id = 1")
    command.upgrade(cfg, '0003')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        verdicts = 'SELECT id, verdict FROM candidates ORDER BY id'
        assert con.execute(verdicts).fetchall() == [(1, 'unmeasured'), (2, 'discard'), (3, 'discard')], (
            "only the daemon's skip verdicts: an agent's discard is its own judgement"
        )
        assert con.execute('SELECT candidate_id FROM measurements').fetchall() == [(3,)], 'the rebuild keeps children'
        assert con.execute("SELECT count(*) FROM events WHERE kind = 'candidate.verdict'").fetchone() == (0,)
        triggers = "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'candidates'"
        assert {t for (t,) in con.execute(triggers)} == {t.name for t in PERSISTENT if t.table == 'candidates'}, (
            'the rebuilt candidates table has its triggers back'
        )
    command.downgrade(cfg, '0002')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute(verdicts).fetchall() == [(1, 'discard'), (2, 'discard'), (3, 'discard')]
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: verdict'):
            con.execute("UPDATE candidates SET verdict = 'unmeasured' WHERE id = 1")


def test_0005_admits_the_recovered_verdict_and_downgrades_it_to_unmeasured(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0004')
    candidate = 'INSERT INTO candidates (project_id, tree_sha, commit_sha, verdict, verdict_by) VALUES'
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        con.execute(f"{candidate} (1, 'a', 'a', 'keep', 'daemon')")
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: verdict'):
            con.execute(f"{candidate} (1, 'b', 'b', 'recovered', 'daemon')")
    command.upgrade(cfg, '0005')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute(f"{candidate} (1, 'b', 'b', 'recovered', 'daemon')")
        assert con.execute('SELECT id, verdict FROM candidates ORDER BY id').fetchall() == [
            (1, 'keep'),
            (2, 'recovered'),
        ]
        triggers = "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'candidates'"
        assert {t for (t,) in con.execute(triggers)} == {t.name for t in PERSISTENT if t.table == 'candidates'}, (
            'the rebuilt candidates table has its triggers back'
        )
    command.downgrade(cfg, '0004')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT id, verdict FROM candidates ORDER BY id').fetchall() == [
            (1, 'keep'),
            (2, 'unmeasured'),
        ]


def test_0006_admits_lessons_names_each_projects_repo_adds_documents_and_the_journal_and_downgrades(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0005')
    clone = Repo.init(tmp_path / 'clone')
    clone.create_remote('origin', 'git@github.com:hiloopai/mudbench.git')
    comment = "INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('project', 1, 'w', ?, ?)"
    lesson = 'family: voting | status: dead | best: +0.002 acc (evaluator#1)\nidea#1'
    config = {
        'name': 'p',
        'roles': {'worker': {'wake_budget': 0}, 'critic': {'prompt': 'critic.md', 'wake_budget': None}},
        'memory': {'documents': ['docs/*.md']},
    }
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', ?, '{}')", (str(clone.working_dir),))
        con.execute("INSERT INTO projects (name, path, config) VALUES ('q', '/gone', '{}')")
        con.execute(comment, ('finding', 'voting gained nothing'))
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: commentkind'):
            con.execute(comment, ('lesson', lesson))
    command.upgrade(cfg, '0006')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT repo FROM projects ORDER BY id').fetchall() == [
            ('github.com/hiloopai/mudbench',),
            ('/gone',),
        ], 'filled from each checkout: its remote, else its path'
        con.execute(comment, ('lesson', lesson))
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: a lesson starts family'):
            con.execute(comment, ('lesson', 'voting is dead'))
        assert [r for (r,) in con.execute("SELECT rowid FROM comments_fts WHERE comments_fts MATCH 'voting'")] == [1, 2]
        triggers = "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'comments'"
        assert {t for (t,) in con.execute(triggers)} == {t.name for t in PERSISTENT if t.table == 'comments'}, (
            'the rebuilt comments table has its triggers back'
        )
        con.execute(
            "INSERT INTO documents (project_id, path, hash, title, body, created_by) VALUES (1, 'r.md', ?, 'R', 'clock', 'me')",
            ('0' * 64,),
        )
        assert con.execute("SELECT rowid FROM documents_fts WHERE documents_fts MATCH 'clock'").fetchall() == [(1,)]
        con.execute("INSERT INTO journal (author, line) VALUES ('orchestrator', 'started')")
        con.execute('UPDATE projects SET config = ? WHERE id = 1', (dumps(config),))
    command.downgrade(cfg, '0005')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT kind, body FROM comments ORDER BY id').fetchall() == [
            ('finding', 'voting gained nothing'),
            ('finding', lesson),
        ], "0005's code has no lessons; the text stays, as a finding"
        names = "SELECT name FROM sqlite_master WHERE name LIKE 'documents%' OR name = 'journal'"
        assert con.execute(names).fetchall() == []
        assert 'repo' not in [c for (_, c, *_) in con.execute('PRAGMA table_info(projects)')]
        assert [loads(c) for (c,) in con.execute('SELECT config FROM projects ORDER BY id')] == [
            {'name': 'p', 'roles': {'worker': {}, 'critic': {'prompt': 'critic.md'}}},
            {},
        ], "0001's code refuses the memory and wake_budget keys"


def test_0007_turns_each_lesson_comment_into_a_row_of_lessons_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0006')
    comment = "INSERT INTO comments (target_kind, target_id, author, kind, body, created_at) VALUES ('project', 1, ?, ?, ?, ?)"
    bodies = [
        'family: Voting | status: dead | best: +0.002 acc (evaluator#1)\nidea#1',
        'family: speed | status: promising | best: -53% to -63% ms per block (worker-measured)\ncandidate#4',
        'family: x | status: untested | best: none measured (evaluator#2)\nnever ran',
        'family: y | status: dead | best: +3\nno metric word',
    ]
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        con.execute(comment, ('research#2', 'finding', 'voting gained nothing', '2026-10-01 00:00:00'))
        for i, body in enumerate(bodies):
            con.execute(comment, (f'watchdog#{i}', 'lesson', body, f'2026-10-0{i + 2} 00:00:00'))
    command.upgrade(cfg, '0007')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute(
            'SELECT project_id, family, status, effect, metric, body, created_by, created_at FROM lessons ORDER BY id'
        ).fetchall() == [
            (1, 'Voting', 'dead', 0.002, 'acc', bodies[0], 'watchdog#0', '2026-10-02 00:00:00'),
            (1, 'speed', 'promising', -53.0, 'ms', bodies[1], 'watchdog#1', '2026-10-03 00:00:00'),
            (1, 'x', 'untested', None, None, bodies[2], 'watchdog#2', '2026-10-04 00:00:00'),
            (1, 'y', 'dead', None, None, bodies[3], 'watchdog#3', '2026-10-05 00:00:00'),
        ], 'in id order, read as the briefs ranked them, the body as it was'
        assert con.execute('SELECT id, kind FROM comments').fetchall() == [(1, 'finding')]
        assert con.execute("SELECT rowid FROM comments_fts WHERE comments_fts MATCH 'voting'").fetchall() == [(1,)], (
            'the index forgot the lesson comments'
        )
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: commentkind'):
            con.execute(comment, ('w', 'lesson', bodies[0], '2026-10-06 00:00:00'))
        triggers = (
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name IN ('comments', 'journal', 'lessons')"
        )
        assert {t for (t,) in con.execute(triggers)} == {
            t.name for t in PERSISTENT if t.table in ('comments', 'journal', 'lessons')
        }
        con.execute(
            'INSERT INTO lessons (project_id, family, status, effect, metric, body, created_by) VALUES'
            " (1, 'pruning', 'promising', 12.5, 'score', 'idea#9 on evaluator#3', 'orchestrator'),"
            " (1, 'two\nlines', 'dead', 1, 'sc\nore', 'idea#10', 'orchestrator')"
        )
    command.downgrade(cfg, '0006')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute("SELECT author, body FROM comments WHERE kind = 'lesson' ORDER BY id").fetchall() == [
            *((f'watchdog#{i}', body) for i, body in enumerate(bodies)),
            ('orchestrator', 'family: pruning | status: promising | best: +12.5 score\nidea#9 on evaluator#3'),
            ('orchestrator', 'family: two lines | status: dead | best: +1 sc ore\nidea#10'),
        ], 'the old bodies as they were; a new lesson gets the first line that 0006 requires'
        gone = "SELECT name FROM sqlite_master WHERE name LIKE 'lessons%' OR name = 'journal_no_replace'"
        assert con.execute(gone).fetchall() == []


def test_0009_marks_every_archiving_made_before_it_as_pruned_when_made_and_complete(tmp_path: Path) -> None:
    """Before 0009 an archiving removed its local copies at once; from 0009 on the daemon prunes later."""
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0008')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        con.execute("INSERT INTO archives (project_id, idea_id, created_at) VALUES (1, 3, '2026-10-01 12:00:00')")
    command.upgrade(cfg, '0009')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT created_at, pruned_at, complete FROM archives').fetchall() == [
            ('2026-10-01 12:00:00', '2026-10-01 12:00:00', 1)
        ], 'the older code pruned when it archived, and took every row for a finished job'
        con.execute('INSERT INTO archives (project_id) VALUES (1)')
        assert con.execute('SELECT pruned_at FROM archives WHERE id = 2').fetchone() == (None,)
    command.downgrade(cfg, '0008')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        columns = [c for (_, c, *_) in con.execute('PRAGMA table_info(archives)')]
        assert 'pruned_at' not in columns and 'complete' not in columns


def test_0010_marks_the_runs_that_a_stopped_daemon_was_scoring_and_checks_the_flag(tmp_path: Path) -> None:
    """A finished run whose idea is ``scheduled`` with no later run was being scored: the old rule, kept as data."""
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0009')
    idea = (
        "INSERT INTO ideas (track, project_id, title, hypothesis, created_by, status) VALUES ('t', 1, ?, 'h', 'c', ?)"
    )
    run = "INSERT INTO runs (idea_id, attempt, base_candidate_id, harness, model, status) VALUES (?, ?, 1, 'h', 'm', ?)"
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        for title, status in (('scored now', 'scheduled'), ('queued again', 'scheduled'), ('settled', 'done')):
            con.execute(idea, (title, status))
        for idea_id, attempt, status in ((1, 1, 'succeeded'), (2, 1, 'failed'), (2, 2, 'queued'), (3, 1, 'succeeded')):
            con.execute(run, (idea_id, attempt, status))
    command.upgrade(cfg, '0010')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT id, scoring FROM runs ORDER BY id').fetchall() == [(1, 1), (2, 0), (3, 0), (4, 0)]
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: runs.scoring needs a finished'):
            con.execute('UPDATE runs SET scoring = 1 WHERE id = 3')
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == sorted(t.name for t in PERSISTENT if t.name not in SINCE_0015), (
            'the rebuilt runs table has its triggers back'
        )
    command.downgrade(cfg, '0009')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert 'scoring' not in [c for (_, c, *_) in con.execute('PRAGMA table_info(runs)')]
        assert con.execute('SELECT count(*) FROM runs').fetchone() == (4,)


STORED_0005 = Path(__file__).parent.parent / 'fixtures' / 'config_0005.json'
"""A ``projects.config`` as piloop stored it at revision 0005: every key dumped, a scout table under research."""


def test_0011_makes_scout_sweeps_scout_rows_and_stored_configs_valid_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0010')
    kinds = 'SELECT kind FROM workers ORDER BY id'
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute('INSERT INTO projects (name, path, config) VALUES (?, ?, ?)', ('p', '/p', STORED_0005.read_text()))
        con.execute("INSERT INTO projects (name, path, config) VALUES ('q', '/q', '{}')")
        released = json.dumps({'roles': {'research': {'buffer': 3}}})
        con.execute("INSERT INTO projects (name, path, config) VALUES ('r', '/r', ?)", (released,))
        con.execute("INSERT INTO workers (kind, project_id) VALUES ('research', 1), ('research', 1)")
        con.execute(
            "INSERT INTO events (kind, actor, target_kind, target_id) VALUES ('scout.requested', 'h', 'worker', 2)"
        )
    command.upgrade(cfg, '0011')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute(kinds).fetchall() == [('research',), ('scout',)]
        stored = [json.loads(c) for (c,) in con.execute('SELECT config FROM projects ORDER BY id')]
        assert stored[1] == {}, 'a config without the keys is left as it is'
        assert stored[2] == {
            'roles': {'research': {'triggers': [{'kind': 'low_water', 'buffer': 3}, {'kind': 'seed'}]}}
        }, "a release's config has no triggers: research's defaults, with its buffer"
        p = ProjectConfig.model_validate(stored[0])
        assert (p.roles.scout.interval, p.roles.scout.seeds) == (86400, 4)
        assert (p.roles.scout.mcp, p.roles.scout.timeout, p.roles.scout.addendum) == (['xai'], 1200, 'research.md'), (
            'the keys of research that the scout used'
        )
        assert [t.model_dump() for t in p.roles.research.triggers] == [
            {'kind': 'low_water', 'buffer': 5},
            {'kind': 'seed'},
        ], 'the scout trigger leaves, and the buffer moves into low_water'
        assert p.roles.research.max_ideas == 2
        assert p.context == ['docs/analysis-*.md'], 'research context is the project context'
        usage = p.harness['mine'].usage
        assert usage is not None and usage.file == '{env:CLAUDE_CONFIG_DIR:~/.claude}/projects/{cwd}/{session_id}.jsonl'
        assert [t.kind for t in p.roles.watchdog.triggers] == ['anomaly', 'cron', 'stopped'], 'the closing review stays'
        con.execute("INSERT INTO workers (kind, project_id) VALUES ('scout', 2)")
        sql = con.execute("SELECT sql FROM sqlite_master WHERE name = 'workers_kind'").fetchone()[0]
        assert sql + ';' == known_role().sql()
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == sorted(t.name for t in PERSISTENT if t.name not in SINCE_0015)
    command.downgrade(cfg, '0010')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute(kinds).fetchall() == [('research',), ('research',), ('research',)]
        scouts = "SELECT target_id FROM events WHERE kind = 'scout.requested' ORDER BY target_id"
        assert con.execute(scouts).fetchall() == [(2,), (3,)], 'every scout row is a scout sweep of research again'
        with pytest.raises(sqlite3.IntegrityError, match='workers.kind must be a role of the project'):
            con.execute("INSERT INTO workers (kind, project_id) VALUES ('scout', 2)")


def test_0012_leaves_the_cached_input_out_of_the_input_of_codex_rows_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0011')
    usage = {'input': 100, 'output': 5, 'cache_read': 60, 'cache_write': 0, 'total': 105, 'turns': 1}
    rollout = '/h/.codex/sessions/2026/01/01/rollout-2026-01-01T00-00-00-x.jsonl'
    rows = [
        ('codex', usage | {'source': rollout}),
        ('claude', usage | {'source': '/h/.claude/projects/-wt/x.jsonl'}),
        ('mine', usage | {'source': '/h/.mine/x.jsonl'}),
        ('gone', usage | {'source': rollout}),
        ('gone', usage | {'source': '/h/elsewhere/x.jsonl'}),
        ('notes', usage | {'source': rollout}),
    ]
    harness = {
        'mine': {'executable': 'mine', 'headless': [], 'usage': {'file': '/h/.mine/*.jsonl', 'tally': 'codex'}},
        'notes': {'executable': 'notes', 'headless': [], 'usage': {'file': '/h/rollout-*.jsonl', 'tally': 'pi'}},
    }
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute(
            "INSERT INTO projects (name, path, config) VALUES ('p', '/p', ?)", (json.dumps({'harness': harness}),)
        )
        for name, row in rows:
            con.execute(
                "INSERT INTO workers (kind, project_id, harness, usage) VALUES ('worker', 1, ?, ?)",
                (name, json.dumps(row)),
            )
        con.execute("INSERT INTO workers (kind, project_id) VALUES ('worker', 1)")
    inputs = "SELECT json_extract(usage, '$.input'), json_extract(usage, '$.total') FROM workers ORDER BY id"
    command.upgrade(cfg, '0012')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert [i for i, _ in con.execute(inputs)] == [40, 100, 40, 40, 100, 100, None], (
            'by the tally of the harness in the stored config or built in, else by a rollout file'
        )
        assert {total for _, total in con.execute(inputs)} == {105, None}, 'total stays'
    command.downgrade(cfg, '0011')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert [i for i, _ in con.execute(inputs)] == [100, 100, 100, 100, 100, 100, None]


def test_0014_adds_the_pause_of_a_project_which_needs_its_reason_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0013')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
    command.upgrade(cfg, '0014')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT paused, paused_reason FROM projects').fetchall() == [(None, None)]
        with pytest.raises(sqlite3.IntegrityError, match='projects.paused and projects.paused_reason go together'):
            con.execute("UPDATE projects SET paused = 'all'")
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: pause'):
            con.execute("UPDATE projects SET paused = 'forever', paused_reason = 'r'")
        con.execute("UPDATE projects SET paused = 'launches', paused_reason = 'evaluator v2'")
        assert con.execute("SELECT count(*) FROM sqlite_master WHERE name = 'workers_kind'").fetchone() == (1,)
    command.downgrade(cfg, '0013')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        columns = [c for _, c, *_ in con.execute('PRAGMA table_info(projects)')]
        assert 'paused' not in columns and 'paused_reason' not in columns
        assert con.execute('SELECT name FROM projects').fetchall() == [('p',)]


def test_0015_makes_every_project_a_root_optimization_campaign_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0014')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}'), ('q', '/q', '{}')")
        con.execute("INSERT INTO workers (kind, project_id) VALUES ('worker', 1)")
    command.upgrade(cfg, '0015')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute('PRAGMA foreign_keys = ON')
        assert con.execute('SELECT type, parent_id FROM projects').fetchall() == [('optimization', None)] * 2
        con.execute('UPDATE projects SET parent_id = 1 WHERE id = 2')
        with pytest.raises(sqlite3.IntegrityError, match='a project cannot be under itself'):
            con.execute('UPDATE projects SET parent_id = 2 WHERE id = 1')
        con.execute("INSERT INTO workers (kind, project_id) VALUES ('worker', 2)")
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == sorted(t.name for t in PERSISTENT), 'projects was not rebuilt: workers_kind still reads it'
        for t in PERSISTENT:
            sql = con.execute('SELECT sql FROM sqlite_master WHERE name = ?', (t.name,)).fetchone()[0]
            assert sql + ';' == t.sql(), f'{t.name}: the migration installs what the package renders'
    command.downgrade(cfg, '0014')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        columns = [c for (_, c, *_) in con.execute('PRAGMA table_info(projects)')]
        assert 'type' not in columns and 'parent_id' not in columns
        assert con.execute('SELECT count(*) FROM projects').fetchone() == (2,)
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == sorted(t.name for t in PERSISTENT if t.name not in SINCE_0015)
