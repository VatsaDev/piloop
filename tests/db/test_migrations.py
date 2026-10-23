"""The schema at head: what the migrations create, that models and migrations agree, and that 0001 upgrades to head."""

import base64
import json
import re
import resource
import shutil
import signal
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
from covalence import Cid
from git import Repo
from sqlalchemy import Engine, create_engine, event
from sqlalchemy.exc import IntegrityError

from piloop.actor import System
from piloop.config import CampaignConfig
from piloop.db import session
from piloop.db.models import Campaign, Idea
from piloop.db.session import (
    MIGRATIONS,
    RELEASES,
    BackupFailed,
    Database,
    Release,
    Revision,
    SchemaAhead,
    SchemaTooOld,
    fingerprints,
    migrate,
    revision,
    revisions,
    schema_mismatch,
)
from piloop.db.triggers import PERSISTENT, campaigns, legacy
from piloop.db.triggers.roles import known_role
from piloop.version import Version, installed
from tests.test_deprecated import piloop_version

INBOX = 'ix_comments_inbox'
SINCE_0015 = {t.name for t in campaigns.acyclic()}
"""The persistent triggers that revision 0015 adds: a test of an older revision expects every other one."""


def triggers_at(revision: str) -> list[str]:
    """The persistent triggers of ``revision``, by name: ``PERSISTENT``, in the names of that schema (``legacy``)."""
    names = [t.name for t in PERSISTENT if revision >= '0015' or t.name not in SINCE_0015]
    old = tuple(s for s in legacy.STAGES if revision < s.revision)
    return sorted(legacy.old_name(n, old) for n in names)


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
            'agent_sessions',
            'alembic_version',
            'archives',
            'campaigns',
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
            'lease_workers',
            'leases',
            'lessons',
            'limits',
            'measurements',
            'runs',
            'sqlite_sequence',
            'techniques',
            'tokens',
        ], 'an external-content FTS5 index has no _content shadow: the text stays in the base table only'
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == sorted(t.name for t in PERSISTENT), 'the package lists what the migrations install'
        con.execute("INSERT INTO campaigns (name, path, config) VALUES ('p', '/p', '{}')")
        con.execute("INSERT INTO candidates (campaign_id, tree_sha, commit_sha) VALUES (1, 't', 'c')")
        with pytest.raises(
            sqlite3.IntegrityError, match='UNIQUE constraint failed: candidates.campaign_id, candidates.tree_sha'
        ):
            con.execute("INSERT INTO candidates (campaign_id, tree_sha, commit_sha) VALUES (1, 't', 'other commit')")
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed'):
            con.execute("UPDATE candidates SET verdict = 'maybe' WHERE id = 1")
        assert con.execute('SELECT status, stopped_reason FROM campaigns').fetchall() == [('active', None)]
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed'):
            con.execute("UPDATE campaigns SET status = 'paused' WHERE id = 1")
    command.check(cfg)
    command.downgrade(cfg, 'base')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('PRAGMA user_version').fetchone() == (0,)
        assert (
            con.execute(  # SQLite keeps sqlite_sequence, which AUTOINCREMENT made, and never drops it
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT IN ('alembic_version', 'sqlite_sequence')"
            ).fetchall()
            == []
        )


def schema(path: Path) -> list[tuple[str, ...]]:
    with closing(sqlite3.connect(f'file:{path}?mode=ro', uri=True)) as con:
        return con.execute('SELECT type, name, sql FROM sqlite_master ORDER BY name').fetchall()


def test_a_database_at_a_revision_this_build_does_not_have_is_refused_and_left_as_it_was(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine, backups=0, daemon=None)
    head = ScriptDirectory.from_config(alembic(path)).get_current_head()
    with closing(sqlite3.connect(path)) as con:
        con.execute("UPDATE alembic_version SET version_num = '0006_role_keys'")
        con.commit()
    before = path.read_bytes()
    this = re.escape(installed())
    with pytest.raises(
        SchemaAhead,
        match=rf'^{re.escape(str(path))} was migrated by a different piloop build \(revision 0006_role_keys is not in '
        rf'the history of this {this}, which knows up to {head}\): install the piloop that migrated it \(`piloop '
        r'upgrade` for a newer release\), or set PILOOP_HOME to another directory$',
    ):
        migrate(engine, backups=2, daemon=None)
    assert schema_mismatch(path, None, System.HUMAN) is not None
    assert path.read_bytes() == before and list(tmp_path.glob('piloop.db.bak-*')) == []
    engine.dispose()


TOO_OLD = tuple(Release(Version(0, minor, 0), Revision(f'000{minor}')) for minor in (1, 2, 3))
"""Releases for a piloop 0.4 that migrates from 0.3.0's head, 0003: a database at 0001 is two releases behind it."""


@pytest.mark.parametrize(
    ('stamp', 'origin', 'stops'),
    [
        ('0001', 'piloop 0.1', ('0.2', '0.3')),
        ('0002', 'piloop 0.2', ('0.3',)),
    ],
)
def test_a_database_older_than_the_previous_release_is_refused_and_left_as_it_was(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stamp: str, origin: str, stops: tuple[str, ...]
) -> None:
    """Each release migrates only from the head of the one before it, so a database upgrades one release at a time."""
    monkeypatch.setattr(session, 'RELEASES', TOO_OLD)
    path = tmp_path / 'piloop.db'
    command.upgrade(alembic(path), stamp)
    before = path.read_bytes()
    engine = create_engine(f'sqlite:///{path}')
    this = re.escape(installed())
    steps = ', then '.join(
        rf"install piloop {m} \(`uv tool install 'piloop=={m}\.\*'`\) and run `piloop upgrade`" for m in stops
    )
    with pytest.raises(
        SchemaTooOld,
        match=rf'^{re.escape(str(path))} is from {origin} \(schema revision {stamp}\), and this {this} migrates from'
        rf' piloop 0\.3 \(schema revision 0003\) or newer: upgrade one release at a time: {steps}, then install this'
        r' piloop again and run `piloop upgrade`$',
    ):
        migrate(engine, backups=3, daemon=None)
    engine.dispose()
    assert path.read_bytes() == before, 'not migrated'
    assert sorted(p.name for p in tmp_path.iterdir()) == ['piloop.db', 'piloop.db.migrate.lock'], 'no backup, no WAL'


def test_a_database_at_the_previous_release_or_newer_migrates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(session, 'RELEASES', TOO_OLD)
    path = tmp_path / 'piloop.db'
    command.upgrade(alembic(path), '0003')
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine, backups=0, daemon=None)
    engine.dispose()
    assert revision(path) == session.head()


def test_a_database_from_before_the_first_recorded_release_names_every_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(session, 'RELEASES', TOO_OLD[1:])
    path = tmp_path / 'piloop.db'
    command.upgrade(alembic(path), '0001')
    engine = create_engine(f'sqlite:///{path}')
    with pytest.raises(
        SchemaTooOld,
        match=r' is from a piloop before 0\.2 \(schema revision 0001\), .*: install piloop 0\.2 .*,'
        r' then install piloop 0\.3 ',
    ):
        migrate(engine, backups=0, daemon=None)
    engine.dispose()


def test_the_floor_is_the_head_of_the_previous_minor_release() -> None:
    """``RELEASES`` records each minor release's ``.0`` and its head, oldest first, and its newest is the floor.

    The release of ``X.Y.0`` records ``X.(Y-1).0`` (CONTRIBUTING.md, Release): at the version bump, this fails until it
    does. Until the next bump, the newest may be the version in ``pyproject.toml``, which is then released.
    """
    this = piloop_version()
    versions = [r.version for r in RELEASES]
    assert versions == sorted(versions) and all(v.patch == 0 for v in versions), 'X.Y.0 releases, oldest first'
    chain = revisions()
    assert all(r.head in chain for r in RELEASES), f'every head is a revision of {MIGRATIONS}'
    positions = [chain.index(r.head) for r in RELEASES]
    assert positions == sorted(positions), 'each head is after the head of the release before it'
    previous = Version(this.major, this.minor - 1, 0)
    assert previous <= versions[-1] <= this, (
        f'piloop {this} migrates from the head of piloop {previous}: add {previous} and its head to RELEASES'
    )


def branch(tmp_path: Path) -> AlembicConfig:
    """The migrations of a branch cut at 0005 that adds its own, different 0006."""
    copy = tmp_path / 'branch'

    def after_0005(_dir: str, names: list[str]) -> list[str]:
        return [n for n in names if n == '__pycache__' or (n[:4].isdigit() and n[:4] > '0005')]

    shutil.copytree(MIGRATIONS, copy, ignore=after_0005)
    (copy / 'versions' / '0006_role_keys.py').write_text(
        'from alembic import op\n\n'
        "revision = '0006'\ndown_revision = '0005'\nbranch_labels = None\ndepends_on = None\n\n\n"
        'def upgrade() -> None:\n    op.execute("UPDATE projects SET config = \'{}\'")\n\n\n'
        'def downgrade() -> None:\n    pass\n'
    )
    cfg = alembic(tmp_path / 'piloop.db')
    cfg.set_main_option('script_location', str(copy))
    return cfg


def test_a_database_that_another_branch_migrated_to_a_revision_of_the_same_id_is_refused_and_left_as_it_was(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'piloop.db'
    command.upgrade(branch(tmp_path), 'head')
    engine = create_engine(f'sqlite:///{path}')
    before, tables = path.read_bytes(), schema(path)
    assert revision(path) == '0006'
    refused = r'revision 0006 is not in the history of this .*: this build has a different 0006 or one before it\)'
    with pytest.raises(SchemaAhead, match=refused):
        migrate(engine, backups=2, daemon=None)
    assert re.search(refused, str(schema_mismatch(path, None, System.HUMAN)))
    assert path.read_bytes() == before and schema(path) == tables and list(tmp_path.glob('piloop.db.bak-*')) == []
    engine.dispose()


def test_a_database_that_a_build_before_fingerprints_migrated_is_trusted_at_a_known_revision_and_fingerprinted(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'piloop.db'
    command.upgrade(alembic(path), '0005')
    with closing(sqlite3.connect(path)) as con:
        con.execute('PRAGMA user_version = 0')
    engine = create_engine(f'sqlite:///{path}')
    migrate(engine, backups=0, daemon=None)
    head = list(fingerprints(ScriptDirectory(str(MIGRATIONS))).values())[-1]
    with closing(sqlite3.connect(path)) as con:
        assert con.execute('PRAGMA user_version').fetchone() == (head,)
    engine.dispose()


def test_renaming_a_released_revision_changes_the_fingerprint_that_databases_carry() -> None:
    assert fingerprints(ScriptDirectory(str(MIGRATIONS)))['0018'] == 1283307147


def test_a_migration_first_backs_up_the_database_and_keeps_the_newest_backups(tmp_path: Path) -> None:
    """Before it applies a pending revision, ``migrate`` copies the database next to it, named after the revision it
    is at, and removes the oldest copies beyond ``backups``. Nothing pending, a new database or ``backups=0``: none."""
    path = tmp_path / 'piloop.db'
    engine = create_engine(f'sqlite:///{path}')
    assert migrate(engine, backups=2, daemon=None) is None, 'a new database has nothing to lose'
    command.downgrade(alembic(path), '0016')
    with closing(sqlite3.connect(path)) as con:
        con.execute("INSERT INTO campaigns (name, path, config) VALUES ('kept', '/p', '{}')")
        con.commit()
    for old in ('0001-20250101T000000Z', '0002-20250102T000000Z'):
        (tmp_path / f'piloop.db.bak-{old}').write_bytes(b'an older backup')
    backup = migrate(engine, backups=2, daemon=None)
    assert backup is not None and re.fullmatch(r'piloop\.db\.bak-0016-\d{8}T\d{6}Z', backup.name), backup
    with closing(sqlite3.connect(f'file:{backup}?mode=ro', uri=True)) as con:
        assert con.execute('SELECT version_num FROM alembic_version').fetchall() == [('0016',)]
        assert con.execute('SELECT name FROM campaigns').fetchall() == [('kept',)]
        assert con.execute('PRAGMA integrity_check').fetchone() == ('ok',)
    assert sorted(p.name for p in tmp_path.glob('piloop.db.bak-*')) == [
        'piloop.db.bak-0002-20250102T000000Z',
        backup.name,
    ]
    assert migrate(engine, backups=2, daemon=None) is None, 'at head: nothing to back up'
    command.downgrade(alembic(path), '0016')
    assert migrate(engine, backups=0, daemon=None) is None and len(list(tmp_path.glob('piloop.db.bak-*'))) == 2
    assert revision(path) == ScriptDirectory.from_config(alembic(path)).get_current_head()
    engine.dispose()


def test_a_backup_that_fails_leaves_the_database_unmigrated_and_no_partial_copy(tmp_path: Path) -> None:
    """A full disk, as a limit on the size of the files this process writes: the copy fails, nothing is migrated."""
    path = tmp_path / 'piloop.db'
    command.upgrade(alembic(path), '0016')
    engine = create_engine(f'sqlite:///{path}')
    limit = resource.getrlimit(resource.RLIMIT_FSIZE)
    previous = signal.signal(signal.SIGXFSZ, signal.SIG_IGN)  # a write past the limit fails instead of killing
    resource.setrlimit(resource.RLIMIT_FSIZE, (64 * 1024, limit[1]))
    try:
        with pytest.raises(BackupFailed, match=r'could not back up .*, so it was not migrated: free some space'):
            migrate(engine, backups=3, daemon=None)
    finally:
        resource.setrlimit(resource.RLIMIT_FSIZE, limit)
        signal.signal(signal.SIGXFSZ, previous)
    assert revision(path) == '0016' and list(tmp_path.glob('piloop.db.bak-*')) == []
    engine.dispose()


def test_idea_links_stay_inside_the_campaign_never_point_at_the_idea_itself_and_go_with_either_idea(
    tmp_path: Path,
) -> None:
    """CHECKs refuse a self-link and an unknown kind, the trigger a link into another campaign (or to no idea)."""
    path = tmp_path / 'piloop.db'
    command.upgrade(alembic(path), 'head')
    link = 'INSERT INTO idea_links (idea_id, target_id, kind) VALUES '
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute('PRAGMA foreign_keys=ON')
        con.executescript(
            "INSERT INTO campaigns (name, path, config) VALUES ('p', '/p', '{}'), ('q', '/q', '{}');"
            'INSERT INTO ideas (campaign_id, title, hypothesis, created_by, track)'
            " VALUES (1, 'first', 'h', 't', 'a'), (2, 'other', 'h', 't', 'a'), (1, 'then', 'h', 't', 'a');"
        )
        refused = {
            "(1, 1, 'after')": 'CHECK constraint failed: idea_links_not_self',
            "(1, 3, 'blocks')": 'CHECK constraint failed: linkkind',
            "(1, 2, 'after')": 'idea_links: idea_id and target_id must be ideas of the same campaign',
            "(1, 99, 'supersedes')": 'idea_links: idea_id and target_id must be ideas of the same campaign',
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
            "INSERT INTO ideas (campaign_id, title, hypothesis, created_by, track) VALUES (1, 'again', 'h', 't', 'a')"
        )
        con.execute(link + "(4, 3, 'after')")
        con.execute('DELETE FROM ideas WHERE id = 4')
        assert con.execute('SELECT count(*) FROM idea_links').fetchone() == (0,), 'and with the idea'


def test_idea_titles_are_unique_per_campaign(db: Database) -> None:
    """Concurrent research sweeps that land on the same title collide at insert; another campaign may reuse it."""
    with db.session() as s:
        s.add(Campaign(name='a', path='/a', config={}))
        s.add(Campaign(name='b', path='/b', config={}))
        s.commit()
        s.add(Idea(track='score', campaign_id=1, title='target encoding', hypothesis='x', created_by='research#1'))
        s.add(Idea(track='score', campaign_id=2, title='target encoding', hypothesis='x', created_by='research#2'))
        s.commit()
        s.add(Idea(track='score', campaign_id=1, title='target encoding', hypothesis='y', created_by='research#3'))
        with pytest.raises(IntegrityError, match='UNIQUE constraint failed: ideas.campaign_id, ideas.title'):
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
    first = threading.Thread(target=migrate, args=(engines[0],), kwargs={'backups': 0, 'daemon': None}, name='first')
    second = threading.Thread(target=migrate, args=(engines[1],), kwargs={'backups': 0, 'daemon': None}, name='second')
    first.start()
    assert inside.wait(5)
    second.start()
    time.sleep(0.3)
    assert entered == ['first'], 'the second migrate entered Alembic while the first was still upgrading'
    release.set()
    first.join(5)
    second.join(5)
    head = ScriptDirectory.from_config(alembic(path)).get_current_head()
    assert entered == ['first', 'second'] and revision(path) == head
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
        assert triggers == triggers_at('0010'), 'the rebuilt runs table has its triggers back'
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
        p = CampaignConfig.model_validate(stored[0])
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
        assert sql + ';' == legacy.rendered(known_role(), legacy.STAGES)[3], (
            'the trigger of today, in the names of then'
        )
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == triggers_at('0011')
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
        with pytest.raises(sqlite3.IntegrityError, match='cannot be under itself'):
            con.execute('UPDATE projects SET parent_id = 2 WHERE id = 1')
        con.execute("INSERT INTO workers (kind, project_id) VALUES ('worker', 2)")
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == triggers_at('0015'), 'projects was not rebuilt: workers_kind still reads it'
    command.downgrade(cfg, '0014')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        columns = [c for (_, c, *_) in con.execute('PRAGMA table_info(projects)')]
        assert 'type' not in columns and 'parent_id' not in columns
        assert con.execute('SELECT count(*) FROM projects').fetchone() == (2,)
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == triggers_at('0014')


TAKES_0015 = 'project_id IN (SELECT "Project_Id" FROM evaluators) AND title NOT LIKE \'%projects%\' -- projects: kept'
"""A role's ``takes`` at revision 0015: identifiers to rename (one quoted, in another case), a literal and a comment."""
WHEN_0015 = "SELECT 1 FROM projects WHERE id = :project AND json_extract(config, '$.evaluator_id') IS NULL"


def test_0016_renames_projects_campaigns_and_evaluators_evaluators_with_every_row_and_downgrades(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0015')
    roles = {
        'fixer': {'takes': TAKES_0015, 'workdir': 'project', 'triggers': [{'kind': 'seed'}]},
        'research': {
            'workdir': 'head',
            'triggers': [
                {'kind': 'sql', 'when': WHEN_0015},
                {'kind': 'sql', 'when': 'SELECT 1 WHERE @project = $Project'},
            ],
        },
        'watchdog': {},
    }
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute(
            "INSERT INTO projects (name, path, config, evaluator_id) VALUES ('p', '/p', ?, 1), ('q', '/q', '{}', NULL)",
            (json.dumps({'roles': roles}),),
        )
        con.execute('UPDATE projects SET parent_id = 1 WHERE id = 2')
        con.execute("INSERT INTO evaluators (project_id, key, definition) VALUES (1, 'k', '{}')")
        con.execute("INSERT INTO candidates (project_id, tree_sha, commit_sha) VALUES (1, 't', 'c')")
        con.execute("INSERT INTO measurements (candidate_id, evaluator_id, metrics) VALUES (1, 1, '{}')")
        con.execute('INSERT INTO evaluations (candidate_id, evaluator_id) VALUES (1, 1)')
        con.execute("INSERT INTO comments (target_kind, target_id, author, body) VALUES ('project', 1, 'h', 'hi')")
        con.execute(
            'INSERT INTO events (kind, actor, target_kind, target_id, payload) VALUES'
            " ('project.status', 'h', 'project', 1, '{\"project\": 1}'), ('idea.status', 'h', 'idea', 1, '{}')"
        )
    command.upgrade(cfg, '0016')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute('PRAGMA foreign_keys = ON')
        assert con.execute('SELECT id, name, evaluator_id, type, parent_id FROM campaigns').fetchall() == [
            (1, 'p', 1, 'optimization', None),
            (2, 'q', None, 'optimization', 1),
        ]
        assert con.execute('SELECT id, campaign_id, key FROM evaluators').fetchall() == [(1, 1, 'k')]
        assert con.execute('SELECT evaluator_id FROM measurements').fetchall() == [(1,)]
        assert con.execute('SELECT target_kind FROM comments').fetchall() == [('campaign',)]
        assert con.execute('SELECT kind, target_kind, payload FROM events ORDER BY id').fetchall() == [
            ('campaign.status', 'campaign', '{"project": 1}'),
            ('idea.status', 'idea', '{}'),
        ], 'the kinds are renamed; a payload keeps its text'
        with pytest.raises(sqlite3.IntegrityError, match='FOREIGN KEY'):
            con.execute('INSERT INTO measurements (candidate_id, evaluator_id) VALUES (1, 9)')
        with pytest.raises(sqlite3.IntegrityError, match='UNIQUE constraint failed: evaluations.candidate_id, eval'):
            con.execute('INSERT INTO evaluations (candidate_id, evaluator_id) VALUES (1, 1)')
        with pytest.raises(sqlite3.IntegrityError, match='campaigns.paused and campaigns.paused_reason go together'):
            con.execute("UPDATE campaigns SET paused = 'all' WHERE id = 1")
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: targetkind'):
            con.execute("INSERT INTO comments (target_kind, target_id, author, body) VALUES ('project', 1, 'h', 'x')")
        with pytest.raises(sqlite3.IntegrityError, match='cannot be under itself'):
            con.execute('UPDATE campaigns SET parent_id = 2 WHERE id = 1')
        con.execute("INSERT INTO workers (kind, campaign_id) VALUES ('worker', 1)")
        assert con.execute("SELECT count(*) FROM comments_fts WHERE comments_fts MATCH 'hi'").fetchone() == (1,)
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == triggers_at('0016'), 'every trigger of the package, in the names of then'
        stored = json.loads(con.execute('SELECT config FROM campaigns WHERE id = 1').fetchone()[0])['roles']
        assert stored['fixer'] == {
            'takes': 'campaign_id IN (SELECT "campaign_id" FROM evaluators) AND title NOT LIKE \'%projects%\''
            ' -- projects: kept',
            'workdir': 'repo',
            'triggers': [{'kind': 'seed'}],
        }, 'identifiers only: the literal and the comment keep their text'
        assert stored['research']['triggers'][0]['when'] == (
            "SELECT 1 FROM campaigns WHERE id = :campaign AND json_extract(config, '$.evaluator_id') IS NULL"
        )
        assert stored['research']['workdir'] == 'head'
        assert stored['research']['triggers'][1]['when'] == 'SELECT 1 WHERE @campaign = $campaign', 'every prefix'
    command.upgrade(cfg, 'head')
    command.check(cfg)
    command.downgrade(cfg, '0015')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT id, evaluator_id, parent_id FROM projects').fetchall() == [
            (1, 1, None),
            (2, None, 1),
        ]
        assert con.execute('SELECT project_id FROM evaluators').fetchall() == [(1,)]
        assert con.execute('SELECT kind, target_kind FROM events ORDER BY id').fetchall() == [
            ('project.status', 'project'),
            ('idea.status', 'idea'),
        ]
        assert con.execute('SELECT target_kind FROM comments').fetchall() == [('project',)]
        restored = json.loads(con.execute('SELECT config FROM projects WHERE id = 1').fetchone()[0])['roles']
        roles['fixer']['takes'] = TAKES_0015.replace('"Project_Id"', '"project_id"')
        roles['research']['triggers'][1]['when'] = 'SELECT 1 WHERE @project = $project'
        assert restored == roles, 'the old names come back, in lower case'
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == triggers_at('0015')
        indexes = "SELECT name FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL ORDER BY name"
        assert 'ix_ideas_project_status_priority' in [i for (i,) in con.execute(indexes)]


def test_0017_gives_every_evaluator_the_process_runner_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0016')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO campaigns (name, path, config) VALUES ('p', '/p', '{}')")
        con.execute("INSERT INTO evaluators (campaign_id, key, definition) VALUES (1, 'k', '{}')")
    command.upgrade(cfg, '0017')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT runner FROM evaluators').fetchall() == [('process',)]
    command.downgrade(cfg, '0016')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert 'runner' not in [c for (_, c, *_) in con.execute('PRAGMA table_info(evaluators)')]
        assert con.execute('SELECT key FROM evaluators').fetchall() == [('k',)]


TAKES_0017 = 'id IN (SELECT run_id FROM "Workers" WHERE kind = \'worker\') -- workers: kept'
"""A role's ``takes`` at revision 0017: the table to rename (quoted, in another case), a literal and a comment."""


def at_0017(con: sqlite3.Connection) -> None:
    """Fill a database at revision 0017 with a row of every kind that names the worker role or the ``workers`` table."""
    roles = {
        'worker': {'takes': TAKES_0017},
        'research': {'triggers': [{'kind': 'sql', 'when': 'SELECT 1 FROM workers'}]},
    }
    con.execute("INSERT INTO campaigns (name, path, config) VALUES ('p', '/p', ?)", (json.dumps({'roles': roles}),))
    con.execute(
        "INSERT INTO ideas (campaign_id, title, hypothesis, track, created_by) VALUES (1, 't', 'h', 'main', 'worker#1')"
    )
    con.execute("INSERT INTO candidates (campaign_id, tree_sha, commit_sha) VALUES (1, 't', 'c')")
    con.execute(
        'INSERT INTO runs (idea_id, attempt, base_candidate_id, harness, model, role, updated_by) VALUES'
        " (1, 1, 1, 'claude', 'm', 'worker', 'worker#1'), (1, 2, 1, 'claude', 'm', 'critic', 'critic#2')"
    )
    con.execute(
        "INSERT INTO workers (kind, campaign_id, run_id, updated_by) VALUES ('worker', 1, 1, NULL), ('research', 1, NULL, NULL)"
    )
    con.execute(
        "INSERT INTO comments (target_kind, target_id, author, body) VALUES ('worker', 1, 'worker#1', 'a worker')"
    )
    con.execute(
        'INSERT INTO events (kind, actor, target_kind, target_id, payload) VALUES'
        " ('worker.wrap_up', 'daemon', 'worker', 1, '{}'),"
        " ('worker.started', 'daemon', 'worker', 1, '{\"kind\": \"worker\"}'),"
        " ('research.finished', 'daemon', 'worker', 2, '{}'),"
        " ('candidate.verdict', 'worker#1', 'candidate', 1, '{\"verdict_by\": \"worker#1\"}'),"
        " ('run.relaunched', 'daemon', 'run', 1, '{\"worker\": 1, \"launches\": 1}')"
    )


def test_0018_renames_workers_agent_sessions_and_the_worker_role_implementer_with_every_row_and_downgrades(
    tmp_path: Path,
) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0017')
    # a table's constraints come in a different order from each process that makes it, so tables by name only
    schema = "SELECT name, iif(type = 'table', type, sql) FROM sqlite_master ORDER BY name"
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        at_0017(con)
        before = con.execute(schema).fetchall()
    command.upgrade(cfg, '0018')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute('PRAGMA foreign_keys = ON')
        assert con.execute('SELECT id, kind, run_id FROM agent_sessions').fetchall() == [
            (1, 'implementer', 1),
            (2, 'research', None),
        ]
        assert con.execute('SELECT role, updated_by FROM runs ORDER BY id').fetchall() == [
            ('implementer', 'implementer#1'),
            ('critic', 'critic#2'),
        ]
        assert con.execute('SELECT created_by FROM ideas').fetchall() == [('implementer#1',)]
        assert con.execute('SELECT target_kind, author, body FROM comments').fetchall() == [
            ('agent_session', 'implementer#1', 'a worker')
        ], 'a body keeps its text'
        assert con.execute('SELECT kind, actor, target_kind, payload FROM events ORDER BY id').fetchall() == [
            ('agent_session.wrap_up', 'daemon', 'agent_session', '{}'),
            ('implementer.started', 'daemon', 'agent_session', '{"kind": "worker"}'),
            ('research.finished', 'daemon', 'agent_session', '{}'),
            ('candidate.verdict', 'implementer#1', 'candidate', '{"verdict_by": "implementer#1"}'),
            ('run.relaunched', 'daemon', 'run', '{"launches":1,"agent_session":1}'),
        ], 'session events take the target kind, role events and signatures the role, a payload key the row'
        stored = json.loads(con.execute('SELECT config FROM campaigns').fetchone()[0])['roles']
        assert stored == {
            'implementer': {
                'takes': 'id IN (SELECT run_id FROM "agent_sessions" WHERE kind = \'worker\') -- workers: kept'
            },
            'research': {'triggers': [{'kind': 'sql', 'when': 'SELECT 1 FROM agent_sessions'}]},
        }, 'the role table is renamed, and the queries rename identifiers only'
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: targetkind'):
            con.execute("INSERT INTO comments (target_kind, target_id, author, body) VALUES ('worker', 1, 'h', 'x')")
        with pytest.raises(sqlite3.IntegrityError, match='agent_sessions.kind must be a role of the campaign'):
            con.execute("INSERT INTO agent_sessions (kind, campaign_id) VALUES ('worker', 1)")
        con.execute("INSERT INTO agent_sessions (kind, campaign_id) VALUES ('implementer', 1)")
        with pytest.raises(sqlite3.IntegrityError, match='FOREIGN KEY'):
            con.execute("INSERT INTO agent_sessions (kind, campaign_id, run_id) VALUES ('implementer', 1, 9)")
        con.execute("INSERT INTO runs (idea_id, attempt, base_candidate_id, harness, model) VALUES (1, 3, 1, 'c', 'm')")
        assert con.execute('SELECT role FROM runs WHERE id = 3').fetchone() == ('implementer',), 'the new default'
        assert con.execute("SELECT count(*) FROM comments_fts WHERE comments_fts MATCH 'worker'").fetchone() == (1,)
        for t in PERSISTENT:
            sql = con.execute('SELECT sql FROM sqlite_master WHERE name = ?', (t.name,)).fetchone()[0]
            assert sql + ';' == t.sql(), f'{t.name}: the migration installs what the package renders'
        con.execute('DELETE FROM agent_sessions WHERE id = 3')
        con.execute('DELETE FROM runs WHERE id = 3')
    command.upgrade(cfg, 'head')
    command.check(cfg)
    command.downgrade(cfg, '0017')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT kind FROM workers ORDER BY id').fetchall() == [('worker',), ('research',)]
        assert con.execute('SELECT role, updated_by FROM runs ORDER BY id').fetchall() == [
            ('worker', 'worker#1'),
            ('critic', 'critic#2'),
        ]
        assert con.execute('SELECT target_kind, author FROM comments').fetchall() == [('worker', 'worker#1')]
        assert [k for (k,) in con.execute('SELECT kind FROM events ORDER BY id')] == [
            'worker.wrap_up',
            'worker.started',
            'research.finished',
            'candidate.verdict',
            'run.relaunched',
        ]
        payload = "SELECT json_extract(payload, '$.worker') FROM events WHERE kind = 'run.relaunched'"
        assert con.execute(payload).fetchall() == [(1,)]
        assert {t for (t,) in con.execute('SELECT DISTINCT target_kind FROM events')} == {'worker', 'candidate', 'run'}
        restored = json.loads(con.execute('SELECT config FROM campaigns').fetchone()[0])['roles']
        assert restored == {
            'worker': {'takes': 'id IN (SELECT run_id FROM "workers" WHERE kind = \'worker\') -- workers: kept'},
            'research': {'triggers': [{'kind': 'sql', 'when': 'SELECT 1 FROM workers'}]},
        }, 'the old names come back, in lower case'
        assert con.execute(schema).fetchall() == before, 'every table as it was, every index and trigger to the letter'
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == triggers_at('0017')
        con.execute("INSERT INTO workers (kind, campaign_id) VALUES ('worker', 1)")


def test_an_0018_upgrade_that_fails_halfway_leaves_the_database_as_it_was(tmp_path: Path) -> None:
    """0018 renames ``workers`` and rebuilds three tables before a bad ``events`` row fails it: all of it goes back."""
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0017')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        at_0017(con)
        con.execute('PRAGMA ignore_check_constraints = ON')
        con.execute("INSERT INTO events (kind, actor, target_kind, target_id) VALUES ('x', 'h', 'bogus', 1)")
    with pytest.raises(IntegrityError, match='CHECK constraint failed: targetkind'):
        command.upgrade(cfg, 'head')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT version_num FROM alembic_version').fetchall() == [('0017',)]
        tables = {t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        assert 'workers' in tables and not {t for t in tables if t.startswith(('_rebuilt', 'agent_sessions'))}
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == triggers_at('0017')
        assert con.execute('SELECT kind FROM workers ORDER BY id').fetchall() == [('worker',), ('research',)]
        assert con.execute('SELECT role FROM runs ORDER BY id').fetchall() == [('worker',), ('critic',)]
        assert con.execute('SELECT target_kind FROM comments').fetchall() == [('worker',)]
        con.execute("DELETE FROM events WHERE target_kind = 'bogus'")
    command.upgrade(cfg, 'head')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT target_kind FROM comments').fetchall() == [('agent_session',)]


def test_an_upgrade_that_fails_halfway_leaves_the_database_as_it_was(tmp_path: Path) -> None:
    """The whole upgrade is one transaction: 0016 renames and drops a lot before it can fail, and all of it goes back."""
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0015')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        con.execute("INSERT INTO comments (target_kind, target_id, author, body) VALUES ('project', 1, 'h', 'hi')")
        con.execute("INSERT INTO projects (name, path, config, parent_id) VALUES ('q', '/q', '{}', 1)")

    def foreign_keys_on(con: sqlite3.Connection, _record: object) -> None:
        con.execute('PRAGMA foreign_keys = ON')  # then 0016 fails at DROP TABLE campaigns, after the renames

    event.listen(Engine, 'connect', foreign_keys_on)
    try:
        with pytest.raises(IntegrityError, match='FOREIGN KEY constraint failed'):
            command.upgrade(cfg, 'head')
    finally:
        event.remove(Engine, 'connect', foreign_keys_on)
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT version_num FROM alembic_version').fetchall() == [('0015',)]
        tables = {t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        assert {'projects', 'evaluators'} <= tables and not {
            t for t in tables if t.startswith(('_rebuilt', 'campaigns'))
        }
        triggers = [t for (t,) in con.execute("SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name")]
        assert triggers == triggers_at('0015')
        assert con.execute('SELECT target_kind FROM comments').fetchall() == [('project',)]
    command.upgrade(cfg, 'head')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT target_kind FROM comments').fetchall() == [('campaign',)]


def test_0019_adds_the_leases_whose_job_is_of_their_kind_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0019')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO leases (kind, evaluation_id) VALUES ('evaluation', 7)")
        assert con.execute('SELECT id, state, job, receipt FROM leases').fetchall() == [(1, 'queued', '{}', '{}')]
        with pytest.raises(sqlite3.IntegrityError, match='a lease has the job of its kind'):
            con.execute("INSERT INTO leases (kind, evaluation_id) VALUES ('agent', 7)")
        con.execute('DELETE FROM leases')
        con.execute("INSERT INTO leases (kind, evaluation_id) VALUES ('evaluation', 7)")
        assert con.execute('SELECT id FROM leases').fetchall() == [(2,)], 'a fencing token is never used again'
    command.downgrade(cfg, '0018')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute("SELECT name FROM sqlite_master WHERE name = 'leases'").fetchall() == []


def test_0020_makes_each_live_agent_that_the_daemon_ended_ending_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0019')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO campaigns (name, path, config) VALUES ('p', '/p', '{}')")
        for state in ('running', 'running', 'starting', 'killed'):
            con.execute(f"INSERT INTO agent_sessions (kind, campaign_id, state) VALUES ('implementer', 1, '{state}')")
        for agent_session in (1, 3, 4):
            con.execute(
                'INSERT INTO events (kind, actor, target_kind, target_id) VALUES'
                f" ('agent_session.stop_ended', 'daemon', 'agent_session', {agent_session})"
            )
        con.execute("INSERT INTO leases (kind, agent_session_id) VALUES ('agent', 1)")
    command.upgrade(cfg, '0020')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT id, state FROM agent_sessions ORDER BY id').fetchall() == [
            (1, 'ending'),
            (2, 'running'),
            (3, 'ending'),
            (4, 'killed'),
        ], 'only a live row that the daemon ended'
        assert con.execute('SELECT agent_session_id FROM leases').fetchall() == [(1,)], 'the rebuild keeps the leases'
        triggers = "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'agent_sessions'"
        assert {t for (t,) in con.execute(triggers)} == {t.name for t in PERSISTENT if t.table == 'agent_sessions'}
    command.downgrade(cfg, '0019')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT id, state FROM agent_sessions ORDER BY id').fetchall() == [
            (1, 'running'),
            (2, 'running'),
            (3, 'running'),
            (4, 'killed'),
        ]
        with pytest.raises(sqlite3.IntegrityError, match='CHECK constraint failed: agentsessionstate'):
            con.execute("UPDATE agent_sessions SET state = 'ending' WHERE id = 1")


def test_0021_gives_every_finished_run_its_end_from_its_status_and_error_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0020')
    rows = [
        ('queued', None, None),
        ('running', None, None),
        ('succeeded', None, 'finished'),
        ('timed_out', 'agent timeout after 60s', 'timed_out'),
        ('failed', 'daemon stopped', 'daemon_stopped'),
        ('failed', 'daemon exited while the run was in progress', 'stale'),
        ('failed', 'campaign stopped', 'campaign_stopped'),
        ('failed', 'its role is no longer in piloop.toml', 'role_removed'),
        ('failed', 'no agent of the run started to work', 'not_started'),
        ('failed', 'cancelled', 'cancelled'),
        ('failed', 'worker lost', 'worker_lost'),
        ('failed', 'blocked on escalation comment#7', 'awaiting_answer'),
        ('failed', 'implementer produced no commits', 'failed'),
        ('failed', None, 'failed'),
    ]
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO campaigns (name, path, config) VALUES ('p', '/p', '{}')")
        con.execute(
            "INSERT INTO ideas (campaign_id, title, hypothesis, track, created_by) VALUES (1, 'i', 'h', 't', 'h')"
        )
        for attempt, (status, error, _) in enumerate(rows, start=1):
            con.execute(
                'INSERT INTO runs (idea_id, attempt, base_candidate_id, harness, model, status, error)'
                " VALUES (1, ?, 1, 'pi', 'm', ?, ?)",
                (attempt, status, error),
            )
        before = con.execute('SELECT id, updated_at FROM runs ORDER BY id').fetchall()
    command.upgrade(cfg, '0021')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT status, error, "end" FROM runs ORDER BY id').fetchall() == rows, (
            'error keeps its text'
        )
        assert con.execute('SELECT id, updated_at FROM runs ORDER BY id').fetchall() == before, 'no run was updated'
        with pytest.raises(sqlite3.IntegrityError, match='runs.end is set once a run is finished'):
            con.execute("UPDATE runs SET status = 'failed' WHERE id = 1")
        with pytest.raises(sqlite3.IntegrityError, match='runs.status follows from runs.end'):
            con.execute("UPDATE runs SET status = 'succeeded' WHERE id = 5")
        with pytest.raises(sqlite3.IntegrityError, match='runend'):
            con.execute('UPDATE runs SET "end" = \'gone\' WHERE id = 5')
        triggers = "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'runs'"
        assert {t for (t,) in con.execute(triggers)} == {t.name for t in PERSISTENT if t.table == 'runs'}
    command.downgrade(cfg, '0020')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT status, error FROM runs ORDER BY id').fetchall() == [(s, e) for s, e, _ in rows]


def test_0023_adds_the_lease_workers_and_keeps_the_fencing_tokens_of_the_leases(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0022')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO leases (kind, evaluation_id, unreachable_at) VALUES ('evaluation', 7, '2026-01-01')")
        con.execute('DELETE FROM leases')
    command.upgrade(cfg, '0023')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO leases (kind, evaluation_id) VALUES ('evaluation', 7)")
        assert con.execute('SELECT id FROM leases').fetchall() == [(2,)], 'a fencing token is never used again'
        con.execute(
            "INSERT INTO lease_workers (id, pool, version, seen_at) VALUES ('serve', 'serve', '0.2.0', '2026-01-01')"
        )
        assert con.execute('SELECT runners, sample, no_lease FROM lease_workers').fetchall() == [('[]', '{}', None)]
    command.downgrade(cfg, '0022')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute("SELECT name FROM sqlite_master WHERE name = 'lease_workers'").fetchall() == []
        assert con.execute('SELECT unreachable_at FROM leases').fetchall() == [(None,)]


def test_0027_spells_each_archived_manifest_as_its_raw_cid_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / 'piloop.db'
    cfg = alembic(path)
    command.upgrade(cfg, '0026')
    digest = 'xwalomq5cvgoyxketuqee7k7loh6z6ljsj4hnzh5fawqloxwraiq'
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute("INSERT INTO campaigns (name, path, config) VALUES ('p', '/p', '{}')")
        con.execute(f"INSERT INTO archives (campaign_id, manifest) VALUES (1, '{digest}'), (1, NULL)")
    command.upgrade(cfg, '0027')
    cid = Cid.from_parts('raw', 'blake3', base64.b32decode(digest.upper() + '===='))
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT manifest FROM archives ORDER BY id').fetchall() == [(str(cid),), (None,)]
    command.downgrade(cfg, '0026')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        assert con.execute('SELECT manifest FROM archives ORDER BY id').fetchall() == [(digest,), (None,)]
    command.upgrade(cfg, '0027')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute(f"UPDATE archives SET manifest = '{Cid.from_parts('drisl', 'blake3', cid.digest)}' WHERE id = 1")
    with pytest.raises(RuntimeError, match='archive 1 names a DASL manifest .* restore the database backup'):
        command.downgrade(cfg, '0026')
