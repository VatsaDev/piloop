"""The import of a predecessor's database: every row with its id, nothing logged twice, the source only read."""

import hashlib
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from alembic import command
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine

from piloop.db.legacy import ACKED_BY, NotImportable, import_database
from piloop.db.session import MIGRATIONS, migrate, revision
from tests.db.test_migrations import alembic

ROWS = (
    "INSERT INTO projects (id, name, path, config, status) VALUES (3, 'p', '/p', '{}', 'active')",
    "INSERT INTO projects (id, name, path, config, status, stopped_reason) VALUES (5, 'q', '/q', '{}', 'stopped', 'x')",
    'INSERT INTO ideas (id, project_id, title, hypothesis, track, created_by) '
    "VALUES (7, 3, 'Soft voting', 'votes help', 'score', 'research#2')",
    "INSERT INTO candidates (id, project_id, tree_sha, commit_sha) VALUES (23, 3, 't', 'c')",
    'INSERT INTO runs (id, idea_id, attempt, base_candidate_id, harness, model, status) '
    "VALUES (11, 7, 1, 23, 'pi', 'm', 'succeeded')",
    "INSERT INTO workers (id, kind, project_id, state) VALUES (13, 'worker', 3, 'exited')",
    'INSERT INTO comments (id, target_kind, target_id, author, kind, body, priority) '
    "VALUES (17, 'project', 3, 'w', 'escalation', 'stuck on voting', 1)",
    'INSERT INTO comments (id, target_kind, target_id, author, kind, body) '
    "VALUES (19, 'idea', 7, 'w', 'decision', 'drop the hard vote')",
    'INSERT INTO comments (id, target_kind, target_id, author, kind, body, priority, acked_at, acked_by) '
    "VALUES (21, 'project', 5, 'w', 'finding', 'seen long ago', 3, '2026-01-02 03:04:05', 'human')",
)


def predecessor(path: Path, *extra: str) -> Path:
    """A database with the predecessor's last schema, which is 0001's, and a few rows, as its daemon left them."""
    command.upgrade(alembic(path), '0001')
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        for statement in (*ROWS, *extra):
            con.execute(statement)
    return path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_import_copies_every_row_with_its_id_logs_nothing_twice_and_migrates_to_head(tmp_path: Path) -> None:
    source = predecessor(tmp_path / 'old.db')
    before = digest(source)
    with closing(sqlite3.connect(source)) as con:
        events = con.execute('SELECT id, kind, payload FROM events ORDER BY id').fetchall()
    assert [kind for _, kind, _ in events] == ['comment.escalation'], 'the source logged its escalation once'
    target = tmp_path / 'home' / 'piloop.db'
    target.parent.mkdir()

    imported = import_database(source, target)

    assert digest(source) == before, 'the source is only read'
    copied = imported.copied
    assert copied['projects'] == 2 and copied['comments'] == 3 and copied['events'] == 1 and copied['techniques'] == 0
    assert imported.acked == 1, 'the one open inbox item; the log comment and the acked item are left alone'
    assert sorted(p.name for p in target.parent.iterdir()) == ['piloop.db'], 'no partial file or lock is left'
    engine = create_engine(f'sqlite:///{target}')
    assert revision(engine) == '0001'
    migrate(engine)
    assert revision(engine) == ScriptDirectory(str(MIGRATIONS)).get_current_head()
    engine.dispose()
    with closing(sqlite3.connect(target)) as con:
        assert con.execute('SELECT id, kind, payload FROM events ORDER BY id').fetchall() == events
        assert con.execute('SELECT id, status, stopped_reason FROM projects ORDER BY id').fetchall() == [
            (3, 'stopped', f'imported from {source}'),
            (5, 'stopped', 'x'),
        ], 'an active campaign stops: the source daemon may still run it on the same repo'
        assert con.execute('SELECT id, acked_by, acked_at IS NOT NULL FROM comments ORDER BY id').fetchall() == [
            (17, ACKED_BY, 1),
            (19, None, 0),
            (21, 'human', 1),
        ], 'the old escalation no longer fills the new inbox; who acked an item before keeps it'
        assert con.execute('SELECT acked_at FROM comments WHERE id = 21').fetchone() == ('2026-01-02 03:04:05',)
        assert con.execute("SELECT rowid FROM ideas_fts WHERE ideas_fts MATCH 'vote'").fetchall() == [(7,)]
        assert con.execute("SELECT rowid FROM comments_fts WHERE comments_fts MATCH 'voting'").fetchall() == [
            (17,),
            (19,),
        ]
        con.execute(
            'INSERT INTO comments (target_kind, target_id, author, kind, body, priority) VALUES '
            "('project', 3, 'w', 'escalation', 'again', 1)"
        )
        assert con.execute("SELECT count(*) FROM events WHERE kind = 'comment.escalation'").fetchone() == (2,), (
            'the triggers are back after the copy'
        )


@pytest.mark.parametrize(
    ('extra', 'refusal'),
    [
        (
            (
                'INSERT INTO runs (idea_id, attempt, base_candidate_id, harness, model, status) '
                "VALUES (7, 2, 23, 'pi', 'm', 'running')",
            ),
            'has 1 runs still open: stop its daemon',
        ),
        (
            ("INSERT INTO workers (kind, project_id) VALUES ('research', 3)",),
            'has 1 workers still open: stop its daemon',
        ),
        (('ALTER TABLE events ADD COLUMN extra',), 'table events has columns'),
        (("INSERT INTO candidates (project_id, tree_sha, commit_sha) VALUES (99, 'u', 'd')",), 'missing rows'),
    ],
)
def test_import_refuses_a_source_it_cannot_copy_whole_and_leaves_nothing(
    tmp_path: Path, extra: tuple[str, ...], refusal: str
) -> None:
    source = predecessor(tmp_path / 'old.db', *extra)
    target = tmp_path / 'home' / 'piloop.db'
    target.parent.mkdir()
    with pytest.raises(NotImportable, match=refusal):
        import_database(source, target)
    assert list(target.parent.iterdir()) == []


def test_import_refuses_an_existing_target_a_missing_source_and_a_database_of_something_else(tmp_path: Path) -> None:
    source = predecessor(tmp_path / 'old.db')
    target = tmp_path / 'piloop.db'
    target.write_bytes(b'')
    with pytest.raises(NotImportable, match='exists: import into a new PILOOP_HOME'):
        import_database(source, target)
    target.unlink()
    with pytest.raises(NotImportable, match='nothing.db is not a file'):
        import_database(tmp_path / 'nothing.db', target)
    other = tmp_path / 'other.db'
    with closing(sqlite3.connect(other)) as con:
        con.execute('CREATE TABLE notes (body TEXT)')
    with pytest.raises(NotImportable, match='is not a piloop database'):
        import_database(other, target)
    assert not target.exists()
