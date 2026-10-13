"""Research memory: the documents a project's ``[memory]`` globs name are synced once per content, and searchable."""

import os
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from git import Repo
from sqlalchemy import select

from piloop.campaign.memory import sync_documents, title_of
from piloop.config import load_project_config
from piloop.db import sql
from piloop.db.models import Document, Project
from piloop.db.session import Database
from piloop.git import identity

REPORT = '# Sync acquisition report\n\nThe clock recovery gained +0.4 gen_frame_correct; candidate#12.\n'


def sync(db: Database, project: Project, *globs: str, config_from: Path | None = None) -> str:
    cfg = load_project_config(config_from or Path(project.path), memory={'documents': list(globs)})
    with db.session() as s:
        line = sync_documents(s, s.get_one(Project, project.id), cfg, 'human').line()
        s.commit()
    return line


def documents(db: Database) -> list[tuple[str, str, bool, str]]:
    with db.session() as s:
        return [(d.path, d.title, d.commit_sha is not None, d.created_by) for d in s.scalars(select(Document))]


def test_a_sync_inserts_each_content_once_and_an_edit_as_a_new_version(db: Database, project: Project) -> None:
    repo = Repo(project.path)
    evidence = Path(project.path, 'docs', 'evidence', 'sync-r1')
    evidence.mkdir(parents=True)
    (evidence / 'report.md').write_text(REPORT)
    (evidence / 'notes.md').write_text('no heading, just notes\n')
    repo.git.add('docs')
    repo.git.commit('-qm', 'reports')
    (evidence / 'draft.md').write_text('# Draft\n')  # untracked: no commit
    (evidence / 'data.bin').write_bytes(b'\xff\xfe')

    assert sync(db, project, 'docs/evidence/**/*.md', 'docs/evidence/sync-r1/report.md') == (
        'documents: 3 added, 0 unchanged'
    ), 'a file two globs match is synced once'
    assert documents(db) == [
        ('docs/evidence/sync-r1/draft.md', 'Draft', False, 'human'),
        ('docs/evidence/sync-r1/notes.md', 'notes', True, 'human'),
        ('docs/evidence/sync-r1/report.md', 'Sync acquisition report', True, 'human'),
    ]
    assert sync(db, project, 'docs/evidence/**/*.md') == 'documents: 0 added, 3 unchanged', 'a re-sync is a no-op'

    (evidence / 'report.md').write_text(REPORT + '\nAddendum: the onset gate is dead.\n')
    assert sync(db, project, 'docs/**/*.md') == 'documents: 1 added, 2 unchanged'
    with db.session() as s:
        report = s.scalars(select(Document).where(Document.path.endswith('report.md')).order_by(Document.id)).all()
        assert [d.commit_sha is not None for d in report] == [True, False], 'the edit is a new row, dirty: no commit'
        assert report[0].commit_sha == repo.head.commit.hexsha

    assert sync(db, project, 'docs/**/*.bin') == (
        'documents: 0 added, 0 unchanged; skipped docs/evidence/sync-r1/data.bin: not UTF-8 text (invalid start byte at'
        ' byte 0)'
    )


@pytest.mark.skipif(os.geteuid() == 0, reason='root reads a file whatever its mode')
def test_unreadable_files_files_outside_the_repo_and_a_repo_without_git_are_skipped_or_uncommitted(
    db: Database, project: Project, tmp_path: Path
) -> None:
    outside = tmp_path / 'secret.md'
    outside.write_text('# Not the repo\n')
    Path(project.path, 'link.md').symlink_to(outside)
    locked = Path(project.path, 'locked.md')
    locked.write_text('# Locked\n')
    locked.chmod(0)
    try:
        assert sync(db, project, '*.md') == (
            'documents: 1 added, 0 unchanged; skipped link.md: resolves outside the repo;'
            ' skipped locked.md: unreadable (Permission denied)'
        ), 'brief.md is the one file read'
    finally:
        locked.chmod(0o600)

    plain = tmp_path / 'not-a-repo'
    plain.mkdir()
    (plain / 'a.md').write_text('a\n')
    config_from = Path(project.path)
    with db.session() as s:
        s.get_one(Project, project.id).path = str(plain)
        s.commit()
    project.path = str(plain)
    assert sync(db, project, '*.md', config_from=config_from) == 'documents: 1 added, 0 unchanged'
    assert documents(db)[-1] == ('a.md', 'a', False, 'human')


def test_an_agent_inserts_a_document_with_the_blake3_function_and_searches_it(db: Database, tmp_path: Path) -> None:
    with closing(sql.connect(tmp_path / 'piloop.db', actor='research#4')) as con:
        sql.run(con, "INSERT INTO projects (name, path, config) VALUES ('p', '/p', '{}')")
        sql.run(
            con,
            'INSERT INTO documents (project_id, path, title, body, hash, created_by) SELECT 1, '
            "'workspace/research/losses.md', 'Losses', body, blake3(body), 'research#4' FROM (SELECT 'The onset gate"
            " loses 31 frames' AS body)",
        )
        assert sql.run(con, "SELECT blake3('x') = blake3('x'), blake3(NULL)").rows == [(1, None)]
        hits = sql.run(
            con,
            "SELECT d.id, snippet(documents_fts, 1, '[', ']', '…', 4) FROM documents_fts"
            " JOIN documents d ON d.id = documents_fts.rowid WHERE documents_fts MATCH 'gates'",
        )
        assert hits.rows == [(1, 'The onset [gate] loses…')]
        with pytest.raises(sqlite3.IntegrityError, match='UNIQUE constraint failed: documents.project_id'):
            sql.run(
                con,
                'INSERT INTO documents (project_id, path, title, body, hash, created_by) SELECT 1, '
                "'workspace/research/losses.md', 'Losses again', body, blake3(body), 'research#5' FROM (SELECT 'The"
                " onset gate loses 31 frames' AS body)",
            )
        with pytest.raises(sqlite3.IntegrityError, match='documents.hash is the blake3 hex digest of body'):
            sql.run(
                con,
                "INSERT INTO documents (project_id, path, title, body, hash, created_by) VALUES (1, 'x', 'x', 'x',"
                " 'not a hash', 'me')",
            )
        with pytest.raises(sqlite3.OperationalError, match='user-defined function raised exception'):
            sql.run(con, 'SELECT blake3(1)')
        with pytest.raises(sqlite3.DatabaseError, match='not authorized'):
            sql.run(con, "INSERT INTO documents_fts (documents_fts) VALUES ('rebuild')")


def test_a_title_is_the_first_heading_else_the_file_name() -> None:
    assert title_of('intro\n## Clock recovery ##\n# Later\n', 'a/b.md') == 'Clock recovery'
    assert title_of('no heading\n#hashtag\n', 'a/onset-notes.md') == 'onset-notes'


@pytest.mark.parametrize(
    ('url', 'expected'),
    [
        ('git@github.com:hiloopai/mudbench.git', 'github.com/hiloopai/mudbench'),
        ('https://github.com/hiloopai/mudbench', 'github.com/hiloopai/mudbench'),
        ('https://user@github.com/hiloopai/mudbench.git/', 'github.com/hiloopai/mudbench'),
        ('ssh://git@example.com:2222/team/repo.git', 'example.com:2222/team/repo'),
    ],
)
def test_a_repo_is_identified_by_its_remote_whatever_the_clone(tmp_path: Path, url: str, expected: str) -> None:
    repo = Repo.init(tmp_path / 'clone')
    repo.create_remote('upstream', 'https://example.com/other/repo')
    repo.create_remote('origin', url)
    assert identity(tmp_path / 'clone') == expected, 'origin wins over the other remotes'


def test_without_origin_the_first_remote_names_the_repo_and_without_any_the_path(tmp_path: Path) -> None:
    repo = Repo.init(tmp_path / 'clone')
    assert identity(tmp_path / 'clone') == str(tmp_path / 'clone')
    repo.create_remote('zeta', 'https://example.com/z/repo')
    repo.create_remote('hiloop', 'git@github.com:hiloopai/mudbench.git')
    assert identity(tmp_path / 'clone') == 'github.com/hiloopai/mudbench'
    assert identity(tmp_path / 'nowhere') == str(tmp_path / 'nowhere')
