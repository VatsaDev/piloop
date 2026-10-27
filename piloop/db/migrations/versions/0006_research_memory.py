"""Research memory: the ``lesson`` comment kind, ``projects.repo``, ``documents`` with ``documents_fts``, ``journal``.

``lesson``: the ``kind`` CHECK of ``comments`` admits it, and two more CHECKs make a lesson a comment on its project
whose first line is ``family: <family> | status: dead|promising|untested | best: …`` (``models.lesson_checks``, frozen
here). Batch mode rebuilds the table. It keeps the row ids, so ``comments_fts`` stays valid, and drops the table's
triggers, so ``recreate`` puts them back.

``projects.repo``: the repository that a project works on, the same for every clone (``identity``). It is
filled from each project's checkout as it is now.

``documents``: text documents for agents to search, with its full-text index, empty at first. ``journal``: the
orchestrator's one-line journal.

The downgrade turns every lesson into a ``finding`` (the older code has no lessons; the text stays). It drops
``journal``, ``documents`` with its index, and ``projects.repo``. It removes the ``memory`` key and the
``wake_budget`` key of every role from stored configs, because the older code's ``extra='forbid'`` would refuse them.

Revision ID: 0006
Revises: 0005
"""

import json
import re
from collections.abc import Sequence
from pathlib import Path

import sqlalchemy as sa
from alembic import op
from git import Repo
from git.exc import InvalidGitRepositoryError, NoSuchPathError

import piloop.db.models
from piloop.db.triggers import recreate
from piloop.db.triggers.fts import fts_table

revision: str = '0006'
down_revision: str | None = '0005'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
BEFORE = ('note', 'finding', 'decision', 'escalation')
AFTER = (*BEFORE, 'lesson')
FIRST_LINE = 'substr(body, 1, instr(body || char(10), char(10)) - 1)'
LESSON_CHECKS = {
    'a lesson is on its project': "kind != 'lesson' OR target_kind = 'project'",
    'a lesson starts family: <family> | status: dead|promising|untested | best: <+N metric> (<evaluator/eval>)': (
        "kind != 'lesson' OR ("
        + ' OR '.join(
            f"{FIRST_LINE} GLOB 'family: ?* | status: {status} | best: ?*'"
            for status in ('dead', 'promising', 'untested')
        )
        + ')'
    ),
}
TEXT = ('path', 'title', 'body', 'created_by')
INDEX = 'documents_fts'


def _timestamp(name: str) -> sa.Column[object]:
    return sa.Column(
        name, piloop.db.models.UTCDateTime(), server_default=sa.text('(CURRENT_TIMESTAMP)'), nullable=False
    )


def identity(path: Path) -> str:
    """The URL of the git checkout's ``origin`` remote (else of its first remote) as ``host/owner/name``, else its path.

    Frozen here, as of this revision, when every project was a git checkout.
    """
    try:
        with Repo(path) as repo:
            urls = {remote.name: remote.url for remote in repo.remotes}
    except InvalidGitRepositoryError, NoSuchPathError:
        return str(path.resolve())
    name = 'origin' if 'origin' in urls else min(urls, default=None)
    if name is None:
        return str(path.resolve())
    url = urls[name].strip().rstrip('/').removesuffix('.git')
    url = re.sub(r'^[a-z+]+://', '', url)
    url = re.sub(r'^[^@/]+@', '', url)
    return re.sub(r'^([^/:]+):(?!\d+/)', r'\1/', url)


def upgrade() -> None:
    with op.batch_alter_table('comments', schema=None, recreate='always') as batch_op:
        batch_op.drop_constraint('commentkind', type_='check')
        batch_op.create_check_constraint('commentkind', _kinds(AFTER))
        for name, condition in LESSON_CHECKS.items():
            batch_op.create_check_constraint(name, condition)
    recreate(op, 'comments')
    op.add_column('projects', sa.Column('repo', sa.String(), nullable=True))
    con = op.get_bind()
    for project_id, path in con.execute(sa.text('SELECT id, path FROM projects')).all():
        con.execute(
            sa.text('UPDATE projects SET repo = :repo WHERE id = :id'),
            {'repo': identity(Path(path)), 'id': project_id},
        )
    op.create_table(
        'documents',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('project_id', sa.Integer(), nullable=False),
        sa.Column('path', sa.String(), nullable=False),
        sa.Column('hash', sa.String(), nullable=False),
        sa.Column('title', sa.String(), nullable=False),
        sa.Column('body', sa.String(), nullable=False),
        sa.Column('commit_sha', sa.String(), nullable=True),
        sa.Column('created_by', sa.String(), nullable=False),
        _timestamp('created_at'),
        _timestamp('updated_at'),
        sa.Column('updated_by', sa.String(), nullable=True),
        sa.CheckConstraint(
            "length(hash) = 64 AND hash NOT GLOB '*[^0-9a-f]*'", name='documents.hash is the blake3 hex digest of body'
        ),
        *(sa.CheckConstraint(f"typeof({c}) IN ('text')", name=f'documents.{c} must be a text value') for c in TEXT),
        sa.CheckConstraint("typeof(commit_sha) IN ('text', 'null')", name='documents.commit_sha must be a text value'),
        sa.ForeignKeyConstraint(['project_id'], ['projects.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('project_id', 'path', 'hash'),
    )
    op.execute(fts_table('documents'))
    recreate(op, 'documents')
    op.create_table(
        'journal',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('author', sa.String(), nullable=False),
        sa.Column('line', sa.String(), nullable=False),
        _timestamp('created_at'),
        sa.CheckConstraint(
            "typeof(line) IN ('text') AND length(CAST(line AS BLOB)) BETWEEN 1 AND 280",
            name='journal.line is text of 1 to 280 bytes',
        ),
        sa.CheckConstraint("typeof(author) IN ('text')", name='journal.author must be a text value'),
        sa.PrimaryKeyConstraint('id'),
    )


def downgrade() -> None:
    op.drop_table('journal')
    op.drop_table('documents')
    op.execute(f'DROP TABLE {INDEX}')
    op.execute('ALTER TABLE projects DROP COLUMN repo')
    op.execute("UPDATE projects SET config = json_remove(config, '$.memory')")
    con = op.get_bind()
    for project_id, config in con.execute(sa.text('SELECT id, config FROM projects')).all():
        stored = json.loads(config)
        for table in stored.get('roles', {}).values():  # every role, built in or not, has the key
            table.pop('wake_budget', None)
        con.execute(
            sa.text('UPDATE projects SET config = :config WHERE id = :id'),
            {'config': json.dumps(stored), 'id': project_id},
        )
    op.execute("UPDATE comments SET kind = 'finding' WHERE kind = 'lesson'")
    with op.batch_alter_table('comments', schema=None, recreate='always') as batch_op:
        for name in LESSON_CHECKS:
            batch_op.drop_constraint(name, type_='check')
        batch_op.drop_constraint('commentkind', type_='check')
        batch_op.create_check_constraint('commentkind', _kinds(BEFORE))
    recreate(op, 'comments')


def _kinds(kinds: tuple[str, ...]) -> str:
    return f'kind IN ({", ".join(f"{k!r}" for k in kinds)})'
