"""A job's artifact names its vcs provider and a revision, and the version of an agent's unsaved work is typed.

The artifact of each lease's job names a ``revision``, not a ``commit``, and gets the ``git`` provider (``$.vcs``); a
``kind`` that a job before 0035 kept goes. ``$.commits.message`` becomes the ``summary`` and the ``run`` that
it was made of. ``documents.commit_sha``, which no code read, is dropped; a batch rebuild drops it with its CHECK,
which drops the table's triggers, so ``recreate`` puts them back.

Revision ID: 0037
Revises: 0036
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from piloop.db.triggers import recreate

revision: str = '0037'
down_revision: str | None = '0036'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ARTIFACTS = ('$.artifact', '$.bar.artifact')
GIT = """json('{"provider": "git"}')"""
TRAILER = 'Piloop-Recovered: run#'
"""What followed the summary in a message, after a blank line."""
CHECK = 'documents.commit_sha must be a text value'


def upgrade() -> None:
    for at in ARTIFACTS:
        op.execute(
            f"UPDATE leases SET job = json_set(json_remove(job, '{at}.commit', '{at}.kind'),"
            f" '{at}.revision', json_extract(job, '{at}.commit'), '{at}.vcs', {GIT})"
            f" WHERE json_type(job, '{at}.commit') = 'text'"
        )
    message = "json_extract(job, '$.commits.message')"
    op.execute(
        f"UPDATE leases SET job = json_set(json_remove(job, '$.commits.message'),"
        f" '$.commits.summary', substr({message}, 1, instr({message}, char(10)) - 1),"
        f" '$.commits.run', CAST(substr({message}, instr({message}, 'run#') + 4) AS INTEGER))"
        " WHERE json_type(job, '$.commits.message') = 'text'"
    )
    with op.batch_alter_table('documents', schema=None, recreate='always') as batch_op:
        batch_op.drop_constraint(CHECK, type_='check')
        batch_op.drop_column('commit_sha')
    recreate(op, 'documents')


def downgrade() -> None:
    with op.batch_alter_table('documents', schema=None, recreate='always') as batch_op:
        batch_op.add_column(sa.Column('commit_sha', sa.String(), nullable=True))
        batch_op.create_check_constraint(CHECK, "typeof(commit_sha) IN ('text', 'null')")
    recreate(op, 'documents')
    op.execute(
        "UPDATE leases SET job = json_set(json_remove(job, '$.commits.summary', '$.commits.run'), '$.commits.message',"
        f" json_extract(job, '$.commits.summary') || char(10) || char(10) || '{TRAILER}'"
        " || json_extract(job, '$.commits.run'))"
        " WHERE json_type(job, '$.commits.summary') = 'text'"
    )
    for at in ARTIFACTS:
        op.execute(
            f"UPDATE leases SET job = json_set(json_remove(job, '{at}.revision', '{at}.vcs'),"
            f" '{at}.commit', json_extract(job, '{at}.revision')) WHERE json_type(job, '{at}.revision') = 'text'"
        )
