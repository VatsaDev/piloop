"""``evaluators.runner``: how piloop runs an evaluator, ``process`` for every evaluator.

Every existing evaluator ran as a process, its evaluate command in a clean worktree, so every row gets ``process``.
The column holds a name with no fixed list: the installed evaluator runners decide what it may be.

Revision ID: 0017
Revises: 0016
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0017'
down_revision: str | None = '0016'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE evaluators ADD COLUMN runner VARCHAR DEFAULT 'process' NOT NULL")


def downgrade() -> None:
    op.execute('ALTER TABLE evaluators DROP COLUMN runner')
