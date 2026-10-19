"""``projects.paused`` and ``projects.paused_reason``: ``piloop pause`` holds a project's launches, as state.

Before this, a campaign was paused with a ``[spend]`` command that read a file, which escalated each time it held and
also stopped every measurement. A CHECK makes the two columns go together, so a pause always says why.

``ADD COLUMN`` takes the CHECKs as column constraints, so the table is not rebuilt: a rebuild of ``projects`` breaks
the ``workers_kind`` trigger, which reads it.

Revision ID: 0014
Revises: 0013
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0014'
down_revision: str | None = '0013'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
PAUSED = "paused VARCHAR(16) CONSTRAINT pause CHECK (paused IN ('launches', 'all'))"
REASON = (
    'paused_reason VARCHAR'
    ' CONSTRAINT "projects.paused and projects.paused_reason go together"'
    ' CHECK ((paused IS NULL) = (paused_reason IS NULL))'
    ' CONSTRAINT "projects.paused_reason must be a text value"'
    " CHECK (typeof(paused_reason) IN ('text', 'null'))"
)


def upgrade() -> None:
    op.execute(f'ALTER TABLE projects ADD COLUMN {PAUSED}')
    op.execute(f'ALTER TABLE projects ADD COLUMN {REASON}')


def downgrade() -> None:
    # paused_reason first: its CHECK reads paused, and SQLite drops a column's own CHECKs with it
    op.execute('ALTER TABLE projects DROP COLUMN paused_reason')
    op.execute('ALTER TABLE projects DROP COLUMN paused')
