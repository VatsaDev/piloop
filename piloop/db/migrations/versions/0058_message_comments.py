"""The ``message`` comment kind: someone's word to the orchestrator on a campaign.

The ``kind`` CHECK of ``comments`` admits it. Batch mode rebuilds the table. It keeps the row ids, so ``comments_fts``
stays valid, and drops the table's triggers, so ``recreate`` puts them back.

The downgrade turns every message into a ``note`` (the older code has no messages; the text stays).

Revision ID: 0058
Revises: 0057
"""

from collections.abc import Sequence

from alembic import op

from piloop.db.triggers import recreate

revision: str = '0058'
down_revision: str | None = '0057'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
KINDS = ('note', 'finding', 'decision', 'escalation')


def upgrade() -> None:
    _rebuild((*KINDS, 'message'))


def downgrade() -> None:
    op.execute("UPDATE comments SET kind = 'note' WHERE kind = 'message'")
    _rebuild(KINDS)


def _rebuild(kinds: Sequence[str]) -> None:
    with op.batch_alter_table('comments', schema=None, recreate='always') as batch_op:
        batch_op.drop_constraint('commentkind', type_='check')
        batch_op.create_check_constraint('commentkind', f'kind IN ({", ".join(repr(k) for k in kinds)})')
    recreate(op, 'comments')
