"""``comments.in_reply_to``: the comment that a reply answers. The target of a reply stays the root of its thread.

The replies written before it keep NULL: no row says which comment of a thread a reply answered. The ``comment.*``
events get the field too, and the events before it keep their payloads. Batch mode rebuilds the table for the foreign
key and the CHECK. It keeps the row ids, so ``comments_fts`` stays valid, and drops the table's triggers, so
``recreate`` puts them back.

Revision ID: 0059
Revises: 0058
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from piloop.db.triggers import recreate

revision: str = '0059'
down_revision: str | None = '0058'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table('comments', schema=None, recreate='always') as batch_op:
        batch_op.add_column(
            sa.Column(
                'in_reply_to',
                sa.Integer(),
                sa.ForeignKey('comments.id', ondelete='SET NULL', name='comments_in_reply_to'),
                nullable=True,
            )
        )
        batch_op.create_check_constraint('reply_is_on_a_comment', "in_reply_to IS NULL OR target_kind = 'comment'")
    recreate(op, 'comments')


def downgrade() -> None:
    with op.batch_alter_table('comments', schema=None, recreate='always') as batch_op:
        batch_op.drop_constraint('reply_is_on_a_comment', type_='check')
        batch_op.drop_column('in_reply_to')
    recreate(op, 'comments')
