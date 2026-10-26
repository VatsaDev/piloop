"""An archiving is of a subject (``subject_kind``, ``subject_id``), which ``archives.idea_id`` was an idea of.

Each ``idea_id`` becomes the subject ``('idea', idea_id)``, and the index is on the subject. ``anomaly.disk`` events
name the subject (``"idea#12"``) in place of the idea (``12``). The downgrade reverses both, and deletes the archivings
of a subject that is not an idea, which ``idea_id`` cannot hold.

Revision ID: 0002
Revises: 0001
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = '0002'
down_revision: str | None = '0001'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Frozen here, as of this revision.
TARGET_KINDS = ('campaign', 'idea', 'run', 'candidate', 'measurement', 'agent_session', 'comment', 'instance', 'home')
DISK = "kind = 'anomaly.disk'"


def upgrade() -> None:
    op.drop_index('ix_archives_campaign_idea', table_name='archives')
    with op.batch_alter_table('archives', recreate='always') as batch:
        batch.add_column(
            sa.Column(
                'subject_kind',
                sa.Enum(*TARGET_KINDS, name='targetkind', native_enum=False, length=16, create_constraint=True),
                nullable=True,
            )
        )
        batch.add_column(sa.Column('subject_id', sa.Integer(), nullable=True))
    op.execute("UPDATE archives SET subject_kind = 'idea', subject_id = idea_id WHERE idea_id IS NOT NULL")
    with op.batch_alter_table('archives', recreate='always') as batch:
        batch.drop_column('idea_id')
        batch.create_check_constraint('archives_subject', '(subject_kind IS NULL) = (subject_id IS NULL)')
    op.create_index('ix_archives_campaign_subject', 'archives', ['campaign_id', 'subject_kind', 'subject_id'])
    subject = "CASE WHEN json_type(payload, '$.idea') = 'integer' THEN 'idea#' || json_extract(payload, '$.idea') END"
    op.execute(
        f"UPDATE events SET payload = json_remove(json_set(payload, '$.subject', {subject}), '$.idea') WHERE {DISK}"
    )


def downgrade() -> None:
    op.drop_index('ix_archives_campaign_subject', table_name='archives')
    op.execute("DELETE FROM archives WHERE subject_kind != 'idea'")
    with op.batch_alter_table('archives', recreate='always') as batch:
        batch.add_column(sa.Column('idea_id', sa.Integer(), nullable=True))
    op.execute('UPDATE archives SET idea_id = subject_id')
    with op.batch_alter_table('archives', recreate='always') as batch:
        batch.drop_constraint('archives_subject', type_='check')
        batch.drop_constraint('targetkind', type_='check')
        batch.drop_column('subject_id')
        batch.drop_column('subject_kind')
    op.create_index('ix_archives_campaign_idea', 'archives', ['campaign_id', 'idea_id'])
    idea = (
        "CASE WHEN json_extract(payload, '$.subject') LIKE 'idea#%'"
        " THEN CAST(substr(json_extract(payload, '$.subject'), 6) AS INTEGER) END"
    )
    op.execute(
        f"UPDATE events SET payload = json_remove(json_set(payload, '$.idea', {idea}), '$.subject') WHERE {DISK}"
    )
