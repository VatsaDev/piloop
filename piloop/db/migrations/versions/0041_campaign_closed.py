"""A stopped campaign is closed (``campaign.closed``) once its closing sweeps are requested.

The daemon now requests them when the runs that the stop wound down are judged, and records the close. The campaigns
that an older daemon stopped had their closing sweeps at the stop, so the upgrade closes each stopped campaign. The
downgrade deletes every close.

Revision ID: 0041
Revises: 0040
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0041'
down_revision: str | None = '0040'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        'INSERT INTO events (actor, target_kind, target_id, payload)'
        """ SELECT 'daemon', 'campaign', id, '{"kind": "campaign.closed"}' FROM campaigns WHERE status = 'stopped'"""
    )


def downgrade() -> None:
    op.execute("DELETE FROM events WHERE kind = 'campaign.closed'")
