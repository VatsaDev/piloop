"""An ask is pending until a ``request.answered`` event names it, and ``ix_events_kind`` finds the asks of a kind.

The asks of 0.2 have no answer. Each latest ask that the 0.2 rule had spent gets one, so that the upgrade acts on
nothing twice:

- a stop, when its campaign was set active again after it: the stop that the ask made is over;
- a remeasure, when a measurement under the campaign's current evaluator came after it;
- a rejudge, when a ``candidate.judged`` or ``candidate.rejudge_failed`` event came after it.

The other asks stay pending, and the daemon answers them on its next tick as 0.2 would have spent them: a stop of a
stopped campaign, and a cancel whose agents ended.

Revision ID: 0046
Revises: 0045
"""

from collections.abc import Sequence

from alembic import op

revision: str = '0046'
down_revision: str | None = '0045'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

LATER = 'SELECT 1 FROM events l WHERE l.target_kind = e.target_kind AND l.target_id = e.target_id AND l.id > e.id'
SPENT = {
    'campaign.stop_requested': f"{LATER} AND l.kind = 'campaign.status' AND json_extract(l.payload, '$.new') = 'active'",
    'candidate.remeasure_requested': """
        SELECT 1 FROM measurements m JOIN candidates c ON c.id = m.candidate_id JOIN campaigns p ON p.id = c.campaign_id
        WHERE c.id = e.target_id AND m.evaluator_id = p.evaluator_id AND m.created_at >= e.at
    """,
    'candidate.rejudge_requested': f"{LATER} AND l.kind IN ('candidate.judged', 'candidate.rejudge_failed')",
}


def upgrade() -> None:
    op.create_index('ix_events_kind', 'events', ['kind', 'target_kind', 'target_id', 'id'])
    for kind, spent in SPENT.items():
        op.execute(
            'INSERT INTO events (actor, target_kind, target_id, payload)'
            " SELECT 'daemon', e.target_kind, e.target_id, json_object('kind', 'request.answered', 'ask', e.id)"
            f" FROM events e WHERE e.kind = '{kind}' AND EXISTS ({spent}) AND NOT EXISTS ("
            '  SELECT 1 FROM events n WHERE n.kind = e.kind AND n.target_kind = e.target_kind'
            '  AND n.target_id = e.target_id AND n.id > e.id'
            ') ORDER BY e.id'
        )


def downgrade() -> None:
    op.execute("DELETE FROM events WHERE kind = 'request.answered'")
    op.drop_index('ix_events_kind', 'events')
