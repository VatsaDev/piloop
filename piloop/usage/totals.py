"""The tokens that agents spent, summed from ``agent_sessions.usage``."""

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from piloop.db.models import AgentSession
from piloop.ids import CampaignId


def campaign_tokens(s: Session, campaign_id: CampaignId) -> int:
    """The tokens that all agents of the campaign spent: ``SUM(json_extract(usage, '$.total'))`` over its agent sessions."""
    total = s.scalar(
        select(func.sum(AgentSession.usage['total'].as_integer())).where(AgentSession.campaign_id == campaign_id)
    )
    return int(total or 0)
