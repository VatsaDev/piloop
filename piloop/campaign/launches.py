"""Launching the agents that campaigns request: the kernel's share of the campaigns' pass."""

from collections import Counter
from collections.abc import Collection, Sequence
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.agents.refill import usage_of
from piloop.campaign.campaigns import vcs_of
from piloop.config import CampaignConfig
from piloop.db.models import LIVE, AgentSession, AgentSessionState, Campaign, utcnow
from piloop.otel import Tracer
from piloop.plugins.vcs.contract import NotARepository
from piloop.usage import Usage


def free_harness(rotation: Sequence[str], turn: str, full: Collection[str]) -> str | None:
    """Return ``turn``, the rotation's pick, unless it is ``full``. Then return the first harness that is not full."""
    return turn if turn not in full else next((h for h in rotation if h not in full), None)


def remove_checkouts(agent_dir: Path, campaign: Campaign, tracer: Tracer) -> None:
    """Remove the checkouts under a run's or a sweep's directory. If the campaign's repo is gone, record an event."""
    try:
        repo = vcs_of(campaign).open(Path(campaign.path))
    except NotARepository:
        tracer.event('worktrees.orphaned', agent_dir=str(agent_dir), campaign_path=campaign.path)
        return
    for checkout in repo.checkouts(agent_dir):
        repo.discard(checkout)


def close(s: Session, agent_session: AgentSession, error: str, tracer: Tracer) -> None:
    """Close an agent session row that the launcher does not close (after a stop or a reap), and read its usage.

    The usage comes from a scan of the session store. The agent can spend many tokens before it is killed. If the
    budget ignored them, a restarted campaign could go past ``max_tokens``. A requested row never started and has
    no session to read.
    """
    agent_session.state, agent_session.ended_at, agent_session.error = AgentSessionState.KILLED, utcnow(), error
    if agent_session.session_id is None:
        return
    found = usage_of(s, agent_session)
    if isinstance(found, Usage):
        agent_session.usage = found
    elif found is not None:
        tracer.event('agent_session.usage_unknown', agent_session=agent_session.id, error=str(found))


def full_harnesses(s: Session, campaign: Campaign, cfg: CampaignConfig) -> set[str]:
    """Return the harnesses at their ``max_concurrent`` cap, counting live agents and the implementers of queued runs.

    A queued run's harness was chosen when it was queued (``_enqueue``), so its requested implementer holds its place.
    A requested sweep gets its harness only when it is taken (``_take``).
    """
    if not cfg.max_concurrent:
        return set()
    queued = (AgentSession.state == AgentSessionState.REQUESTED) & AgentSession.kind.in_(cfg.roles.doing_runs())
    on = select(AgentSession.harness).where(
        AgentSession.campaign_id == campaign.id,
        AgentSession.state.in_(LIVE) | queued,
        AgentSession.harness.in_(cfg.max_concurrent),
    )
    agents = Counter(s.scalars(on))
    return {harness for harness, cap in cfg.max_concurrent.items() if agents[harness] >= cap}
