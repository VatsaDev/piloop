"""The runtime registry, and the runtime of a campaign or of a launched agent session row."""

from pathlib import Path
from typing import Protocol

from sqlalchemy.orm import Session

from piloop.db.models import AgentSession, Campaign
from piloop.plugins.runtime.contract import RUNTIMES, Handle, Runtime


def make_runtime(kind: str, *, campaign: str, campaign_path: Path) -> Runtime:
    """Create the runtime called ``kind``.

    Herdr uses the campaign to name its workspace. The subprocess runtime ignores it.
    """
    return RUNTIMES.get(kind).model(campaign, campaign_path)


class RuntimeFactory(Protocol):
    """The signature of ``make_runtime``. Tests substitute their own."""

    def __call__(self, kind: str, /, *, campaign: str, campaign_path: Path) -> Runtime:
        """Create the runtime called ``kind`` for the campaign."""
        ...


def handle_for(agent_session: AgentSession) -> Handle:
    """Return the runtime address of a launched agent session row. A requested row has none yet."""
    if agent_session.runtime is None:
        raise ValueError(f'agent_session#{agent_session.id} is {agent_session.state}: not launched yet')
    return Handle(agent_session.runtime, agent_session.handle)


def view_of(agent_session: AgentSession) -> str | None:
    """Return where you can see a launched agent (``Runtime.view``), or None.

    None also for a row not launched yet, and for a runtime that is no longer installed: this only labels the agent.
    """
    runtime = RUNTIMES.providers().get(agent_session.runtime) if agent_session.runtime is not None else None
    return None if runtime is None else runtime.model.view(handle_for(agent_session))


def runtime_for(
    s: Session, agent_session: AgentSession, fallback_path: Path, factory: RuntimeFactory = make_runtime
) -> Runtime:
    """Return the runtime that owns a launched agent session row. Its campaign names the Herdr workspace."""
    campaign = s.get(Campaign, agent_session.campaign_id) if agent_session.campaign_id else None
    return factory(
        handle_for(agent_session).runtime,
        campaign=campaign.name if campaign else 'piloop',
        campaign_path=Path(campaign.path) if campaign else fallback_path,
    )
