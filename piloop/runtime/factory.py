"""The runtime registry, and the runtime of a campaign or of a launched worker row."""

from collections.abc import Callable
from pathlib import Path

from sqlalchemy.orm import Session

from piloop.db.models import Campaign, Worker
from piloop.plugins.contracts.runtime import RUNTIMES
from piloop.runtime import Handle, Runtime


def make_runtime(kind: str, *, campaign: str, campaign_path: Path) -> Runtime:
    """Create the runtime called ``kind``.

    Herdr uses the campaign to name its workspace. The subprocess runtime ignores it.
    """
    return RUNTIMES.get(kind)(campaign, campaign_path)


type RuntimeFactory = Callable[..., Runtime]
"""The signature of ``make_runtime``: ``(kind, *, campaign, campaign_path) -> Runtime``. Tests substitute their own."""


def handle_for(worker: Worker) -> Handle:
    """Return the runtime address of a launched worker row. A requested row has none yet."""
    if worker.runtime is None:
        raise ValueError(f'worker#{worker.id} is {worker.state}: not launched yet')
    return Handle(worker.runtime, worker.handle)


def view_of(worker: Worker) -> str | None:
    """Return where you can see a launched agent (``Runtime.view``), or None.

    None also for a row not launched yet, and for a runtime that is no longer installed: this only labels the agent.
    """
    runtime = RUNTIMES.providers().get(worker.runtime) if worker.runtime is not None else None
    return None if runtime is None else runtime.value.view(handle_for(worker))


def runtime_for(s: Session, worker: Worker, fallback_path: Path, factory: RuntimeFactory = make_runtime) -> Runtime:
    """Return the runtime that owns a launched worker row. Its campaign names the Herdr workspace."""
    campaign = s.get(Campaign, worker.campaign_id) if worker.campaign_id else None
    return factory(
        handle_for(worker).runtime,
        campaign=campaign.name if campaign else 'piloop',
        campaign_path=Path(campaign.path) if campaign else fallback_path,
    )
