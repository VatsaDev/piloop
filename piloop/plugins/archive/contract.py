"""The archive capability: moves the finished parts of campaign workspaces into the CAS, and writes them back.

``[plugins.archive]`` in ``config.toml`` chooses the home's provider: ``local`` by default. The daemon opens it once
(``open``), and runs its consumer (``Archiver.contribution``). A part of a workspace is final when its subject settles
(``piloop.db.events.SubjectSettled``), and the rest when its campaign stops.
"""

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Final, Protocol

from sqlalchemy.orm import Session

from piloop.db.models import Campaign
from piloop.db.session import Database
from piloop.ids import TargetRef
from piloop.otel import Tracer
from piloop.plugins.cas.contract import Cas
from piloop.plugins.contribution import Contributes, Contribution
from piloop.plugins.host import Capability, Choice, Parameters, Scope
from piloop.store.tree import Materialised

CONSUMER: Final = 'archiver'
"""The name of the archiver's consumer in the daemon's loop, which ``ArchiveHost.wake`` runs."""


@dataclass(frozen=True)
class ArchiveHost:
    """What piloop gives an archive provider."""

    db: Database
    cas: Cas
    """The CAS that the workspaces go into."""
    home: Path
    reserve: int
    """Bytes kept free on the disk of ``home`` (``[storage] min_free_gb``), which the CAS and the executor share."""
    tracer: Tracer
    wake: Callable[[], None]
    """Runs its consumer in the next turn of the daemon's loop: a job ended."""


class Archiver(Contributes, Protocol):
    """An opened archive provider. Its work is its consumer (``CONSUMER``), and runs outside the daemon's loop."""

    def contribution(self) -> Contribution:
        """Its consumer ``CONSUMER``."""
        ...

    def stop(self, timeout: float) -> None:
        """Stop, and wait up to ``timeout`` seconds for the job in flight. The daemon's stop calls it."""
        ...


@dataclass(frozen=True)
class Restored:
    """What a restore wrote back, from how many archivings."""

    archives: int
    done: Materialised


@dataclass(frozen=True)
class CampaignArchived:
    """What the archive holds of one campaign's workspace."""

    name: str
    archivings: int
    files: int
    bytes: int


@dataclass(frozen=True)
class Summary:
    """What ``piloop store status`` prints of the archive."""

    campaigns: tuple[CampaignArchived, ...]
    subjects_due: int
    """The settled subjects whose entries wait for their archiving."""
    campaigns_due: int
    """The stopped campaigns whose workspaces wait for their archiving."""
    latest: datetime | None
    """When the latest archiving was made."""


class ArchiveConfig(Parameters, ABC):
    """An archive provider: its parameters, the keys of ``[plugins.archive]`` besides ``provider``."""

    @abstractmethod
    def open(self, host: ArchiveHost) -> Archiver:
        """Make the archiver. The daemon calls it once, when it starts."""

    @abstractmethod
    def restore(self, s: Session, cas: Cas, home: Path, campaign: Campaign, target: TargetRef | str | None) -> Restored:
        """Write archived files back into ``campaign``'s workspace: a subject's, a workspace-relative path's, or all.

        A file already there with other content is kept.
        """

    @abstractmethod
    def summary(self, s: Session) -> Summary:
        """What the archive holds and what waits for its archiving, for ``piloop store status``."""


ARCHIVES = Capability('archive', ArchiveConfig, choice=Choice(Scope.HOME, default='local'), noun='archive provider')
"""Every archive provider by name. ``local``, built in and the default, records each archiving in the home's
database."""
