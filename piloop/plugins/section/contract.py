"""The sections: the titled blocks that a campaign's readers see (https://piloop.autoresearch.inc/concepts/roles#sections).

A section is a provider of ``SECTIONS``: its fields are its parameters, from its entry in a list of sections. ``piloop
status``, the API and a UI show the campaign's ``[status] sections``. A brief shows its role's
``[roles.<name>] sections``. A list that is not set shows the installed sections that declare its surface
(``Section.SHOWN_IN``), in their ``ORDER``. Each reader gets the sections in the order of the list, and leaves out a
section with nothing to show. A package adds a section with an entry point in the group of ``SECTIONS``: nothing in
piloop changes.

A section composes the blocks of ``piloop.api.views`` (``Text``, ``Lines``, ``Figures``, ``Table``). Each reader
renders each kind of block its own way, in one place: ``markdown`` for a brief and ``piloop status``, and the UI's own
renderer.
"""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, ClassVar

from piloop.api.views import Rendered
from piloop.plugins.cas.contract import Cas
from piloop.plugins.host import Capability, Parameters

if TYPE_CHECKING:  # piloop.db.models types campaigns.config with the sections: only the annotations name the rows
    from sqlalchemy.orm import Session

    from piloop.db.models import Campaign


@dataclass(frozen=True)
class Rendering:
    """What a section reads: core makes one for each rendering of a list of sections.

    A section reads with ``s`` and writes nothing.
    """

    s: Session
    """The session to read with. A section writes nothing."""
    now: datetime
    """The time of the rendering. A section that shows an age reads this, not the clock."""
    campaign: Campaign
    """The campaign, with its config (``campaign.config``)."""
    workspace: Path
    """The campaign's shared workspace, where agents leave notes and artifacts for each other."""
    cas: Cas
    """The home's CAS: the agents' sessions are there (``agent_sessions.session_file``)."""
    given: tuple[str, ...] = ()
    """The anomaly lines that the agent who reads got to investigate, in order. Empty for every other reader."""


class Surface(StrEnum):
    """Where sections are shown. A list that is not set shows the sections that name its surface."""

    STATUS = 'status'
    """``piloop status``, the API's overview and a UI: ``[status] sections``."""
    SWEEP = 'sweep'
    """The brief of a sweeping role: ``[roles.<name>] sections``."""
    RUN = 'run'
    """The brief of a role that does runs: ``[roles.<name>] sections``."""


class Section(Parameters, ABC):
    """One entry of a list of sections: ``{ provider = "<section>", <parameter> = <value> }``.

    A section declares where it is shown by default and in what order, so that installing a package adds it there,
    and a list in the configuration replaces those defaults.
    """

    SHOWN_IN: ClassVar[frozenset[Surface]] = frozenset()
    """The surfaces whose list shows the section, with its defaults, when the list is not set. Empty: only a list that
    names it shows it."""
    ORDER: ClassVar[int] = 500
    """Where the section goes in a default list: a lower number comes first. The built-in sections leave gaps,
    so that a package can put one between two of them."""

    @abstractmethod
    def render(self, r: Rendering) -> Rendered | None:
        """Return the section now, or None when it has nothing to show: then the reader leaves it out."""


SECTIONS = Capability('section', Section)
"""Every section by name. The lists of sections name them in their entries, so no single key selects one."""

SectionEntry = Annotated[Section, SECTIONS.entry]
"""The type of one entry of a list of sections: the section that its ``provider`` names, with its parameters."""


def defaults(surface: Surface) -> list[Section]:
    """The sections that ``surface`` shows when its list is not set, by ``ORDER`` and then by name.

    They are every installed section with ``surface`` in its ``SHOWN_IN``, with its defaults.
    """
    shown = [(p.model.ORDER, name, p.model) for name, p in SECTIONS.providers().items() if surface in p.model.SHOWN_IN]
    return [model() for _, _, model in sorted(shown, key=lambda found: found[:2])]


def chosen(listed: Sequence[Section] | None, surface: Surface) -> list[Section]:
    """The sections that a list shows: the ones that it sets, else the defaults of ``surface``."""
    return defaults(surface) if listed is None else list(listed)


def rendered(sections: Sequence[Section], r: Rendering) -> list[Rendered]:
    """Render ``sections`` in order, without the ones that have nothing to show."""
    return [found for section in sections if (found := section.render(r)) is not None]


def markdown(sections: Sequence[Section], r: Rendering) -> str:
    """Render ``sections`` as one markdown document for a brief: each section's ``markdown``, a blank line between."""
    return '\n\n'.join(found.markdown() for found in rendered(sections, r))
