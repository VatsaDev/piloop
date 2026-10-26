"""The fact sources: where a campaign's facts come from (``[[facts]]``).

A source is a provider of ``FACT_SOURCES``: its fields are its parameters, from its entry in ``[[facts]]``. It names
the facts that it supplies (``supplies``), and gathers the values of those that a reader asks for (``gather``). A
stop rule or a section reads the facts of every source of the campaign by name (``piloop.plugins.fact_source.facts``):
piloop gives no fact a meaning of its own, a cost or a spend among them.

A list that is not set is every installed source that is on by default (``ON_BY_DEFAULT``): ``campaign`` and
``optimization``. A source may also read on its own: the daemon calls its ``read`` when it is ``due``, outside
any transaction, and then its ``record``. ``kit.readings.Readings`` does that for a source of named values, and
``command``, which runs a command that prints ``METRIC <name>=<value>`` lines as the evaluate command does, is one.
"""

from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, ClassVar

from piloop.plugins.fact_source.facts import Fact, FactValue
from piloop.plugins.host import Capability, Parameters

if TYPE_CHECKING:  # piloop.config imports this module, and the database imports piloop.config
    from sqlalchemy.orm import Session

    from piloop.db.models import Campaign


class FactReadError(Exception):
    """A source read no values: it failed, timed out or printed something else. An unknown value is never 0."""


class FactSource(Parameters, ABC):
    """One entry of ``[[facts]]``: ``{ provider = "<source>", <parameter> = <value> }``."""

    ON_BY_DEFAULT: ClassVar[bool] = False
    """Whether a campaign that does not list its ``[[facts]]`` has this source, with its defaults."""

    @abstractmethod
    def supplies(self) -> tuple[Fact[FactValue], ...]:
        """The facts that it supplies. Two sources of a campaign never supply the same one."""

    @abstractmethod
    def gather(
        self, s: Session, campaign: Campaign, wanted: frozenset[Fact[FactValue]], now: datetime
    ) -> dict[Fact[FactValue], FactValue]:
        """The values of the facts in ``wanted`` that it supplies, as of ``now``. It reads with ``s``, never writes.

        The campaign's config is ``campaign.config``.
        """

    def due(self, s: Session, campaign: Campaign) -> datetime | None:
        """When the daemon calls ``read`` next: a time at or before now reads now.

        None, by default: the source reads nothing on its own.
        """
        return None

    def read(self, repo: Path) -> dict[str, float]:
        """Read values by name, at the campaign's repo root. The daemon calls it outside any transaction, when it is due.

        Raises:
            FactReadError: No values were read.
        """
        raise NotImplementedError  # a source whose ``due`` is ever set implements it

    def record(self, s: Session, campaign: Campaign, values: dict[str, float] | FactReadError) -> None:
        """Record what ``read`` returned or raised. The caller commits. By default, nothing."""


FACT_SOURCES = Capability('fact_source', FactSource, group='piloop.fact_sources', noun='fact source')
"""Every fact source by name. ``[[facts]]`` lists a campaign's sources."""

FactSourceEntry = Annotated[FactSource, FACT_SOURCES.entry]
"""The type of an entry of ``[[facts]]``."""


def on_by_default() -> list[FactSource]:
    """The sources of a campaign that does not list its ``[[facts]]``: every installed source that is on by default."""
    return [p.model() for _, p in sorted(FACT_SOURCES.providers().items()) if p.model.ON_BY_DEFAULT]


def sources(listed: Sequence[FactSource] | None) -> list[FactSource]:
    """The sources of a campaign: those that it lists, else ``on_by_default``."""
    return on_by_default() if listed is None else list(listed)
