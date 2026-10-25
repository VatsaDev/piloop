"""The role capability: what the agents of a role do, and the defaults of its ``[roles.<name>]`` table.

Every role is a provider of ``ROLES``, a built-in one or a package's, and so are its defaults: a subclass of
``SweepRole`` or of ``ImplementerRole`` whose fields have the role's defaults. Its prompt is a resource of its package
(``PROMPT``). A table with another name defines a new role (``new_role``).
"""

from collections.abc import Mapping
from enum import StrEnum
from importlib.resources import files
from importlib.resources.abc import Traversable
from typing import TYPE_CHECKING, ClassVar, Literal, NamedTuple, Protocol, Self

from pydantic import Field, model_validator

from piloop import deprecated
from piloop.plugins.host import Capability, Parameters
from piloop.plugins.runtime.contract import RuntimeName
from piloop.plugins.section.contract import Section, SectionEntry, Surface, chosen
from piloop.plugins.trigger.contract import Pass, TriggerEntry

if TYPE_CHECKING:  # piloop.db.models types campaigns.config with the roles: only the annotations name the rows
    from piloop.db.models import AgentSession


class Files(StrEnum):
    """What the agents of a role may file with ``piloop idea``. The CLI refuses the rest."""

    NOTHING = 'nothing'
    """No ideas and no seeds. A hunch goes in a finding."""
    SEEDS = 'seeds'
    """Seeds (``piloop idea seed``), for research to develop."""
    IDEAS = 'ideas'
    """Defined ideas (``piloop idea add``, ``piloop idea develop``) and seeds."""


class RoleConfig(Parameters):
    """The keys that every ``[roles.<name>]`` table accepts. Each role's table below adds keys and sets defaults."""

    @model_validator(mode='before')
    @classmethod
    def _no_workdir(cls, data: object) -> object:
        if isinstance(data, dict) and 'workdir' in data:
            raise ValueError(deprecated.removed_workdir())
        return data

    PROMPT: ClassVar[Traversable | None] = None
    """The prompt that starts the brief of the role's agents: a resource of the role's package, such as
    ``files(__package__) / 'auditor.md'``. ``prompt`` in the table overrides it. A role with neither is refused."""
    IDEAS: ClassVar[str] = ''
    """The ideas line of a built-in role's brief when the line for its ``files`` does not fit it. ``{piloop}`` is the CLI
    and ``{campaign}`` is the campaign's name. Empty: the line for ``files`` (``piloop.agents.roles.IDEAS``)."""

    prompt: str | None = None
    """The file that starts the brief of this role: a path relative to the repo root, or an absolute path. When unset,
    the role's own prompt (``PROMPT``). A new role needs it."""
    runtime: RuntimeName | None = None
    """Where the agents of this role run. When unset, the role uses the campaign's ``runtime``."""
    harnesses: list[str] | None = Field(default=None, min_length=1)
    """The harness rotation of this role. When unset, the role uses the campaign's ``harnesses``."""
    model: dict[str, str] | None = None
    """The model for each harness in this role. It is merged over the campaign's ``model``."""
    effort: str = 'high'
    """How hard the agents of this role think, as the harness names it, for example ``high``. It fills ``{effort}`` in
    the harness's argv, and the harness checks it."""
    timeout: float = 600
    """The number of seconds one agent may run before it is stopped."""
    max_concurrent: int = Field(default=1, ge=1)
    """The maximum number of agents of this role that run at the same time. More requests stay ``requested`` until a
    slot is free."""
    addendum: str | None = None
    """A repo-relative file that is appended to ``brief.md`` in the briefs of this role only. Use it for instructions
    that only this role needs, for example how the research agent searches."""
    mcp: list[str] = []
    """The MCP servers that the agents of this role get, by the name of their ``[mcp.<name>]`` table. Every harness in
    the role's rotation must be able to take them in the invocation of the role's runtime (``[harness.<name>] mcp``)."""
    writes: list[str] = []
    """What the agents of this role may write, as their brief states it. Only ``files`` is enforced."""
    files: Files = Files.SEEDS
    """What the agents of this role may file with ``piloop idea``: ``nothing``, ``seeds`` or ``ideas``. The CLI
    refuses the rest."""
    sections: list[SectionEntry] | None = None
    """What the briefs of this role show after the role's context: a list of ``{ provider = "<section>", ... }``
    entries, in order. When unset, every installed section that a brief of this kind of role shows by default, in
    their order (``Section.SHOWN_IN``, ``Section.ORDER``)."""

    SURFACE: ClassVar[Surface]
    """Where the briefs of this kind of role show sections: the default of ``sections``."""

    def shown(self) -> list[Section]:
        """The sections of this role's briefs: its ``sections``, else the defaults of its surface."""
        return chosen(self.sections, self.SURFACE)


class ImplementerRole(RoleConfig):
    """``[roles.implementer]``, and every ``[roles.<name>]`` table with ``runs = true``: a role that does runs.

    The run queue starts its agents. Each agent does one idea in a new checkout at its run's base, and its versions are
    scored and judged when it ends. A new role of this kind takes the implementer's defaults, its prompt included.
    """

    PROMPT: ClassVar[Traversable | None] = files(__package__) / 'prompts' / 'implementer.md'

    runs: Literal[True] = True
    """The agents of this role do runs. A table without ``runs = true`` is a sweeping role."""
    takes: str | None = None
    """The ideas that this role takes: an SQL condition on the ``ideas`` table, without parameters, for example
    ``priority >= 3`` or ``track = 'speed'``. When unset, the role takes every runnable idea. A free slot of a role
    that you define is filled before a slot of the implementer, so the implementer takes what the other roles leave."""

    timeout: float = 1800
    """The number of seconds one implementer may run before it is stopped. Its brief states it. A quarter of it before,
    at most 10 minutes, the daemon tells the implementer to wrap up."""
    max_concurrent: int = Field(default=4, ge=1)
    """The maximum number of implementers that run at the same time, on all workers together. A run that is being
    scored after its implementer ended does not count."""
    writes: list[str] = ['versions', 'comments(finding) on your idea', 'comments(note) on your run']
    """What an implementer may write, as its brief states it."""
    files: Files = Files.NOTHING
    """An implementer files no ideas and no seeds. Its hunches go in its finding, which research reads."""
    SURFACE: ClassVar[Surface] = Surface.RUN
    recovery_requeues: int = Field(default=2, ge=0)
    """How many times one idea goes back to the queue from the last version of a run that did not succeed: a new
    implementer then starts from that version to finish the work, also when the idea has no attempts left. When the
    head moved past the version, it starts on the head, and a note names the change to take. After that,
    the idea settles as after any failed run, so an idea whose implementers keep dying is not run forever. 0 never
    requeues."""
    explore_every: int = Field(default=4, ge=0)
    """At least one run of this role in this many starts from an alternative starting point. When none of the role's
    last ``explore_every - 1`` runs did, the next slot goes to the most urgent ready idea that is pinned to a base
    other than the head (a reference, an older keep or a note), whatever its priority. Nothing changes while no such
    idea is ready. 0 turns this off."""


def ideas_taken(takes: str) -> str:
    """Return the query that selects the ids of the ideas that ``[roles.<name>] takes`` selects."""
    return f'SELECT id FROM ideas WHERE {takes}'


class Stall(NamedTuple):
    """How close the campaign is to a stall (``piloop.campaign.stop.stall``)."""

    runs: int
    """The measured runs since the last keep."""
    mark: int
    """The runs without a keep that mark a stall."""
    why: str
    """How the mark was derived, for the brief."""


class Sweep(Pass, Protocol):
    """What a sweeping role reads to give its agent its context: its trigger's ``Pass``, and the agent's row."""

    @property
    def agent_session(self) -> AgentSession:
        """The row of the agent that the brief is for."""
        ...

    def stall(self) -> Stall:
        """The runs without a keep against the stall mark."""
        ...


class SweepRole(RoleConfig):
    """``[roles.<name>]`` of a sweeping role: the watchdog, research, the scout, or a new role that you define.

    A sweep is an agent without a run. Its triggers request it, and nothing is scored when it ends. A table with a new
    name defines a new role, and needs ``prompt`` and ``triggers``.
    """

    INCIDENT_PROMPT: ClassVar[Traversable | None] = None
    """The role's own ``incident_prompt``: a resource of its package, or None."""
    ROUTINE_PROMPT: ClassVar[Traversable | None] = None
    """The role's own ``routine_prompt``: a resource of its package, or None."""

    runs: Literal[False] = False
    """The agents of this role do no runs. Set ``runs = true`` in a new table to define a role that does runs."""
    triggers: list[TriggerEntry]
    """When the daemon requests an agent of this role: a list of ``{ provider = "<trigger>", ... }`` entries, checked in
    order on each pass. A package can add triggers. An empty list means that agents start only on request
    (``piloop launch``)."""
    interval: float = Field(default=0, ge=0)
    """The number of seconds between two routine sweeps of the ``cron`` trigger, counted from the start of the last
    one. A sweep that investigates an anomaly is not a routine sweep. 0 turns the timer off."""
    max_ideas: int = Field(default=1, ge=0)
    """The maximum number of new ideas one sweep may insert, as its brief states it. The seeds it develops do not
    count. ``low_water`` requests enough sweeps to fill the backlog at this many ideas each. 0 pauses the role's new
    ideas: ``low_water`` is off, and an idea that a sweep of the role adds (``piloop idea add``) is ``proposed``, so it
    waits for a review, also from a sweep that started before the change."""
    incident_prompt: str | None = None
    """A file that follows ``prompt`` in the brief of an incident sweep, a sweep that got anomalies (the ``anomaly``
    trigger): a path relative to the repo root, or an absolute path. When unset, the role's own (the watchdog's
    ``watchdog_incident.md``), else nothing."""
    routine_prompt: str | None = None
    """A file that follows ``prompt`` in the brief of every other sweep. When unset, the role's own (the watchdog's
    ``watchdog_progress.md``), else nothing."""
    SURFACE: ClassVar[Surface] = Surface.SWEEP

    def context(self, sweep: Sweep) -> list[str]:
        """The lines that the role adds to its agent's context, after the daemon's own and before the sections.

        By default, a role that defines ideas (``files = "ideas"``) is told its ``max_ideas``, which ``low_water``
        counts on, and any other role adds nothing.
        """
        if self.files is Files.IDEAS:
            return [f'- New ideas of your own in this sweep: at most {self.max_ideas} (`max_ideas`).']
        return []

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.prompt is None and self.PROMPT is None:
            raise ValueError('a new role needs a prompt: the file that starts its brief')
        return self


ROLES = Capability('role', RoleConfig)
"""Every role by name, with its defaults: the built-in ones, then the ones that packages add. Each entry point of the
group names a subclass of ``SweepRole`` or of ``ImplementerRole``, whose defaults are the role's."""


def new_role(table: Mapping[str, object]) -> type[ImplementerRole] | type[SweepRole]:
    """The class of a ``[roles.<name>]`` table with a new name: with ``runs = true`` a role that does runs."""
    return ImplementerRole if table.get('runs') else SweepRole
