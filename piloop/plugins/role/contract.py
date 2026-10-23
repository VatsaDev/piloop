"""The role capability: what the agents of a role do, and the defaults of its ``[roles.<name>]`` table."""

from enum import StrEnum
from typing import Annotated, ClassVar, Literal, Self

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator, model_validator

from piloop.db.sql import sql_error
from piloop.plugins.host import Capability
from piloop.plugins.runtime.contract import RuntimeName
from piloop.plugins.trigger.contract import TriggerEntry


class Workdir(StrEnum):
    """Where the agent of a sweeping role works. An agent that does a run works in a new worktree at its run's base."""

    HEAD = 'head'
    """A detached worktree at the head. It is removed when the agent ends, and nothing committed in it is scored."""
    REPO = 'repo'
    """The campaign's repository checkout itself, shared with you. piloop does not reset or remove it."""


class Files(StrEnum):
    """What the agents of a role may file with ``piloop idea``. The CLI refuses the rest."""

    NOTHING = 'nothing'
    """No ideas and no seeds. A hunch goes in a finding."""
    SEEDS = 'seeds'
    """Seeds (``piloop idea seed``), for research to develop."""
    IDEAS = 'ideas'
    """Defined ideas (``piloop idea add``, ``piloop idea develop``) and seeds."""


class RoleConfig(BaseModel):
    """The keys that every ``[roles.<name>]`` table accepts. Each role's table below adds keys and sets defaults."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    PROMPT: ClassVar[str] = ''
    """The built-in prompt of a built-in role, a file under ``piloop/agents/prompts/``. Empty for other roles."""
    IDEAS: ClassVar[str] = ''
    """The ideas line of a built-in role's brief when the line for its ``files`` does not fit it. ``{piloop}`` is the CLI
    and ``{campaign}`` is the campaign's name. Empty: the line for ``files`` (``piloop.agents.roles.IDEAS``)."""

    prompt: str | None = None
    """The file that starts the brief of this role: a path relative to the repo root, or an absolute path. When unset,
    a built-in role uses its built-in prompt. A new role needs it."""
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
    wake_budget: int | None = Field(default=None, ge=0)
    """The size in tokens of the lessons overview that opens this role's briefs. When unset, ``[memory]
    wake_budget``. 0 turns it off for this role."""


class ImplementerRole(RoleConfig):
    """``[roles.implementer]``, and every ``[roles.<name>]`` table with ``runs = true``: a role that does runs.

    The run queue starts its agents. Each agent does one idea in a new worktree at its run's base, and its commits are
    scored and judged when it ends. A new role of this kind takes the implementer's defaults, its prompt included.
    """

    PROMPT: ClassVar[str] = 'implementer.md'

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
    """The maximum number of implementers that run at the same time. Each implementer is one harness process on this machine. A
    run that is being scored after its implementer ended does not count."""
    writes: list[str] = ['commits', 'comments(finding) on your idea', 'comments(note) on your run']
    """What an implementer may write, as its brief states it."""
    files: Files = Files.NOTHING
    """An implementer files no ideas and no seeds. Its hunches go in its finding, which research reads."""
    recovery_requeues: int = Field(default=2, ge=0)
    """How many times one idea goes back to the queue from the last commit of a run that did not succeed: a new
    implementer then starts from that commit to finish the work, also when the idea has no attempts left. When the head
    moved past the commit, it starts on the head, and a note gives the ``git cherry-pick`` of the commits. After that,
    the idea settles as after any failed run, so an idea whose implementers keep dying is not run forever. 0 never
    requeues."""
    explore_every: int = Field(default=4, ge=0)
    """At least one run of this role in this many starts from an alternative starting point. When none of the role's
    last ``explore_every - 1`` runs did, the next slot goes to the most urgent ready idea that is pinned to a base
    other than the head (a reference, an older keep or a note), whatever its priority. Nothing changes while no such
    idea is ready. 0 turns this off."""

    @field_validator('takes')
    @classmethod
    def _reads_ideas(cls, takes: str | None) -> str | None:
        if takes is not None and (error := sql_error(ideas_taken(takes))) is not None:
            raise ValueError(f'the condition does not run on the ideas table: {error}')
        return takes


def ideas_taken(takes: str) -> str:
    """Return the query that selects the ids of the ideas that ``[roles.<name>] takes`` selects."""
    return f'SELECT id FROM ideas WHERE {takes}'


class SweepRole(RoleConfig):
    """``[roles.<name>]`` of a sweeping role: the watchdog, research, the scout, or a new role that you define.

    A sweep is an agent without a run. Its triggers request it, and nothing is scored when it ends. A table with a new
    name defines a new role, and needs ``prompt`` and ``triggers``.
    """

    INCIDENT_PROMPT: ClassVar[str] = ''
    """The built-in ``incident_prompt`` of a built-in role, a file under ``piloop/agents/prompts/``. Empty: none."""
    ROUTINE_PROMPT: ClassVar[str] = ''
    """The built-in ``routine_prompt`` of a built-in role, a file under ``piloop/agents/prompts/``. Empty: none."""

    runs: Literal[False] = False
    """The agents of this role do no runs. Set ``runs = true`` in a new table to define a role that does runs."""
    triggers: list[TriggerEntry]
    """When the daemon requests an agent of this role: a list of ``{ kind = "<trigger>", ... }`` entries, checked in
    order on each tick. A package can add triggers. An empty list means that agents start only on request
    (``piloop launch``)."""
    workdir: Workdir = Workdir.HEAD
    """Where the agent works: ``head``, a detached worktree at the head, or ``campaign``, the campaign checkout."""
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
    trigger): a path relative to the repo root, or an absolute path. When unset, a built-in role uses its built-in
    file (the watchdog's ``watchdog_incident.md``), and another role adds nothing."""
    routine_prompt: str | None = None
    """A file that follows ``prompt`` in the brief of every other sweep. When unset, a built-in role uses its built-in
    file (the watchdog's ``watchdog_progress.md``), and another role adds nothing."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.prompt is None and not self.PROMPT:
            raise ValueError('a new role needs a prompt: the file that starts its brief')
        return self


def _role(table: object) -> object:
    """Validate a ``[roles.<name>]`` table with a new name: a role that does runs with ``runs = true``, else a sweep."""
    does_runs = isinstance(table, ImplementerRole) or (isinstance(table, dict) and bool(table.get('runs')))
    return (ImplementerRole if does_runs else SweepRole).model_validate(table)


RoleEntry = Annotated[ImplementerRole | SweepRole, BeforeValidator(_role)]
"""The type of a ``[roles.<name>]`` table with a new name."""


ROLES = Capability('role', RoleConfig)
"""Every role by name, with its defaults: the built-in ones, then the ones that packages add. Each entry point of the
group names a subclass of ``SweepRole`` or of ``ImplementerRole``, whose defaults are the role's."""
