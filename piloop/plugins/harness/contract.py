"""The harnesses: the coding-agent CLIs that piloop launches (``[plugins.harness.<name>]`` in ``piloop.toml``).

A harness is a provider of ``HARNESSES``. Its fields are the keys of its table: the program, the argv of a headless
run and of a resumed one, its home, its environment. A provider gives them their defaults, and its methods hold all
that piloop knows about its CLI: how it takes the MCP servers of a role (``mcp``), where its session files are and
how to read their tokens (``usage``), what its hook prints for a message (``messages``) and where it reads user skills
(``skills_dir``). The built-in providers are ``pi``, ``claude`` and ``codex``.

A campaign has an instance of every provider, by its name, over the provider's defaults. A table with another name is a
new harness: with ``provider = "<name>"`` it is that provider over its defaults, else a ``Custom`` harness, which runs
its argv and does nothing else. The rotation (``[plugins.harness] use``) defaults to the providers that are ``DEFAULT``.
"""

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, ClassVar, Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from piloop import deprecated
from piloop.mcp import EnvName, McpServer
from piloop.plugins.host import Capability, Many, Parameters, Pick, Scope, Use
from piloop.settings import Settings
from piloop.usage import Usage

SESSION_ID = '{session_id}'
PROMPT = '{prompt}'
EFFORT = '{effort}'
MCP = '{mcp}'
"""The argv element that ``McpRenderer.render`` expands into zero or more arguments."""
TAKES_NONE = 'it takes no MCP servers'


def fill(template: str, **values: str) -> str:
    """Substitute ``{name}`` placeholders without ``str.format``, so that other braces in the argv stay as they are."""
    for key, value in values.items():
        template = template.replace(f'{{{key}}}', value)
    return template


def takes_session_id(args: list[str]) -> bool:
    """Whether an argument list passes a piloop-minted session id to the agent."""
    return any(SESSION_ID in a for a in args)


class SecretRef(BaseModel):
    """``{ secret = "NAME" }``: the value of the secret ``NAME`` (``piloop secret set``), in place of a literal."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    secret: EnvName
    """The name of the secret."""


class McpError(RuntimeError):
    """The servers of a role cannot be given to this launch. The message says why and what to change."""


@dataclass(frozen=True)
class McpLaunch:
    """What a harness's MCP renderer knows about the agent it renders for."""

    servers: dict[str, McpServer]
    """The servers of the role, by name. May be empty."""
    executable: str
    """The harness program. Codex's renderer runs it to ask which servers the user has."""
    cwd: Path
    log_dir: Path
    """The agent's own directory, outside the worktree. A renderer may write a config file here."""
    env: dict[str, str]
    """The agent's environment."""
    argv: list[str]
    """The agent's arguments, with the placeholders filled in and ``{mcp}`` still in them. Codex's renderer reads the
    config flags that the harness passes."""


@dataclass(frozen=True)
class McpArgs:
    """What ``{mcp}`` expands to, and the variables to add to the agent's environment."""

    args: list[str]
    env: dict[str, str] = field(default_factory=dict)


def expand(argv: list[str], rendered: McpArgs) -> list[str]:
    """``argv`` with its ``{mcp}`` element replaced by the rendered arguments."""
    return [a for item in argv for a in (rendered.args if item == MCP else [item])]


class McpRenderer(Protocol):
    """How one harness takes the MCP servers of a role, in its own flags.

    API keys are never written down. A server names the environment variables that hold them, and a renderer writes
    those names in its harness's own expansion syntax.
    """

    def render(self, launch: McpLaunch) -> McpArgs:
        """Expand ``{mcp}`` for one launch.

        Raises:
            McpError: The servers cannot be given to this agent.
        """
        ...

    def refuses(self, argv: list[str]) -> str | None:
        """Why ``argv`` cannot take servers, or None when it can. Checked when a config loads, for roles with servers."""
        ...


class UsageReader(Protocol):
    """A harness's session files: the tokens an agent spent, what it said, and the file that piloop keeps."""

    def read(self, session_id: str | None, cwd: Path | None, log_dir: Path | None) -> Usage:
        """The usage of the ended session. ``cwd`` and ``log_dir`` are the agent's, when they are still known.

        Raises:
            UsageError: Nothing could be read. The message says why.
        """
        ...

    def said(self, session: bytes, lines: int = 1) -> list[str]:
        """The last ``lines`` lines of text that the agent wrote in ``session``, the bytes of its session file.

        Empty when there are none. Never raises.
        """
        ...

    def session_file(self, session_id: str, cwd: Path | None) -> Path | None:
        """The session's own file on this machine, which piloop puts in the CAS. None when there is none (yet)."""
        ...


class Harness(Parameters):
    """``[plugins.harness.<name>]``: how to launch one coding agent, with which model, and how many at once.

    piloop substitutes these placeholders in the argument lists: ``{model}``, ``{effort}`` (the role's ``effort``),
    ``{session_id}``, ``{cwd}`` (the agent's working directory), ``{log_dir}`` (the directory of the agent's brief and
    logs) and ``{prompt}`` (the first prompt, which names the file that holds the brief). An element that is exactly
    ``{mcp}`` expands to the role's MCP servers, in the harness's own flags. The built-in harnesses run without
    permission prompts: agents run in worktrees that the daemon owns and throws away, and a sandbox that cannot reach
    the repo's ``.git`` cannot commit.

    When ``headless`` contains ``{session_id}``, piloop creates the session id. Otherwise the runtime reads it with
    ``session_id_pattern``. When the agent ends, the harness finds its session file by this id and reads the tokens
    the agent spent. Without either, no tokens are recorded.
    """

    DEFAULT: ClassVar[bool] = False
    """Whether a campaign that does not set ``[plugins.harness] use`` rotates through this one. Only the class that sets it is a
    default: a subclass is not (``is_default``)."""

    model: str | None = None
    """The model id, which fills ``{model}`` in the argv, for example ``claude-opus-5-5``. A role overrides it
    (``[plugins.role.<name>] model``). A harness in a rotation needs one of the two."""
    max_concurrent: int | None = Field(default=None, ge=1)
    """The maximum number of agents of this harness, across all roles of the campaign, that are started or queued to
    start. Example: 2 when the provider of its model limits the request rate. When it is the turn of a harness at its
    limit, the slot goes to the next harness of the rotation. An idea or request that pins this harness waits. When
    unset, only the limits of the roles apply."""
    executable: str
    """The program to run."""
    headless: list[str]
    """The arguments for a run without a terminal. Must include ``{prompt}``."""
    resume: list[str] = []
    """The arguments that continue the session of a run with a new prompt. When the agent's turn ends with messages
    queued for it (comments, ``piloop prompt``), the runtime runs the harness again with these, ``{prompt}`` being the
    messages and ``{session_id}`` the session's id. Must include both. When empty, the agent takes no message while it
    runs."""
    session_id_pattern: str | None = None
    """A regex with one group that finds the session id in the first lines of the process's stdout. Use it in a
    harness that cannot take its id as an argument (``{session_id}``). For example, Codex
    ``exec --json`` prints ``{"type":"thread.started","thread_id":"<uuid>"}`` first."""
    home: str | None = None
    """The directory where the agent keeps its config and sessions: a path, which may start with ``~`` or name a
    variable as ``{env:NAME:default}``. A harness that reads usage finds its session files under it, and the daemon's
    service unit carries ``NAME``."""
    session_env: list[str] = []
    """The variables the agent sets for the processes of its session. piloop clears them for every agent, so an agent
    started from inside such a session is not its child."""
    env: dict[EnvName, str | SecretRef] = {}
    """Variables of the agent's environment: a value, or a secret, ``{ secret = "NAME" }``. For example a model
    gateway, ``{ ANTHROPIC_BASE_URL = "https://gw.example.com", ANTHROPIC_API_KEY = { secret = "ANTHROPIC_API_KEY" }
    }``. When unset, the agent uses the login of the machine that runs it."""

    def mcp(self) -> McpRenderer | None:
        """How it takes the MCP servers of a role. None, by default: it takes none."""
        return None

    def sessions(self) -> UsageReader | None:
        """How to read the session files that its CLI leaves. None, by default: it leaves none that piloop reads."""
        return None

    def usage(self) -> UsageReader | None:
        """How to read the tokens of an agent that ended: its ``sessions``, when the session id can be known.

        That is ``{session_id}`` in ``headless``, or a ``session_id_pattern``. Otherwise no tokens are recorded.
        """
        if self.session_id_pattern is None and not takes_session_id(self.headless):
            return None
        return self.sessions()

    def restore(self, session_id: str, cwd: Path) -> Path | None:
        """Where to write a session file back from the CAS, so that ``resume`` continues it in ``cwd`` on any machine.

        None, by default: its sessions start anew, and ``continuity = "resume"`` falls back to a fresh brief.
        """
        return None

    @classmethod
    def messages(cls, lines: list[str]) -> str:
        """What ``piloop agent messages --harness <provider>`` prints for the messages it takes: by default, one a line.

        A hook of the harness runs that command in the agent's turn.
        """
        return '\n'.join(lines)

    def skills_dir(self, settings: Settings) -> Path:
        """Where its CLI reads user skills: by default, the Agent Skills directory (``agent_skills_dir``)."""
        return settings.agent_skills_dir

    @property
    def literals(self) -> dict[str, str]:
        """The variables of ``env`` that have a value."""
        return {name: value for name, value in self.env.items() if isinstance(value, str)}

    @property
    def secrets(self) -> dict[EnvName, EnvName]:
        """The variables of ``env`` that take a secret, with the secret's name."""
        return {name: value.secret for name, value in self.env.items() if isinstance(value, SecretRef)}

    @property
    def needs_session_id(self) -> bool:
        """Whether the agent's session id is needed when it ends: ``usage`` finds the session file by it."""
        return self.usage() is not None

    def refuses_servers(self) -> str | None:
        """Return why ``headless`` cannot take MCP servers, or None when it can."""
        renderer = self.mcp()
        if renderer is None:
            return TAKES_NONE
        if MCP not in self.headless:
            return f'its headless argv has no {MCP} element'
        return renderer.refuses(self.headless)

    @model_validator(mode='before')
    @classmethod
    def _removed(cls, data: object) -> object:
        if isinstance(data, Mapping) and (removed := deprecated.removed_harness_keys(data)) is not None:
            raise ValueError(removed)
        return data

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.resume and not all(any(p in a for a in self.resume) for p in (SESSION_ID, PROMPT)):
            raise ValueError(f'resume must include {SESSION_ID} and {PROMPT}')
        if self.mcp() is None and MCP in [*self.headless, *self.resume]:
            raise ValueError(f'{MCP} in the argv of a harness that takes no MCP servers ({TAKES_NONE})')
        return self


class Custom(Harness):
    """A ``[plugins.harness.<name>]`` table with a new name and no ``provider``: it runs its argv, and nothing else."""


def custom(name: str, table: Mapping[str, object]) -> type[Harness]:
    """The class of a table with a new name and no ``provider``."""
    return Custom


HARNESSES = Capability(
    'harness',
    Harness,
    table=Many(Scope.CAMPAIGN, new=custom, use=Use(Scope.CAMPAIGN, Pick.ORDERED)),
    group='piloop.harnesses',
    noun='harness provider',
)
"""Every harness provider by name: ``pi``, ``claude``, ``codex``, and those that packages add."""

HarnessName = Annotated[str, HARNESSES.ref]
"""The name of a harness of the campaign: ``[plugins.harness.<name>]``, or a provider."""


def of_provider(harness: Harness) -> Harness | None:
    """``harness`` when a provider has its class, else None: a ``Custom`` harness, which a job does not carry."""
    return None if isinstance(harness, Custom) else harness


def each_provider() -> dict[str, Harness]:
    """Every provider by name, with its defaults."""
    return {name: p.model.model_validate({}) for name, p in HARNESSES.providers().items()}


def cleared_env() -> frozenset[str]:
    """The session variables of every provider (``session_env``), which the launcher clears for every agent."""
    return frozenset(name for harness in each_provider().values() for name in harness.session_env)
