"""MCP servers for agents: one definition in ``piloop.toml``, turned into each harness's own flags at launch.

A server is data (``McpServer``). A role lists the servers it gets (``[roles.<kind>] mcp``). This module knows no
harness. A harness definition names a *renderer* (``Harness.mcp``). The renderers of the built-in harnesses are
separate modules (``claude``, ``codex``, ``pi``) that register themselves in ``RENDERERS``, as the usage tallies do.
Each one expands the ``{mcp}`` element of its harness's argv, so a change for one harness does not touch the others.

API keys are never written down. A server names the environment variables that hold them. Each renderer writes those
*names* in its harness's own expansion syntax, and the launcher copies the variables from the daemon's environment
into the agent's (``forwarded_env``), as it does PATH. No value goes into a file, an argv or a log.
"""

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

MCP = '{mcp}'
"""The argv element that a renderer expands into zero or more arguments."""
AUTHORIZATION = 'authorization'

type ServerName = Annotated[str, StringConstraints(pattern=r'^[A-Za-z0-9_-]+$')]
"""Letters, digits, ``_`` and ``-``. pi accepts these, and they are safe in Codex's ``-c mcp_servers.<name>`` key."""
type EnvName = Annotated[str, StringConstraints(pattern=r'^[A-Za-z_][A-Za-z0-9_]*$')]
"""A POSIX environment variable name. The ``${NAME}`` expansion of every harness accepts it."""


class McpServer(BaseModel):
    """``[mcp.<name>]``: one MCP server, either streamable HTTP (``url``) or stdio (``command``).

    Do not write API keys here. Name the environment variables that hold them. piloop passes these variables from the
    daemon's environment to the agent, and the harness puts their values into the header or the server's environment.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    url: str | None = None
    """The URL of a streamable HTTP server. SSE is not supported, because Codex and pi reject it."""
    headers_env: dict[str, EnvName] = {}
    """HTTP only. Maps each header name to the environment variable whose value is sent in it, for example
    ``{ "x-api-key" = "EXA_API_KEY" }``."""
    bearer_token_env: EnvName | None = None
    """HTTP only. The environment variable whose value is sent as ``Authorization: Bearer <value>``."""
    command: list[str] | None = Field(default=None, min_length=1)
    """The command (argv) of a stdio server, for example ``["python3", "/path/to/server.py"]``."""
    env: list[EnvName] = []
    """stdio only. The environment variables that the server reads, for example ``["XAI_API_KEY"]``."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        if (self.url is None) == (self.command is None):
            raise ValueError('an MCP server has either url (streamable HTTP) or command (stdio)')
        if self.url is None and (self.headers_env or self.bearer_token_env):
            raise ValueError('headers_env and bearer_token_env apply to a url server only')
        if self.command is None and self.env:
            raise ValueError('env applies to a command server only')
        if self.bearer_token_env and AUTHORIZATION in {h.lower() for h in self.headers_env}:
            raise ValueError('bearer_token_env sets Authorization; drop it from headers_env')
        return self

    @property
    def env_names(self) -> list[str]:
        """Every environment variable that this server needs."""
        return [*self.headers_env.values(), *([self.bearer_token_env] if self.bearer_token_env else []), *self.env]

    def expanded_headers(self) -> dict[str, str]:
        """The headers as ``${NAME}`` references, which Claude Code and pi-mcp-adapter expand in header values."""
        bearer = {'Authorization': f'Bearer ${{{self.bearer_token_env}}}'} if self.bearer_token_env else {}
        return {h: f'${{{name}}}' for h, name in self.headers_env.items()} | bearer

    def expanded_env(self) -> dict[str, str]:
        """The server's environment as ``${NAME}`` references, for Claude Code and pi-mcp-adapter."""
        return {name: f'${{{name}}}' for name in self.env}


class McpError(RuntimeError):
    """The servers of a role cannot be given to this launch. The message says why and what to change."""


def env_names(servers: Iterable[McpServer]) -> list[str]:
    """The environment variables that ``servers`` need, each once, in order."""
    return list(dict.fromkeys(name for s in servers for name in s.env_names))


def missing_env(servers: Iterable[McpServer], names: Iterable[str]) -> list[str]:
    """The variables that ``servers`` need and that are not in ``names``."""
    have = set(names)
    return [name for name in env_names(servers) if name not in have]


def forwarded_env(servers: Mapping[str, McpServer], environ: Mapping[str, str]) -> dict[str, str]:
    """The variables that the servers need, taken from ``environ`` (the daemon's) for the agent's environment.

    Raises:
        McpError: A variable is not set. The message names the variable and never shows a value.
    """
    if missing := missing_env(servers.values(), environ):
        raise McpError(
            f'MCP servers {sorted(servers)} need {", ".join(missing)} in the environment piloop serve runs in;'
            ' set them there and restart the daemon'
        )
    return {name: environ[name] for name in env_names(servers.values())}


@dataclass(frozen=True)
class Launch:
    """What a renderer knows about the agent it renders for."""

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
class Rendered:
    """What ``{mcp}`` expands to, and the variables to add to the agent's environment."""

    args: list[str]
    env: dict[str, str] = field(default_factory=dict)


type Render = Callable[[Launch], Rendered]
"""Expands ``{mcp}`` for one launch. Raises ``McpError`` when the servers cannot be given to this agent."""
type Refuses = Callable[[list[str]], str | None]
"""Why an argv cannot take servers, or None when it can. Checked when a config loads, for roles with servers."""


def accepts(_argv: list[str]) -> str | None:
    """The default ``Refuses``: every argv with ``{mcp}`` in it can take servers."""
    return None


@dataclass(frozen=True)
class Renderer:
    """How one harness takes the servers of a role."""

    render: Render
    refuses: Refuses = accepts


RENDERERS: dict[str, Renderer] = {}
"""The renderers by name, as ``Harness.mcp`` names them. To support a new harness, add one module that registers
its renderer."""


def renderer(name: str, *, refuses: Refuses = accepts) -> Callable[[Render], Render]:
    """Register a render function under ``name``. The decorator returns the function unchanged."""

    def register(render: Render) -> Render:
        RENDERERS[name] = Renderer(render, refuses)
        return render

    return register


def expand(argv: list[str], rendered: Rendered) -> list[str]:
    """``argv`` with its ``{mcp}`` element replaced by the rendered arguments."""
    return [a for item in argv for a in (rendered.args if item == MCP else [item])]


# The built-in renderers register themselves on import, so they are imported after the names they use exist.
from piloop.mcp import claude, codex, pi  # noqa: E402, F401
