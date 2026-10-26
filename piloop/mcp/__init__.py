"""MCP servers for agents: one definition in ``piloop.toml``, turned into each harness's own flags at launch.

A server is data (``McpServer``). A role lists the servers it gets (``[roles.<kind>] mcp``). This module knows no
harness: each harness provider renders the servers in its own flags (``Harness.mcp``).

API keys are never written down. A server names the environment variables that hold them, each the name of a secret
(``piloop.plugins.secrets``). Each renderer writes those *names* in its harness's own expansion syntax, and the agent's
job puts the secrets' values in the agent's environment. No value goes into a file, an argv or a log.
"""

from collections.abc import Iterable
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

AUTHORIZATION = 'authorization'

type ServerName = Annotated[str, StringConstraints(pattern=r'^[A-Za-z0-9_-]+$')]
"""Letters, digits, ``_`` and ``-``. pi accepts these, and they are safe in Codex's ``-c mcp_servers.<name>`` key."""
type EnvName = Annotated[str, StringConstraints(pattern=r'^[A-Za-z_][A-Za-z0-9_]*$')]
"""A POSIX environment variable name. The ``${NAME}`` expansion of every harness accepts it."""


class McpServer(BaseModel):
    """``[mcp.<name>]``: one MCP server, either streamable HTTP (``url``) or stdio (``command``).

    Do not write API keys here. Name the environment variables that hold them: each is the name of a secret
    (``piloop secret set``). piloop puts the secrets in the agent's environment, and the harness puts their values into
    the header or the server's environment.
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


def env_names(servers: Iterable[McpServer]) -> list[str]:
    """The environment variables that ``servers`` need, each once, in order."""
    return list(dict.fromkeys(name for s in servers for name in s.env_names))
