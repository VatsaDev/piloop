"""Codex: turn off the user's servers, plugins and apps, then add ``-c mcp_servers.<name>={…}`` for each role server.

Codex has no strict mode, and ``-c`` deep-merges into the loaded config, so ``-c mcp_servers={}`` removes nothing.
The renderer therefore asks Codex which servers it would load, and turns each one off. It runs
``codex mcp list --json`` in the agent's cwd and environment, with the harness's own ``-c`` flags, so Codex applies
its system, user, profile and project layers by its own rules. A user server with the same name as a role server
fails the launch: the two tables would merge, and no ``-c`` flag deletes a key.

Two sources cannot be turned off that way. The servers of a plugin are not ``mcp_servers`` tables:
``-c mcp_servers.<name>.enabled=false`` on one creates a table with no transport, and Codex does not start ("invalid
transport"). ``features.plugins=false`` keeps every plugin out instead. The apps of the user's ChatGPT account load as
one server, ``codex_apps``, and ``features.apps=false`` keeps it out.

Then Codex lists the servers once more under the rendered flags. A flag that Codex rejects fails the launch with
Codex's own error, and so does any server it would load in addition to the role's.

API keys are named, not written. ``env_http_headers`` and ``bearer_token_env_var`` read a header's value from Codex's
environment, and ``env_vars`` passes variables to a stdio server (Codex starts it with a minimal environment). So the
variables are in the agent's environment, and ``shell_environment_policy.exclude`` keeps them out of the shell
commands that Codex runs.
"""

import json
import re
import subprocess
from shlex import join

from piloop.mcp import Launch, McpError, McpServer, Rendered, env_names, renderer

LIST_SECONDS = 30
BARE_KEY = re.compile(r'^[A-Za-z0-9_-]+$')
CONFIG_FLAGS = ('-c', '--config')
OWN = ['-c', 'features.apps=false', '-c', 'features.plugins=false']


def toml(value: object) -> str:
    """``value`` as a TOML inline value. JSON string escapes are valid TOML, except that TOML needs DEL escaped."""
    match value:
        case str():
            return json.dumps(value, ensure_ascii=False).replace('\x7f', '\\u007f')
        case list():
            return '[' + ', '.join(toml(v) for v in value) + ']'
        case dict():
            return '{' + ', '.join(f'{_key(str(k))} = {toml(v)}' for k, v in value.items()) + '}'
        case _:
            raise TypeError(f'no TOML rendering for {type(value).__name__}')


def _key(key: str) -> str:
    return key if BARE_KEY.match(key) else toml(key)


def server(s: McpServer) -> dict[str, object]:
    """One ``mcp_servers.<name>`` table."""
    if s.url is not None:
        headers = {'env_http_headers': s.headers_env} if s.headers_env else {}
        bearer = {'bearer_token_env_var': s.bearer_token_env} if s.bearer_token_env else {}
        return {'url': s.url, **headers, **bearer}
    assert s.command is not None, 'a stdio server'
    return {'command': s.command[0], 'args': s.command[1:], **({'env_vars': s.env} if s.env else {})}


def config_flags(argv: list[str]) -> list[str]:
    """The ``-c key=value`` pairs in the harness's argv. They change the config that Codex loads, trust included."""
    return [a for flag, value in zip(argv, argv[1:], strict=False) if flag in CONFIG_FLAGS for a in ('-c', value)]


def listed(launch: Launch, flags: list[str]) -> dict[str, bool]:
    """The servers that Codex would load for this agent under ``flags``, and whether each one is enabled."""
    argv = [launch.executable, *config_flags(launch.argv), *flags, 'mcp', 'list', '--json']
    try:
        done = subprocess.run(
            argv,
            cwd=launch.cwd,
            env=launch.env,
            capture_output=True,
            text=True,
            timeout=LIST_SECONDS,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.TimeoutExpired) as e:
        raise McpError(f'`{join(argv)}` failed: {type(e).__name__}: {e}') from e
    if done.returncode != 0:
        raise McpError(f'Codex rejects its config: `{join(argv)}` exit {done.returncode}: {done.stderr.strip()[-500:]}')
    try:
        return {entry['name']: entry['enabled'] for entry in json.loads(done.stdout)}
    except (ValueError, TypeError, KeyError) as e:
        raise McpError(f'`{join(argv)}` printed no server list: {done.stdout[:200]!r}') from e


@renderer('codex')
def render(launch: Launch) -> Rendered:
    """Turn off the user's servers, plugins and ChatGPT apps, add the role's, and check what Codex then loads."""
    theirs = listed(launch, OWN)
    clash = sorted(theirs.keys() & launch.servers.keys())
    if clash:
        raise McpError(
            f"Codex's own config defines MCP servers {clash}, which piloop's [mcp.*] tables also name, and Codex"
            ' would merge the two tables: rename the piloop server or the Codex one (`codex mcp list` shows where)'
        )
    off = [a for name in theirs for a in ('-c', f'mcp_servers.{name}.enabled=false')]
    mine = [a for name, s in launch.servers.items() for a in ('-c', f'mcp_servers.{name}={toml(server(s))}')]
    keys = ['-c', f'shell_environment_policy.exclude={toml(env_names(launch.servers.values()))}']
    args = [*OWN, *off, *mine, *keys]
    loads = sorted(name for name, enabled in listed(launch, args).items() if enabled)
    if loads != sorted(launch.servers):
        raise McpError(
            f"Codex would load MCP servers {loads} under piloop's flags, not the role's {sorted(launch.servers)}"
        )
    return Rendered(args)
