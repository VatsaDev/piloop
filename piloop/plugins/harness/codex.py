"""``codex``: the Codex CLI."""

import json
import re
import subprocess
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from shlex import join

from piloop.mcp import McpServer, env_names
from piloop.plugins.harness.contract import EFFORT, MCP, PROMPT, SESSION_ID, Harness, McpArgs, McpError, McpLaunch
from piloop.plugins.harness.kit.jsonl import (
    Counts,
    JsonlSessions,
    Record,
    add,
    as_int,
    child,
    no_cwd,
    texts,
    zero,
)
from piloop.settings import expand_env

NO_MEMORY = 'features.memories=false'
"""``-c`` for every Codex agent. Codex keeps memories per repo, so the notes of one agent would load into every later
agent of the campaign. They are off by default; the flag makes sure that a user's config cannot turn them on."""
TRUST = ['-c', 'projects={"{cwd}"={trust_level="trusted"}}']
"""Trust the agent's worktree for this launch only. Codex then never opens its "Trust this folder?" dialog (it checks
the directory before the repo root), and the user's ``config.toml`` is not written. The value is a table, not a dotted
key, because Codex splits ``-c`` keys on every ``.`` and the path can contain dots."""
TOOL_ENV = [
    '-c',
    'shell_environment_policy.inherit="all"',
    '-c',
    'shell_environment_policy.ignore_default_excludes=true',
    '-c',
    'shell_environment_policy.include_only=[]',
]
"""Give Codex's shell commands the agent's environment, as Claude and pi do, whatever the user's
``shell_environment_policy`` says. piloop finds an agent's background jobs by ``PILOOP_PROCESS_TAG``, and
``PILOOP_AGENT_TOKEN`` signs the agent's writes. ``inherit = "core"`` or an ``include_only`` would drop them, and the
default excludes (``*KEY*``, ``*SECRET*``, ``*TOKEN*``) other variables that Claude and pi pass on. The MCP renderer's
``shell_environment_policy.exclude`` keeps the role's API keys out."""
FLAGS = [
    '--json',
    '--dangerously-bypass-approvals-and-sandbox',
    '--skip-git-repo-check',
    '--model',
    '{model}',
    '-c',
    f'model_reasoning_effort="{EFFORT}"',
    '-c',
    NO_MEMORY,
    *TRUST,
    *TOOL_ENV,
    MCP,
]
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


def listed(launch: McpLaunch, flags: list[str]) -> dict[str, bool]:
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


@dataclass(frozen=True)
class CodexMcp:
    """Codex: turn off the user's servers, plugins and apps, then add ``-c mcp_servers.<name>={…}`` for each role server.

    Codex has no strict mode, and ``-c`` deep-merges into the loaded config, so ``-c mcp_servers={}`` removes nothing.
    The renderer therefore asks Codex which servers it would load, and turns each one off. It runs
    ``codex mcp list --json`` in the agent's cwd and environment, with the harness's own ``-c`` flags, so Codex applies
    its system, user, profile and campaign layers by its own rules. A user server with the same name as a role server
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

    def render(self, launch: McpLaunch) -> McpArgs:
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
        return McpArgs(args)

    def refuses(self, argv: list[str]) -> str | None:
        """Every argv with ``{mcp}`` in it takes servers."""
        return None


def tally(records: Iterable[Record]) -> tuple[Counts, int]:
    """The last ``token_count`` event with an ``info``. The number of turns is the number of these events."""
    counts = zero()
    turns = 0
    for rec in records:
        payload = child(rec, 'payload')
        if rec.get('type') != 'event_msg' or payload is None or payload.get('type') != 'token_count':
            continue
        info = child(payload, 'info')
        usage = child(info, 'total_token_usage') if info is not None else None
        if usage is None:
            continue
        turns += 1
        counts = zero()
        add(
            counts,
            usage,
            input='input_tokens',
            output='output_tokens',
            cache_read='cached_input_tokens',
            cache_write='cache_write_input_tokens',
        )
        counts.input -= counts.cache_read
        counts.total = as_int(usage.get('total_tokens'))
    return counts, turns


def said(rec: Record) -> str | None:
    """The ``output_text`` blocks of an assistant ``response_item`` message, including text between tool calls.

    ``{"type": "response_item", "payload": {"type": "message", "role": "assistant", "content": [{"type":
    "output_text", "text"}]}}`` in Codex 0.153 rollouts.
    """
    payload = child(rec, 'payload')
    if rec.get('type') != 'response_item' or payload is None or payload.get('role') != 'assistant':
        return None
    return texts(payload.get('content'), 'output_text')


class Codex(Harness):
    """``codex exec --json``, which reports its session id (the thread id) on its first line.

    Codex files its sessions by date, not by cwd: the thread id in the name of ``<home>/sessions/*/*/*/rollout-*-<id>.jsonl``
    finds the file. A ``token_count`` event with running totals follows every response, and the last one is the total.
    The convention is OpenAI's: ``input_tokens`` includes the cached part, which piloop subtracts, so that ``input`` is
    the uncached input, as for the other harnesses. Checked against Codex 0.153.
    """

    executable: str = 'codex'
    headless: list[str] = ['exec', *FLAGS, PROMPT]
    resume: list[str] = ['exec', 'resume', *FLAGS, SESSION_ID, PROMPT]
    session_id_pattern: str | None = r'"thread_id":"([0-9a-f-]{36})"'
    home: str = '{env:CODEX_HOME:~/.codex}'

    def mcp(self) -> CodexMcp:
        return CodexMcp()

    def sessions(self) -> JsonlSessions:
        return JsonlSessions(f'{self.home}/sessions/*/*/*/rollout-*-{{session_id}}.jsonl', no_cwd, tally, said)

    def restore(self, session_id: str, cwd: Path) -> Path:
        """A rollout under a date folder, with a timestamped name.

        ``exec resume`` takes no other (measured with Codex 0.164.0). The date says nothing; the thread id finds the file.
        """
        name = f'sessions/2000/01/01/rollout-2000-01-01T00-00-00-{session_id}.jsonl'
        return Path(expand_env(f'{self.home}/{name}')).expanduser()
