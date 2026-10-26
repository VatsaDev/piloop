"""``claude``: Claude Code."""

import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from piloop.mcp import McpServer
from piloop.plugins.harness.contract import EFFORT, MCP, PROMPT, SESSION_ID, Harness, McpArgs, McpLaunch
from piloop.plugins.harness.kit.jsonl import (
    Counts,
    JsonlSessions,
    Record,
    add,
    child,
    dash_non_alnum,
    texts,
    zero,
)
from piloop.settings import Settings, expand_env

TAKE_MESSAGES = 'if [ -s "$PILOOP_MESSAGES" ]; then exec "$PILOOP_BIN" agent messages --harness claude; fi'
"""The ``PostToolUse`` hook of every Claude agent: it hands the agent the messages that wait for it, in its turn.

It checks the queue first, so a tool call with no message costs a file check, not a start of piloop."""
SETTINGS = json.dumps(
    {
        'autoMemoryEnabled': False,
        'env': {'BASH_DEFAULT_TIMEOUT_MS': '300000', 'BASH_MAX_TIMEOUT_MS': '600000'},
        'hooks': {'PostToolUse': [{'hooks': [{'type': 'command', 'command': TAKE_MESSAGES}]}]},
    },
    separators=(',', ':'),
)
"""``--settings`` for every Claude agent. They take precedence over the user's own settings.

Auto-memory is off. Claude keeps it per repo (``~/.claude/projects/<cwd>/memory``), so the notes of one agent would
load into every later agent of the campaign, outside piloop's database and brief.

A foreground Bash command moves to the background after 5 minutes, and after 10 minutes at most. Without this, a
user's long ``BASH_DEFAULT_TIMEOUT_MS`` lets an agent block its turn on one command for an hour.

A message reaches the turn in progress (``TAKE_MESSAGES``)."""


def argv(session: str) -> list[str]:
    """The argv of a headless run (``--session-id``) or of a resumed one (``--resume``)."""
    return [
        '-p',
        '--output-format',
        'json',
        session,
        SESSION_ID,
        '--model',
        '{model}',
        '--effort',
        EFFORT,
        '--dangerously-skip-permissions',
        MCP,
        '--settings',
        SETTINGS,
        PROMPT,
    ]


def server(s: McpServer) -> dict[str, object]:
    """One entry of ``mcpServers``."""
    if s.url is not None:
        headers = s.expanded_headers()
        return {'type': 'http', 'url': s.url, **({'headers': headers} if headers else {})}
    assert s.command is not None, 'a stdio server'
    env = s.expanded_env()
    return {'type': 'stdio', 'command': s.command[0], 'args': s.command[1:], **({'env': env} if env else {})}


@dataclass(frozen=True)
class ClaudeMcp:
    """``--strict-mcp-config --mcp-config '<inline JSON>'``.

    Strict mode gives the agent the role's servers and none of the user's. API keys are ``${NAME}`` references, which
    Claude expands from its own environment when it loads the config. ``--mcp-config`` takes several values, so
    another flag must follow ``{mcp}`` in the argv, not the prompt.
    """

    def render(self, launch: McpLaunch) -> McpArgs:
        """Strict mode also without servers, so that the agent then has no servers at all."""
        if not launch.servers:
            return McpArgs(['--strict-mcp-config'])
        config = {'mcpServers': {name: server(s) for name, s in launch.servers.items()}}
        return McpArgs(['--strict-mcp-config', '--mcp-config', json.dumps(config)])

    def refuses(self, argv: list[str]) -> str | None:
        """Every argv with ``{mcp}`` in it takes servers."""
        return None


def tally(records: Iterable[Record]) -> tuple[Counts, int]:
    """Add up the usage of each distinct ``message.id``. ``total`` is the sum of the four fields."""
    counts = zero()
    seen: set[object] = set()
    for rec in records:
        message = child(rec, 'message')
        if rec.get('type') != 'assistant' or message is None:
            continue
        usage = child(message, 'usage')
        if usage is None or message.get('id') in seen:
            continue
        seen.add(message.get('id'))
        add(
            counts,
            usage,
            input='input_tokens',
            output='output_tokens',
            cache_read='cache_read_input_tokens',
            cache_write='cache_creation_input_tokens',
        )
    counts.total = counts.input + counts.output + counts.cache_read + counts.cache_write
    return counts, len(seen)


def said(rec: Record) -> str | None:
    """The ``text`` blocks of an ``assistant`` line. Thinking and ``tool_use`` blocks are not text for the human."""
    message = child(rec, 'message')
    return texts(message.get('content'), 'text') if rec.get('type') == 'assistant' and message is not None else None


class Claude(Harness):
    """``claude -p`` with JSON output, the session id that piloop makes, and ``--settings`` of its own (``SETTINGS``).

    Its session file is ``<home>/projects/-<cwd>/<session id>.jsonl``. Streaming writes one ``assistant`` line for
    each content block, each with the usage of the whole response, so each ``message.id`` counts once. The convention
    is Anthropic's: ``input`` does not include cached tokens. Checked against Claude Code 2.1. Its hook takes the
    messages for the agent in the turn, and it reads user skills from its own directory only.
    """

    executable: str = 'claude'
    headless: list[str] = argv('--session-id')
    resume: list[str] = argv('--resume')
    home: str = '{env:CLAUDE_CONFIG_DIR:~/.claude}'
    # measured in the shell of a Claude Code 2.1 session; an agent with all of them empty still saves its transcript
    session_env: list[str] = [
        'CLAUDECODE',
        'CLAUDE_CODE_CHILD_SESSION',
        'CLAUDE_CODE_ENTRYPOINT',
        'CLAUDE_CODE_EXECPATH',
        'CLAUDE_CODE_MESSAGING_SOCKET',
        'CLAUDE_CODE_MESSAGING_TOKEN',
        'CLAUDE_CODE_SESSION_ATTENDED',
        'CLAUDE_CODE_SESSION_ID',
        'CLAUDE_EFFORT',
        'CLAUDE_PID',
    ]

    def mcp(self) -> ClaudeMcp:
        return ClaudeMcp()

    def sessions(self) -> JsonlSessions:
        return JsonlSessions(f'{self.home}/projects/{{cwd}}/{{session_id}}.jsonl', dash_non_alnum, tally, said)

    @classmethod
    def messages(cls, lines: list[str]) -> str:
        """The output of a ``PostToolUse`` hook, which gives the agent the messages (``additionalContext``)."""
        return json.dumps(
            {'hookSpecificOutput': {'hookEventName': 'PostToolUse', 'additionalContext': '\n'.join(lines)}}
        )

    def skills_dir(self, settings: Settings) -> Path:
        """``claude_skills_dir``, else ``<home>/skills``."""
        return settings.claude_skills_dir or Path(expand_env(f'{self.home}/skills')).expanduser()
