"""Built-in coding-agent harness definitions. A campaign may override any of them in ``piloop.toml``."""

import json
from collections.abc import Mapping
from enum import StrEnum
from typing import Self, assert_never

from pydantic import BaseModel, ConfigDict, model_validator

from piloop import deprecated
from piloop.mcp import MCP, RENDERERS, EnvName
from piloop.usage import UsageSpec

SESSION_ID = '{session_id}'
PROMPT = '{prompt}'
EFFORT = '{effort}'
TAKE_MESSAGES = 'if [ -s "$PILOOP_MESSAGES" ]; then exec "$PILOOP_BIN" agent messages --format claude-hook; fi'
"""The ``PostToolUse`` hook of every Claude agent: it hands the agent the messages that wait for it, in its turn.

It checks the queue first, so a tool call with no message costs a file check, not a start of piloop."""
CLAUDE_SETTINGS = json.dumps(
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
user's long ``BASH_DEFAULT_TIMEOUT_MS`` lets an agent block its turn on one command for an hour. pi's bash tool has no
default timeout, so there is nothing to set for pi.

A message reaches the turn in progress (``TAKE_MESSAGES``)."""


class MessageFormat(StrEnum):
    """How ``piloop agent messages`` prints the messages that it takes: the hook of a harness names the one it reads."""

    LINES = 'lines'
    """One message per line."""
    CLAUDE_HOOK = 'claude-hook'
    """The output of a Claude Code ``PostToolUse`` hook, which gives the agent the messages (``additionalContext``)."""


def render_messages(messages: list[str], form: MessageFormat) -> str:
    """Return ``messages`` as ``form`` prints them."""
    lines = '\n'.join(messages)
    match form:
        case MessageFormat.LINES:
            return lines
        case MessageFormat.CLAUDE_HOOK:
            return json.dumps({'hookSpecificOutput': {'hookEventName': 'PostToolUse', 'additionalContext': lines}})
        case _:
            assert_never(form)


NO_CODEX_MEMORY = 'features.memories=false'
"""``-c`` for every Codex agent, for the same reason. Codex memories are off by default. The flag makes sure that a
user's config cannot turn them on for piloop's agents. pi has no memory store of its own."""
CODEX_TRUST = ['-c', 'projects={"{cwd}"={trust_level="trusted"}}']
"""Trust the agent's worktree for this launch only. Codex then never opens its "Trust this folder?" dialog (it checks
the directory before the repo root), and the user's ``config.toml`` is not written. The value is a table, not a dotted
key, because Codex splits ``-c`` keys on every ``.`` and the path can contain dots."""
CODEX_TOOL_ENV = [
    '-c',
    'shell_environment_policy.inherit="all"',
    '-c',
    'shell_environment_policy.ignore_default_excludes=true',
    '-c',
    'shell_environment_policy.include_only=[]',
]
"""Give Codex's shell commands the agent's environment, as Claude and pi do, whatever the user's
``shell_environment_policy`` says. piloop finds an agent's background jobs by ``PILOOP_PROCESS_TAG``, and
``PILOOP_ACTOR`` signs the agent's writes. ``inherit = "core"`` or an ``include_only`` would drop them, and the
default excludes (``*KEY*``, ``*SECRET*``, ``*TOKEN*``) other variables that Claude and pi pass on. The MCP renderer's
``shell_environment_policy.exclude`` keeps the role's API keys out."""


PI_HOME = '{env:PI_CODING_AGENT_DIR:~/.pi/agent}'
CLAUDE_HOME = '{env:CLAUDE_CONFIG_DIR:~/.claude}'
CODEX_HOME = '{env:CODEX_HOME:~/.codex}'


class SecretRef(BaseModel):
    """``{ secret = "NAME" }``: the value of the secret ``NAME`` (``piloop secret set``), in place of a literal."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    secret: EnvName
    """The name of the secret."""


def takes_session_id(args: list[str]) -> bool:
    """Whether an argument list passes a piloop-minted session id to the agent."""
    return any(SESSION_ID in a for a in args)


def fill(template: str, **values: str) -> str:
    """Substitute ``{name}`` placeholders without ``str.format``, so that other braces in the argv stay as they are."""
    for key, value in values.items():
        template = template.replace(f'{{{key}}}', value)
    return template


class Harness(BaseModel):
    """``[harness.<name>]``: how to launch one coding agent.

    piloop substitutes these placeholders in the argument lists: ``{model}``, ``{effort}`` (the role's ``effort``),
    ``{session_id}``, ``{cwd}`` (the agent's working directory), ``{log_dir}`` (the directory of the agent's brief and
    logs) and ``{prompt}`` (the first prompt, which names the file that holds the brief). An element that is exactly ``{mcp}`` expands to the role's MCP
    servers, in the form that ``mcp`` names. The built-in harnesses run without permission prompts: agents run
    in worktrees that the daemon owns and throws away, and a sandbox that cannot reach the repo's ``.git`` cannot
    commit. The built-in Claude harness passes ``--settings`` that move a foreground Bash command to the background
    after 5 minutes.

    When ``headless`` contains ``{session_id}``, piloop creates the session id. Otherwise the runtime
    reports it, or reads it with ``session_id_pattern``.
    When the agent ends, ``usage`` uses this id to find the session file and read the tokens the agent spent.

    A table named after a built-in harness changes the keys that it sets, and keeps the built-in's other keys.
    """

    model_config = ConfigDict(extra='forbid', use_attribute_docstrings=True)

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
    usage: UsageSpec | None = None
    """``[harness.<name>.usage]``: how to read the tokens an agent spent, when it ends, into ``agent_sessions.usage``. When
    unset, token usage is not recorded."""
    home: str | None = None
    """The directory where the agent keeps its config and sessions, as ``{env:NAME:default}``. piloop's tests move it
    to a temporary directory through ``NAME``."""
    session_env: list[str] = []
    """The variables the agent sets for the processes of its session. piloop clears them for every agent, so an agent
    started from inside such a session is not its child."""
    mcp: str | None = None
    """The renderer that expands the ``{mcp}`` element of the argv into the role's ``[mcp.<name>]`` servers. Each
    renderer also keeps the user's own servers out, where its harness allows it.
    When unset, the harness takes no servers, and a role that lists servers cannot use it."""
    env: dict[EnvName, str | SecretRef] = {}
    """Variables of the agent's environment: a value, or a secret, ``{ secret = "NAME" }``. For example a model
    gateway, ``{ ANTHROPIC_BASE_URL = "https://gw.example.com", ANTHROPIC_API_KEY = { secret = "ANTHROPIC_API_KEY" }
    }``. When unset, the agent uses the login of the machine that runs it."""

    @property
    def literals(self) -> dict[str, str]:
        """The variables of ``env`` that have a value."""
        return {name: value for name, value in self.env.items() if isinstance(value, str)}

    @property
    def secrets(self) -> dict[EnvName, EnvName]:
        """The variables of ``env`` that take a secret, with the secret's name."""
        return {name: value.secret for name, value in self.env.items() if isinstance(value, SecretRef)}

    def refuses_servers(self) -> str | None:
        """Return why ``headless`` cannot take MCP servers, or None when it can."""
        if self.mcp is None or MCP not in self.headless:
            return f'it takes no MCP servers in its headless argv; give it mcp = "<renderer>" and a {MCP} element'
        return RENDERERS[self.mcp].refuses(self.headless)

    @property
    def needs_session_id(self) -> bool:
        """Whether the agent's session id is needed when it ends: ``usage`` finds the session file by it."""
        return self.usage is not None

    @model_validator(mode='before')
    @classmethod
    def _removed(cls, data: object) -> object:
        if isinstance(data, Mapping) and (removed := deprecated.removed_harness_keys(data)) is not None:
            raise ValueError(removed)
        return data

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.mcp is not None and self.mcp not in RENDERERS:
            raise ValueError(f'unknown mcp renderer {self.mcp!r}; known: {sorted(RENDERERS)}')
        if self.resume and not all(any(p in a for a in self.resume) for p in (SESSION_ID, PROMPT)):
            raise ValueError(f'resume must include {SESSION_ID} and {PROMPT}')
        if self.mcp is None and MCP in [*self.headless, *self.resume]:
            raise ValueError(f'{MCP} in the argv needs mcp set to a renderer: {sorted(RENDERERS)}')
        if self.needs_session_id and self.session_id_pattern is None and not takes_session_id(self.headless):
            raise ValueError(
                f'headless must pass {SESSION_ID} to the agent or session_id_pattern must be set when usage is set'
            )
        return self


DEFAULT_HARNESSES: dict[str, Harness] = {
    'pi': Harness(
        executable='pi',
        # no --session-dir: usage reads the session from pi's own store (~/.pi/agent/sessions/<cwd>)
        headless=[
            '-p',
            '--mode',
            'json',
            '--no-extensions',
            '--no-skills',
            '--no-context-files',
            '--approve',
            '--session-id',
            SESSION_ID,
            '--model',
            '{model}',
            '--thinking',
            EFFORT,
            MCP,
            '{prompt}',
        ],
        # --session-id continues the session when it exists (verified with pi 0.99.2)
        resume=[
            '-p',
            '--mode',
            'json',
            '--no-extensions',
            '--no-skills',
            '--no-context-files',
            '--approve',
            '--session-id',
            SESSION_ID,
            '--model',
            '{model}',
            '--thinking',
            EFFORT,
            MCP,
            '{prompt}',
        ],
        home=PI_HOME,
        usage=UsageSpec(
            file=f'{PI_HOME}/sessions/{{cwd}}/*_{{session_id}}.jsonl', cwd='dash_separators_wrapped', tally='pi'
        ),
        mcp='pi',
    ),
    'claude': Harness(
        executable='claude',
        headless=[
            '-p',
            '--output-format',
            'json',
            '--session-id',
            SESSION_ID,
            '--model',
            '{model}',
            '--effort',
            EFFORT,
            '--dangerously-skip-permissions',
            MCP,
            '--settings',
            CLAUDE_SETTINGS,
            '{prompt}',
        ],
        resume=[
            '-p',
            '--output-format',
            'json',
            '--resume',
            SESSION_ID,
            '--model',
            '{model}',
            '--effort',
            EFFORT,
            '--dangerously-skip-permissions',
            MCP,
            '--settings',
            CLAUDE_SETTINGS,
            '{prompt}',
        ],
        home=CLAUDE_HOME,
        # measured in the shell of a Claude Code 2.1 session; an agent with all of them empty still saves its transcript
        session_env=[
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
        ],
        usage=UsageSpec(
            file=f'{CLAUDE_HOME}/projects/{{cwd}}/{{session_id}}.jsonl',
            cwd='dash_non_alnum',
            tally='claude',
        ),
        mcp='claude',
    ),
    'codex': Harness(
        executable='codex',
        headless=[
            'exec',
            '--json',
            '--dangerously-bypass-approvals-and-sandbox',
            '--skip-git-repo-check',
            '--model',
            '{model}',
            '-c',
            f'model_reasoning_effort="{EFFORT}"',
            '-c',
            NO_CODEX_MEMORY,
            *CODEX_TRUST,
            *CODEX_TOOL_ENV,
            MCP,
            '{prompt}',
        ],
        resume=[
            'exec',
            'resume',
            '--json',
            '--dangerously-bypass-approvals-and-sandbox',
            '--skip-git-repo-check',
            '--model',
            '{model}',
            '-c',
            f'model_reasoning_effort="{EFFORT}"',
            '-c',
            NO_CODEX_MEMORY,
            *CODEX_TRUST,
            *CODEX_TOOL_ENV,
            MCP,
            SESSION_ID,
            '{prompt}',
        ],
        session_id_pattern=r'"thread_id":"([0-9a-f-]{36})"',
        # codex files its sessions by date, not by cwd; the thread id in the file name finds the file
        home=CODEX_HOME,
        usage=UsageSpec(file=f'{CODEX_HOME}/sessions/*/*/*/rollout-*-{{session_id}}.jsonl', tally='codex'),
        mcp='codex',
    ),
}
SESSION_ENV = frozenset(name for harness in DEFAULT_HARNESSES.values() for name in harness.session_env)
"""The session variables of every built-in harness, which the launcher clears for every agent with those of its own."""
