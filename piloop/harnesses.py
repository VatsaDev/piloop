"""Built-in coding-agent harness definitions. A campaign may override any of them in ``piloop.toml``."""

from enum import StrEnum
from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator

from piloop.mcp import MCP, RENDERERS
from piloop.usage import UsageSpec

SESSION_ID = '{session_id}'
PROMPT = '{prompt}'
CLAUDE_SETTINGS = (
    '{"autoMemoryEnabled":false,"env":{"BASH_DEFAULT_TIMEOUT_MS":"300000","BASH_MAX_TIMEOUT_MS":"600000"}}'
)
"""``--settings`` for every Claude agent. They take precedence over the user's own settings.

Auto-memory is off. Claude keeps it per repo (``~/.claude/projects/<cwd>/memory``), so the notes of one agent would
load into every later agent of the campaign, outside piloop's database and brief.

A foreground Bash command moves to the background after 5 minutes, and after 10 minutes at most. Without this, a
user's long ``BASH_DEFAULT_TIMEOUT_MS`` lets an agent block its turn on one command for an hour. pi's bash tool has no
default timeout, so there is nothing to set for pi."""
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


class Invocation(StrEnum):
    """Which argument list of a harness a runtime starts (``Runtime.invocation``)."""

    INTERACTIVE = 'interactive'
    """``Harness.interactive``: a session in a terminal. The runtime types the first prompt after the start."""
    HEADLESS = 'headless'
    """``Harness.headless``: a process without a terminal. The first prompt is in the arguments."""


def takes_session_id(args: list[str]) -> bool:
    """Whether an argument list passes a piloop-minted session id to the agent."""
    return any(SESSION_ID in a for a in args)


def fill(template: str, **values: str) -> str:
    """Substitute ``{name}`` placeholders without ``str.format``, so that other braces in the argv stay as they are."""
    for key, value in values.items():
        template = template.replace(f'{{{key}}}', value)
    return template


class QueueKey(BaseModel):
    """``queue_key``: the key that queues a typed message for the end of the agent's turn.

    The message is typed into the composer, then the key is pressed. A harness that reads a burst of input as a paste
    swallows a key that arrives inside the burst, so the key waits until the text is drawn, and is pressed again while
    ``pending`` is still at the bottom of the screen, which shows that the agent did not take it yet.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    key: str
    """The key, as ``herdr agent send-keys`` names it, for example ``tab``."""
    pending: str
    """Text at the bottom of the screen while the composer still holds the typed message."""


class Harness(BaseModel):
    """``[harness.<name>]``: how to launch one coding agent.

    piloop substitutes these placeholders in both argument lists: ``{model}``, ``{session_id}``, ``{cwd}`` (the
    agent's working directory), ``{log_dir}`` (the directory of the agent's brief and logs) and ``{prompt}`` (the first
    prompt, which names the file that holds the brief). An element that is exactly ``{mcp}`` expands to the role's MCP
    servers, in the form that ``mcp`` names. The built-in harnesses run without permission prompts: agents run
    in worktrees that the daemon owns and throws away, and a sandbox that cannot reach the repo's ``.git`` cannot
    commit. The built-in Claude harness passes ``--settings`` that move a foreground Bash command to the background
    after 5 minutes.

    When the argument list in use contains ``{session_id}``, piloop creates the session id. Otherwise the runtime
    reports it, or reads it with ``session_id_pattern``.
    When the agent ends, ``entire session attach`` takes this id, and ``usage`` uses it to find the session file and
    read the tokens the agent spent.
    """

    model_config = ConfigDict(extra='forbid', use_attribute_docstrings=True)

    executable: str
    """The program to run. A runtime can also use it to recognise the agent."""
    interactive: list[str] = []
    """The arguments for an interactive session, for a runtime whose ``invocation`` is ``interactive``. The runtime
    types the first prompt, which names the file that holds the brief, after the agent starts."""
    headless: list[str]
    """The arguments for a run without a terminal, for a runtime whose ``invocation`` is ``headless``. Must include
    ``{prompt}``."""
    resume: list[str] = []
    """The arguments that continue the session of a run without a terminal with a new prompt, for a ``headless``
    runtime. When the agent's turn ends with messages queued for it (comments, ``piloop prompt``), the runtime runs the
    harness again with these, ``{prompt}`` being the messages and ``{session_id}`` the session's id.
    Must include both. When empty, the agent takes no message while it runs."""
    startup_dialogs: list[tuple[str, list[str]]] = []
    """The dialogs the agent shows when it first starts in a new directory, in the order they appear. Each entry is
    the text to look for on the screen and the keys that accept the dialog."""
    busy_screens: list[str] | None = None
    """For an interactive runtime: texts at the bottom of a still screen that show that the agent works while the
    runtime reads it as idle, for example a prompt that the agent holds in its queue, or a retry of the API. When
    unset, those of the built-in harness with the same ``executable``, so an override of ``claude`` keeps them."""
    failed_turn_screens: list[str] | None = None
    """For an interactive runtime: texts at the bottom of the screen that show that the turn ended on an API error, not
    because the agent was done. The agent is then resumed. When unset, those of the built-in harness with the same
    ``executable``."""
    queue_key: QueueKey | None = None
    """For an interactive runtime: how to queue a message for the end of the agent's turn, for a harness that ends its
    turn to take a message submitted while it works. When unset, the queue key of the built-in harness with the same
    ``executable``, if it has one; else the message is submitted, and the agent queues it by itself."""
    composer: str | None = None
    """For an interactive runtime: the marker at the start of the agent's input line. The runtime reads the text after
    the last one to tell a first prompt that the TUI left typed but not submitted, and submits it with Enter. When
    unset, the marker of the built-in harness with the same ``executable``; without one, a prompt is never submitted
    again."""
    entire_agent: str | None = None
    """Entire's name for this agent, as ``entire agent list`` shows it. When it is set and ``entire`` is installed,
    the agent's session is attached to Entire as a checkpoint when the agent ends. When unset, the session is not
    captured."""
    session_id_pattern: str | None = None
    """A regex with one group that finds the session id in the first lines of the process's stdout. Use it in a
    ``headless`` runtime for a harness that cannot take its id as an argument (``{session_id}``). For example, Codex
    ``exec --json`` prints ``{"type":"thread.started","thread_id":"<uuid>"}`` first."""
    usage: UsageSpec | None = None
    """``[harness.<name>.usage]``: how to read the tokens an agent spent, when it ends, into ``agent_sessions.usage``. When
    unset, token usage is not recorded."""
    mcp: str | None = None
    """The renderer that expands the ``{mcp}`` element of the argv into the role's ``[mcp.<name>]`` servers. Each
    renderer also keeps the user's own servers out, where its harness allows it.
    When unset, the harness takes no servers, and a role that lists servers cannot use it."""

    def argv(self, invocation: Invocation) -> list[str]:
        """The argument list that a runtime of ``invocation`` starts."""
        return {Invocation.INTERACTIVE: self.interactive, Invocation.HEADLESS: self.headless}[invocation]

    def refuses_servers(self, invocation: Invocation) -> str | None:
        """Return why the argument list of ``invocation`` cannot take MCP servers, or None when it can."""
        argv = self.argv(invocation)
        if self.mcp is None or MCP not in argv:
            return f'it takes no MCP servers in its {invocation} argv; give it mcp = "<renderer>" and a {MCP} element'
        return RENDERERS[self.mcp].refuses(argv)

    @property
    def screens(self) -> tuple[tuple[str, ...], tuple[str, ...], QueueKey | None, str | None]:
        """The ``busy_screens``, ``failed_turn_screens``, ``queue_key`` and ``composer`` in use.

        Each one that this definition leaves unset comes from the built-in harness with the same ``executable``.
        """
        built_in = next((h for h in DEFAULT_HARNESSES.values() if h.executable == self.executable), self)
        busy = self.busy_screens if self.busy_screens is not None else built_in.busy_screens
        failed = self.failed_turn_screens if self.failed_turn_screens is not None else built_in.failed_turn_screens
        return (
            tuple(busy or ()),
            tuple(failed or ()),
            self.queue_key or built_in.queue_key,
            self.composer or built_in.composer,
        )

    @property
    def captures_session(self) -> bool:
        """Whether the agent's session id is needed when it ends: for Entire, for token usage, or for both."""
        return bool(self.entire_agent or self.usage)

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.mcp is not None and self.mcp not in RENDERERS:
            raise ValueError(f'unknown mcp renderer {self.mcp!r}; known: {sorted(RENDERERS)}')
        if self.resume and not all(any(p in a for a in self.resume) for p in (SESSION_ID, PROMPT)):
            raise ValueError(f'resume must include {SESSION_ID} and {PROMPT}')
        if self.mcp is None and MCP in [*self.interactive, *self.headless, *self.resume]:
            raise ValueError(f'{MCP} in the argv needs mcp set to a renderer: {sorted(RENDERERS)}')
        if self.captures_session and self.session_id_pattern is None and not takes_session_id(self.headless):
            raise ValueError(
                f'headless must pass {SESSION_ID} to the agent or session_id_pattern must be set when entire_agent'
                ' or usage is set'
            )
        return self


DEFAULT_HARNESSES: dict[str, Harness] = {
    'pi': Harness(
        executable='pi',
        # the user's extensions stay on: Herdr detects whether pi is working or idle through one of them
        interactive=['--session-id', SESSION_ID, '--model', '{model}', '--thinking', 'high', MCP],
        # no --session-dir: entire finds the session only in pi's own store (~/.pi/agent/sessions/<cwd>)
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
            'high',
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
            'high',
            MCP,
            '{prompt}',
        ],
        entire_agent='pi',
        usage=UsageSpec(
            file='{home}/.pi/agent/sessions/{cwd}/*_{session_id}.jsonl', cwd='dash_separators_wrapped', tally='pi'
        ),
        mcp='pi',
    ),
    'claude': Harness(
        executable='claude',
        interactive=[
            '--session-id',
            SESSION_ID,
            '--model',
            '{model}',
            '--effort',
            'high',
            '--dangerously-skip-permissions',
            MCP,
            '--settings',
            CLAUDE_SETTINGS,
        ],
        headless=[
            '-p',
            '--output-format',
            'json',
            '--session-id',
            SESSION_ID,
            '--model',
            '{model}',
            '--effort',
            'high',
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
            'high',
            '--dangerously-skip-permissions',
            MCP,
            '--settings',
            CLAUDE_SETTINGS,
            '{prompt}',
        ],
        startup_dialogs=[('trust this folder', ['down', 'enter'])],
        # Herdr reads Claude Code's state from the title spinner only, which stops while it holds a queued prompt,
        # waits to retry the API (``Retrying in 0s`` can hold still) or thinks at length with no timer on screen.
        # A ``503 included_quota_unknown`` is a busy screen while the retries count down, and a failed turn after.
        # TODO: remove a busy screen once Herdr detects that state for Claude Code.
        busy_screens=['Press up to edit queued messages', 'Waiting for API response', 'Retrying in', '… (thinking'],
        failed_turn_screens=['API Error', 'included_quota_unknown'],
        composer='❯',
        entire_agent='claude-code',
        usage=UsageSpec(
            file='{env:CLAUDE_CONFIG_DIR:~/.claude}/projects/{cwd}/{session_id}.jsonl',
            cwd='dash_non_alnum',
            tally='claude',
        ),
        mcp='claude',
    ),
    'codex': Harness(
        executable='codex',
        # when a newer codex is released, codex opens an "Update available" dialog before the prompt; agents skip it
        interactive=[
            '--model',
            '{model}',
            '-c',
            'model_reasoning_effort="high"',
            '-c',
            'check_for_update_on_startup=false',
            '-c',
            NO_CODEX_MEMORY,
            *CODEX_TRUST,
            *CODEX_TOOL_ENV,
            '--sandbox',
            'danger-full-access',
            '--ask-for-approval',
            'never',
            MCP,
        ],
        headless=[
            'exec',
            '--json',
            '--dangerously-bypass-approvals-and-sandbox',
            '--skip-git-repo-check',
            '--model',
            '{model}',
            '-c',
            'model_reasoning_effort="high"',
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
            'model_reasoning_effort="high"',
            '-c',
            NO_CODEX_MEMORY,
            *CODEX_TRUST,
            *CODEX_TOOL_ENV,
            MCP,
            SESSION_ID,
            '{prompt}',
        ],
        # the hooks review has two forms: a table with a key legend when the directory is already trusted, and a
        # numbered list ("1. Review hooks / 2. Trust all and continue") right after the trust dialog
        startup_dialogs=[
            ('Press t to trust all', ['t']),
            ('Hooks need review', ['down', 'enter']),
        ],
        # Codex holds a prompt submitted before its app server is up under "Waiting for startup", which can take 25 s
        # or more on a loaded host, and Herdr reports idle for all of it. TODO: remove it once Herdr detects that state.
        busy_screens=['Waiting for startup'],
        failed_turn_screens=[
            'Selected model is at capacity',
            'exceeded retry limit',
            'stream disconnected before completion',
            'unsupported websocket request type',
        ],
        # Codex ends its turn to take a message submitted with Enter mid-turn: it sends ``response.interrupt``, which
        # its websocket backend refuses with ``400 unsupported websocket request type``, and the turn is lost. Its
        # ``composer.queue`` key, Tab, queues the message as a follow-up that Codex submits when the turn ends, and
        # sends it at once when Codex is idle (verified with codex-cli 0.162.1 under Herdr 0.8.2).
        queue_key=QueueKey(key='tab', pending='tab to queue message'),
        composer='›',
        entire_agent='codex',
        session_id_pattern=r'"thread_id":"([0-9a-f-]{36})"',
        # codex files its sessions by date, not by cwd; the thread id in the file name finds the file
        usage=UsageSpec(file='{env:CODEX_HOME:~/.codex}/sessions/*/*/*/rollout-*-{session_id}.jsonl', tally='codex'),
        mcp='codex',
    ),
}
