"""Built-in coding-agent harness definitions. A project may override any of them in ``piloop.toml``."""

from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator

from piloop.mcp import MCP, RENDERERS
from piloop.usage import UsageSpec

SESSION_ID = '{session_id}'
CLAUDE_SETTINGS = (
    '{"autoMemoryEnabled":false,"env":{"BASH_DEFAULT_TIMEOUT_MS":"300000","BASH_MAX_TIMEOUT_MS":"600000"}}'
)
"""``--settings`` for every Claude agent. They take precedence over the user's own settings.

Auto-memory is off. Claude keeps it per repo (``~/.claude/projects/<cwd>/memory``), so the notes of one agent would
load into every later agent of the project, outside piloop's database and brief.

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
``shell_environment_policy`` says. piloop finds an agent's background jobs by ``PILOOP_WORKER_TOKEN``, and
``PILOOP_ACTOR`` signs the agent's writes. ``inherit = "core"``, the default ``*TOKEN*`` exclude or an
``include_only`` would drop them. The MCP renderer's ``shell_environment_policy.exclude`` keeps the role's API keys
out."""


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

    piloop substitutes ``{model}``, ``{session_id}`` and ``{cwd}`` (the agent's working directory) in both argument
    lists. It substitutes ``{prompt}`` (the first prompt, which names the file that holds the brief) and ``{log_dir}``
    in ``headless`` only. An element that is exactly ``{mcp}`` expands to the role's MCP servers, in the form that
    ``mcp`` names. The three built-in harnesses run without permission prompts: workers run in worktrees that the
    daemon owns and throws away, and a sandbox that cannot reach the repo's ``.git`` cannot commit. A Claude agent
    moves a foreground Bash command to the background after 5 minutes.

    When the argument list in use contains ``{session_id}``, piloop creates the session id. Otherwise the runtime
    reports it: Herdr detects it for every harness it knows, and the subprocess runtime needs ``session_id_pattern``.
    When the agent ends, ``entire session attach`` takes this id, and ``usage`` uses it to find the session file and
    read the tokens the agent spent.
    """

    model_config = ConfigDict(extra='forbid', use_attribute_docstrings=True)

    executable: str
    """The program to run. Herdr also uses it as ``--kind`` to detect the agent."""
    interactive: list[str] = []
    """The arguments for an interactive session (Herdr runtime). piloop types the first prompt, which names the file
    that holds the brief, after the agent starts."""
    headless: list[str]
    """The arguments for a run without a terminal (subprocess runtime). Must include ``{prompt}``."""
    startup_dialogs: list[tuple[str, list[str]]] = []
    """The dialogs the agent shows when it first starts in a new directory, in the order they appear. Each entry is
    the text to look for on the screen and the keys that accept the dialog."""
    entire_agent: str | None = None
    """Entire's name for this agent: ``pi``, ``claude-code`` or ``codex`` (``entire agent list`` shows them). When it is
    set and ``entire`` is installed, the agent's session is attached to Entire as a checkpoint when the agent ends.
    When unset, the session is not captured."""
    session_id_pattern: str | None = None
    """A regex with one group that finds the session id in the first lines of the process's stdout. Use it under the
    subprocess runtime for a harness that cannot take its id as an argument (``{session_id}``). For example, Codex
    ``exec --json`` prints ``{"type":"thread.started","thread_id":"<uuid>"}`` first."""
    usage: UsageSpec | None = None
    """``[harness.<name>.usage]``: how to read the tokens an agent spent, when it ends, into ``workers.usage``. See
    ``UsageSpec``. When unset, token usage is not recorded."""
    mcp: str | None = None
    """The renderer that expands the ``{mcp}`` element of the argv into the role's ``[mcp.<name>]`` servers:
    ``claude``, ``codex`` or ``pi``. Each renderer also keeps the user's own servers out, where its harness allows it.
    When unset, the harness takes no servers, and a role that lists servers cannot use it."""

    @property
    def captures_session(self) -> bool:
        """Whether the agent's session id is needed when it ends: for Entire, for token usage, or for both."""
        return bool(self.entire_agent or self.usage)

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.mcp is not None and self.mcp not in RENDERERS:
            raise ValueError(f'unknown mcp renderer {self.mcp!r}; known: {sorted(RENDERERS)}')
        if self.mcp is None and MCP in [*self.interactive, *self.headless]:
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
        startup_dialogs=[('trust this folder', ['down', 'enter'])],
        entire_agent='claude-code',
        usage=UsageSpec(file='{home}/.claude/projects/{cwd}/{session_id}.jsonl', cwd='dash_non_alnum', tally='claude'),
        mcp='claude',
    ),
    'codex': Harness(
        executable='codex',
        # when a newer codex is released, codex opens an "Update available" dialog before the prompt; workers skip it
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
        # the hooks review has two forms: a table with a key legend when the directory is already trusted, and a
        # numbered list ("1. Review hooks / 2. Trust all and continue") right after the trust dialog
        startup_dialogs=[
            ('Press t to trust all', ['t']),
            ('Hooks need review', ['down', 'enter']),
        ],
        entire_agent='codex',
        session_id_pattern=r'"thread_id":"([0-9a-f-]{36})"',
        # codex files its sessions by date, not by cwd; the thread id in the file name finds the file
        usage=UsageSpec(file='{home}/.codex/sessions/*/*/*/rollout-*-{session_id}.jsonl', tally='codex'),
        mcp='codex',
    ),
}
