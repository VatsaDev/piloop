"""Agents as Herdr agents: one Herdr session for piloop, one workspace per campaign, one tab per agent.

Every ``herdr`` call targets the session that ``herdr_settings.session`` names (``--session``). piloop starts its
headless server when it is not running. Nothing piloop starts lands in the session the human uses for other work.

The workspace has the campaign name as its label and the campaign path as its ``root`` metadata token, because Herdr
keeps no path on a workspace. Its first tab is ``home``: a shell on the left for the human and ``piloop watch`` on the
right. Each agent gets its own tab after it, labelled with ``spec.label`` (``agent-12 idea#5 stacking``). The tab's
root pane runs the agent. Tabs are used instead of splits, so every agent is equally readable and closing an agent
removes exactly its tab. A human selects workspace and tab in the Herdr sidebar, or presses ``enter`` on an agent in
``piloop watch``. The daemon addresses agents by ``spec.name``.

An agent is finished when it is idle, nothing it started still runs, and its first turn is past the startup grace
(``startup_grace_seconds``), within which a quiet agent may still be starting. Harnesses run tool commands in
sessions of their own (Claude Code's background shells, Codex's commands). The agent, its MCP servers and its helpers
share the session of the pane's shell. Every process started in the pane inherits ``TAG_ENV``. So a tagged process
in another session is work that the agent started and still waits on, even after its parent shell exited.

``herdr`` prints ``{"result": …}`` JSON on stdout, and ``{"error": {"code", "message"}}`` on stderr with exit status
1. Exceptions: ``pane run`` prints nothing, ``read`` prints the terminal text, and ``session list --json`` prints
``{"sessions": […]}``.
"""

import json
import os
import shlex
import shutil
import subprocess
import sys
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, Self
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, InitSettingsSource, PydanticBaseSettingsSource, SettingsConfigDict

from piloop import deprecated, settings as settings_module
from piloop.harnesses import Harness, Invocation, QueueKey
from piloop.plugins.runtime.contract import (
    TAG_ENV,
    AgentExit,
    AgentSpec,
    AgentStatus,
    Check,
    ExitStatus,
    Handle,
    NotSteerable,
    RuntimeFailure,
    StartFailed,
)
from piloop.plugins.runtime.kit.tags import kill_tagged, tagged
from piloop.proc import PILOOP_ARGV, child_env
from piloop.settings import runtime_settings

HERDR = 'herdr'
STATUS_MAP = {
    'idle': AgentStatus.IDLE,
    'done': AgentStatus.IDLE,
    'working': AgentStatus.WORKING,
    'blocked': AgentStatus.BLOCKED,
    'unknown': AgentStatus.WORKING,
}
GONE_CODES = {'agent_not_found', 'agent_not_running', 'pane_not_found', 'tab_not_found', 'workspace_not_found'}
BLOCKED_ERROR = 'agent is blocked waiting for input'
SERVER_NOT_RUNNING = 'server_not_running'
SERVER_UNAVAILABLE = 'server_unavailable'
"""Herdr's code when its server took the request but did not answer in time, e.g. on a loaded host."""
RETRIED_READS = frozenset(
    {
        ('agent', 'wait'),
        ('agent', 'get'),
        ('agent', 'read'),
        ('pane', 'read'),
        ('pane', 'process-info'),
        ('workspace', 'list'),
        ('session', 'list'),
        ('tab', 'close'),
    }
)
"""The ``herdr`` commands that are retried while the server does not answer (``herdr_settings.unavailable_seconds``).

Only reads, and ``tab close``, which is the same when sent twice. A prompt, keys or a new tab may have been applied by a
server that did not answer, so they are never sent again."""
UNAVAILABLE_BACKOFF_SECONDS = 1
UNAVAILABLE_MAX_BACKOFF_SECONDS = 30
SHELL_READY_POLL_SECONDS = 0.5
DIALOG_REDRAW_POLL_SECONDS = 0.25
SERVER_POLL_SECONDS = 0.1
PICKUP_POLL_SECONDS = 0.25
HOME_TAB = 'home'
"""The label of the first tab of a campaign workspace: a shell beside ``piloop watch``."""
SHELL_SHARE = 0.5
"""The shell pane's share of the home tab's width (``pane split --ratio``); ``piloop watch`` gets the rest."""
ROOT_TOKEN = 'root'
"""The workspace metadata token that holds the campaign path, so two directories with the same name get two
workspaces."""
METADATA_SOURCE = 'piloop'
JOBS_POLL_SECONDS = 5
"""How often to check the running jobs of an idle agent until they end."""
BLOCKED_GRACE_SECONDS = 600
"""How long the screen of a blocked agent can hold still before the agent counts as finished.

Claude Code asks about some commands (``rm -rf $VAR/…``) even under ``--dangerously-skip-permissions``, and denies
them by itself after about four minutes. A screen that changes starts the grace again: Codex's
``request_user_input_async`` shows a question that Herdr reads as blocked while the agent keeps working.
"""
JOBS_DONE_PROMPT = (
    '[piloop] The jobs you left running have ended. Read their results and carry on with your idea; your run ends when'
    ' you stop with nothing left running.'
)
STARTUP_SCREEN_LINES = 500
"""How many lines of a pane that failed to start go into its ``transcript.txt``: enough for all of its output."""
QUIET_SECONDS = 3
"""How long the screen of an agent that Herdr reports idle must hold still before the agent counts as idle.

Claude Code reports idle as soon as a prompt is typed into it while it streams, before it draws the queued prompt. A
streaming answer or a spinner's timer changes the screen within this time.
"""
SCREEN_TAIL_LINES = 12
"""How many non-blank lines at the bottom of an idle agent's screen to search for the ``busy_screens`` and
``failed_turn_screens`` of its harness. This covers the prompt box and the last output above it. The scrollback is not searched,
because an old banner or the agent's own output could match there."""


class HerdrSettings(BaseSettings):
    """Settings of the Herdr runtime: ``[runtimes.herdr]`` in ``config.toml``. ``PILOOP_HERDR_*`` variables win."""

    model_config = SettingsConfigDict(env_prefix='PILOOP_HERDR_', frozen=True, use_attribute_docstrings=True)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Arguments, then the environment, then ``[runtimes.herdr]`` of ``config.toml``."""
        return init_settings, env_settings, InitSettingsSource(settings_cls, runtime_settings(HERDR))

    session: str = 'piloop'
    """The name of the Herdr session that piloop owns. It has one workspace for each campaign, and every agent that
    piloop starts runs there, never in the session you use for other work. ``piloop attach`` attaches a terminal to
    it. piloop starts its server when it first needs it."""

    socket_path: str | None = Field(default=None, validation_alias='HERDR_SOCKET_PATH')
    """The socket of the Herdr session that this process runs in. Herdr sets it in every pane it manages.
    ``piloop attach`` reads it to tell whether it already runs inside the piloop session."""

    server_start_seconds: float = 10
    """The number of seconds to wait for the Herdr server of the piloop session to answer after piloop starts it."""

    start_timeout_seconds: float = 60
    """The number of seconds Herdr may take to detect an agent that just started."""

    cli_timeout_seconds: float = 30
    """The timeout in seconds of one ``herdr`` CLI call, in addition to any wait that the call itself does."""

    unavailable_seconds: float = 300
    """The number of seconds to keep retrying a read of the Herdr server that does not answer (``server_unavailable``,
    or a ``herdr`` call that times out) before the read fails. A loaded host can stall the server for a while, and a
    failed read of a working agent ends it. Calls that change something (a prompt, keys, a new tab) are never retried,
    because the server may have applied them."""

    prompt_pickup_seconds: float = 5
    """The number of seconds to wait for an idle agent to start working again after piloop prompts it to resume, and
    for Herdr to report the session id of a started agent. The first prompt has ``startup_grace_seconds``."""

    startup_grace_seconds: float = 60
    """The number of seconds an agent has to start its first turn after its brief is submitted.

    Within it, an agent whose state has not changed yet is still starting, not a lost prompt, and a quiet idle screen
    is not a finished turn: under load a TUI can take many seconds to show that it took the brief. After it, a brief
    that was never picked up is lost, and an agent that never worked counts as failed to start."""

    shell_ready_seconds: float = 10
    """The number of seconds to retry ``agent start`` while the shell of a new tab is still starting."""

    wait_chunk_seconds: float = 10
    """The longest ``agent wait`` call, in seconds. piloop notices an agent whose tab was closed within this time."""


herdr_settings = HerdrSettings()


class Screens(BaseModel):
    """What the runtime looks for on the screen of an agent, from its harness (``Harness``), as its handle holds it.

    The handle has them from the start of the agent, so a daemon that re-attaches to it reads the same ones.
    """

    model_config = ConfigDict(extra='ignore', frozen=True)

    busy_screens: tuple[str, ...] = ()
    failed_turn_screens: tuple[str, ...] = ()
    queue_key: QueueKey | None = None
    composer: str | None = None

    @classmethod
    def of(cls, handle: Handle) -> Self:
        """Return the screens in ``handle``.

        A handle that an older daemon stored has none of them: it gets those of the built-in harness of its executable.
        """
        if 'busy_screens' in handle.data:
            return cls.model_validate(handle.data)
        deprecated.until('0.3.0', 'a Herdr handle without screens, from a daemon before 0.2.0: delete this fallback')
        busy, failed, queue, composer = Harness(executable=str(handle.data.get('kind')), headless=['{prompt}']).screens
        return cls(busy_screens=busy, failed_turn_screens=failed, queue_key=queue, composer=composer)


QUEUE_ECHO_SECONDS = 5
"""How long to wait for the typed message to be drawn before the queue key is pressed anyway."""
QUEUE_KEY_DELAY_SECONDS = 0.5
"""The pause before each press of the queue key, and before the screen is read to see whether it was taken."""
QUEUE_KEY_PRESSES = 3
QUEUE_ECHO_CHARS = 40
"""How much of the start of the message must be on screen (whitespace ignored: the composer wraps it) to count as
drawn."""


RESUME_PROMPT = '[piloop] Your last turn ended on an API error. Carry on with your idea from where you stopped.'
RESUME_MARK = RESUME_PROMPT[:46]
"""The part of ``RESUME_PROMPT`` that fits on one screen line. An error banner above it was already resumed."""
RESUME_BACKOFF_SECONDS = (60, 120, 240)
"""The wait before each resume of a turn that ended on an API error. Capacity errors usually clear within minutes."""
DRAFT_SUBMITS = 2
"""How many times a first prompt still sitting in the composer is submitted with Enter alone (``_deliver``). A paste
whose submit key the TUI lost leaves the exact brief there and starts no turn; it is never pasted again."""
DRAFT_ECHO_CHARS = 40
"""How much of the start of the prompt, whitespace ignored, the composer must hold to count as holding the draft."""
TURN_FAILED_ERROR = 'the agent stopped on an API error and did not recover'
PROMPTED_AT = 'prompted_at'
"""The handle key that holds when the first prompt was submitted (``time.time()``), for the startup grace.

Wall-clock time, because the handle is stored in ``agent_sessions.handle`` and a daemon restarted in place re-attaches to it.
A handle from a daemon older than this key gets no grace."""


class Screen(StrEnum):
    """What the screen of an agent that Herdr reports idle shows (``HerdrRuntime._idle_screen``)."""

    WORKING = 'working'
    FAILED_TURN = 'failed_turn'
    DONE = 'done'


@dataclass
class Block:
    """The grace of a blocked agent: when it last started and the screen at that time."""

    since: float
    screen: str


class HerdrError(RuntimeFailure):
    """A ``herdr`` command failed. ``code`` is Herdr's machine-readable error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f'{code}: {message}')
        self.code = code
        self.message = message


class HerdrRuntime:
    """Each agent in a tab of its own in the piloop Herdr session, where you can read it and type into it.

    It runs the ``herdr`` CLI against that session, and starts its server when needed.
    """

    kind: ClassVar[str] = 'herdr'
    invocation: ClassVar[Invocation] = Invocation.INTERACTIVE

    def __init__(self, campaign: str, campaign_path: Path, *, sleep: Callable[[float], None] = time.sleep) -> None:
        if shutil.which(HERDR) is None:
            raise HerdrError('not_installed', 'herdr is not on PATH; install it or set runtime = "subprocess"')
        self._campaign = campaign
        self._campaign_path = campaign_path
        self._workspace_id: str | None = None
        self._sleep = sleep
        """Used for every wait in this runtime's loops, so a test can count them without other threads' sleeps."""

    def start(self, spec: AgentSpec) -> Handle:
        """Create the agent's tab, start the agent in its pane, then submit the first prompt."""
        workspace_id = self._workspace()
        token = uuid4().hex
        tab = _call(
            'tab',
            'create',
            '--workspace',
            workspace_id,
            '--cwd',
            str(spec.cwd),
            '--label',
            spec.label,
            '--no-focus',
            '--env',
            f'{TAG_ENV}={token}',
        )
        pane_id = tab['root_pane']['pane_id']
        handle = Handle(
            self.kind,
            {
                'workspace_id': workspace_id,
                'tab_id': tab['tab']['tab_id'],
                'pane_id': pane_id,
                'agent': spec.name,
                'token': token,
                'kind': spec.executable,
                **Screens(
                    busy_screens=spec.busy_screens,
                    failed_turn_screens=spec.failed_turn_screens,
                    queue_key=spec.queue_key,
                    composer=spec.composer,
                ).model_dump(mode='json'),
            },
        )
        try:
            _export(pane_id, spec.env, spec.log_dir / f'env-{token}.fifo')
            self._start_agent(spec.name, spec.executable, spec.args, pane_id, spec.startup_dialogs)
            self._deliver(handle, spec.prompt)
        except Exception as e:  # kill() closes the pane, so read what the agent was stuck on first
            screen = _best_effort(lambda: _run('pane', 'read', pane_id, '--lines', str(STARTUP_SCREEN_LINES)))
            self.kill(handle)
            raise StartFailed(f'{type(e).__name__}: {e}', screen) from e
        return handle

    def _deliver(self, handle: Handle, prompt: str) -> None:
        """Submit the first prompt once and confirm that the agent took it within ``startup_grace_seconds``.

        A TUI that still draws its welcome screen can drop a prompt. Pickup is a state change (Herdr's
        ``state_change_seq`` advances after the prompt), not a state. On a loaded host the change can come many
        seconds after the agent started its turn: Codex records ``task_started`` a fraction of a second after the
        prompt while Herdr still sees no change 5 s later. So pickup has the whole startup grace, not a short window.
        An agent whose first-run dialogs were answered in a background tab already reports ``done``, and a wait for
        ``working`` or ``done`` would take that for a finished turn. Without a state change, a ``busy_screens`` or
        ``failed_turn_screens`` text at the bottom of the screen shows that the agent took the prompt while it starts up
        or retries the API. ``wait`` judges what follows. The prompt is never pasted again, because the composer or a
        queued turn may already hold it.

        A paste whose submit key the TUI lost leaves the exact prompt in the composer and starts no turn. After each
        pickup window (``prompt_pickup_seconds``) without a state change, a composer that holds the prompt
        (``_holds_draft``) gets Enter alone, up to ``DRAFT_SUBMITS`` times. The ``prompt_lost`` error says whether the
        composer still held it, and ``start`` keeps the screen.

        A prompt is usually lost because the host is too loaded to draw the TUI, or because the Herdr integration is
        missing. The screen that ``start`` keeps shows which.
        """
        agent = _agent(handle)
        before = self._state_seq(agent)
        _call('agent', 'prompt', agent, prompt)
        handle.data[PROMPTED_AT] = time.time()
        grace = time.monotonic() + herdr_settings.startup_grace_seconds
        submits = 0
        while True:
            window = max(0.0, min(herdr_settings.prompt_pickup_seconds, grace - time.monotonic()))
            if self._state_changed(agent, before, window):
                return
            if time.monotonic() >= grace:
                break
            if submits < DRAFT_SUBMITS and self._holds_draft(handle, prompt):
                _call('agent', 'send-keys', agent, 'enter')
                submits += 1
        tail = _tail(self._visible(handle))
        screens = Screens.of(handle)
        if any(text in tail for text in (*screens.busy_screens, *screens.failed_turn_screens)):
            return
        held = (
            f'its brief is still in the composer after {submits} presses of enter'
            if self._holds_draft(handle, prompt)
            else 'the composer does not hold it'
        )
        raise HerdrError('prompt_lost', f'{agent} did not start working after its brief was submitted; {held}')

    def _holds_draft(self, handle: Handle, prompt: str) -> bool:
        """Whether the agent's composer still holds ``prompt``, typed but not submitted, rather than an accepted turn.

        The composer is the text after the last ``composer`` marker of the agent's harness at the bottom of the screen,
        up to a rule line. A brief that the agent took sits above it, in the history, with an empty composer below.
        """
        marker = Screens.of(handle).composer
        if marker is None:
            return False
        lines = self._visible(handle).splitlines()
        start = next((i for i in range(len(lines) - 1, -1, -1) if lines[i].lstrip().startswith(marker)), None)
        if start is None:
            return False
        held: list[str] = []
        for line in lines[start:]:
            if held and (not line.strip() or set(line.strip()) <= set('─')):
                break
            held.append(line.lstrip().removeprefix(marker) if not held else line)
        draft = ''.join(prompt.split())[:DRAFT_ECHO_CHARS]
        return ''.join(''.join(held).split()).startswith(draft)

    def _state_seq(self, agent: str) -> int:
        return int(_call('agent', 'get', agent)['agent']['state_change_seq'])

    def _state_changed(self, agent: str, before: int, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while True:
            if self._state_seq(agent) > before:
                return True
            if time.monotonic() >= deadline:
                return False
            self._sleep(PICKUP_POLL_SECONDS)

    def _start_agent(
        self, name: str, kind: str, args: list[str], pane_id: str, startup_dialogs: list[tuple[str, list[str]]]
    ) -> None:
        """Run ``agent start``, retried while the new tab's shell starts, then answer the first-launch dialogs.

        Herdr reports some dialogs as ``idle``: the start succeeds with the dialog on screen. It reports others as
        ``agent_not_ready``: the start fails, but the name stays usable for ``send-keys``. Both cases end in the same
        screen check.
        """
        start = [
            'agent',
            'start',
            name,
            '--kind',
            kind,
            '--pane',
            pane_id,
            '--timeout',
            _ms(herdr_settings.start_timeout_seconds),
        ]
        deadline = time.monotonic() + herdr_settings.shell_ready_seconds
        grace = herdr_settings.cli_timeout_seconds
        ready = False
        while not ready:
            try:
                # herdr waits up to --timeout for the agent, so the CLI timeout must be longer
                _call(*start, '--', *args, cli_timeout=herdr_settings.start_timeout_seconds + grace)
                ready = True
            except HerdrError as e:
                if e.code == 'agent_pane_busy' and time.monotonic() < deadline:
                    self._sleep(SHELL_READY_POLL_SECONDS)
                    continue
                if e.code == 'agent_not_ready' and startup_dialogs:
                    break
                raise
        if startup_dialogs:
            self._accept_dialogs(name, pane_id, startup_dialogs, ready)

    def _accept_dialogs(self, agent: str, pane_id: str, dialogs: list[tuple[str, list[str]]], ready: bool) -> None:
        """Answer each configured dialog once, in the order it appears on screen, until no dialog is left."""
        remaining = list(dialogs)
        screen = self._screen(pane_id)
        while True:
            dialog = next((d for d in remaining if d[0] in screen), None)
            if dialog is None:
                if ready:
                    return
                raise HerdrError('agent_not_ready', f'{agent} is blocked on an unexpected startup dialog')
            remaining.remove(dialog)
            for key in dialog[1]:
                _call('agent', 'send-keys', agent, key)
            screen = self._await_redraw(agent, pane_id, dialog[0], remaining)
            result = self._agent_wait(agent, ['idle', 'done', 'blocked'], herdr_settings.start_timeout_seconds)
            ready = result['agent']['agent_status'] != 'blocked'

    def _await_redraw(self, agent: str, pane_id: str, answered: str, pending: list[tuple[str, list[str]]]) -> str:
        """Return the screen once the answered dialog is gone from it or a pending dialog is on it.

        ``agent wait`` cannot do this. Herdr reports these dialogs as ``idle``, so ``agent wait`` returns before the
        TUI redraws, and the next read would show the answered dialog again.
        """
        deadline = time.monotonic() + herdr_settings.start_timeout_seconds
        while True:
            screen = self._screen(pane_id)
            if answered not in screen or any(text in screen for text, _ in pending):
                return screen
            if time.monotonic() >= deadline:
                raise HerdrError('agent_not_ready', f'{agent} still shows a startup dialog after it was answered')
            self._sleep(DIALOG_REDRAW_POLL_SECONDS)

    def _screen(self, pane_id: str) -> str:
        return _run('pane', 'read', pane_id, '--source', 'visible')

    def wait(self, handle: Handle, timeout: float) -> AgentExit:
        """Wait until the agent is finished, or kill it at ``timeout``.

        The agent is finished when it is idle with no jobs running, or blocked on a still screen for
        ``BLOCKED_GRACE_SECONDS``. These states are not finished:

        - Idle with jobs running: the agent ended its turn to wait for them. When they end, an agent that did not
          resume by itself is prompted to read their results (``JOBS_DONE_PROMPT``).
        - Blocked, until the grace runs out on a screen that held still. A dialog can be answered or time out by
          itself, and an agent can keep working beside a question it queued.
        - Idle on a screen that changes within ``QUIET_SECONDS`` or shows a ``busy_screens`` text: Herdr misread a
          working agent.
        - Idle on a ``failed_turn_screens`` text: the agent is resumed. When the backoffs are spent, the outcome has
          ``TURN_FAILED_ERROR``.
        - Idle on a quiet screen within ``startup_grace_seconds`` of the first prompt (``PROMPTED_AT``): the
          agent may still be starting its first turn. A first-turn Codex can show its submitted brief above an empty
          composer, with no busy text, for seconds after its turn started.

        The wait is done in chunks. ``agent wait`` does not return when another thread closes the tab (``kill``), so
        each chunk asks again and gets ``agent_not_found`` once the agent is gone.
        """
        agent = _agent(handle)
        started = time.monotonic()
        deadline = started + timeout
        waited_on_jobs: int | None = None
        """``state_change_seq`` of the last idle state seen with jobs in flight."""
        block: Block | None = None
        resumes = 0
        while True:
            now = time.monotonic()
            remaining = deadline - now
            if remaining <= 0:
                self.kill(handle)
                return AgentExit(ExitStatus.TIMED_OUT, timeout, error=f'agent timeout after {timeout:g}s')
            chunk = min(remaining, herdr_settings.wait_chunk_seconds)
            window = self._window(handle, block, now, chunk)
            if isinstance(window, str):
                # the dialog is only on the visible screen, and the pane closes with the agent: keep it in the error
                return AgentExit(ExitStatus.EXITED, now - started, error=f'{BLOCKED_ERROR}:\n{window.strip()}')
            until, chunk = window
            try:
                info = self._agent_wait(agent, until, chunk)['agent']
            except HerdrError as e:
                if e.code in GONE_CODES:
                    return AgentExit(ExitStatus.EXITED, time.monotonic() - started, error=None)
                if e.code == 'timeout':
                    continue
                raise
            status = STATUS_MAP[info['agent_status']]
            if status is AgentStatus.BLOCKED:
                block = block or Block(time.monotonic(), self._visible(handle))
                continue
            block = None
            if status is AgentStatus.WORKING:
                continue
            seq = int(info['state_change_seq'])
            if self.jobs(handle):
                waited_on_jobs = seq
                self._sleep(min(JOBS_POLL_SECONDS, remaining))
                continue
            if waited_on_jobs == seq:  # idle since its jobs ended; prompt only once, so a refused prompt cannot loop
                waited_on_jobs = None
                if self._resume(agent, seq, JOBS_DONE_PROMPT):
                    continue
            says, tail = self._idle_screen(handle, remaining)
            if says is Screen.WORKING:
                continue
            if says is Screen.FAILED_TURN:
                if resumes == len(RESUME_BACKOFF_SECONDS):
                    return AgentExit(
                        ExitStatus.EXITED, time.monotonic() - started, error=f'{TURN_FAILED_ERROR}:\n{tail}'
                    )
                self._sleep(min(RESUME_BACKOFF_SECONDS[resumes], remaining))
                resumes += 1
                self._resume(agent, seq, RESUME_PROMPT)
                continue
            return AgentExit(ExitStatus.EXITED, time.monotonic() - started, error=None)

    def _window(self, handle: Handle, block: Block | None, now: float, chunk: float) -> tuple[list[str], float] | str:
        """Return the states the next ``agent wait`` stops on and for how long, or the screen of a stuck agent.

        A blocked agent is stuck when its screen held still for the whole grace. A screen that changed starts the grace
        again: the agent works beside a question it queued.
        """
        if block is None:
            return ['idle', 'done', 'blocked'], chunk
        grace = block.since + BLOCKED_GRACE_SECONDS - now
        if grace <= 0:
            screen = self._visible(handle)
            if screen == block.screen:
                return screen
            block.since, block.screen, grace = now, screen, BLOCKED_GRACE_SECONDS
        return ['working', 'idle', 'done'], min(chunk, grace)

    def _idle_screen(self, handle: Handle, remaining: float) -> tuple[Screen, str]:
        """Return what the screen of an agent that Herdr reports idle shows, and its bottom lines (``_tail``).

        The agent is working when the screen changed within ``QUIET_SECONDS`` (output streams in, a spinner's timer
        counts) or shows a ``busy_screens`` text, or when a quiet screen is still within the startup grace of the first
        prompt (``_starting``). A screen that is gone with its pane shows done: the next ``agent wait`` finds the agent
        gone.
        """
        first = self._visible(handle)
        self._sleep(min(QUIET_SECONDS, remaining))
        screen = self._visible(handle)
        tail = _tail(screen)
        screens = Screens.of(handle)
        if screen != first or any(text in tail for text in screens.busy_screens):
            return Screen.WORKING, tail
        if any(text in tail for text in screens.failed_turn_screens):
            return Screen.FAILED_TURN, tail
        if _starting(handle):
            return Screen.WORKING, tail
        return Screen.DONE, tail

    def _visible(self, handle: Handle) -> str:
        """Return the visible screen of the agent's pane, or an empty string once the pane is gone."""
        try:
            return self._screen(str(handle.data['pane_id']))
        except HerdrError as e:
            if e.code not in GONE_CODES:
                raise
            return ''

    def _resume(self, agent: str, seq: int, prompt: str) -> bool:
        """Return whether an idle agent starts to work again, by itself or after ``prompt``.

        Claude Code is told when one of its background shells ends, and resumes by itself. An orphaned job tells
        nobody. A refused prompt counts as resumed, because the next ``agent wait`` shows what the agent does.
        """
        pickup = herdr_settings.prompt_pickup_seconds
        if self._state_changed(agent, seq, pickup):
            return True
        try:
            _call('agent', 'prompt', agent, prompt)
        except HerdrError:
            return True
        return self._state_changed(agent, seq, pickup)

    def jobs(self, handle: Handle) -> list[str]:
        """Return ``<pid> <command line>`` of each live process that the agent started outside its own session.

        Empty for a handle without a token, and once the pane is gone.
        """
        token = handle.data.get('token')
        if token is None:
            return []
        try:
            shell = int(
                _call('pane', 'process-info', '--pane', str(handle.data['pane_id']))['process_info']['shell_pid']
            )
            session = os.getsid(shell)
        except HerdrError as e:
            if e.code in GONE_CODES:
                return []
            raise
        except ProcessLookupError:
            return []
        return [f'{pid} {cmd}' for pid, sid, cmd in tagged(f'{TAG_ENV}={token}') if sid != session]

    def _agent_wait(self, agent: str, until: list[str], timeout: float) -> dict[str, Any]:
        flags = [flag for state in until for flag in ('--until', state)]
        return _call(
            'agent',
            'wait',
            agent,
            *flags,
            '--timeout',
            str(int(timeout * 1000)),
            cli_timeout=timeout + herdr_settings.cli_timeout_seconds,
        )

    def status(self, handle: Handle) -> AgentStatus:
        """Return the state that Herdr detected."""
        try:
            result = _call('agent', 'get', _agent(handle))
        except HerdrError as e:
            if e.code in GONE_CODES:
                return AgentStatus.EXITED
            raise
        return STATUS_MAP[result['agent']['agent_status']]

    def session_id(self, handle: Handle) -> str | None:
        """Return the session id that Herdr detected for the agent (``agent_session.value`` of ``agent get``).

        Herdr learns it from the harness after the harness starts, so this polls for up to
        ``prompt_pickup_seconds``. None when the id does not appear or the agent is gone.
        """
        deadline = time.monotonic() + herdr_settings.prompt_pickup_seconds
        while True:
            try:
                session = _call('agent', 'get', _agent(handle))['agent'].get('agent_session')
            except HerdrError as e:
                if e.code in GONE_CODES:
                    return None
                raise
            if session:
                return str(session['value'])
            if time.monotonic() >= deadline:
                return None
            self._sleep(PICKUP_POLL_SECONDS)

    def prompt(self, handle: Handle, text: str) -> None:
        """Types the message into the agent's pane, where the agent keeps it until its turn ends.

        A harness that would end its turn to take the message has a queue key (``[harness.<name>] queue_key``), which
        piloop presses to queue it. While the agent waits on a dialog, the message is refused: answer the dialog in the
        agent's tab.
        """
        queue = Screens.of(handle).queue_key
        try:
            if queue is None:
                _call('agent', 'prompt', _agent(handle), text)
            else:
                self._queue(handle, text, queue)
        except HerdrError as e:
            if e.code == 'agent_blocked':
                raise NotSteerable(
                    f'{_agent(handle)} is blocked on an approval or question; read it and answer with herdr agent send-keys'
                ) from e
            raise

    def _queue(self, handle: Handle, text: str, queue: QueueKey) -> None:
        """Type ``text`` into the agent's composer as one line and press ``queue.key`` until the agent takes it.

        A newline would submit the text into the running turn, so lines are joined with spaces. A dialog would take the
        typed text as its answer, so a blocked agent gets nothing (``agent_blocked``, as ``agent prompt`` refuses it).
        A working agent took the text when ``queue.pending`` leaves the screen; an idle one starts a turn with it, which
        Herdr's ``state_change_seq`` shows.
        """
        agent, pane = _agent(handle), str(handle.data['pane_id'])
        info = _call('agent', 'get', agent)['agent']
        if info['agent_status'] == 'blocked':
            raise HerdrError('agent_blocked', f'agent {agent} is blocked')
        working, seq = info['agent_status'] == 'working', int(info['state_change_seq'])
        line = ' '.join(text.split())
        _call('pane', 'send-text', pane, line)
        echo = ''.join(line[:QUEUE_ECHO_CHARS].split())
        deadline = time.monotonic() + QUEUE_ECHO_SECONDS
        while echo not in ''.join(self._visible(handle).split()) and time.monotonic() < deadline:
            self._sleep(PICKUP_POLL_SECONDS)
        for _ in range(QUEUE_KEY_PRESSES):
            self._sleep(QUEUE_KEY_DELAY_SECONDS)
            _call('agent', 'send-keys', agent, queue.key)
            if working:
                self._sleep(QUEUE_KEY_DELAY_SECONDS)
                if queue.pending not in _tail(self._visible(handle)):
                    return
            elif self._state_changed(agent, seq, herdr_settings.prompt_pickup_seconds):
                return
        raise HerdrError('prompt_not_queued', f'{agent} still holds the message in its composer after {queue.key}')

    def read(self, handle: Handle, lines: int = 80) -> str:
        """Return the recent terminal output, unwrapped and without ANSI codes."""
        return _run('agent', 'read', _agent(handle), '--source', 'recent-unwrapped', '--lines', str(lines))

    def kill(self, handle: Handle) -> None:
        """Close the agent's tab, which ends its process, then kill what it left running. A gone tab is no error.

        A job in a session of its own does not get the pane's hangup signal, so it would outlive the agent.
        """
        _close_tab(str(handle.data['tab_id']))
        token = handle.data.get('token')
        if token is None:
            return
        kill_tagged(f'{TAG_ENV}={token}')

    def focus(self, handle: Handle) -> str:
        """Brings the agent's tab, and its workspace, to the front of the piloop Herdr session."""
        tab = str(handle.data['tab_id'])
        _call('tab', 'focus', tab)
        return f'focused tab {tab}'

    def attach(self, handle: Handle) -> str:
        """Focuses the agent's tab, then opens the piloop Herdr session in this terminal.

        In that session already, it only focuses the tab. You can then go to the other agents of the session.

        Note:
            The Herdr client replaces this process. Without a terminal on stdout, nothing replaces it.
        """
        focused = self.focus(handle)
        session = herdr_settings.session
        if not sys.stdout.isatty():
            return f'{focused}; not a terminal, so it does not attach to Herdr session {session}'
        if herdr_settings.socket_path == session_socket():
            return focused
        os.execvp(HERDR, [HERDR, '--session', session])

    @classmethod
    def view(cls, handle: Handle) -> str | None:
        """The agent's tab in the piloop Herdr session. None while the agent starts: its tab is not known yet."""
        tab = handle.data.get('tab_id')
        return None if tab is None else f'tab {tab}'

    @classmethod
    def checks(cls, executables: set[str]) -> list[Check]:
        """Check that Herdr is on PATH, its piloop session answers, and its hooks are current for ``executables``.

        Never start the server: a server that is not running starts on first use.
        """
        if shutil.which(HERDR) is None:
            return [Check('herdr', 'a campaign uses the herdr runtime and herdr is not on PATH', 'install herdr')]
        try:
            running = server_running()
        except (HerdrError, subprocess.TimeoutExpired, OSError) as e:
            server = Check('herdr', f'the piloop session does not answer: {e}', '`herdr server stop`, then retry')
        else:
            answers = 'the piloop session answers' if running else 'server not running; started on first use'
            server = Check('herdr', answers)
        return [server, _hooks(executables)]

    def _split_watch(self, pane_id: str) -> None:
        """Run ``piloop watch`` on this campaign in a new pane to the right of ``pane_id``.

        The watch uses this process's interpreter and ``PILOOP_HOME``, so it reads the daemon's database, whatever
        ``piloop`` the pane's shell finds first. Herdr starts a pane with its server's environment, not the caller's,
        so every ``PILOOP_*`` setting of this process goes in with ``--env``: the server may have been started by a
        human with other settings, or none.
        """
        env = {k: v for k, v in child_env().items() if k.startswith('PILOOP_')}
        env['PILOOP_HOME'] = str(settings_module.settings.home)
        split = _call(
            'pane',
            'split',
            pane_id,
            '--direction',
            'right',
            '--ratio',
            str(SHELL_SHARE),
            '--cwd',
            str(self._campaign_path),
            *(arg for k, v in env.items() for arg in ('--env', f'{k}={v}')),
            '--no-focus',
        )
        watch = [*PILOOP_ARGV, 'watch', str(self._campaign_path)]
        _call('pane', 'run', str(split['pane']['pane_id']), shlex.join(watch))

    def _workspace(self) -> str:
        """Return the campaign's workspace id. Find it by its ``root`` token or create it. Cached per instance."""
        if self._workspace_id is None:
            listed = _call('workspace', 'list')['workspaces']
            root = str(self._campaign_path)
            match = next((w for w in listed if w.get('tokens', {}).get(ROOT_TOKEN) == root), None)
            if match is None:
                match = self._create_workspace(listed)
            self._workspace_id = str(match['workspace_id'])
        return self._workspace_id

    def _create_workspace(self, existing: list[dict[str, Any]]) -> dict[str, Any]:
        """Create the workspace. Its label is ``<name> (<parent dir>)`` when another directory already has the name.

        Herdr opens a workspace with one tab labelled ``1``. It becomes ``home``: the watch goes in a split of its
        pane, and the pane stays a shell for the human.
        """
        label = self._campaign
        if any(w['label'] == label for w in existing):
            label = f'{label} ({self._campaign_path.parent.name})'
        created = _call('workspace', 'create', '--cwd', str(self._campaign_path), '--label', label, '--no-focus')
        workspace = created['workspace']
        _call(
            'workspace',
            'report-metadata',
            str(workspace['workspace_id']),
            '--source',
            METADATA_SOURCE,
            '--token',
            f'{ROOT_TOKEN}={self._campaign_path}',
        )
        _call('tab', 'rename', str(created['tab']['tab_id']), HOME_TAB)
        self._split_watch(str(created['root_pane']['pane_id']))
        return workspace


def _close_tab(tab_id: str) -> None:
    try:
        _call('tab', 'close', tab_id)
    except HerdrError as e:
        if e.code not in GONE_CODES:
            raise


def session_socket() -> str | None:
    """Return the socket path of the piloop session, or None when Herdr does not know that session name."""
    sessions = _as_dict(json.loads(_run('session', 'list', '--json')))['sessions']
    return next((str(s['socket_path']) for s in sessions if s['name'] == herdr_settings.session), None)


def server_running() -> bool:
    """Return whether the piloop session's server answers. Never start it, because ``piloop doctor`` is read-only.

    Raises:
        HerdrError: herdr answered with an error other than "server not running".
    """
    try:
        _cli('workspace', 'list')
    except HerdrError as e:
        if e.code != SERVER_NOT_RUNNING:
            raise
        return False
    return True


def integrations() -> dict[str, str]:
    """Return each agent kind that Herdr has hooks for, and the state of the hooks (``current``, ``not installed``).

    Parses ``herdr integration status``. The hooks are in the user's harness config, not in a session, so no server is
    asked.

    Raises:
        subprocess.CalledProcessError: herdr failed, for example because it is too old to have integrations.
    """
    out = subprocess.run(
        [HERDR, 'integration', 'status'],
        capture_output=True,
        text=True,
        timeout=herdr_settings.cli_timeout_seconds,
        check=True,
    ).stdout
    lines = (line.partition(': ') for line in out.splitlines())
    return {kind: state.rsplit(' (', 1)[0] for kind, sep, state in lines if sep}


def _hooks(executables: set[str]) -> Check:
    """Check that Herdr's hooks are installed and current for each harness in ``executables`` that Herdr knows.

    Without the hooks, Herdr reports no session for the agent. Then piloop cannot read its tokens, and a fast agent
    that committed can look dead at startup.
    """
    try:
        states = integrations()
    except (subprocess.SubprocessError, OSError) as e:
        return Check('herdr hooks', f'`herdr integration status` failed: {e}', 'upgrade herdr')
    known = sorted(executables & states.keys())
    if stale := [k for k in known if not states[k].startswith('current')]:
        return Check(
            'herdr hooks',
            ', '.join(f'{k}: {states[k]}' for k in stale),
            ', '.join(f'`herdr integration install {k}`' for k in stale),
        )
    return Check('herdr hooks', f'current for {", ".join(known) or "no harness Herdr has hooks for"}')


def _call(*args: str, cli_timeout: float | None = None) -> dict[str, Any]:
    """Run ``herdr <args>`` and return its ``result`` object, or an empty dict when the command prints nothing."""
    stdout = _run(*args, cli_timeout=cli_timeout)
    return _as_dict(json.loads(stdout)['result']) if stdout.strip() else {}


def _run(*args: str, cli_timeout: float | None = None) -> str:
    """Run ``_answered``. When the server is not running, start it and try once more."""
    try:
        return _answered(*args, cli_timeout=cli_timeout)
    except HerdrError as e:
        if e.code != SERVER_NOT_RUNNING:
            raise
    _start_server()
    return _answered(*args, cli_timeout=cli_timeout)


def _answered(*args: str, cli_timeout: float | None = None) -> str:
    """Run ``_cli``. Retry a ``RETRIED_READS`` command, with a doubling backoff, while the server does not answer.

    The server does not answer when it reports ``server_unavailable`` or when the call times out. After
    ``herdr_settings.unavailable_seconds``, the last error is raised.
    """
    if tuple(args[:2]) not in RETRIED_READS:
        return _cli(*args, cli_timeout=cli_timeout)
    deadline = time.monotonic() + herdr_settings.unavailable_seconds
    backoff = UNAVAILABLE_BACKOFF_SECONDS
    while True:
        try:
            return _cli(*args, cli_timeout=cli_timeout)
        except HerdrError as e:
            if e.code != SERVER_UNAVAILABLE or time.monotonic() >= deadline:
                raise
        except subprocess.TimeoutExpired:
            if time.monotonic() >= deadline:
                raise
        time.sleep(backoff)
        backoff = min(backoff * 2, UNAVAILABLE_MAX_BACKOFF_SECONDS)


def _cli(*args: str, cli_timeout: float | None = None) -> str:
    """Run ``herdr --session <piloop> <args>`` and return stdout. Raise ``HerdrError`` when the command fails."""
    proc = subprocess.run(
        [HERDR, '--session', herdr_settings.session, *args],
        capture_output=True,
        text=True,
        timeout=cli_timeout or herdr_settings.cli_timeout_seconds,
    )
    if proc.returncode != 0:
        raise HerdrError(*_error_fields(proc.stderr or proc.stdout))
    return proc.stdout


def _start_server() -> None:
    """Start the piloop session's headless server (no terminal attached) and wait until it answers.

    Nothing waits for the server process, and ``Popen``'s finaliser reports that as a leak. So the handle is dropped
    with that one warning silenced.
    """
    server = subprocess.Popen(
        [HERDR, '--session', herdr_settings.session, 'server'],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', ResourceWarning)
        del server
    deadline = time.monotonic() + herdr_settings.server_start_seconds
    while True:
        try:
            _cli('workspace', 'list')
            return
        except HerdrError as e:
            if e.code != SERVER_NOT_RUNNING or time.monotonic() > deadline:
                raise
            time.sleep(SERVER_POLL_SECONDS)


def _tail(screen: str) -> str:
    """Return the last ``SCREEN_TAIL_LINES`` non-blank lines of ``screen`` below the latest ``RESUME_MARK``."""
    lines = [line for line in screen.rpartition(RESUME_MARK)[2].splitlines() if line.strip()]
    return '\n'.join(lines[-SCREEN_TAIL_LINES:])


def _best_effort(read: Callable[[], str]) -> str:
    """Return what ``read`` returns, or why it failed, so that reading a screen never hides the error it records."""
    try:
        return read()
    except Exception as e:  # the pane can be gone or the server down; the caller's error is the one to raise
        return f'(unreadable: {type(e).__name__}: {e})'


def _ms(seconds: float) -> str:
    return str(int(seconds * 1000))


def _error_fields(text: str) -> tuple[str, str]:
    try:
        error = _as_dict(json.loads(text)['error'])
    except ValueError, KeyError, TypeError:
        return 'cli_error', text.strip() or 'herdr exited non-zero without output'
    return str(error['code']), str(error['message'])


def _as_dict(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise HerdrError('bad_response', f'expected a JSON object, got {type(value).__name__}')
    return value


def _agent(handle: Handle) -> str:
    return str(handle.data['agent'])


def _starting(handle: Handle) -> bool:
    """Whether the agent's first prompt was submitted less than ``startup_grace_seconds`` ago."""
    prompted = handle.data.get(PROMPTED_AT)
    grace = herdr_settings.startup_grace_seconds
    return isinstance(prompted, float) and time.time() < prompted + grace


def _export(pane_id: str, env: dict[str, str], fifo: Path) -> None:
    """Set ``env`` in the pane's shell, which reads it from a FIFO. No value reaches an argv (``ps``) or the disk.

    ``tab create --env`` would put every value, MCP keys included, on the herdr command line. The write end opens only
    after the shell opens the read end. So a shell that never runs the line fails the start, instead of starting the
    agent without its environment. The agent's command, typed after this line, runs after the exports.

    The shell runs ``eval "$(cat <fifo>)"``, not ``. <fifo>``: bash 3.2 (macOS's ``/bin/sh``) cannot source a FIFO.
    """
    fifo.parent.mkdir(parents=True, exist_ok=True)
    os.mkfifo(fifo, 0o600)
    try:
        _call('pane', 'run', pane_id, f'eval "$(cat {shlex.quote(str(fifo))})"')
        with os.fdopen(_open_writer(fifo), 'w') as out:
            out.write(''.join(f'export {k}={shlex.quote(v)}\n' for k, v in env.items()))
    finally:
        fifo.unlink()


def _open_writer(fifo: Path) -> int:
    """Open the FIFO's write end once a reader has the read end open (``ENXIO`` until then), and make it blocking."""
    deadline = time.monotonic() + herdr_settings.shell_ready_seconds
    while True:
        try:
            fd = os.open(fifo, os.O_WRONLY | os.O_NONBLOCK)
        except OSError as e:
            if time.monotonic() >= deadline:
                raise HerdrError('shell_not_ready', f'the pane shell did not read its environment from {fifo}') from e
            time.sleep(SHELL_READY_POLL_SECONDS)
            continue
        os.set_blocking(fd, True)
        return fd
