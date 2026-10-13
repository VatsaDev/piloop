"""Workers as Herdr agents: one Herdr session for piloop, one workspace per project, one tab per worker.

Every ``herdr`` call targets the session that ``settings.herdr_session`` names (``--session``). piloop starts its
headless server when it is not running. Nothing piloop starts lands in the session the human uses for other work.

The workspace has the project name as its label and the project path as its ``root`` metadata token, because Herdr
keeps no path on a workspace. Its first tab is ``home``: a shell on the left for the human and ``piloop watch`` on the
right. Each agent gets its own tab after it, labelled with ``spec.label`` (``worker-12 idea#5 stacking``). The tab's
root pane runs the agent. Tabs are used instead of splits, so every worker is equally readable and closing a worker
removes exactly its tab. A human selects workspace and tab in the Herdr sidebar, or presses ``enter`` on an agent in
``piloop watch``. The daemon addresses agents by ``spec.name``.

An agent is finished when it is idle and nothing it started still runs. Harnesses run tool commands in sessions of
their own (Claude Code's background shells, Codex's commands). The agent, its MCP servers and its helpers share the
session of the pane's shell. Every process started in the pane inherits ``TOKEN_ENV``. So a tagged process in another
session is work that the agent started and still waits on, even after its parent shell exited.

``herdr`` prints ``{"result": …}`` JSON on stdout, and ``{"error": {"code", "message"}}`` on stderr with exit status
1. Exceptions: ``pane run`` prints nothing, ``read`` prints the terminal text, and ``session list --json`` prints
``{"sessions": […]}``.
"""

import json
import os
import shlex
import shutil
import signal
import subprocess
import sys
import time
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

from piloop import settings as settings_module
from piloop.proc import tagged
from piloop.runtime import Handle, NotSteerable, Outcome, StartFailed, WorkerSpec, WorkerStatus

HERDR = 'herdr'
STATUS_MAP = {
    'idle': WorkerStatus.IDLE,
    'done': WorkerStatus.IDLE,
    'working': WorkerStatus.WORKING,
    'blocked': WorkerStatus.BLOCKED,
    'unknown': WorkerStatus.WORKING,
}
GONE_CODES = {'agent_not_found', 'agent_not_running', 'pane_not_found', 'tab_not_found', 'workspace_not_found'}
BLOCKED_ERROR = 'agent is blocked waiting for input'
SERVER_NOT_RUNNING = 'server_not_running'
SHELL_READY_POLL_SECONDS = 0.5
DIALOG_REDRAW_POLL_SECONDS = 0.25
SERVER_POLL_SECONDS = 0.1
PICKUP_POLL_SECONDS = 0.25
HOME_TAB = 'home'
"""The label of the first tab of a project workspace: a shell beside ``piloop watch``."""
SHELL_SHARE = 0.5
"""The shell pane's share of the home tab's width (``pane split --ratio``); ``piloop watch`` gets the rest."""
ROOT_TOKEN = 'root'
"""The workspace metadata token that holds the project path, so two directories with the same name get two
workspaces."""
METADATA_SOURCE = 'piloop'
TOKEN_ENV = 'PILOOP_WORKER_TOKEN'
"""Set in the worker's pane to a value unique to the worker. Every process the agent starts inherits it."""
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
"""How many non-blank lines at the bottom of an idle agent's screen to search for ``BUSY_SCREENS`` and
``FAILED_TURN_SCREENS``. This covers the prompt box and the last output above it. The scrollback is not searched,
because an old banner or the agent's own output could match there."""
BUSY_SCREENS: dict[str, tuple[str, ...]] = {
    'claude': ('Press up to edit queued messages', 'Waiting for API response', 'Retrying in', '… (thinking'),
    'codex': ('Waiting for startup',),
}
"""For each Herdr agent kind, text at the bottom of a still screen that shows the agent works while Herdr reports
it idle.

Herdr reads Claude Code's state from the title spinner only. The spinner stops while Claude Code holds a queued
prompt, while it waits to retry the API (``Retrying in 0s`` can hold still) and while it thinks at length with no
timer on screen. Codex holds a prompt submitted before its app server is up under ``Waiting for startup``, and
submits it when the server is up. On a loaded host this can take 25 s or more, and Herdr reports idle for all of it.
TODO: remove an entry once Herdr detects that state for its kind.
"""
FAILED_TURN_SCREENS: dict[str, tuple[str, ...]] = {
    'claude': ('API Error', 'included_quota_unknown'),
    'codex': ('Selected model is at capacity', 'exceeded retry limit', 'stream disconnected before completion'),
}
"""For each Herdr agent kind, text at the bottom of the screen that shows the turn ended on an API error, not because
the agent was done.

The agent is resumed with ``RESUME_PROMPT`` after each wait in ``RESUME_BACKOFF_SECONDS``. After the last one, the run
fails. A ``503 included_quota_unknown`` is transient: while Claude Code counts down a retry, it is a ``BUSY_SCREENS``
text, and when the retries stop, it is a failed turn.
"""
RESUME_PROMPT = '[piloop] Your last turn ended on an API error. Carry on with your idea from where you stopped.'
RESUME_MARK = RESUME_PROMPT[:46]
"""The part of ``RESUME_PROMPT`` that fits on one screen line. An error banner above it was already resumed."""
RESUME_BACKOFF_SECONDS = (60, 120, 240)
"""The wait before each resume of a turn that ended on an API error. Capacity errors usually clear within minutes."""
TURN_FAILED_ERROR = 'the agent stopped on an API error and did not recover'


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


class HerdrError(RuntimeError):
    """A ``herdr`` command failed. ``code`` is Herdr's machine-readable error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f'{code}: {message}')
        self.code = code
        self.message = message


class HerdrRuntime:
    """Runs the ``herdr`` CLI against the piloop session, and starts its server when needed."""

    kind = 'herdr'

    def __init__(self, project: str, project_path: Path, *, sleep: Callable[[float], None] = time.sleep) -> None:
        if shutil.which(HERDR) is None:
            raise HerdrError('not_installed', 'herdr is not on PATH; install it or set runtime = "subprocess"')
        self._project = project
        self._project_path = project_path
        self._workspace_id: str | None = None
        self._sleep = sleep
        """Used for every wait in this runtime's loops, so a test can count them without other threads' sleeps."""

    def start(self, spec: WorkerSpec) -> Handle:
        """Create the worker's tab, start the agent in its pane, then submit the first prompt."""
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
            f'{TOKEN_ENV}={token}',
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
        """Submit the first prompt once and confirm that the agent took it.

        A TUI that still draws its welcome screen can drop a prompt. Pickup is a state change (Herdr's
        ``state_change_seq`` advances after the prompt), not a state. An agent whose first-run dialogs were answered
        in a background tab already reports ``done``, and a wait for ``working`` or ``done`` would take that for a
        finished turn. Without a state change, a ``BUSY_SCREENS`` or ``FAILED_TURN_SCREENS`` text at the bottom of the
        screen shows that the agent took the prompt while it starts up or retries the API. ``wait`` judges what
        follows. The prompt is never pasted again, because the composer or a queued turn may already hold it.

        A prompt is usually lost because the host is too loaded to draw the TUI, or because the Herdr integration is
        missing. The screen that ``start`` keeps shows which.
        """
        agent = _agent(handle)
        before = self._state_seq(agent)
        _call('agent', 'prompt', agent, prompt)
        if self._state_changed(agent, before, settings_module.settings.herdr_prompt_pickup_seconds):
            return
        tail = _tail(self._visible(handle))
        kind = str(handle.data['kind'])
        if any(text in tail for text in (*BUSY_SCREENS.get(kind, ()), *FAILED_TURN_SCREENS.get(kind, ()))):
            return
        raise HerdrError('prompt_lost', f'{agent} did not start working after its brief was submitted')

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
            _ms(settings_module.settings.herdr_start_timeout_seconds),
        ]
        deadline = time.monotonic() + settings_module.settings.herdr_shell_ready_seconds
        grace = settings_module.settings.herdr_cli_timeout_seconds
        ready = False
        while not ready:
            try:
                # herdr waits up to --timeout for the agent, so the CLI timeout must be longer
                _call(*start, '--', *args, cli_timeout=settings_module.settings.herdr_start_timeout_seconds + grace)
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
            result = self._agent_wait(
                agent, ['idle', 'done', 'blocked'], settings_module.settings.herdr_start_timeout_seconds
            )
            ready = result['agent']['agent_status'] != 'blocked'

    def _await_redraw(self, agent: str, pane_id: str, answered: str, pending: list[tuple[str, list[str]]]) -> str:
        """Return the screen once the answered dialog is gone from it or a pending dialog is on it.

        ``agent wait`` cannot do this. Herdr reports these dialogs as ``idle``, so ``agent wait`` returns before the
        TUI redraws, and the next read would show the answered dialog again.
        """
        deadline = time.monotonic() + settings_module.settings.herdr_start_timeout_seconds
        while True:
            screen = self._screen(pane_id)
            if answered not in screen or any(text in screen for text, _ in pending):
                return screen
            if time.monotonic() >= deadline:
                raise HerdrError('agent_not_ready', f'{agent} still shows a startup dialog after it was answered')
            self._sleep(DIALOG_REDRAW_POLL_SECONDS)

    def _screen(self, pane_id: str) -> str:
        return _run('pane', 'read', pane_id, '--source', 'visible')

    def wait(self, handle: Handle, timeout: float) -> Outcome:
        """Wait until the agent is finished, or kill it at ``timeout``.

        The agent is finished when it is idle with no jobs running, or blocked on a still screen for
        ``BLOCKED_GRACE_SECONDS``. These states are not finished:

        - Idle with jobs running: the agent ended its turn to wait for them. When they end, an agent that did not
          resume by itself is prompted to read their results (``JOBS_DONE_PROMPT``).
        - Blocked, until the grace runs out on a screen that held still. A dialog can be answered or time out by
          itself, and an agent can keep working beside a question it queued.
        - Idle on a screen that changes within ``QUIET_SECONDS`` or shows a ``BUSY_SCREENS`` text: Herdr misread a
          working agent.
        - Idle on a ``FAILED_TURN_SCREENS`` text: the agent is resumed. When the backoffs are spent, the outcome has
          ``TURN_FAILED_ERROR``.

        The wait is done in chunks. ``agent wait`` does not return when another thread closes the tab (``kill``), so
        each chunk asks again and gets ``agent_not_found`` once the worker is gone.
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
                return Outcome(WorkerStatus.TIMED_OUT, timeout, error=f'worker timeout after {timeout:g}s')
            chunk = min(remaining, settings_module.settings.herdr_wait_chunk_seconds)
            window = self._window(handle, block, now, chunk)
            if isinstance(window, str):
                # the dialog is only on the visible screen, and the pane closes with the worker: keep it in the error
                return Outcome(WorkerStatus.BLOCKED, now - started, error=f'{BLOCKED_ERROR}:\n{window.strip()}')
            until, chunk = window
            try:
                info = self._agent_wait(agent, until, chunk)['agent']
            except HerdrError as e:
                if e.code in GONE_CODES:
                    return Outcome(WorkerStatus.EXITED, time.monotonic() - started, error=None)
                if e.code == 'timeout':
                    continue
                raise
            status = STATUS_MAP[info['agent_status']]
            if status is WorkerStatus.BLOCKED:
                block = block or Block(time.monotonic(), self._visible(handle))
                continue
            block = None
            if status is WorkerStatus.WORKING:
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
                    return Outcome(WorkerStatus.IDLE, time.monotonic() - started, error=f'{TURN_FAILED_ERROR}:\n{tail}')
                self._sleep(min(RESUME_BACKOFF_SECONDS[resumes], remaining))
                resumes += 1
                self._resume(agent, seq, RESUME_PROMPT)
                continue
            return Outcome(WorkerStatus.IDLE, time.monotonic() - started, error=None)

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
        counts) or shows a ``BUSY_SCREENS`` text. A screen that is gone with its pane shows done: the next
        ``agent wait`` finds the agent gone.
        """
        first = self._visible(handle)
        self._sleep(min(QUIET_SECONDS, remaining))
        screen = self._visible(handle)
        tail = _tail(screen)
        kind = str(handle.data.get('kind'))
        if screen != first or any(text in tail for text in BUSY_SCREENS.get(kind, ())):
            return Screen.WORKING, tail
        if any(text in tail for text in FAILED_TURN_SCREENS.get(kind, ())):
            return Screen.FAILED_TURN, tail
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
        pickup = settings_module.settings.herdr_prompt_pickup_seconds
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
        return [f'{pid} {cmd}' for pid, sid, cmd in tagged(f'{TOKEN_ENV}={token}') if sid != session]

    def _agent_wait(self, agent: str, until: list[str], timeout: float) -> dict[str, Any]:
        flags = [flag for state in until for flag in ('--until', state)]
        return _call(
            'agent',
            'wait',
            agent,
            *flags,
            '--timeout',
            str(int(timeout * 1000)),
            cli_timeout=timeout + settings_module.settings.herdr_cli_timeout_seconds,
        )

    def status(self, handle: Handle) -> WorkerStatus:
        """Return the state that Herdr detected."""
        try:
            result = _call('agent', 'get', _agent(handle))
        except HerdrError as e:
            if e.code in GONE_CODES:
                return WorkerStatus.EXITED
            raise
        return STATUS_MAP[result['agent']['agent_status']]

    def session_id(self, handle: Handle) -> str | None:
        """Return the session id that Herdr detected for the agent (``agent_session.value`` of ``agent get``).

        Herdr learns it from the harness after the harness starts, so this polls for up to
        ``herdr_prompt_pickup_seconds``. None when the id does not appear or the agent is gone.
        """
        deadline = time.monotonic() + settings_module.settings.herdr_prompt_pickup_seconds
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
        """Submit a steering message. Herdr refuses it while the agent is blocked on a dialog."""
        try:
            _call('agent', 'prompt', _agent(handle), text)
        except HerdrError as e:
            if e.code == 'agent_blocked':
                raise NotSteerable(
                    f'{_agent(handle)} is blocked on an approval or question; read it and answer with herdr agent send-keys'
                ) from e
            raise

    def read(self, handle: Handle, lines: int = 80) -> str:
        """Return the recent terminal output, unwrapped and without ANSI codes."""
        return _run('agent', 'read', _agent(handle), '--source', 'recent-unwrapped', '--lines', str(lines))

    def kill(self, handle: Handle) -> None:
        """Close the worker's tab, which ends its process, then kill what it left running. A gone tab is no error.

        A job in a session of its own does not get the pane's hangup signal, so it would outlive the worker.
        """
        _close_tab(str(handle.data['tab_id']))
        token = handle.data.get('token')
        if token is None:
            return
        for pid, _, _ in tagged(f'{TOKEN_ENV}={token}'):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                continue

    def _split_watch(self, pane_id: str) -> None:
        """Run ``piloop watch`` on this project in a new pane to the right of ``pane_id``.

        The watch uses this process's interpreter and ``PILOOP_HOME``, so it reads the daemon's database, whatever
        ``piloop`` the pane's shell finds first.
        """
        split = _call(
            'pane',
            'split',
            pane_id,
            '--direction',
            'right',
            '--ratio',
            str(SHELL_SHARE),
            '--cwd',
            str(self._project_path),
            '--env',
            f'PILOOP_HOME={settings_module.settings.home}',
            '--no-focus',
        )
        watch = [sys.executable, '-m', 'piloop', 'watch', str(self._project_path)]
        _call('pane', 'run', str(split['pane']['pane_id']), shlex.join(watch))

    def _workspace(self) -> str:
        """Return the project's workspace id. Find it by its ``root`` token or create it. Cached per instance."""
        if self._workspace_id is None:
            listed = _call('workspace', 'list')['workspaces']
            root = str(self._project_path)
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
        label = self._project
        if any(w['label'] == label for w in existing):
            label = f'{label} ({self._project_path.parent.name})'
        created = _call('workspace', 'create', '--cwd', str(self._project_path), '--label', label, '--no-focus')
        workspace = created['workspace']
        _call(
            'workspace',
            'report-metadata',
            str(workspace['workspace_id']),
            '--source',
            METADATA_SOURCE,
            '--token',
            f'{ROOT_TOKEN}={self._project_path}',
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


def focus_tab(tab_id: str) -> None:
    """Bring a tab of the piloop session, and its workspace, to the front."""
    _call('tab', 'focus', tab_id)


def session_socket() -> str | None:
    """Return the socket path of the piloop session, or None when Herdr does not know that session name."""
    sessions = _as_dict(json.loads(_run('session', 'list', '--json')))['sessions']
    return next((str(s['socket_path']) for s in sessions if s['name'] == settings_module.settings.herdr_session), None)


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
        timeout=settings_module.settings.herdr_cli_timeout_seconds,
        check=True,
    ).stdout
    lines = (line.partition(': ') for line in out.splitlines())
    return {kind: state.rsplit(' (', 1)[0] for kind, sep, state in lines if sep}


def _call(*args: str, cli_timeout: float | None = None) -> dict[str, Any]:
    """Run ``herdr <args>`` and return its ``result`` object, or an empty dict when the command prints nothing."""
    stdout = _run(*args, cli_timeout=cli_timeout)
    return _as_dict(json.loads(stdout)['result']) if stdout.strip() else {}


def _run(*args: str, cli_timeout: float | None = None) -> str:
    """Run ``_cli``. When the server is not running, start it and try once more."""
    try:
        return _cli(*args, cli_timeout=cli_timeout)
    except HerdrError as e:
        if e.code != SERVER_NOT_RUNNING:
            raise
    _start_server()
    return _cli(*args, cli_timeout=cli_timeout)


def _cli(*args: str, cli_timeout: float | None = None) -> str:
    """Run ``herdr --session <piloop> <args>`` and return stdout. Raise ``HerdrError`` when the command fails."""
    proc = subprocess.run(
        [HERDR, '--session', settings_module.settings.herdr_session, *args],
        capture_output=True,
        text=True,
        timeout=cli_timeout or settings_module.settings.herdr_cli_timeout_seconds,
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
        [HERDR, '--session', settings_module.settings.herdr_session, 'server'],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', ResourceWarning)
        del server
    deadline = time.monotonic() + settings_module.settings.herdr_server_start_seconds
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
    deadline = time.monotonic() + settings_module.settings.herdr_shell_ready_seconds
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
