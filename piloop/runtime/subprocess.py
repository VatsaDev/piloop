"""Agents as plain background processes: the default runtime. Herdr is the optional one with visible panes."""

import fcntl
import json
import os
import re
import signal
import subprocess
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import ClassVar
from uuid import uuid4

from piloop.harnesses import Invocation, fill
from piloop.proc import TOKEN_ENV, alive, child_env, kill_group, kill_tagged, started
from piloop.runtime import Check, Handle, NotAttachable, NotSteerable, Outcome, WorkerSpec, WorkerStatus

STDOUT = 'worker.stdout'
STDERR = 'worker.stderr'
POLL_SECONDS = 0.1
SESSION_ID_SECONDS = 30
"""How long a live process has to print the line that ``session_id_pattern`` matches. After that, the session id is
unknown."""
QUEUE = 'queued-messages'
"""Under the agent's log directory: the messages ``prompt`` queued for its next turn, one JSON string per line. A file,
because ``prompt`` runs in another runtime instance, or another process (``piloop prompt``), than ``wait``."""
RESUMER = 'worker.resumer'
"""Under the agent's log directory: present while the instance that started the agent waits on it and can resume it.
``wait`` in any other instance (a re-attach after an in-place restart) removes it, so ``prompt`` refuses from then
on instead of queueing messages that no turn would take."""
CURRENT = 'worker.current'
"""Under the agent's log directory: the pid and start time of the process of the agent's latest turn, once a resumed
turn replaced the first process. Every instance reads it, so ``kill`` and ``status`` reach the agent's live process."""


class SubprocessRuntime:
    """Each agent as a background process without a terminal, one process per turn of the agent.

    The first prompt is in ``spec.args``.

    A message (``prompt``) waits in a queue until the turn ends: when the process exits, ``wait`` starts the harness
    again with ``spec.resume_args``, which continue the same session with the queued messages as the prompt. Nothing
    reaches a turn in progress, as with Herdr. Only the instance that started the agent can resume it, because the
    environment, secrets included, is never written down: after an in-place restart, a re-attached agent takes no
    more messages (``prompt`` refuses them) and its queue is left as it is.

    This instance keeps the processes it started, so it knows their exit codes. A handle loaded from the database in
    another process can still be observed and killed by its pid and start time, but has no exit code.
    """

    kind: ClassVar[str] = 'subprocess'
    invocation: ClassVar[Invocation] = Invocation.HEADLESS

    def __init__(self, project: str = '', project_path: Path | None = None) -> None:
        """Take the same arguments as every runtime constructor. A child process needs neither of them."""
        self._procs: dict[int, subprocess.Popen[bytes]] = {}
        """The process of each agent's latest turn, by the pid of its first process (the handle's)."""
        self._specs: dict[int, WorkerSpec] = {}
        """The spec of each agent this instance started with resume arguments, by the handle's pid."""

    def start(self, spec: WorkerSpec) -> Handle:
        """Start the process in its own session, with stdout and stderr written under ``spec.log_dir``.

        Its environment has ``TOKEN_ENV``, which every process it starts inherits, so ``kill`` finds them all.
        """
        spec.log_dir.mkdir(parents=True, exist_ok=True)
        token = uuid4().hex
        proc = _spawn(spec, spec.args, 'w', token)
        try:
            if spec.resume_args:
                (spec.log_dir / RESUMER).touch()
            handle = Handle(
                self.kind,
                {
                    'pid': proc.pid,
                    # start_new_session makes the child its group's leader. os.getpgid would ask the system, and
                    # macOS refuses (ESRCH) for a child that already exited, which failed the start of a quick agent.
                    'pgid': proc.pid,
                    'start_time': started(proc.pid),
                    'started_at': time.time(),
                    'log_dir': str(spec.log_dir),
                    'session_id_pattern': spec.session_id_pattern,
                    'token': token,
                },
            )
        except BaseException:
            # No handle reaches the caller, so nothing would wait for this process: end it here.
            kill_group(proc)
            proc.wait()
            raise
        self._procs[proc.pid] = proc
        if spec.resume_args:
            self._specs[proc.pid] = spec
        return handle

    def wait(self, handle: Handle, timeout: float) -> Outcome:
        """Poll until the agent is gone, resuming it while messages wait for it. On timeout, kill its process group.

        A turn that ends with messages in the queue, exit status 0 and a known session starts the next turn
        (``_resume``). All turns share the timeout.
        """
        started = time.monotonic()
        if _pid(handle) not in self._specs:  # a re-attach: nothing here can resume the agent
            (_log_dir(handle) / RESUMER).unlink(missing_ok=True)
        while True:
            while self._alive(handle):
                if time.monotonic() - started > timeout:
                    self.kill(handle)
                    seconds = time.monotonic() - started
                    return Outcome(WorkerStatus.TIMED_OUT, seconds, None, f'worker timeout after {timeout:g}s')
                time.sleep(POLL_SECONDS)
            if not self._resume(handle):
                break
        rc = self._exit_code(handle)
        return Outcome(
            WorkerStatus.EXITED, time.monotonic() - started, rc, None if rc in (0, None) else f'worker exit {rc}'
        )

    def status(self, handle: Handle) -> WorkerStatus:
        """A live process is working. A one-shot process is never idle."""
        return WorkerStatus.WORKING if self._alive(handle) else WorkerStatus.EXITED

    def session_id(self, handle: Handle) -> str | None:
        """Return the first match of the spec's ``session_id_pattern`` in the captured stdout. None without a pattern.

        Poll while the process lives, up to ``SESSION_ID_SECONDS``. Read once more after it exits, so a process that
        prints nothing and ends causes no wait.
        """
        pattern = handle.data.get('session_id_pattern')
        if pattern is None:
            return None
        regex = re.compile(str(pattern))
        deadline = time.monotonic() + SESSION_ID_SECONDS
        while True:
            alive = self._alive(handle)
            found = regex.search(_stdout(handle))
            if found is not None:
                return found.group(1)
            if not alive or time.monotonic() >= deadline:
                return None
            time.sleep(POLL_SECONDS)

    def jobs(self, handle: Handle) -> list[str]:
        """Return no jobs: a one-shot process ends with its work. ``kill`` still ends what it left running."""
        return []

    def prompt(self, handle: Handle, text: str) -> None:
        """Keeps the message in a queue, and continues the agent's session with it when the turn ends.

        piloop runs the harness again with ``[harness.<name>] resume``, with the queued messages as the prompt. A
        harness without ``resume`` takes no message. Neither does an agent that the daemon re-attached to after
        ``piloop restart``.

        Note:
            The queue is ``QUEUE``: the process of this turn has no input channel. Raise ``NotSteerable`` when nothing
            would take the message: the harness cannot continue its session, the daemon that could resume it was
            restarted (``RESUMER``), or the agent has ended. The append holds the queue's lock, so a turn that starts
            meanwhile takes the message or leaves it for the next one.
        """
        log_dir = handle.data.get('log_dir')  # a handle that names none has no queue either
        if log_dir is None or not (Path(str(log_dir)) / RESUMER).is_file():
            raise NotSteerable(
                f'subprocess worker pid {_pid(handle)} cannot take a message: its harness has no resume arguments, or'
                ' the daemon that started it was restarted'
            )
        if not self._alive(handle):
            raise NotSteerable(f'subprocess worker pid {_pid(handle)} has ended')
        with _locked(_log_dir(handle)), (_log_dir(handle) / QUEUE).open('a') as queue:
            queue.write(json.dumps(text) + '\n')

    def focus(self, handle: Handle) -> str:
        """Cannot: the agent is a background process without a terminal. Its output is in ``worker.stdout`` instead."""
        raise NotAttachable(_elsewhere(handle))

    def attach(self, handle: Handle) -> str:
        """Cannot: the agent is a background process without a terminal. Its output is in ``worker.stdout`` instead."""
        raise NotAttachable(_elsewhere(handle))

    @classmethod
    def view(cls, handle: Handle) -> str | None:
        """None: a background process has no place to see it."""
        return None

    @classmethod
    def checks(cls, executables: set[str]) -> list[Check]:
        """None: a background process needs nothing more than its harness, which ``piloop doctor`` checks already."""
        return []

    def _resume(self, handle: Handle) -> bool:
        """Start the agent's next turn with the queued messages. False when there is none to start.

        There is none when the queue is empty, the turn failed (its exit status is not 0), this instance did not
        start the agent, or its session is unknown. The messages then stay queued. The new process appends to the
        agent's stdout and stderr, and ``CURRENT`` names it.
        """
        log_dir = _log_dir(handle)
        spec = self._specs.get(_pid(handle))
        if spec is None or self._exit_code(handle) != 0:
            return False
        session = self.session_id(handle) or ''
        if not session and any('{session_id}' in a for a in spec.resume_args):
            return False
        with _locked(log_dir):
            queue = log_dir / QUEUE
            messages = [json.loads(line) for line in queue.read_text().splitlines() if line] if queue.is_file() else []
            if not messages:
                return False
            queue.unlink()
        prompt = '\n'.join(messages)
        args = [fill(a, prompt=prompt, session_id=session) for a in spec.resume_args]
        proc = _spawn(spec, args, 'a', str(handle.data['token']))
        self._procs[_pid(handle)] = proc
        # replaced in one step: another instance may read it at any moment (status, kill, prompt)
        current = log_dir / f'{CURRENT}.tmp'
        current.write_text(json.dumps({'pid': proc.pid, 'start_time': started(proc.pid)}))
        current.replace(log_dir / CURRENT)
        return True

    def read(self, handle: Handle, lines: int = 80) -> str:
        """Return the last ``lines`` lines of the captured stdout."""
        return '\n'.join(_stdout(handle).splitlines()[-lines:])

    def kill(self, handle: Handle) -> None:
        """Send SIGKILL to the process group, then to every process that carries the agent's token.

        Harnesses run tool commands in sessions of their own (Claude Code's Bash calls), outside the group, and those
        outlive the agent. A group or a process that is already gone is not an error. A handle from before the token
        reaches only the group.
        """
        if self._alive(handle):
            with suppress(ProcessLookupError):
                os.killpg(_current(handle)[0], signal.SIGKILL)  # each process leads its own group: pgid = pid
                while self._alive(handle):
                    time.sleep(POLL_SECONDS)
        if (token := handle.data.get('token')) is not None:
            kill_tagged(f'{TOKEN_ENV}={token}')

    def _alive(self, handle: Handle) -> bool:
        proc = self._procs.get(_pid(handle))
        if proc is not None:
            return proc.poll() is None
        return alive(*_current(handle))

    def _exit_code(self, handle: Handle) -> int | None:
        proc = self._procs.get(_pid(handle))
        return proc.returncode if proc is not None else None


def _pid(handle: Handle) -> int:
    return int(str(handle.data['pid']))


def _current(handle: Handle) -> tuple[int, float]:
    """The pid and start time of the agent's latest process: ``CURRENT`` once a turn was resumed, else the handle's."""
    path = _log_dir(handle) / CURRENT
    data = json.loads(path.read_text()) if path.is_file() else handle.data
    return int(str(data['pid'])), float(str(data['start_time']))


@contextmanager
def _locked(log_dir: Path) -> Generator[None]:
    """Hold the agent's queue lock: ``prompt`` appends and ``_resume`` drains under it, across processes."""
    with (log_dir / f'{QUEUE}.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _spawn(spec: WorkerSpec, args: list[str], mode: str, token: str) -> subprocess.Popen[bytes]:
    """Start one turn of the agent in a session of its own, writing (``w``) or appending (``a``) its output files."""
    with (spec.log_dir / STDOUT).open(mode) as out, (spec.log_dir / STDERR).open(mode) as err:
        return subprocess.Popen(
            [spec.executable, *args],
            cwd=spec.cwd,
            stdout=out,
            stderr=err,
            stdin=subprocess.DEVNULL,
            env=child_env() | spec.env | {TOKEN_ENV: token},
            start_new_session=True,
        )


def _elsewhere(handle: Handle) -> str:
    """Say where the output of a background process is."""
    return f'a background process has no terminal to show: read {_log_dir(handle) / STDOUT}'


def _log_dir(handle: Handle) -> Path:
    return Path(str(handle.data['log_dir']))


def _stdout(handle: Handle) -> str:
    path = _log_dir(handle) / STDOUT
    return path.read_text(errors='replace') if path.is_file() else ''
