"""Agents as plain background processes: the built-in runtime."""

import json
import os
import re
import signal
import subprocess
import time
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from piloop import deprecated
from piloop.harnesses import fill
from piloop.plugins.runtime.contract import (
    MESSAGES_ENV,
    QUEUE,
    RUNTIMES,
    TAG_ENV,
    AgentExit,
    AgentSpec,
    ExitStatus,
    Handle,
    NotSteerable,
    RuntimeConfig,
    put,
    take,
)
from piloop.plugins.runtime.kit.tags import kill_tagged, kill_tree
from piloop.proc import alive, child_env, kill_group, started

STDOUT = 'agent_session.stdout'
STDERR = 'agent_session.stderr'
POLL_SECONDS = 0.1
SESSION_ID_SECONDS = 30
"""How long a live process has to print the line that ``session_id_pattern`` matches. After that, the session id is
unknown."""
RESUMER = 'agent_session.resumer'
"""Under the agent's log directory: present while the instance that started the agent waits on it and can resume it.
``wait`` in any other instance (a re-attach after an in-place restart) removes it, so ``prompt`` refuses from then
on instead of queueing messages that no turn would take."""
CURRENT = 'agent_session.current'
"""Under the agent's log directory: the pid and start time of the process of the agent's latest turn, once a resumed
turn replaced the first process. Every instance reads it, so ``kill`` and ``status`` reach the agent's live process."""
deprecated.until('0.3.0', 'worker.current of an agent that 0.1.0 resumed: delete OLD_CURRENT and its read in _current')
OLD_CURRENT = 'worker.current'
"""``CURRENT`` before 0.2.0: an agent that an older daemon resumed outlives an upgrade with it."""


class Subprocess(RuntimeConfig):
    """``subprocess``: each agent as a background process on this machine. It has no settings."""

    def open(self) -> SubprocessRuntime:
        return SubprocessRuntime(RUNTIMES.name_of(self))


class SubprocessRuntime:
    """Each agent as a background process without a terminal, one process per turn of the agent.

    The first prompt is in ``spec.args``.

    A message (``prompt``) waits in a queue. A hook of the harness can take it during the turn (``MESSAGES_ENV``).
    Otherwise, when the process exits, ``wait`` starts the harness again with ``spec.resume_args``, which continue the
    same session with the queued messages as the prompt. Only the instance that started the agent can resume it, because the
    environment, secrets included, is never written down: after an in-place restart, a re-attached agent takes no
    more messages (``prompt`` refuses them) and its queue is left as it is.

    This instance keeps the processes it started, so it knows their exit codes. A handle loaded from the database in
    another process can still be observed and killed by its pid and start time, but has no exit code.
    """

    def __init__(self, name: str) -> None:
        """``name``: the runtime's, which its handles carry."""
        self.name = name
        self._procs: dict[int, subprocess.Popen[bytes]] = {}
        """The process of each agent's latest turn, by the pid of its first process (the handle's)."""
        self._specs: dict[int, AgentSpec] = {}
        """The spec of each agent this instance started with resume arguments, by the handle's pid."""

    def start(self, spec: AgentSpec) -> Handle:
        """Start the process in its own session, with stdout and stderr written under ``spec.log_dir``.

        Its environment has ``TAG_ENV``, which every process it starts inherits, so ``kill`` finds them all.
        """
        spec.log_dir.mkdir(parents=True, exist_ok=True)
        token = uuid4().hex
        proc = _spawn(spec, spec.args, 'w', token)
        try:
            if spec.resume_args:
                (spec.log_dir / RESUMER).touch()
            handle = Handle(
                self.name,
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

    def wait(self, handle: Handle, timeout: float) -> AgentExit:
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
                    return AgentExit(ExitStatus.TIMED_OUT, seconds, None, f'agent timeout after {timeout:g}s')
                time.sleep(POLL_SECONDS)
            if not self._resume(handle):
                break
        rc = self._exit_code(handle)
        return AgentExit(
            ExitStatus.EXITED, time.monotonic() - started, rc, None if rc in (0, None) else f'agent exit {rc}'
        )

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

    def prompt(self, handle: Handle, text: str) -> None:
        """Keeps the message in a queue, which a hook of the harness can take in the turn, or else the next turn takes.

        When the turn ends with messages queued, piloop runs the harness again with ``[harness.<name>] resume``, with the queued messages as the prompt. A
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
                f'subprocess agent pid {_pid(handle)} cannot take a message: its harness has no resume arguments, or'
                ' the daemon that started it was restarted'
            )
        if not self._alive(handle):
            raise NotSteerable(f'subprocess agent pid {_pid(handle)} has ended')
        put(_log_dir(handle), text)

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
        if not (messages := take(log_dir / QUEUE)):
            return False
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
        """Send SIGKILL to the process group, then to every process that carries the agent's process tag.

        The handle keeps the tag under its ``token`` key: a handle is stored, and its agent can outlive an upgrade.

        Harnesses run tool commands in sessions of their own (Claude Code's Bash calls), outside the group, and those
        outlive the agent. A group or a process that is already gone is not an error. A handle from before the tag
        (0.1.0's) has its agent's descendants killed instead, while the agent still lives to name them.
        """
        token = handle.data.get('token')
        if token is None:
            deprecated.until('0.3.0', 'a handle that 0.1.0 started has no tag: delete this branch and kill_tree')
            kill_tree(*_current(handle))
        if self._alive(handle):
            with suppress(ProcessLookupError):
                os.killpg(_current(handle)[0], signal.SIGKILL)  # each process leads its own group: pgid = pid
                while self._alive(handle):
                    time.sleep(POLL_SECONDS)
        if token is not None:
            kill_tagged(f'{TAG_ENV}={token}')

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
    path = next((p for p in (_log_dir(handle) / n for n in (CURRENT, OLD_CURRENT)) if p.is_file()), None)
    data = json.loads(path.read_text()) if path is not None else handle.data
    return int(str(data['pid'])), float(str(data['start_time']))


def _spawn(spec: AgentSpec, args: list[str], mode: str, token: str) -> subprocess.Popen[bytes]:
    """Start one turn of the agent in a session of its own, writing (``w``) or appending (``a``) its output files."""
    with (spec.log_dir / STDOUT).open(mode) as out, (spec.log_dir / STDERR).open(mode) as err:
        return subprocess.Popen(
            [spec.executable, *args],
            cwd=spec.cwd,
            stdout=out,
            stderr=err,
            stdin=subprocess.DEVNULL,
            env=child_env() | spec.env | {TAG_ENV: token, MESSAGES_ENV: str(spec.log_dir / QUEUE)},
            start_new_session=True,
        )


def _log_dir(handle: Handle) -> Path:
    return Path(str(handle.data['log_dir']))


def _stdout(handle: Handle) -> str:
    path = _log_dir(handle) / STDOUT
    return path.read_text(errors='replace') if path.is_file() else ''
