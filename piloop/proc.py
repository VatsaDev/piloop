"""Processes: the environment children get, how to kill a process group, and how to look at other processes.

Looking at other processes goes through psutil, so it works the same on Linux and macOS.
"""

import contextlib
import os
import secrets
import signal
import subprocess
import sys
import threading
from collections.abc import Collection
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Self, assert_never

import psutil

from piloop import deprecated

STDERR_IN_ERROR = 500
"""The number of characters of a failed command's stderr that piloop keeps in the error it stores on a row."""
UNITS_ENV = 'PILOOP_UNITS_FILE'
"""Where an evaluate command that prints ``METRIC`` lines writes its per-unit results. Set only for piloop's own
evaluations."""
RESULT_ENV = 'PILOOP_RESULT_FILE'
"""The pipe that a command which piloop reads writes its results to, one JSON object per line (``ResultPipe``)."""
ACTOR_ENV = 'PILOOP_ACTOR'
"""The signature of an agent. ``child_env`` drops it: a daemon or an evaluation that an agent's ``piloop`` command
starts must not sign as that agent. Nor must it die with the agent: the CLI drops the agent's process tag when it starts
(``piloop.cli.main``)."""
AGENT_TOKEN_ENV = 'PILOOP_AGENT_TOKEN'
"""The token of an agent, which its worker sets at its start (``piloop.net.tokens.mint_agent``): it acts as that agent
with it, and as nobody else. ``child_env`` drops it, as it drops ``ACTOR_ENV``."""
PILOOP_ARGV = (sys.executable, '-P', '-m', 'piloop')
"""The command that runs this process's piloop, from any cwd. ``-P`` keeps the cwd off ``sys.path``: an agent's cwd is a
campaign's worktree, and its own ``json.py`` or ``piloop/`` would otherwise shadow piloop's imports."""


@deprecated.until('0.4.0', 'METRIC lines on stdout: delete Channel, Sample.channel and the METRIC_LINES branches')
class Channel(StrEnum):
    """How a command that piloop reads reported its results."""

    RESULT = 'result'
    """JSON lines on ``RESULT_ENV``."""
    METRIC_LINES = 'metric_lines'
    """``METRIC <name>=<value>`` lines on stdout, and per-unit results in ``UNITS_ENV``: the protocol before
    ``RESULT_ENV``."""


@dataclass(frozen=True)
class Started:
    """A process that is no child of this one: its pid, and the start time that tells it from a reused pid."""

    pid: int
    start: float


type Leader = subprocess.Popen[bytes] | Started
"""The process that leads a group: a child of this process, or one that ``Started`` names."""


def kill_group(leader: Leader) -> None:
    """Send SIGKILL to the whole group of ``leader``. Do nothing if the group is already gone.

    macOS answers EPERM, not ESRCH, to a group whose processes all exited but are not reaped yet. EPERM is gone too
    when the leader exited (a child is reaped then); while it runs, EPERM is a real permission error, and raised.
    """
    try:
        os.killpg(leader.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except PermissionError:
        if running(leader):
            raise


def running(leader: Leader) -> bool:
    """Whether ``leader`` has not exited. A child that exited is reaped."""
    match leader:
        case subprocess.Popen():
            return leader.poll() is None
        case Started(pid=pid, start=start):
            return alive(pid, start)
        case _:
            assert_never(leader)


def child_env(*drop: str) -> dict[str, str]:
    """The environment for subprocesses: this process's environment without the variables that it must not pass on.

    These are VIRTUAL_ENV, ``UNITS_ENV``, ``RESULT_ENV``, ``ACTOR_ENV``, ``AGENT_TOKEN_ENV`` and ``drop``. Without
    VIRTUAL_ENV, uv does not warn about the campaign's own virtual environment. Only the commands that piloop reads get
    ``UNITS_ENV`` and ``RESULT_ENV``, so no agent gets a path of them. The launcher signs each agent and its runtime
    tags it, after this.

    This is the only place outside ``Settings`` that reads the environment. It passes the values on and does not
    interpret them.
    """
    dropped = ('VIRTUAL_ENV', UNITS_ENV, RESULT_ENV, ACTOR_ENV, AGENT_TOKEN_ENV, *drop)
    return {k: v for k, v in os.environ.items() if k not in dropped}


class ResultPipe:
    """A named pipe at ``path`` that a child appends lines to, read while the child runs: ``RESULT_ENV``.

    A pipe, not a file, so that each line is read when it is written. A thread reads it with blocking reads: macOS's
    kqueue does not wake on a named pipe. The reader holds a write end of its own, so the stream does not end when a
    child that opens and closes the path for each line closes it, nor when no child has opened it yet. ``close`` ends
    the stream with a line of its own that no child can guess: what the child wrote before is read, and a line that
    comes later is not. Call it after the child exited. A line that lacks its final newline at the end counts as a line.
    """

    def __init__(self, path: Path) -> None:
        os.mkfifo(path, 0o600)
        self.path = path
        self._read = os.open(path, os.O_RDONLY | os.O_NONBLOCK)  # opening without a writer would wait
        self._hold = os.open(path, os.O_WRONLY)
        os.set_blocking(self._read, True)
        self._end = secrets.token_hex(16).encode()
        self._lines: list[str] = []
        self._thread = threading.Thread(target=self._follow, name=f'results-{path.name}', daemon=True)
        self._thread.start()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> list[str]:
        """End the stream, and return every line that the child wrote, in order. Closing twice returns them again."""
        if self._thread.is_alive():
            os.write(self._hold, b'\n' + self._end + b'\n')
            self._thread.join()
            os.close(self._read)
            os.close(self._hold)
            self.path.unlink(missing_ok=True)
        return self._lines

    def _follow(self) -> None:
        pending = b''
        while True:  # no end of file: the reader holds a writer until the end line
            *whole, pending = (pending + os.read(self._read, 1 << 16)).split(b'\n')
            for line in whole:
                if line == self._end:
                    return
                if line.strip():
                    self._lines.append(line.decode(errors='replace'))


def withhold(names: Collection[str]) -> None:
    """Remove ``names`` from this process's environment, so that no child gets them: a credential that it holds itself.

    A worker withholds its own (``NetworkConfig.CREDENTIALS``) once it dialed the server, as the jobs that it runs are
    not the worker.
    """
    for name in names:
        os.environ.pop(name, None)


def started(pid: int) -> float | None:
    """Return the start time of ``pid``, or None when it is gone. A reused pid has a different start time.

    A zombie still counts, because its pid is not reused yet.
    """
    try:
        return psutil.Process(pid).create_time()
    except psutil.Error:
        return None


def alive(pid: int, start: float) -> bool:
    """Whether ``pid`` is still the process that started at ``start`` and has not exited (a zombie has exited)."""
    try:
        proc = psutil.Process(pid)
        return proc.create_time() == start and proc.status() != psutil.STATUS_ZOMBIE
    except psutil.Error:
        return False


def command_line(pid: int) -> str | None:
    """Return the command line of ``pid``, or None when it is gone or not readable."""
    try:
        return ' '.join(psutil.Process(pid).cmdline())
    except psutil.Error:
        return None


def environment(pid: int) -> dict[str, str] | None:
    """Return the environment that ``pid`` started with, or None when it is gone or belongs to another user."""
    try:
        return psutil.Process(pid).environ()
    except psutil.Error:
        return None


class Inherited:
    """The children that a process already had when it began: an in-place restart (``exec``) hands them over.

    ``piloop restart`` execs the daemon while its threads still wait on subprocesses (an agent's turn, ``git``).
    They stay children of the pid, but the new program has no ``Popen`` for them, so each one that exits would stay a
    zombie until the pid exits: the campaign build's daemon had 26 after two restarts. ``watch`` waits for each one in a
    thread of its own, and only for these pids, so it never takes the exit status of a child that a ``Popen`` of this
    program owns.
    """

    def __init__(self, pids: set[int]) -> None:
        self._pids = pids

    @classmethod
    def now(cls) -> Inherited:
        """The children of this process now. Take it before this program starts any child of its own."""
        return cls({child.pid for child in psutil.Process().children()})

    def watch(self) -> None:
        """Reap each inherited child when it exits: one thread for each, that waits for it."""
        for pid in sorted(self._pids):
            threading.Thread(target=_reap, args=(pid,), name=f'inherited-{pid}', daemon=True).start()


def _reap(pid: int) -> None:
    with contextlib.suppress(ChildProcessError):  # not a child any more: reaped elsewhere
        os.waitpid(pid, 0)
