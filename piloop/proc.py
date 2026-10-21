"""Processes: the environment children get, how to kill a process group, and how to look at other processes.

Looking at other processes goes through psutil, so it works the same on Linux and macOS.
"""

import contextlib
import os
import signal
import subprocess
import sys

import psutil

STDERR_IN_ERROR = 500
"""The number of characters of a failed command's stderr that piloop keeps in the error it stores on a row."""
UNITS_ENV = 'PILOOP_UNITS_FILE'
"""Where the evaluate command writes its per-unit results. Set only for the daemon's own evaluations."""
ACTOR_ENV = 'PILOOP_ACTOR'
"""The signature of an agent. ``child_env`` drops it: a daemon or an evaluation that an agent's ``piloop`` command
starts must not sign as that agent. Nor must it die with the agent: the CLI drops the agent's process tag when it starts
(``piloop.cli.main``)."""
PILOOP_ARGV = (sys.executable, '-P', '-m', 'piloop')
"""The command that runs this process's piloop, from any cwd. ``-P`` keeps the cwd off ``sys.path``: an agent's cwd is a
campaign's worktree, and its own ``json.py`` or ``piloop/`` would otherwise shadow piloop's imports."""


def kill_group(proc: subprocess.Popen[bytes]) -> None:
    """Send SIGKILL to the whole session of the child, which is its leader. Do nothing if the group is already gone."""
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGKILL)


def child_env(*drop: str) -> dict[str, str]:
    """The environment for subprocesses: this process's environment without the variables that it must not pass on.

    These are VIRTUAL_ENV, ``UNITS_ENV``, ``ACTOR_ENV`` and ``drop``. Without VIRTUAL_ENV, uv does not warn about the
    campaign's own virtual environment. Only ``run_evaluate`` sets ``UNITS_ENV``, so no agent gets a units path. The
    launcher signs each agent and its runtime tags it, after this.

    This is the only place outside ``Settings`` that reads the environment. It passes the values on and does not
    interpret them.
    """
    dropped = ('VIRTUAL_ENV', UNITS_ENV, ACTOR_ENV, *drop)
    return {k: v for k, v in os.environ.items() if k not in dropped}


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

    ``piloop restart`` execs the daemon while its threads still wait on subprocesses (``herdr agent wait``, ``git``).
    They stay children of the pid, but the new program has no ``Popen`` for them, so each one that exits stays a zombie
    until the pid exits: the campaign build's daemon had 26 after two restarts. ``reap`` waits for the ones that have
    exited, and only for these pids, so it never takes the exit status of a child that a ``Popen`` of this program owns.
    """

    def __init__(self, pids: set[int]) -> None:
        self._pids = pids

    @classmethod
    def now(cls) -> Inherited:
        """The children of this process now. Take it before this program starts any child of its own."""
        return cls({child.pid for child in psutil.Process().children()})

    def reap(self) -> int:
        """Wait for each inherited child that has exited, without blocking. Return how many were reaped."""
        reaped = 0
        for pid in sorted(self._pids):
            try:
                done, _ = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:  # not a child any more: reaped elsewhere
                self._pids.discard(pid)
                continue
            if done:
                self._pids.discard(pid)
                reaped += 1
        return reaped
