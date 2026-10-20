"""Processes: the environment children get, how to kill a process group, and how to look at other processes.

Looking at other processes goes through psutil, so it works the same on Linux and macOS.
"""

import contextlib
import os
import signal
import subprocess
import sys
import time

import psutil

STDERR_IN_ERROR = 500
"""The number of characters of a failed command's stderr that piloop keeps in the error it stores on a row."""
UNITS_ENV = 'PILOOP_UNITS_FILE'
"""Where the evaluate command writes its per-unit results. Set only for the daemon's own evaluations."""
TOKEN_ENV = 'PILOOP_WORKER_TOKEN'
"""Set for each agent to a value unique to the agent. Every process the agent starts inherits it, also one that leaves
the agent's session, so ``tagged`` finds them all."""
AGENT_ENV = ('PILOOP_ACTOR', TOKEN_ENV)
"""The variables that piloop sets for one agent. ``child_env`` drops them: a daemon or an evaluation that an agent's
``piloop`` command starts must not sign as that agent, nor die when the agent is killed."""
PILOOP_ARGV = (sys.executable, '-P', '-m', 'piloop')
"""The command that runs this process's piloop, from any cwd. ``-P`` keeps the cwd off ``sys.path``: an agent's cwd is a
campaign's worktree, and its own ``json.py`` or ``piloop/`` would otherwise shadow piloop's imports."""
KILL_SCANS = 50
"""How many rounds ``kill_tagged`` makes, at most, to end the processes that a tree forks meanwhile."""
KILL_POLL_SECONDS = 0.05
"""How long ``kill_tagged`` lets the processes of one round exit before it reads the process table again."""


def kill_group(proc: subprocess.Popen[bytes]) -> None:
    """Send SIGKILL to the whole session of the child, which is its leader. Do nothing if the group is already gone."""
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGKILL)


def child_env(*drop: str) -> dict[str, str]:
    """The environment for subprocesses: this process's environment without the variables that it must not pass on.

    These are VIRTUAL_ENV, ``UNITS_ENV``, ``AGENT_ENV`` and ``drop``. Without VIRTUAL_ENV, uv does not warn about the
    campaign's own virtual environment. Only ``run_evaluate`` sets ``UNITS_ENV``, so no agent gets a units path. The
    launcher signs each agent and its runtime tags it, after this.

    This is the only place outside ``Settings`` that reads the environment. It passes the values on and does not
    interpret them.
    """
    dropped = ('VIRTUAL_ENV', UNITS_ENV, *AGENT_ENV, *drop)
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


def tagged(marker: str) -> list[tuple[int, int, str]]:
    """Return ``(pid, session id, command line)`` of each live process whose environment holds ``marker``.

    ``marker`` is ``NAME=value``. Skips processes of other users, zombies and processes that end while they are read.
    """
    found: list[tuple[int, int, str]] = []
    for proc in _tagged(marker):
        try:
            found.append((proc.pid, os.getsid(proc.pid), ' '.join(proc.cmdline())))
        except psutil.Error, ProcessLookupError, PermissionError:
            continue
    return found


def kill_tagged(marker: str) -> None:
    """End every process whose environment holds ``marker`` (see ``tagged``), in any session.

    A build forks while the process table is read, and a fork that is under way when SIGSTOP arrives still completes.
    So each round stops (SIGSTOP) every tagged process that the scan found, so none of them forks while the others are
    killed, then kills them all (SIGKILL), and gives them ``KILL_POLL_SECONDS`` to exit. The rounds go on until a scan
    finds none, at most ``KILL_SCANS`` times. ``psutil`` checks that a pid is still the process that the scan found
    before it sends a signal, so a reused pid is never signalled.
    """
    for _ in range(KILL_SCANS):
        found = _tagged(marker)
        if not found:
            return
        for proc in found:
            with contextlib.suppress(psutil.Error):
                proc.suspend()
        for proc in found:
            with contextlib.suppress(psutil.Error):
                proc.kill()
        time.sleep(KILL_POLL_SECONDS)


def _tagged(marker: str) -> list[psutil.Process]:
    name, _, value = marker.partition('=')
    found: list[psutil.Process] = []
    for proc in psutil.process_iter():
        try:
            if proc.status() != psutil.STATUS_ZOMBIE and proc.environ().get(name) == value:
                found.append(proc)
        except psutil.Error:
            continue
    return found


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
