"""Processes: the environment children get, how to kill a process group, and how to look at other processes.

Looking at other processes goes through psutil, so it works the same on Linux and macOS.
"""

import contextlib
import os
import signal
import subprocess

import psutil

STDERR_IN_ERROR = 500
"""The number of characters of a failed command's stderr that piloop keeps in the error it stores on a row."""
UNITS_ENV = 'PILOOP_UNITS_FILE'
"""Where the evaluate command writes its per-unit results. Set only for the daemon's own evaluations."""


def kill_group(proc: subprocess.Popen[bytes]) -> None:
    """Send SIGKILL to the whole session of the child, which is its leader. Do nothing if the group is already gone."""
    with contextlib.suppress(ProcessLookupError):
        os.killpg(proc.pid, signal.SIGKILL)


def child_env(*drop: str) -> dict[str, str]:
    """The environment for subprocesses: this process's environment without VIRTUAL_ENV, ``UNITS_ENV`` and ``drop``.

    Without VIRTUAL_ENV, uv does not warn about the project's own virtual environment. Only ``run_evaluate`` sets
    ``UNITS_ENV``, so no agent gets a units path.

    This is the only place outside ``Settings`` that reads the environment. It passes the values on and does not
    interpret them.
    """
    return {k: v for k, v in os.environ.items() if k not in ('VIRTUAL_ENV', UNITS_ENV) and k not in drop}


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
    name, _, value = marker.partition('=')
    found: list[tuple[int, int, str]] = []
    for proc in psutil.process_iter():
        try:
            if proc.status() == psutil.STATUS_ZOMBIE or proc.environ().get(name) != value:
                continue
            found.append((proc.pid, os.getsid(proc.pid), ' '.join(proc.cmdline())))
        except psutil.Error, ProcessLookupError, PermissionError:
            continue
    return found
