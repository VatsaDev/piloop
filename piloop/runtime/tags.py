"""The process tag: how a runtime finds and ends every process of an agent, also one that left the agent's session.

A runtime starts each agent with ``TAG_ENV`` set to a random value of its own, and every process that the agent starts
inherits it. The tag says nothing about who the agent is: ``PILOOP_ACTOR`` does. ``piloop.proc.child_env`` drops it,
so a daemon or an evaluation that an agent's ``piloop`` command starts does not die with the agent.
"""

import contextlib
import os
import time

import psutil

from piloop import deprecated

TAG_ENV = 'PILOOP_PROCESS_TAG'
"""The variable that holds an agent's process tag."""
deprecated.until('0.3.0', 'PILOOP_WORKER_TOKEN of an agent that 0.1.0 started: delete OLD_TAG_ENV and its uses')
OLD_TAG_ENV = 'PILOOP_WORKER_TOKEN'
"""``TAG_ENV`` before 0.2.0. ``tagged`` still finds the processes of an agent that an older daemon started: the agent
outlives an upgrade."""
KILL_SCANS = 50
"""How many rounds ``kill_tagged`` makes, at most, to end the processes that a tree forks meanwhile."""
KILL_POLL_SECONDS = 0.05
"""How long ``kill_tagged`` lets the processes of one round exit before it reads the process table again."""


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
    names = (name, OLD_TAG_ENV) if name == TAG_ENV else (name,)
    found: list[psutil.Process] = []
    for proc in psutil.process_iter():
        try:
            if proc.status() != psutil.STATUS_ZOMBIE and value in (proc.environ().get(n) for n in names):
                found.append(proc)
        except psutil.Error:
            continue
    return found
