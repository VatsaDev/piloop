"""The process tag: how a runtime finds and ends every process of an agent, also one that left the agent's session.

A runtime starts each agent with ``TAG_ENV`` set to a random value of its own, and every process that the agent starts
inherits it. The tag says nothing about who the agent is: ``PILOOP_ACTOR`` does. ``piloop.proc.child_env`` drops it,
so a daemon or an evaluation that an agent's ``piloop`` command starts does not die with the agent.
"""

import contextlib
import os
import time
from collections.abc import Callable

import psutil

from piloop.plugins.runtime.contract import OLD_TAG_ENV, TAG_ENV
from piloop.proc import alive

KILL_SCANS = 50
"""How many rounds ``kill_tagged`` and ``kill_tree`` make, at most, to end the processes that a tree forks meanwhile."""
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
    _kill(lambda: _tagged(marker))


def kill_tree(pid: int, start: float) -> None:
    """End ``pid``, if it is still the process that started at ``start``, and all its descendants, in any session.

    For an agent whose processes carry no tag: one that 0.1.0 started. A descendant is found by its parent, and the
    child of a killed process goes to init, out of reach. So the scan stops the root, then the children of each
    stopped process, until it finds no new one, and only then are they killed, as ``kill_tagged`` kills. A descendant
    whose parent exited before the kill is with init already, and is not found.
    """
    try:
        root = psutil.Process(pid)
    except psutil.Error:
        return
    _kill(lambda: _stopped_tree(root) if alive(pid, start) else [])


def _kill(find: Callable[[], list[psutil.Process]]) -> None:
    for _ in range(KILL_SCANS):
        found = find()
        if not found:
            return
        for proc in found:
            with contextlib.suppress(psutil.Error):
                proc.suspend()
        for proc in found:
            with contextlib.suppress(psutil.Error):
                proc.kill()
        time.sleep(KILL_POLL_SECONDS)


def _stopped_tree(root: psutil.Process) -> list[psutil.Process]:
    """Stop ``root`` and its descendants, each level before the next is read, so none forks unseen. Return them."""
    tree: list[psutil.Process] = []
    level = [root]
    while level:
        for proc in level:
            with contextlib.suppress(psutil.Error):
                proc.suspend()
        tree += level
        parents = {proc.pid for proc in level}
        level = [proc for proc in psutil.process_iter(['ppid']) if proc.info['ppid'] in parents]
    return tree


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
