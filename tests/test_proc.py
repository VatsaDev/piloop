import os
import subprocess
import sys
import time

import pytest

from piloop.proc import UNITS_ENV, Inherited, child_env, kill_group


def test_kill_group_tolerates_a_reaped_child() -> None:
    proc = subprocess.Popen(['true'], start_new_session=True)
    proc.wait()
    kill_group(proc)


def test_no_child_inherits_a_units_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(UNITS_ENV, '/somewhere')
    assert UNITS_ENV not in child_env()


def test_no_child_inherits_the_signature_of_the_agent_that_ran_piloop(monkeypatch: pytest.MonkeyPatch) -> None:
    """A daemon or an evaluation that an agent's ``piloop`` command starts must not sign as that agent."""
    monkeypatch.setenv('PILOOP_ACTOR', 'implementer#1')
    assert 'PILOOP_ACTOR' not in child_env()


AFTER_EXEC = """
import time
import psutil
from piloop.proc import Inherited

inherited = Inherited.now()
zombies = lambda: sum(c.status() == psutil.STATUS_ZOMBIE for c in psutil.Process().children())
alive = lambda: len(psutil.Process().children())
inherited.watch()
deadline = time.monotonic() + 10
while alive() and time.monotonic() < deadline:
    time.sleep(0.01)
print(alive(), zombies())
"""
BEFORE_EXEC = f"""
import os, subprocess, sys
subprocess.Popen(['sleep', '0.2'])  # still running at the exec, as an agent's turn is
os.execv(sys.executable, [sys.executable, '-c', {AFTER_EXEC!r}])
"""


def test_a_child_that_outlives_an_exec_is_reaped_by_the_program_after_it() -> None:
    """The campaign build's daemon kept 26 zombie children: each in-place restart (an exec) left the
    subprocesses its threads were waiting on, and nothing in the new program waited for them."""
    out = subprocess.run([sys.executable, '-c', BEFORE_EXEC], capture_output=True, text=True, timeout=30, check=True)
    assert out.stdout.split() == ['0', '0'], 'the child was reaped when it exited: no zombie is left'


def test_a_child_still_running_is_reaped_when_it_exits() -> None:
    """The child blocks on a pipe until the watch began, whatever the host's speed."""
    read, write = os.pipe()
    actions = [(os.POSIX_SPAWN_DUP2, read, 0), (os.POSIX_SPAWN_CLOSE, write)]
    child = os.posix_spawn('/bin/sh', ['sh', '-c', 'read line'], os.environ, file_actions=actions)
    os.close(read)
    Inherited({child}).watch()
    os.close(write)  # end of input: the child exits
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and _child(child):
        time.sleep(0.05)
    assert not _child(child), 'reaped'


def test_a_pid_that_is_no_child_is_left_alone() -> None:
    Inherited({os.getpid()}).watch()  # the watch of a pid that is no child ends at once


def _child(pid: int) -> bool:
    """Whether ``pid`` is still a child of this process, one that runs or that nobody reaped."""
    try:
        os.waitpid(pid, os.WNOHANG)
    except ChildProcessError:
        return False
    return True
