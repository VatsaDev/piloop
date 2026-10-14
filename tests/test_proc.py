import os
import subprocess
import sys

import pytest

from piloop.proc import UNITS_ENV, Inherited, child_env, kill_group


def test_kill_group_tolerates_a_reaped_child() -> None:
    proc = subprocess.Popen(['true'], start_new_session=True)
    proc.wait()
    kill_group(proc)


def test_no_child_inherits_a_units_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(UNITS_ENV, '/somewhere')
    assert UNITS_ENV not in child_env()


AFTER_EXEC = """
import time
import psutil
from piloop.proc import Inherited

inherited = Inherited.now()
while not any(c.status() == psutil.STATUS_ZOMBIE for c in psutil.Process().children()):
    time.sleep(0.01)
zombies = lambda: sum(c.status() == psutil.STATUS_ZOMBIE for c in psutil.Process().children())
before = zombies()
reaped = inherited.reap()
print(before, reaped, zombies(), inherited.reap())
"""
BEFORE_EXEC = f"""
import os, subprocess, sys
subprocess.Popen(['sleep', '0.2'])  # still running at the exec, as a daemon thread's herdr call is
os.execv(sys.executable, [sys.executable, '-c', {AFTER_EXEC!r}])
"""


def test_a_child_that_outlives_an_exec_is_reaped_by_the_program_after_it() -> None:
    """The campaign build's daemon kept 26 zombie herdr and git children: each in-place restart (an exec) left the
    subprocesses its threads were waiting on, and nothing in the new program waited for them."""
    out = subprocess.run([sys.executable, '-c', BEFORE_EXEC], capture_output=True, text=True, timeout=30, check=True)
    assert out.stdout.split() == ['1', '1', '0', '0'], 'one zombie before the reap, none after, nothing twice'


def test_a_pid_that_is_no_child_is_dropped() -> None:
    inherited = Inherited({os.getpid()})
    assert inherited.reap() == 0
    assert inherited.reap() == 0
