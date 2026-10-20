import contextlib
import os
import subprocess
import sys
import time

import psutil
import pytest

from piloop.proc import TOKEN_ENV, UNITS_ENV, Inherited, child_env, kill_group, kill_tagged, tagged


def test_kill_group_tolerates_a_reaped_child() -> None:
    proc = subprocess.Popen(['true'], start_new_session=True)
    proc.wait()
    kill_group(proc)


def test_no_child_inherits_a_units_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(UNITS_ENV, '/somewhere')
    assert UNITS_ENV not in child_env()


def test_no_child_inherits_the_token_or_the_signature_of_the_agent_that_ran_piloop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An agent that runs ``piloop`` would hand its token to what piloop starts, a daemon or an evaluation, and the end
    of the agent would kill them. The runtime gives each agent its own token, after this."""
    monkeypatch.setenv(TOKEN_ENV, 'w-1')
    monkeypatch.setenv('PILOOP_ACTOR', 'worker#1')
    assert TOKEN_ENV not in child_env() and 'PILOOP_ACTOR' not in child_env()


FORKER = """
import os, time
while True:
    if os.fork() == 0:
        time.sleep(60)
        os._exit(0)
    time.sleep(0.002)
"""


def test_kill_tagged_tolerates_a_process_that_ends_after_the_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    child = subprocess.Popen(['sleep', '30'])
    scanned = psutil.Process(child.pid)
    child.kill()
    child.wait()
    scans = iter([[scanned]])
    monkeypatch.setattr('piloop.proc._tagged', lambda marker: next(scans, []))
    kill_tagged('X=1')
    monkeypatch.setattr('piloop.proc._tagged', lambda marker: [scanned])
    assert tagged('X=1') == [], 'gone before its session and command line were read'


def test_kill_tagged_ends_what_it_found_in_its_last_round(monkeypatch: pytest.MonkeyPatch) -> None:
    token = os.urandom(8).hex()
    child = subprocess.Popen(['sleep', '30'], env={**os.environ, TOKEN_ENV: token})
    monkeypatch.setattr('piloop.proc.KILL_SCANS', 1)
    kill_tagged(f'{TOKEN_ENV}={token}')
    assert child.wait(5) == -9


def test_kill_tagged_ends_a_tree_that_forks_while_it_is_killed() -> None:
    """A build (``make -j``, ``lake build``) forks while the kill reads the process table: one pass of SIGKILL over
    the processes it found left about 20 of 330 alive. Each is stopped first, and the scan repeats until none is new."""
    token = os.urandom(8).hex()
    marker = f'{TOKEN_ENV}={token}'
    forker = subprocess.Popen(
        [sys.executable, '-c', FORKER], env={**os.environ, TOKEN_ENV: token}, start_new_session=True
    )
    try:
        deadline = time.monotonic() + 10
        while len(tagged(marker)) < 100 and time.monotonic() < deadline:
            time.sleep(0.05)
        kill_tagged(marker)
        assert tagged(marker) == []
    finally:
        for pid, _, _ in tagged(marker):
            with contextlib.suppress(psutil.Error):
                psutil.Process(pid).kill()
        forker.kill()
        forker.wait()


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


def test_a_child_still_running_is_waited_for_on_a_later_reap() -> None:
    """The child blocks on a pipe until the first reap is done, whatever the host's speed."""
    read, write = os.pipe()
    actions = [(os.POSIX_SPAWN_DUP2, read, 0), (os.POSIX_SPAWN_CLOSE, write)]
    child = os.posix_spawn('/bin/sh', ['sh', '-c', 'read line'], os.environ, file_actions=actions)
    os.close(read)
    inherited = Inherited({child})
    assert inherited.reap() == 0, 'still running: nothing to reap, and it stays watched'
    os.close(write)  # end of input: the child exits
    deadline = time.monotonic() + 10
    reaped = 0
    while not reaped and time.monotonic() < deadline:
        time.sleep(0.05)
        reaped = inherited.reap()
    assert reaped == 1


def test_a_pid_that_is_no_child_is_dropped() -> None:
    inherited = Inherited({os.getpid()})
    assert inherited.reap() == 0
    assert inherited.reap() == 0
