import os
import subprocess
import sys
import time
from pathlib import Path

import psutil
import pytest

from piloop.proc import (
    AGENT_TOKEN_ENV,
    RESULT_ENV,
    UNITS_ENV,
    Inherited,
    ResultPipe,
    Started,
    child_env,
    kill_group,
    started,
    withhold,
)


def test_kill_group_tolerates_a_reaped_child() -> None:
    proc = subprocess.Popen(['true'], start_new_session=True)
    proc.wait()
    kill_group(proc)


def test_kill_group_takes_eperm_for_gone_when_the_child_exited(monkeypatch: pytest.MonkeyPatch) -> None:
    """macOS answers EPERM, not ESRCH, to a group whose processes all exited but are not reaped yet."""
    proc = subprocess.Popen(['true'], start_new_session=True)
    deadline = time.monotonic() + 10
    while psutil.Process(proc.pid).status() != psutil.STATUS_ZOMBIE and time.monotonic() < deadline:
        time.sleep(0.01)
    monkeypatch.setattr(os, 'killpg', _eperm)
    kill_group(proc)
    assert proc.returncode == 0, 'reaped'


def test_kill_group_raises_eperm_while_the_child_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    proc = subprocess.Popen(['sleep', '30'], start_new_session=True)
    try:
        monkeypatch.setattr(os, 'killpg', _eperm)
        with pytest.raises(PermissionError):
            kill_group(proc)
    finally:
        monkeypatch.undo()
        kill_group(proc)
        proc.wait()


def test_kill_group_of_a_process_that_is_no_child_takes_eperm_for_gone_once_it_exited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(os, 'killpg', _eperm)
    kill_group(Started(os.getpid(), -1.0))  # a reused pid: the process that it named exited
    with pytest.raises(PermissionError):
        kill_group(Started(os.getpid(), started(os.getpid()) or -1.0))


def _eperm(*_: object) -> None:
    raise PermissionError(1, 'Operation not permitted')


def test_no_child_inherits_a_units_or_result_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(UNITS_ENV, '/somewhere')
    monkeypatch.setenv(RESULT_ENV, '/somewhere')
    assert UNITS_ENV not in child_env() and RESULT_ENV not in child_env()


def test_results_reads_lines_of_a_child_that_reopens_the_pipe_for_each(tmp_path: Path) -> None:
    """A pipe ends when its last writer closes it: the reader's own write end keeps it open across the child's."""
    code = (
        "import os\nfor i in range(3):\n    with open(os.environ['PILOOP_RESULT_FILE'], 'a') as f: f.write(f'{i}\\n')"
    )
    with ResultPipe(tmp_path / 'r') as results:
        subprocess.run([sys.executable, '-c', code], env={RESULT_ENV: str(results.path)}, check=True)
    assert results.close() == ['0', '1', '2']
    assert not (tmp_path / 'r').exists()


def test_results_of_a_child_that_wrote_nothing_are_none(tmp_path: Path) -> None:
    with ResultPipe(tmp_path / 'r') as results:
        pass
    assert results.close() == []


def test_no_child_inherits_the_signature_of_the_agent_that_ran_piloop(monkeypatch: pytest.MonkeyPatch) -> None:
    """A daemon or an evaluation that an agent's ``piloop`` command starts must not sign as that agent."""
    monkeypatch.setenv('PILOOP_ACTOR', 'implementer#1')
    assert 'PILOOP_ACTOR' not in child_env()


AFTER_EXEC = """
import time
from pathlib import Path
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


def test_a_withheld_variable_and_an_agent_s_token_reach_no_child(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('WORKER_CREDENTIAL', 'secret')
    monkeypatch.setenv(AGENT_TOKEN_ENV, 'plt_agent')
    assert child_env()['WORKER_CREDENTIAL'] == 'secret' and AGENT_TOKEN_ENV not in child_env()
    withhold({'WORKER_CREDENTIAL', 'NEVER_SET'})
    assert 'WORKER_CREDENTIAL' not in child_env() and 'WORKER_CREDENTIAL' not in os.environ
