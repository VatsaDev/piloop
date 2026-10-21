import contextlib
import os
import subprocess
import sys
import time

import psutil
import pytest

from piloop.cli import main as cli_main
from piloop.proc import child_env, started
from piloop.runtime.tags import OLD_TAG_ENV, TAG_ENV, kill_tagged, kill_tree, tagged


def test_nothing_that_an_agents_piloop_command_starts_carries_its_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    """A daemon or an evaluation that an agent's ``piloop`` starts would die with the agent. The command drops the tag
    when it starts, under either name; the runtime gives each agent its own tag, after this."""
    monkeypatch.setenv(TAG_ENV, 'w-1')
    monkeypatch.setenv(OLD_TAG_ENV, 'w-0')
    seen: list[dict[str, str]] = []
    monkeypatch.setattr(cli_main, 'app', lambda: seen.append(child_env()))
    cli_main.main()
    assert not {TAG_ENV, OLD_TAG_ENV} & set(seen[0])


def test_an_agent_that_an_older_daemon_tagged_is_still_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An agent outlives an upgrade, with the tag under its 0.1.0 name."""
    token = os.urandom(8).hex()
    child = subprocess.Popen(['sleep', '30'], env={**os.environ, OLD_TAG_ENV: token})
    try:
        deadline = time.monotonic() + 5
        while not tagged(f'{TAG_ENV}={token}') and time.monotonic() < deadline:
            time.sleep(0.02)
        assert [pid for pid, _, _ in tagged(f'{TAG_ENV}={token}')] == [child.pid]
        kill_tagged(f'{TAG_ENV}={token}')
        assert child.wait(5) == -9
    finally:
        child.kill()


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
    monkeypatch.setattr('piloop.runtime.tags._tagged', lambda marker: next(scans, []))
    kill_tagged('X=1')
    monkeypatch.setattr('piloop.runtime.tags._tagged', lambda marker: [scanned])
    assert tagged('X=1') == [], 'gone before its session and command line were read'


def test_kill_tagged_ends_what_it_found_in_its_last_round(monkeypatch: pytest.MonkeyPatch) -> None:
    token = os.urandom(8).hex()
    child = subprocess.Popen(['sleep', '30'], env={**os.environ, TAG_ENV: token})
    monkeypatch.setattr('piloop.runtime.tags.KILL_SCANS', 1)
    kill_tagged(f'{TAG_ENV}={token}')
    assert child.wait(5) == -9


def test_kill_tagged_ends_a_tree_that_forks_while_it_is_killed() -> None:
    """A build (``make -j``, ``lake build``) forks while the kill reads the process table: one pass of SIGKILL over
    the processes it found left about 20 of 330 alive. Each is stopped first, and the scan repeats until none is new."""
    token = os.urandom(8).hex()
    marker = f'{TAG_ENV}={token}'
    forker = subprocess.Popen(
        [sys.executable, '-c', FORKER], env={**os.environ, TAG_ENV: token}, start_new_session=True
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


def test_kill_tree_ends_a_tree_that_forks_while_it_is_killed() -> None:
    """A handle without a tag names only the root, so the kill finds the rest by descent, in any session. A child
    whose parent is killed first goes to init, out of reach, so the whole tree is stopped before any of it is killed."""
    tree = os.urandom(8).hex()
    marker = f'PILOOP_TEST_TREE={tree}'  # only to find what is left: kill_tree does not read it
    forker = subprocess.Popen(
        [sys.executable, '-c', FORKER], env={**os.environ, 'PILOOP_TEST_TREE': tree}, start_new_session=True
    )
    try:
        deadline = time.monotonic() + 10
        while len(tagged(marker)) < 100 and time.monotonic() < deadline:
            time.sleep(0.05)
        start = started(forker.pid)
        assert start is not None
        kill_tree(forker.pid, start)
        assert forker.wait(5) == -9
        assert tagged(marker) == []
    finally:
        for pid, _, _ in tagged(marker):
            with contextlib.suppress(psutil.Error):
                psutil.Process(pid).kill()
        forker.kill()
        forker.wait()


def test_kill_tree_leaves_a_reused_pid_alone() -> None:
    kill_tree(os.getpid(), -1.0)
    kill_tree(2**22 + 12345, 0.0)
