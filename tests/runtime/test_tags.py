import contextlib
import os
import subprocess
import sys
import time

import psutil
import pytest

from piloop.cli import main as cli_main
from piloop.plugins.runtime.contract import TAG_ENV
from piloop.plugins.runtime.kit.tags import kill_tagged, tagged
from piloop.proc import child_env


def test_nothing_that_an_agents_piloop_command_starts_carries_its_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    """A daemon or an evaluation that an agent's ``piloop`` starts would die with the agent. The command drops the tag
    when it starts; the runtime gives each agent its own tag, after this."""
    monkeypatch.setenv(TAG_ENV, 'w-1')
    seen: list[dict[str, str]] = []
    monkeypatch.setattr(cli_main, 'app', lambda: seen.append(child_env()))
    cli_main.main()
    assert TAG_ENV not in seen[0]


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
    monkeypatch.setattr('piloop.plugins.runtime.kit.tags._tagged', lambda marker: next(scans, []))
    kill_tagged('X=1')
    monkeypatch.setattr('piloop.plugins.runtime.kit.tags._tagged', lambda marker: [scanned])
    assert tagged('X=1') == [], 'gone before its session and command line were read'


def test_kill_tagged_ends_what_it_found_in_its_last_round(monkeypatch: pytest.MonkeyPatch) -> None:
    token = os.urandom(8).hex()
    child = subprocess.Popen(['sleep', '30'], env={**os.environ, TAG_ENV: token})
    monkeypatch.setattr('piloop.plugins.runtime.kit.tags.KILL_SCANS', 1)
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
