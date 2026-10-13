import subprocess

import pytest

from piloop.proc import UNITS_ENV, child_env, kill_group


def test_kill_group_tolerates_a_reaped_child() -> None:
    proc = subprocess.Popen(['true'], start_new_session=True)
    proc.wait()
    kill_group(proc)


def test_no_child_inherits_a_units_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(UNITS_ENV, '/somewhere')
    assert UNITS_ENV not in child_env()
