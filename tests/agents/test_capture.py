"""Session capture: the Entire version floor (attach itself is exercised through the launcher and the daemon)."""

from pathlib import Path

import pytest

from piloop.agents import capture
from piloop.agents.capture import MIN_ENTIRE, AttachError, entire_version, usable
from piloop.settings import Settings
from tests.conftest import write_script


def test_entire_version_is_parsed_from_the_first_line(entire_log, monkeypatch: pytest.MonkeyPatch) -> None:
    assert entire_version() == '0.10.6'
    monkeypatch.setenv('FAKE_ENTIRE_VERSION', '1.2')
    assert entire_version() == '1.2'
    assert [c['argv'] for c in entire_log()] == [['version'], ['version']]


def test_an_entire_that_is_missing_or_prints_no_version_is_an_attach_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv('PATH', str(tmp_path))
    with pytest.raises(AttachError, match='entire is not on PATH; install it: curl -fsSL https://entire.io/install.sh'):
        entire_version()
    write_script(tmp_path / 'entire', '#!/bin/sh\necho "entire: something else"\n')
    with pytest.raises(AttachError, match='entire version printed no version: entire: something else'):
        entire_version()
    write_script(tmp_path / 'entire', '#!/bin/sh\necho broken >&2\nexit 2\n')
    with pytest.raises(AttachError, match='entire version exit 2: broken'):
        entire_version()
    monkeypatch.setattr(capture, 'settings', Settings(home=tmp_path, entire_attach_timeout_seconds=0.2))
    write_script(tmp_path / 'entire', '#!/bin/sh\n/bin/sleep 5\n')
    with pytest.raises(AttachError, match='entire version timed out after 0.2s'):
        entire_version()


def test_usable_is_the_tested_version_or_newer() -> None:
    assert MIN_ENTIRE == '0.10.6'
    assert usable('0.10.6') and usable('0.11.0') and usable('1.0') and usable('0.10.10')
    assert not usable('0.10.5') and not usable('0.9.9') and not usable('0.10')
