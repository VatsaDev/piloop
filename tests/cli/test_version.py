"""`piloop --version`: the package version with the installed commit when uv recorded one."""

import re
import subprocess
import sys
from importlib.metadata import version

from piloop.cli.version import describe, installed
from tests.cli.test_cli import run_cli

GIT_INSTALL = '{"url":"https://github.com/hiloopai/piloop","vcs_info":{"vcs":"git","commit_id":"5780463db30736b134d173a7b340c6403da0e558"}}'
DIRECTORY_INSTALL = '{"url":"file:///home/me/piloop","dir_info":{}}'


def test_describe_adds_the_short_commit_of_a_git_install() -> None:
    assert describe('0.1.0', GIT_INSTALL) == 'piloop 0.1.0 (5780463)'
    assert describe('0.1.0', DIRECTORY_INSTALL) == 'piloop 0.1.0'
    assert describe('0.2.0', None) == 'piloop 0.2.0', 'an index install has no direct_url.json'


def test_installed_reads_the_real_metadata() -> None:
    text = installed()
    assert re.fullmatch(rf'piloop {re.escape(version("piloop"))}( \([0-9a-f]{{7}}\))?', text), text


def test_version_flag(capsys) -> None:
    run_cli(['--version'])
    assert capsys.readouterr().out.strip() == installed()
    assert subprocess.run(
        [sys.executable, '-m', 'piloop', '--version'], capture_output=True, text=True
    ).stdout.strip() == (installed())
