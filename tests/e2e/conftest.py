"""The fixtures that make campaigns, and the hooks that skip a test whose earlier steps failed and print the costs.

A scenario is one test module. Its module-scoped fixture calls ``make_campaign`` with the ``piloop.toml`` keys that
it needs; its tests are the steps, in the order of the module (see ``campaign.needs``).
"""

import os
import re
import shutil
import signal
import subprocess
import tempfile
import time
import tomllib
from collections.abc import Generator, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import pytest
import tomli_w

from piloop.settings import UNDER_TEST_ENV
from tests.conftest import wait_until
from tests.e2e.campaign import CHEAPEST_MODEL, ROOT, TEMPLATES, Smoke, Toml, merged
from tests.isolation import CALLER_ENV

PASSED = pytest.StashKey[set[str]]()
SUMMARY = pytest.StashKey[list[str]]()
DROPPED_ENV = re.compile(r'^(PILOOP_|CLAUDECODE$|CLAUDE_CODE_|CLAUDE_PID$|CLAUDE_EFFORT$|VIRTUAL_ENV$)')
"""Variables of the calling shell that must not reach the campaigns: a piloop setting of the real install, or the
session of a Claude Code that runs the tests (its messaging socket would reach the agents)."""


def caller_env() -> dict[str, str]:
    """The environment the test session started with (``CALLER_ENV``), with its real HOME and CLAUDE_CONFIG_DIR, which
    the agents need to log in, and without ``DROPPED_ENV``. Every piloop in it still refuses a real install's home."""
    return {k: v for k, v in CALLER_ENV.items() if not DROPPED_ENV.match(k)} | {UNDER_TEST_ENV: '1'}


@dataclass(frozen=True)
class Install:
    """piloop installed from this checkout, once per session."""

    root: Path
    """The session's temporary directory: the venv, then one directory per campaign. It stays for the logs."""
    env: dict[str, str]
    """``caller_env()``, with only this install's ``piloop`` first on PATH."""


class MakeCampaign(Protocol):
    def __call__(
        self,
        *,
        template: str = 'enwik8',
        config: Toml | None = None,
        home_config: Toml | None = None,
        harness: str | None = None,
        model: str | None = None,
    ) -> Smoke: ...


@pytest.fixture(scope='session')
def piloop_install() -> Iterator[Install]:
    """Install this checkout into a virtual environment of its own. SIGTERM ends the session like Ctrl-C meanwhile,
    so that every campaign's teardown runs then too."""
    # Not tmp_path_factory: pytest keeps the last 3 base temps of any pytest run, so it would delete the logs.
    root = Path(tempfile.mkdtemp(prefix='piloop-e2e-'))
    venv, bin_dir = root / 'venv', root / 'bin'
    subprocess.run(['uv', 'venv', '--quiet', '--python', '3.14', str(venv)], check=True)
    subprocess.run(['uv', 'pip', 'install', '--quiet', '--python', str(venv / 'bin' / 'python'), str(ROOT)], check=True)
    bin_dir.mkdir()
    (bin_dir / 'piloop').symlink_to(venv / 'bin' / 'piloop')
    base = caller_env()
    install = Install(root, base | {'PATH': f'{bin_dir}{os.pathsep}{base["PATH"]}'})
    version = subprocess.run(['piloop', '--version'], env=install.env, capture_output=True, text=True, check=True)
    expected = tomllib.loads((ROOT / 'pyproject.toml').read_text())['project']['version']
    if expected not in version.stdout:
        raise RuntimeError(f'installed piloop says {version.stdout.strip()!r}, the checkout is {expected}')
    previous = signal.signal(signal.SIGTERM, signal.default_int_handler)
    try:
        yield install
    finally:
        signal.signal(signal.SIGTERM, previous)


@pytest.fixture(scope='session')
def e2e_summary(request: pytest.FixtureRequest) -> list[str]:
    """The lines that the terminal summary prints at the end, for a scenario that makes its campaign itself.

    A fixture, not ``SUMMARY``: a test module that imports this conftest gets a second copy of it, with another key.
    """
    return request.config.stash.setdefault(SUMMARY, [])


@pytest.fixture(scope='module')
def make_campaign(request: pytest.FixtureRequest, piloop_install: Install) -> Iterator[MakeCampaign]:
    """Make campaigns as a user does, each with its daemon running; stop and kill them all when the module ends.

    Each campaign copies the bundled example ``template``, writes ``config`` over its ``piloop.toml`` (tables merge
    key by key), and writes ``home_config`` as the ``config.toml`` of a new ``PILOOP_HOME``. Then it runs
    ``piloop init <repo> --example <template>`` (which keeps that ``piloop.toml`` and fetches the example's data),
    ``piloop install-skills`` into temporary skills directories, and ``piloop serve --detach``. ``harness`` and
    ``model`` default to ``--e2e-harness`` and ``--e2e-model``; the campaign uses that harness only.
    """
    made: list[tuple[Smoke, str, float]] = []
    module = request.module.__name__.rpartition('.')[2]

    def make(
        *,
        template: str = 'enwik8',
        config: Toml | None = None,
        home_config: Toml | None = None,
        harness: str | None = None,
        model: str | None = None,
    ) -> Smoke:
        harness = harness or request.config.getoption('e2e_harness')
        model = model or request.config.getoption('e2e_model') or CHEAPEST_MODEL.get(harness)
        if model is None:
            raise pytest.UsageError(f'pass --e2e-model for the harness {harness}')
        if not shutil.which(harness, path=piloop_install.env['PATH']):
            raise RuntimeError(f'{harness} is not on PATH')
        directory = piloop_install.root / f'{module}-{len(made)}'
        repo, home = directory / 'repo', directory / 'home'
        shutil.copytree(TEMPLATES / template, repo, ignore=shutil.ignore_patterns('data', '.venv', '__pycache__'))
        toml = merged(
            merged(tomllib.loads((repo / 'piloop.toml').read_text()), config or {}),
            {'harnesses': [harness], 'model': {harness: model}},
        )
        (repo / 'piloop.toml').write_text(tomli_w.dumps(toml))
        home.mkdir()
        if home_config:
            (home / 'config.toml').write_text(tomli_w.dumps(home_config))
        env = piloop_install.env | {
            'PILOOP_HOME': str(home),
            'PILOOP_AGENT_SKILLS_DIR': str(directory / 'skills' / 'agents'),
            'PILOOP_CLAUDE_SKILLS_DIR': str(directory / 'skills' / 'claude'),
        }
        live_campaign = Smoke(directory, str(toml['name']), env)
        made.append((live_campaign, f'harness {harness}, model {model}', time.monotonic()))
        live_campaign.piloop('init', str(repo), '--example', template)
        live_campaign.piloop('install-skills')
        live_campaign.piloop('serve', '--detach')
        wait_until((home / 'serve.pid').is_file)
        return live_campaign

    try:
        yield make
    finally:
        summary = request.config.stash.setdefault(SUMMARY, [])
        for live_campaign, models, started in made:
            try:
                live_campaign.shut_down()
            finally:
                summary += [
                    f'{module}: {models}, {(time.monotonic() - started) / 60:.1f} minutes',
                    *live_campaign.summary(),
                ]


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    if report.when == 'call' and report.passed:
        item.config.stash.setdefault(PASSED, set()).add(item.nodeid)
    return report


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip a test marked ``needs`` (see ``campaign.needs``) unless the tests of its module that it names passed."""
    passed = item.config.stash.get(PASSED, set())
    module = item.nodeid.rpartition('::')[0]
    for mark in item.iter_markers('needs'):
        if missing := [name for name in mark.args if f'{module}::{name}' not in passed]:
            pytest.skip(f'needs {", ".join(missing)}, which did not pass')


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter, config: pytest.Config) -> None:
    if lines := config.stash.get(SUMMARY, None):
        terminalreporter.write_sep('=', 'e2e campaigns')
        terminalreporter.write_line('The temporary directories stay for these logs: remove them when you are done.')
        for line in lines:
            terminalreporter.write_line(line)
