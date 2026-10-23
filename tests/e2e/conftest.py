"""The fixtures that make campaigns, and the hooks that skip a test whose earlier steps failed and print the costs.

A scenario is one test module. Its module-scoped fixture calls ``make_campaign`` with the ``piloop.toml`` keys that
it needs; its tests are the steps, in the order of the module (see ``campaign.needs``). Each scenario runs in two modes,
with the same expected results (``Mode``): ``--e2e-mode`` picks one.
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
from enum import StrEnum
from pathlib import Path
from typing import Protocol

import pytest
import tomli_w

from piloop.harnesses import SESSION_ENV
from piloop.plugins.network.https import TOKEN_ENV
from piloop.settings import UNDER_TEST_ENV
from tests.conftest import wait_until
from tests.e2e.campaign import CHEAPEST_MODEL, ROOT, TEMPLATES, WORKER_LOG, Elsewhere, Smoke, Toml, merged
from tests.fixtures.netfault import Proxy
from tests.isolation import CALLER_ENV

PASSED = pytest.StashKey[set[str]]()
SUMMARY = pytest.StashKey[list[str]]()
DROPPED_ENV = re.compile(r'^(PILOOP_|VIRTUAL_ENV$)')
"""Variables of the calling shell that must not reach the campaigns: a piloop setting of the real install."""


def caller_env(home: Path) -> dict[str, str]:
    """The environment the test session started with (``CALLER_ENV``), with the real harness homes that the agents
    need to log in, but without ``DROPPED_ENV`` and without the session of a harness that runs the tests (a Claude
    Code's messaging socket would reach the agents), and with ``home`` as ``PILOOP_HOME``. ``HOME`` is the real one
    here, so without a ``PILOOP_HOME`` every piloop in it would refuse its default, the real install's home."""
    kept = {k: v for k, v in CALLER_ENV.items() if not DROPPED_ENV.match(k) and k not in SESSION_ENV}
    return kept | {UNDER_TEST_ENV: '1', 'PILOOP_HOME': str(home)}


class Mode(StrEnum):
    """Where a scenario's jobs run."""

    LOCAL = 'local'
    """On the daemon's own worker, in its home."""
    DISTRIBUTED = 'distributed'
    """On ``piloop worker --server`` in a second home, which reaches ``piloop serve --listen`` through a fault proxy
    (``tests/fixtures/netfault.py``) and shares its CAS through ``piloop cas serve``. The daemon's own worker runs
    nothing: the scenario's pool ``serve`` is the worker's."""


ELSEWHERE = 'elsewhere'
"""The worker of distributed mode, and its pool."""


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Run each scenario that makes its campaign with ``make_campaign`` in each mode of ``--e2e-mode``."""
    if 'e2e_mode' in metafunc.fixturenames:
        modes = metafunc.config.getoption('e2e_mode') or list(Mode)
        metafunc.parametrize('e2e_mode', [Mode(m) for m in modes], ids=str, scope='module')


def distributed(home_config: Toml | None) -> dict[str, object]:
    """``home_config`` with the pool ``serve`` moved to the worker elsewhere, and none of its slots left on the daemon."""
    plugins = (home_config or {}).get('plugins', {})
    executor = plugins.get('executor', {}) if isinstance(plugins, dict) else {}
    pools = executor.get('pools', {}) if isinstance(executor, dict) else {}
    serve = pools.get('serve', {}) if isinstance(pools, dict) else {}
    nothing = {'agents': 0, 'evaluations': 0}
    return merged(
        home_config or {},
        {'plugins': {'executor': {'provider': 'workers', 'pools': {'serve': nothing, ELSEWHERE: serve}}}},
    )


@dataclass(frozen=True)
class Install:
    """piloop installed from this checkout, once per session."""

    root: Path
    """The session's temporary directory: the venv, then one directory per campaign. It stays for the logs."""
    bin_dir: Path
    """The directory with only this install's ``piloop``."""

    def env(self, home: Path) -> dict[str, str]:
        """``caller_env(home)``, with this install's ``piloop`` first on PATH."""
        base = caller_env(home)
        return base | {'PATH': f'{self.bin_dir}{os.pathsep}{base["PATH"]}'}

    def version(self) -> str:
        """What ``piloop --version`` of this install prints, in a home of its own."""
        env = self.env(self.root / 'version-home')
        return subprocess.run(['piloop', '--version'], env=env, capture_output=True, text=True, check=True).stdout


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
    install = Install(root, bin_dir)
    version = install.version()
    expected = tomllib.loads((ROOT / 'pyproject.toml').read_text())['project']['version']
    if expected not in version:
        raise RuntimeError(f'installed piloop says {version.strip()!r}, the checkout is {expected}')
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
def make_campaign(request: pytest.FixtureRequest, piloop_install: Install, e2e_mode: Mode) -> Iterator[MakeCampaign]:
    """Make campaigns as a user does, each with its daemon running; stop and kill them all when the module ends.

    Each campaign copies the bundled example ``template``, writes ``config`` over its ``piloop.toml`` (tables merge
    key by key), and writes ``home_config`` as the ``config.toml`` of a new ``PILOOP_HOME``. Then it runs
    ``piloop init <repo> --example <template>`` (which keeps that ``piloop.toml`` and fetches the example's data),
    ``piloop install-skills`` into temporary skills directories, and ``piloop serve --detach``. ``harness`` and
    ``model`` default to ``--e2e-harness`` and ``--e2e-model``; every role of the campaign uses that harness and model.
    In distributed mode, the daemon listens, and a worker elsewhere runs every job (``elsewhere``).
    """
    made: list[tuple[Smoke, str, float]] = []
    module = f'{request.module.__name__.rpartition(".")[2]}[{e2e_mode}]'

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
        directory = piloop_install.root / f'{module.replace("[", "-").rstrip("]")}-{len(made)}'
        repo, home = directory / 'repo', directory / 'home'
        if not shutil.which(harness, path=piloop_install.env(home)['PATH']):
            raise RuntimeError(f'{harness} is not on PATH')
        shutil.copytree(TEMPLATES / template, repo, ignore=shutil.ignore_patterns('data', '.venv', '__pycache__'))
        toml = merged(
            merged(tomllib.loads((repo / 'piloop.toml').read_text()), config or {}),
            {'harnesses': [harness], 'model': {harness: model}, 'roles': {'research': {'model': {harness: model}}}},
        )
        (repo / 'piloop.toml').write_text(tomli_w.dumps(toml))
        home.mkdir()
        if e2e_mode is Mode.DISTRIBUTED:
            home_config = distributed(home_config)
        if home_config:
            (home / 'config.toml').write_text(tomli_w.dumps(home_config))
        env = piloop_install.env(home) | {
            'PILOOP_AGENT_SKILLS_DIR': str(directory / 'skills' / 'agents'),
            'PILOOP_CLAUDE_SKILLS_DIR': str(directory / 'skills' / 'claude'),
        }
        live_campaign = Smoke(directory, str(toml['name']), env)
        made.append((live_campaign, f'harness {harness}, model {model}', time.monotonic()))
        live_campaign.piloop('init', str(repo), '--example', template)
        live_campaign.piloop('install-skills')
        match e2e_mode:
            case Mode.LOCAL:
                live_campaign.piloop('serve', '--detach')
                wait_until((home / 'serve.pid').is_file)
            case Mode.DISTRIBUTED:
                live_campaign.elsewhere = elsewhere(live_campaign, piloop_install, directory / 'worker')
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


def elsewhere(live_campaign: Smoke, install: Install, home: Path) -> Elsewhere:
    """Start the daemon on a loopback port, ``piloop cas serve`` beside it, and ``piloop worker`` in ``home``.

    The worker reaches the daemon through a fault proxy, and the CAS directly: as a machine elsewhere does.
    """
    live_campaign.piloop('serve', '--detach', '--listen', '127.0.0.1:0')
    wait_until(lambda: live_campaign.status()['daemon']['listen'], timeout=60)
    listen = live_campaign.status()['daemon']['listen']
    assert listen is not None
    secret = live_campaign.piloop('token', 'create', ELSEWHERE).strip()
    argv = ['piloop', 'cas', 'serve', '--listen', '127.0.0.1:0']
    cas = subprocess.Popen(argv, env=live_campaign.env, cwd=live_campaign.dir, stdout=subprocess.PIPE, text=True)
    assert cas.stdout is not None
    serving = cas.stdout.readline()
    if not serving.startswith('cas serving on '):
        cas.kill()
        raise RuntimeError(f'piloop cas serve did not start: {serving!r}')
    proxy = Proxy(('127.0.0.1', int(listen.rpartition(':')[2])))
    home.mkdir()
    cas_url = serving.split()[-1]
    (home / 'config.toml').write_text(tomli_w.dumps({'plugins': {'cas': {'provider': 'http', 'url': cas_url}}}))
    server = f'http://127.0.0.1:{proxy.port}'
    with (home / WORKER_LOG).open('wb') as log:
        worker = subprocess.Popen(
            ['piloop', 'worker', '--server', server, '--pool', ELSEWHERE],
            env=install.env(home) | {TOKEN_ENV: secret},
            cwd=home,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    found = Elsewhere(home, worker, cas, proxy)
    try:
        wait_until(lambda: live_campaign.sql('SELECT id FROM lease_workers WHERE id = ?', ELSEWHERE), timeout=60)
    except AssertionError:
        found.stop()
        raise AssertionError((home / WORKER_LOG).read_text()) from None
    return found


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item: pytest.Item) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    report = yield
    if report.when == 'call' and report.passed:
        item.config.stash.setdefault(PASSED, set()).add(item.nodeid)
    return report


def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip a test marked ``needs`` (see ``campaign.needs``) unless the tests of its module that it names passed.

    In the same mode: ``test_x[distributed]`` needs ``test_y[distributed]``.
    """
    passed = item.config.stash.get(PASSED, set())
    module, _, test = item.nodeid.rpartition('::')
    mode = test[test.index('[') :] if '[' in test else ''
    for mark in item.iter_markers('needs'):
        if missing := [name for name in mark.args if f'{module}::{name}{mode}' not in passed]:
            pytest.skip(f'needs {", ".join(missing)}, which did not pass')


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter, config: pytest.Config) -> None:
    if lines := config.stash.get(SUMMARY, None):
        terminalreporter.write_sep('=', 'e2e campaigns')
        terminalreporter.write_line('The temporary directories stay for these logs: remove them when you are done.')
        for line in lines:
            terminalreporter.write_line(line)
