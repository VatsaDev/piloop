import tests.isolation  # noqa: F401  # first: piloop reads its home from the environment when it is imported

# isort: split
import re
import shutil
import stat
import subprocess
import sys
import threading
import time
import tomllib
import weakref
from collections.abc import Callable, Iterator
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from typing import Concatenate

import httpx2 as httpx
import pytest
from git import Repo
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import select

from piloop.campaign import archive as archive_module, campaigns as campaign_module
from piloop.config import load_campaign_config
from piloop.daemon.main import ACTOR
from piloop.daemon.run_controller import RunController
from piloop.db.models import (
    AgentSession,
    AgentSessionState,
    Campaign,
    Candidate,
    Measurement,
    RunEnd,
    RunStatus,
    Usage,
    Verdict,
)
from piloop.db.session import Database, migrate
from piloop.harnesses import DEFAULT_HARNESSES
from piloop.ids import CampaignId
from piloop.otel import Tracer
from piloop.plugins.cas.contract import CAS, Cas
from piloop.plugins.executor.kit.leases import Timing
from piloop.plugins.executor.workers import Workers, WorkersExecutor
from piloop.plugins.executor.workers.worker import Worker
from piloop.plugins.network.https import TOKEN_ENV, Https
from piloop.plugins.runtime.contract import Handle, Runtime
from piloop.plugins.runtime.subprocess import SubprocessRuntime
from piloop.runtime.factory import RuntimeFactory
from piloop.scoring.evaluate import run_evaluate
from piloop.scoring.ledger import DAEMON, current_evaluator
from piloop.settings import Settings, env_names
from piloop.store import tree as tree_module

# Harness stand-ins: any argv that edits the worktree and commits.
HARNESS_APPEND = '#!/bin/sh\necho line >> solve.txt\ngit commit -qam "add a line"\n'
HARNESS_TWO_COMMITS = (
    '#!/bin/sh\necho a >> solve.txt\ngit commit -qam "first"\necho b >> solve.txt\ngit commit -qam "second"\n'
)
HARNESS_FORGETS_COMMIT = '#!/bin/sh\necho line >> solve.txt\n'
HARNESS_COMMITS_AND_LEAVES_DIRT = (
    '#!/bin/sh\necho line >> solve.txt\ngit commit -qam "the idea"\necho more >> solve.txt\n'
)
HARNESS_NOOP = '#!/bin/sh\ntrue\n'
HARNESS_FAILS = '#!/bin/sh\nexit 3\n'
HARNESS_SLEEPS = '#!/bin/sh\nsleep 30 &\necho $! > ../child.pid\nwait\n'
HARNESS_WORSE = '#!/bin/sh\n: > solve.txt\ngit commit -qam "wipe"\n'
HARNESS_TRY_AND_REVERT = (
    '#!/bin/sh\necho line >> solve.txt\ngit commit -qam "try"\ngit revert --no-edit HEAD >/dev/null\n'
)
HARNESS_PROMPT_TO_FILE = '#!/bin/sh\nprintf %s "$1" > prompt.txt\n'
WRITE_PID = 'echo $$ > ../agent.pid.part\nmv ../agent.pid.part ../agent.pid\n'
"""The agent's pid, by a rename: a test that finds the file never reads it empty."""

EVALUATE = ['sh', '-c', 'echo METRIC score=$(wc -l < solve.txt); echo METRIC size=$(wc -c < solve.txt)']

TOML = """
name = "demo"
evaluate = {evaluate}
runtime = "subprocess"
harnesses = ["script"]
evaluate_timeout = 30
link = ["data"]

[model]
script = "test-model"

[roles.implementer]
timeout = {implementer_timeout}
max_concurrent = {max_implementers}

[roles.watchdog]
harnesses = ["script"]
model = {{ script = "watchdog-model" }}
timeout = 20

[roles.research]
timeout = 20
max_ideas = 0
max_concurrent = 1

[[metrics]]
name = "score"
direction = "higher"
role = "primary"

[[metrics]]
name = "size"
direction = "lower"

[harness.script]
executable = "{script}"
headless = ["{{prompt}}"]
"""


def spent(total: int, turns: int = 1) -> Usage:
    """The usage of a test's agent that spent ``total`` tokens in ``turns`` turns."""
    return Usage(input=0, output=0, cache_read=0, cache_write=0, total=total, turns=turns, source='/x.jsonl')


def end_of(status: RunStatus) -> RunEnd | None:
    """The end of a test's run in ``status``: none while it is open, else the plain end of a run of that status."""
    match status:
        case RunStatus.QUEUED | RunStatus.RUNNING:
            return None
        case RunStatus.SUCCEEDED:
            return RunEnd.FINISHED
        case RunStatus.TIMED_OUT:
            return RunEnd.TIMED_OUT
        case RunStatus.FAILED:
            return RunEnd.FAILED


def write_script(path: Path, body: str) -> Path:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def write_toml(
    campaign: Path,
    harness_body: str,
    *,
    max_implementers: int = 1,
    implementer_timeout: float = 20,
    evaluate: list[str] = EVALUATE,
) -> None:
    script = write_script(campaign.parent / 'harness.sh', harness_body)
    (campaign / 'piloop.toml').write_text(
        TOML.format(
            evaluate=repr(evaluate).replace("'", '"'),
            implementer_timeout=implementer_timeout,
            max_implementers=max_implementers,
            script=script,
        )
    )


def package(root: Path, name: str, version: str, entry_points: str) -> None:
    """Install a distribution called ``name`` at ``version`` under ``root``, with only ``entry_points`` in it.

    Put ``root`` on ``sys.path`` and ``importlib.metadata`` finds its entry points, as those of an installed package.
    """
    info = root / f'{name.replace("-", "_")}-{version}.dist-info'
    info.mkdir(parents=True)
    (info / 'METADATA').write_text(f'Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n')
    (info / 'entry_points.txt').write_text(entry_points)


EXAMPLES = Path(__file__).parent.parent / 'examples' / 'plugins'
"""The example plugin packages. Their modules import (``pythonpath`` in pyproject.toml); ``example`` installs one."""


def piloop_entry_points(group: str) -> list[EntryPoint]:
    """piloop's own entry points of ``group``: its built-in providers, which every test that fakes the others keeps."""
    return [ep for ep in entry_points(group=group) if ep.dist is not None and ep.dist.name == 'piloop']


def example(root: Path, name: str) -> None:
    """Install the example plugin ``examples/plugins/<name>`` under ``root``, with the entry points of its pyproject."""
    campaign = tomllib.loads((EXAMPLES / name / 'pyproject.toml').read_text())['project']
    groups = campaign['entry-points']
    entries = ''.join(f'[{g}]\n' + ''.join(f'{n} = {v}\n' for n, v in eps.items()) for g, eps in groups.items())
    package(root, campaign['name'], campaign['version'], entries)


def pytest_addoption(parser: pytest.Parser) -> None:
    """The options of tests/e2e. They are here because pytest reads options only from the root conftest."""
    group = parser.getgroup('e2e', 'the end-to-end campaign (make e2e)')
    group.addoption('--e2e-harness', default='claude', help='the harness of every agent (default: claude)')
    group.addoption('--e2e-model', help="the harness's model (default: its cheapest, claude-haiku-4-5 for claude)")
    group.addoption(
        '--e2e-mode',
        choices=('local', 'distributed'),
        action='append',
        help='run the scenarios in this mode only; give it twice for both (default: both)',
    )


@pytest.fixture(autouse=True)
def no_real_skill_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every built-in harness has its home under tmp_path, so every piloop a test starts installs skills there."""
    for name, harness in DEFAULT_HARNESSES.items():
        for variable in env_names(harness.home or ''):
            monkeypatch.setenv(variable, str(tmp_path / 'harnesses' / name))
    monkeypatch.setenv('PILOOP_AGENT_SKILLS_DIR', str(tmp_path / 'agents-skills'))


@pytest.fixture
def campaign_path(tmp_path: Path) -> Path:
    """A git repo with solve.txt (one line), an untracked data/ dir, brief.md and a piloop.toml using HARNESS_APPEND."""
    path = tmp_path / 'campaign'
    path.mkdir()
    (path / 'solve.txt').write_text('base\n')
    (path / 'brief.md').write_text('# Demo\nAdd lines.\n')
    (path / 'data').mkdir()
    (path / 'data' / 'x.csv').write_text('1\n')
    (path / '.gitignore').write_text('data\nworkspace\n')
    write_toml(path, HARNESS_APPEND)
    repo = Repo.init(path, initial_branch='main')
    repo.git.add('-A')
    repo.git.commit('-qm', 'baseline')
    return path


@pytest.fixture(scope='session')
def schema(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A database that ``migrate`` brought to head, once per session: a copy of it starts at head for free.

    Migrating from scratch costs about a second (each DDL statement of every revision commits on its own), and most
    tests need a database. ``tests/db/test_migrations.py`` still migrates from scratch.
    """
    path = tmp_path_factory.mktemp('schema') / 'piloop.db'
    database = Database(f'sqlite:///{path}', actor=ACTOR)
    migrate(database.engine, backups=0, daemon=None)
    database.close()
    return path


def at_head(schema: Path, path: Path) -> Path:
    """Put a copy of the ``schema`` database at ``path``; returns ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(schema, path)
    return path


@pytest.fixture
def workers(monkeypatch: pytest.MonkeyPatch) -> weakref.WeakSet[Worker]:
    """Every worker that the test makes."""
    made: weakref.WeakSet[Worker] = weakref.WeakSet()

    def tracking[**P](init: Callable[Concatenate[Worker, P], None]) -> Callable[Concatenate[Worker, P], None]:
        def tracked(self: Worker, *args: P.args, **kwargs: P.kwargs) -> None:
            init(self, *args, **kwargs)
            made.add(self)

        return tracked

    monkeypatch.setattr(Worker, '__init__', tracking(Worker.__init__))
    return made


@pytest.fixture
def db(tmp_path: Path, schema: Path, workers: weakref.WeakSet[Worker]) -> Iterator[Database]:
    database = Database(f'sqlite:///{at_head(schema, tmp_path / "piloop.db")}', actor=ACTOR)
    migrate(
        database.engine, backups=0, daemon=None
    )  # a no-op at head, as when a client opens a database the daemon created
    yield database
    for worker in list(workers):  # a job that the test left running ends before its database closes
        worker.stop()
        worker.join(10)
    database.close()  # and what waits for one gives up


@pytest.fixture
def campaign(db: Database, campaign_path: Path) -> Campaign:
    """Registered campaign with its baseline candidate measured (score=1)."""
    return register(db, campaign_path)


def register(db: Database, campaign_path: Path) -> Campaign:
    cfg = load_campaign_config(campaign_path)
    repo = Repo(campaign_path)
    sha = repo.head.commit.hexsha
    result = run_evaluate(
        cfg.evaluate,
        cwd=campaign_path,
        log_dir=campaign_path.parent,
        timeout=30,
        required={m.name for m in cfg.metrics},
    )
    with db.session() as s:
        proj = Campaign(name=cfg.name, path=str(campaign_path), config=cfg.model_dump(mode='json'))
        s.add(proj)
        s.flush()
        evaluator = current_evaluator(s, proj, cfg)
        base = Candidate(
            campaign_id=proj.id,
            tree_sha=repo.head.commit.tree.hexsha,
            commit_sha=sha,
            summary='baseline',
            verdict=Verdict.KEEP,
            verdict_by=DAEMON,
        )
        s.add(base)
        s.flush()
        s.add(Measurement(candidate_id=base.id, evaluator_id=evaluator.id, metrics=result.metrics))
        s.commit()
        return proj


def baseline(db: Database, campaign_id: CampaignId) -> Candidate:
    with db.session() as s:
        return s.scalars(
            select(Candidate).where(Candidate.campaign_id == campaign_id).order_by(Candidate.id).limit(1)
        ).one()


def console_lines(err: str) -> list[str]:
    """The tracer's console lines (``piloop.otel``) without the clock: no time of day, and each span's duration as ``Ns``.

    A duration is wall-clock time, so a loaded machine changes it: ``0.0s`` on a quiet one is ``0.1s`` on a busy one.
    """
    return [re.sub(r'^(\S+) \d+\.\ds\b', r'\1 Ns', line.partition(' ')[2]) for line in err.splitlines()]


class ScriptRuntime(SubprocessRuntime):
    """The real subprocess runtime, recording kills so tests can assert on them."""

    def __init__(self) -> None:
        super().__init__()
        self.killed: list[Handle] = []

    def kill(self, handle: Handle) -> None:
        self.killed.append(handle)
        super().kill(handle)


@pytest.fixture
def runtime() -> ScriptRuntime:
    return ScriptRuntime()


class Gate:
    """Holds a wrapped callable at its entry until the test releases it: a deterministic race window."""

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()

    def wrap[**P, R](self, fn: Callable[P, R]) -> Callable[P, R]:
        def paused(*args: P.args, **kwargs: P.kwargs) -> R:
            self.entered.set()
            self.release.wait(10)
            return fn(*args, **kwargs)

        return paused


HOLD_LOCK = Path(__file__).parent / 'fixtures' / 'hold_lock.py'
"""A stand-in daemon: holds the pid file's lock with its pid inside, like ``piloop.daemon.serve.acquire`` does."""


@pytest.fixture
def lock_holder(tmp_path: Path) -> Iterator[subprocess.Popen[str]]:
    yield from hold_lock(Settings(home=tmp_path).pid_file)


def hold_lock(pid_file: Path, *args: str) -> Iterator[subprocess.Popen[str]]:
    """Run ``HOLD_LOCK`` on ``pid_file`` until the test ends; yields once the lock is held."""
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    with subprocess.Popen(
        [sys.executable, str(HOLD_LOCK), str(pid_file), *args], stdout=subprocess.PIPE, text=True
    ) as proc:
        assert proc.stdout is not None and proc.stdout.readline() == 'held\n'
        yield proc
        proc.kill()


WRITER = (
    'import sqlite3, sys; con = sqlite3.connect(sys.argv[1], isolation_level=None);'
    " con.execute('PRAGMA journal_mode=WAL'); con.execute('BEGIN IMMEDIATE'); print('held', flush=True); sys.stdin.read()"
)
"""A client that opens a write transaction and holds it until its stdin closes."""


@pytest.fixture
def writer(db: Database, tmp_path: Path) -> Iterator[subprocess.Popen[str]]:
    """Another process holding the write lock of the ``db`` fixture's database until the test ends."""
    argv = [sys.executable, '-c', WRITER, str(tmp_path / 'piloop.db')]
    with subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as proc:
        assert proc.stdout is not None and proc.stdout.readline() == 'held\n'
        yield proc
        proc.kill()


type Dial = Callable[[str, str], httpx.Client]


@pytest.fixture
def dial(monkeypatch: pytest.MonkeyPatch) -> Dial:
    """``https``'s client of a server with a secret, which it reads from ``TOKEN_ENV`` as on a worker's machine."""

    def make(server: str, secret: str) -> httpx.Client:
        monkeypatch.setenv(TOKEN_ENV, secret)
        return Https().client(server)

    return make


def wait_until(predicate: Callable[[], object], *, timeout: float = 30) -> None:
    """Poll ``predicate`` until it is true; fail the test once ``timeout`` passes.

    The timeout only bounds a test that would fail anyway, so it is generous: the suite runs in parallel, often on a
    loaded host, where a step that takes a second alone can take ten.
    """
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, 'condition not met in time'
        time.sleep(0.05)


def wait_for_agent_session_handle(db: Database) -> dict[str, object]:
    """Poll until some implementer is ``running`` with a published handle; returns the handle.

    As generous as ``wait_until``: with a daemon process, this waits for its start too. That takes about two seconds
    alone, and more than ten on a loaded host.
    """
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        with db.session() as s:
            agent_session = s.scalars(
                select(AgentSession).where(AgentSession.state == AgentSessionState.RUNNING)
            ).first()
            if agent_session is not None and agent_session.handle:
                return agent_session.handle
        time.sleep(0.05)
    raise AssertionError('implementer never reached running')


@pytest.fixture
def spans() -> InMemorySpanExporter:
    """Every span the ``tracer`` fixture finishes."""
    return InMemorySpanExporter()


@pytest.fixture
def tracer(spans: InMemorySpanExporter) -> Tracer:
    return Tracer(None, spans)


def events(spans: InMemorySpanExporter, name: str) -> list[dict[str, object]]:
    """Attributes of every span event called ``name``, across all finished spans."""
    return [dict(e.attributes or {}) for span in spans.get_finished_spans() for e in span.events if e.name == name]


def span_named(spans: InMemorySpanExporter, name: str) -> ReadableSpan:
    [span] = [s for s in spans.get_finished_spans() if s.name == name]
    return span


def timed(timing: Timing) -> Workers:
    """``[plugins.executor]`` for ``workers`` with the lease timing ``timing``."""
    return Workers(renew_seconds=timing.renew, grace_seconds=timing.grace, toleration_seconds=timing.toleration)


def serving(controller: RunController) -> RunController:
    """``controller`` with its executor's worker started, as the daemon's ``reap`` leaves it: its slots count."""
    controller.executor.adopt([])
    return controller


def workers_of(controller: RunController) -> WorkersExecutor:
    """The ``workers`` executor of ``controller``: its worker and its leases."""
    assert isinstance(controller.executor, WorkersExecutor)
    return controller.executor


def disk(home: Path) -> Cas:
    """The default CAS of the home ``home``: ``disk``, under ``<home>/store``."""
    return CAS.selected({}).open(home)


def factory(rt: Runtime) -> RuntimeFactory:
    def make(_kind: str) -> Runtime:
        return rt

    return make


@pytest.fixture(autouse=True)
def plenty_of_disk(monkeypatch: pytest.MonkeyPatch) -> None:
    """The archiver keeps ``[storage] min_free_gb`` free: a test must not depend on the free space of its machine."""
    monkeypatch.setattr(archive_module, 'free', lambda home: 1 << 50)
    monkeypatch.setattr(tree_module, 'free', lambda home: 1 << 50)


@pytest.fixture(autouse=True)
def isolated_campaigns_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``register`` measures the baseline under the global ``settings.home``: give each test a home of its own."""
    monkeypatch.setattr(campaign_module, 'settings', Settings(home=tmp_path / 'home'))


@pytest.fixture(autouse=True)
def git_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in {
        'GIT_AUTHOR_NAME': 'piloop-test',
        'GIT_AUTHOR_EMAIL': 'test@piloop',
        'GIT_COMMITTER_NAME': 'piloop-test',
        'GIT_COMMITTER_EMAIL': 'test@piloop',
    }.items():
        monkeypatch.setenv(key, value)
