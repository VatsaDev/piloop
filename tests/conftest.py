import json
import os
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from git import Repo
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import select

from piloop.campaign import project as project_module
from piloop.config import RuntimeKind, load_project_config
from piloop.daemon.main import ACTOR
from piloop.db.models import Candidate, Measurement, Project, Verdict, Worker, WorkerState
from piloop.db.session import Database, migrate
from piloop.otel import Tracer
from piloop.runtime import Handle, Runtime
from piloop.runtime.factory import RuntimeFactory
from piloop.runtime.subprocess import SubprocessRuntime
from piloop.scoring.evaluate import run_evaluate
from piloop.scoring.ledger import DAEMON, current_evaluator
from piloop.settings import Settings

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

[roles.worker]
timeout = {worker_timeout}
max_concurrent = {max_workers}

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
interactive = []
"""


def write_script(path: Path, body: str) -> Path:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return path


def write_toml(
    project: Path,
    harness_body: str,
    *,
    max_workers: int = 1,
    worker_timeout: float = 20,
    evaluate: list[str] = EVALUATE,
) -> None:
    script = write_script(project.parent / 'harness.sh', harness_body)
    (project / 'piloop.toml').write_text(
        TOML.format(
            evaluate=repr(evaluate).replace("'", '"'),
            worker_timeout=worker_timeout,
            max_workers=max_workers,
            script=script,
        )
    )


@pytest.fixture(autouse=True)
def no_real_skill_dirs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every Settings built in a test, and every piloop it starts, installs and checks skills under tmp_path only."""
    monkeypatch.setenv('CLAUDE_CONFIG_DIR', str(tmp_path / 'claude-config'))
    monkeypatch.setenv('PILOOP_AGENT_SKILLS_DIR', str(tmp_path / 'agents-skills'))


@pytest.fixture
def project_path(tmp_path: Path) -> Path:
    """A git repo with solve.txt (one line), an untracked data/ dir, brief.md and a piloop.toml using HARNESS_APPEND."""
    path = tmp_path / 'project'
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


@pytest.fixture
def db(tmp_path: Path) -> Iterator[Database]:
    database = Database(f'sqlite:///{tmp_path / "piloop.db"}', actor=ACTOR)
    migrate(database.engine)
    yield database
    database.close()


@pytest.fixture
def project(db: Database, project_path: Path) -> Project:
    """Registered project with its baseline candidate measured (score=1)."""
    return register(db, project_path)


def register(db: Database, project_path: Path) -> Project:
    cfg = load_project_config(project_path)
    repo = Repo(project_path)
    sha = repo.head.commit.hexsha
    result = run_evaluate(
        cfg.evaluate,
        cwd=project_path,
        log_dir=project_path.parent,
        timeout=30,
        required={m.name for m in cfg.metrics},
    )
    with db.session() as s:
        proj = Project(name=cfg.name, path=str(project_path), config=cfg.model_dump(mode='json'))
        s.add(proj)
        s.flush()
        evaluator = current_evaluator(s, proj, cfg)
        base = Candidate(
            project_id=proj.id,
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


def baseline(db: Database, project_id: int) -> Candidate:
    with db.session() as s:
        return s.scalars(
            select(Candidate).where(Candidate.project_id == project_id).order_by(Candidate.id).limit(1)
        ).one()


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


def wait_for_worker_handle(db: Database) -> dict[str, object]:
    """Poll until some worker is ``running`` with a published handle; returns the handle."""
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        with db.session() as s:
            worker = s.scalars(select(Worker).where(Worker.state == WorkerState.RUNNING)).first()
            if worker is not None and worker.handle:
                return worker.handle
        time.sleep(0.05)
    raise AssertionError('worker never reached running')


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


def factory(rt: Runtime) -> RuntimeFactory:
    def make(_kind: RuntimeKind, *, project: str, project_path: Path) -> Runtime:
        return rt

    return make


@pytest.fixture(autouse=True)
def isolated_projects_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``register`` measures the baseline under the global ``settings.projects_dir``: keep it out of ``~/.piloop``."""
    monkeypatch.setattr(project_module, 'settings', Settings(home=tmp_path / 'home'))


@pytest.fixture(autouse=True)
def git_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in {
        'GIT_AUTHOR_NAME': 'piloop-test',
        'GIT_AUTHOR_EMAIL': 'test@piloop',
        'GIT_COMMITTER_NAME': 'piloop-test',
        'GIT_COMMITTER_EMAIL': 'test@piloop',
    }.items():
        monkeypatch.setenv(key, value)


FAKE_HERDR = Path(__file__).parent / 'fixtures' / 'fake_herdr.py'


@pytest.fixture
def herdr_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Put the fake ``herdr`` first on PATH; returns the argv log it appends to."""
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir()
    (bin_dir / 'herdr').symlink_to(FAKE_HERDR)
    log = tmp_path / 'herdr.log'
    monkeypatch.setenv('PATH', f'{bin_dir}{os.pathsep}{os.environ["PATH"]}')
    monkeypatch.setenv('FAKE_HERDR_LOG', str(log))
    monkeypatch.delenv('FAKE_HERDR_SCRIPT', raising=False)
    return log


FAKE_ENTIRE = Path(__file__).parent / 'fixtures' / 'fake_entire.py'


@pytest.fixture
def entire_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Callable[[], list[dict[str, object]]]:
    """Put the fake ``entire`` first on PATH; returns a reader of the ``{argv, cwd}`` records it appends."""
    bin_dir = tmp_path / 'entire-bin'
    bin_dir.mkdir()
    (bin_dir / 'entire').symlink_to(FAKE_ENTIRE)
    log = tmp_path / 'entire.log'
    monkeypatch.setenv('PATH', f'{bin_dir}{os.pathsep}{os.environ["PATH"]}')
    monkeypatch.setenv('FAKE_ENTIRE_LOG', str(log))
    monkeypatch.delenv('FAKE_ENTIRE_MODE', raising=False)
    monkeypatch.delenv('FAKE_ENTIRE_VERSION', raising=False)
    return lambda: [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
