"""Run a tiny real campaign end to end, with real agents, and report what works.

    uv run -m scripts.smoke_campaign [--harness claude] [--model claude-haiku-4-5-20251001] [--report PATH]

Everything lives in a fresh temporary directory: piloop installed from this checkout, its ``PILOOP_HOME``, a copy of
the bundled enwik8 example, and the skills directories. Nothing touches ``~/.piloop``, a running daemon or the real
skills directories. Three ideas run on one harness with tight limits: one run is cancelled, one is judged, and the
campaign is stopped gracefully while the last one runs. Each check prints one pass or fail line; the report (markdown)
lists them with their timings, the runs with their tokens and cost, and the daemon log. The exit code is non-zero
when a check fails. The processes are cleaned up on every exit.

To add a check, write a function that takes the ``Smoke`` and returns a detail line (or raises ``Failed``), and add
it to ``CHECKS``. The checks run in order and share the state on ``Smoke``.
"""

import argparse
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType
from typing import TypedDict

from pydantic import TypeAdapter

from piloop.proc import TOKEN_ENV, kill_tagged, tagged
from piloop.reports.feed import Batch, Envelope, ProjectState
from piloop.runtime.subprocess import STDOUT

ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = 'enwik8'
PROJECT = 'enwik8'
HARNESS = 'claude'
MODEL = 'claude-haiku-4-5-20251001'
"""The cheapest Claude model. ``--model`` picks another."""
DROPPED_ENV = re.compile(r'^(PILOOP_|CLAUDECODE$|CLAUDE_CODE_|CLAUDE_PID$|CLAUDE_EFFORT$|VIRTUAL_ENV$)')
"""Variables of the calling shell that must not reach the smoke's processes: a piloop setting of the real install,
or the session of a Claude Code that runs this script (its messaging socket would reach the agents)."""
LIMITS = {
    'ROLES__WORKER__MAX_CONCURRENT': '2',
    'ROLES__WORKER__TIMEOUT': '600',
    'ROLES__RESEARCH__TRIGGERS': '[]',
    'ROLES__WATCHDOG__TRIGGERS': '[]',
    'STOP__MAX_RUNS': '5',
    'STOP__MAX_HOURS': '1',
    'STOP__MAX_TOKENS': '20000000',
    'EVALUATE_TIMEOUT': '300',
}
"""``PILOOP_PROJECT_<KEY>`` overrides for ``piloop init``: two workers, no sweeps (the ideas are added here), and stop
conditions only as a backstop (the smoke stops the campaign itself). The copy of the template stays as shipped."""
MAX_EVALUATIONS = 1
"""One evaluation slot, so that the evaluation queue has something to order."""


@dataclass(frozen=True)
class IdeaSpec:
    """An idea that the smoke adds, and what it is for."""

    key: str
    title: str
    plan: str
    priority: int


IDEAS = (
    IdeaSpec(
        'slow',
        'Write an order-2 context-mixing compressor with arithmetic coding',
        'Implement an order-2 context model with an adaptive binary arithmetic coder in compressor.py with numpy.'
        ' Measure with `uv run python evaluate.py`, then commit.',
        0,
    ),
    IdeaSpec(
        'lzma',
        'Replace zlib with lzma at preset 9 extreme',
        'In compressor.py, compress with lzma.compress(data, preset=9 | lzma.PRESET_EXTREME) and decompress with'
        ' lzma.decompress. Run `uv run python evaluate.py`, then commit. Change nothing else.',
        1,
    ),
    IdeaSpec(
        'bz2',
        'Replace zlib with bz2 at its best level',
        'In compressor.py, compress with bz2.compress(data, level) and decompress with bz2.decompress. Try the levels'
        ' 1, 5 and 9, each measured with `uv run python evaluate.py`, then commit the best one. Change nothing else.',
        2,
    ),
)
"""The slow idea runs first and is cancelled; lzma should be a keep; bz2 runs while the campaign stops."""
CANCEL_AFTER = 40
"""Seconds that the slow run works before the smoke cancels it."""
REQUIRED_EVENTS = (
    'project.paused',
    'worker.started',
    'run.started',
    'run.cancel_requested',
    'measurement.recorded',
    'candidate.judged',
    'project.stop_requested',
    'project.status',
)
"""Event kinds that the feed must show after the snapshot that the smoke starts from."""
OPEN = {'queued', 'running'}
EVENT_PAGE = 25
"""A small page, so the feed is read in many batches, each by a new process from the saved cursor."""


class Daemon(TypedDict):
    """``daemon`` in ``piloop status --json``."""

    pid: int | None


class Status(TypedDict):
    """The document of ``piloop status --json``."""

    cursor: int
    daemon: Daemon
    projects: list[ProjectState]


STATUS = TypeAdapter(Status)
BATCH = TypeAdapter(Batch)


class Failed(Exception):
    """A check failed. The message is the evidence."""


@dataclass
class Smoke:
    """The state that the checks share."""

    tmp: Path
    harness: str
    model: str
    env: dict[str, str] = field(default_factory=dict)
    ideas: dict[str, int] = field(default_factory=dict)
    snapshot: int = 0
    """The cursor of the ``status --json`` snapshot, where the event reader starts."""
    cursor: int = 0
    events: list[Envelope] = field(default_factory=list)
    event_reads: int = 0
    queue_samples: list[tuple[int, int, int]] = field(default_factory=list)
    """``(running, waiting, slots)`` from each ``piloop eval queue``."""
    worker_at_stop: str = ''
    stop_threads: threading.Event = field(default_factory=threading.Event)
    threads: list[threading.Thread] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def home(self) -> Path:
        return self.tmp / 'home'

    @property
    def repo(self) -> Path:
        return self.tmp / PROJECT

    @property
    def db(self) -> Path:
        return self.home / 'piloop.db'

    def piloop(self, *args: str, check: bool = True, env: dict[str, str] | None = None, timeout: float = 600) -> str:
        """Run the piloop installed in the temporary directory, and return its stdout."""
        proc = subprocess.run(
            ['piloop', *args],
            env=self.env | (env or {}),
            cwd=self.tmp,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if check and proc.returncode != 0:
            raise Failed(f'`piloop {" ".join(args)}` exited {proc.returncode}: {(proc.stderr or proc.stdout)[-800:]}')
        return proc.stdout

    def status(self) -> Status:
        """``piloop status --json`` of the project, checked against its documented shape."""
        return STATUS.validate_json(self.piloop('status', PROJECT, '--json'))

    def feed(self, after: int, *args: str) -> Batch:
        """``piloop events --after <after> --json``, checked against its documented shape."""
        return BATCH.validate_json(self.piloop('events', '--after', str(after), *args, '--json'))

    def sql(self, statement: str, *params: object) -> list[sqlite3.Row]:
        """Read the campaign's database directly, read-only."""
        with closing(sqlite3.connect(f'file:{self.db}?mode=ro', uri=True, timeout=30)) as db:
            db.row_factory = sqlite3.Row
            return db.execute(statement, params).fetchall()

    def run_of(self, key: str) -> sqlite3.Row | None:
        """The latest run of one of the smoke's ideas."""
        rows = self.sql('SELECT * FROM runs WHERE idea_id = ? ORDER BY id DESC LIMIT 1', self.ideas[key])
        return rows[0] if rows else None

    def run_in(self, key: str, statuses: set[str], within: bool = True) -> sqlite3.Row | None:
        """The latest run of the idea when its status is (``within``) or is not in ``statuses``, else None."""
        run = self.run_of(key)
        return run if run is not None and (run['status'] in statuses) == within else None


def until[T](what: str, timeout: float, probe: Callable[[], T | None], every: float = 2) -> T:
    """Return the first truthy result of ``probe``, or fail after ``timeout`` seconds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if found := probe():
            return found
        time.sleep(every)
    raise Failed(f'timed out after {timeout:.0f}s waiting for {what}')


def install(smoke: Smoke) -> str:
    """Install this checkout into a virtual environment of its own, and put only its ``piloop`` on PATH."""
    venv, bin_dir = smoke.tmp / 'venv', smoke.tmp / 'bin'
    subprocess.run(['uv', 'venv', '--quiet', '--python', '3.14', str(venv)], check=True)
    subprocess.run(['uv', 'pip', 'install', '--quiet', '--python', str(venv / 'bin' / 'python'), str(ROOT)], check=True)
    bin_dir.mkdir()
    (bin_dir / 'piloop').symlink_to(venv / 'bin' / 'piloop')
    base = {k: v for k, v in os.environ.items() if not DROPPED_ENV.match(k)}
    smoke.env = base | {
        'PATH': f'{bin_dir}{os.pathsep}{base["PATH"]}',
        'PILOOP_HOME': str(smoke.home),
        'PILOOP_MAX_EVALUATIONS': str(MAX_EVALUATIONS),
        'PILOOP_AGENT_SKILLS_DIR': str(smoke.tmp / 'skills' / 'agents'),
    }
    if not shutil.which(smoke.harness, path=smoke.env['PATH']):
        raise Failed(f'{smoke.harness} is not on PATH')
    version = smoke.piloop('--version').strip()
    expected = tomllib.loads((ROOT / 'pyproject.toml').read_text())['project']['version']
    if expected not in version:
        raise Failed(f'installed piloop says {version!r}, the checkout is {expected}')
    return f'{version} from {ROOT}'


def init(smoke: Smoke) -> str:
    """Copy the bundled template, then ``piloop init`` it with the smoke's limits: it measures the baseline."""
    shutil.copytree(ROOT / 'piloop' / 'campaign' / 'templates' / TEMPLATE, smoke.repo)
    limits = {f'PILOOP_PROJECT_{k}': v for k, v in LIMITS.items()} | {
        'PILOOP_PROJECT_HARNESSES': json.dumps([smoke.harness]),
        f'PILOOP_PROJECT_MODEL__{smoke.harness.upper()}': smoke.model,
    }
    smoke.piloop('init', str(smoke.repo), '--example', TEMPLATE, env=limits)
    rows = smoke.sql(
        'SELECT c.verdict, m.metrics FROM candidates c JOIN measurements m ON m.candidate_id = c.id'
        ' JOIN projects p ON p.id = c.project_id WHERE p.name = ?',
        PROJECT,
    )
    if not rows:
        raise Failed('no measured baseline candidate')
    return f'baseline {rows[0]["verdict"]}: {rows[0]["metrics"]}'


def skills(smoke: Smoke) -> str:
    """Install the skills into the temporary skills directories only."""
    return smoke.piloop('install-skills', env=claude_config(smoke)).strip().splitlines()[-1]


def claude_config(smoke: Smoke) -> dict[str, str]:
    """``CLAUDE_CONFIG_DIR`` for the commands that read Claude Code's skills, never for the agents (their login)."""
    return {'CLAUDE_CONFIG_DIR': str(smoke.tmp / 'skills' / 'claude')}


def serve(smoke: Smoke) -> str:
    """Start the daemon in the background, and start the readers that watch it."""
    smoke.piloop('serve', '--detach')
    pid_file = smoke.home / 'serve.pid'
    pid = until('serve.pid', 30, lambda: pid_file if pid_file.is_file() else None)
    for name, target in (('events', read_events), ('eval queue', sample_queue)):
        thread = threading.Thread(target=watch, args=(smoke, name, target), daemon=True)
        thread.start()
        smoke.threads.append(thread)
    return f'pid {pid.read_text().strip()}, log {smoke.home / "serve.log"}'


def watch(smoke: Smoke, name: str, target: Callable[[Smoke], None]) -> None:
    """Call ``target`` until the smoke stops its readers. An error is kept for the check that reads the result."""
    while not smoke.stop_threads.is_set():
        try:
            target(smoke)
        except Exception as e:  # kept as evidence: the events and queue checks report it
            smoke.errors.append(f'{name}: {e}')
        smoke.stop_threads.wait(1)


def read_events(smoke: Smoke) -> None:
    """Read one page of the feed after the saved cursor, as a client that reconnects for every read does."""
    if not smoke.cursor:
        return
    batch = smoke.feed(smoke.cursor, '--limit', str(EVENT_PAGE))
    smoke.event_reads += 1
    smoke.events.extend(batch['events'])
    smoke.cursor = batch['next']


def sample_queue(smoke: Smoke) -> None:
    """Record what ``piloop eval queue`` shows."""
    out = smoke.piloop('eval', 'queue')
    if found := re.search(r'(\d+) running, (\d+) waiting; (\d+) slot', out):
        smoke.queue_samples.append((int(found[1]), int(found[2]), int(found[3])))


def doctor(smoke: Smoke) -> str:
    """``piloop doctor`` passes, with the skills read from the temporary directories."""
    out = smoke.piloop('doctor', env=claude_config(smoke), check=False)
    failing = [line for line in out.splitlines() if line.startswith('FAIL')]
    if failing:
        raise Failed('; '.join(failing))
    return f'{len(out.splitlines())} lines, no FAIL'


def status_json(smoke: Smoke) -> str:
    """``piloop status --json`` shows the daemon and the project at a cursor, which the event reader starts from."""
    doc = smoke.status()
    pid = int((smoke.home / 'serve.pid').read_text().split()[0])
    if doc['daemon']['pid'] != pid:
        raise Failed(f'daemon {doc["daemon"]}, serve.pid has {pid}')
    if [p['name'] for p in doc['projects']] != [PROJECT] or doc['cursor'] <= 0:
        raise Failed(f'cursor {doc["cursor"]}, projects {doc["projects"]}')
    project = doc['projects'][0]
    if project['status'] != 'active' or project['head'] is None:
        raise Failed(f'not active with a head: {project}')
    smoke.snapshot = smoke.cursor = doc['cursor']
    return f'cursor {doc["cursor"]}, head {project["head"]}'


def pause(smoke: Smoke) -> str:
    """``piloop pause``, then the ideas: none of them is launched while the project is paused."""
    smoke.piloop('pause', PROJECT, '--reason', 'smoke: hold launches while the ideas are added')
    for idea in IDEAS:
        smoke.piloop('idea', 'add', PROJECT, idea.title, '--plan', idea.plan, '--priority', str(idea.priority))
        smoke.ideas[idea.key] = smoke.sql('SELECT id FROM ideas WHERE title = ?', idea.title)[0]['id']
    time.sleep(10)  # five ticks
    runs = smoke.sql('SELECT id, status FROM runs')
    workers = smoke.sql("SELECT id, state FROM workers WHERE kind = 'worker'")
    paused = smoke.status()['projects'][0]['paused']
    if runs or workers or paused != 'launches':
        raise Failed(f'status --json paused={paused!r}; runs {list(map(dict, runs))}, workers {len(workers)}')
    return f'{len(IDEAS)} ideas ready, no run after 10s, status --json paused={paused}'


def resume(smoke: Smoke) -> str:
    """``piloop resume``: the two most urgent ideas start."""
    smoke.piloop('resume', PROJECT)

    def running() -> list[sqlite3.Row] | None:
        rows = smoke.sql("SELECT id, idea_id FROM runs WHERE status = 'running'")
        return rows if len(rows) >= 2 else None

    both = until('two running runs', 180, running)
    started = {r['idea_id'] for r in both}
    if started != {smoke.ideas['slow'], smoke.ideas['lzma']}:
        raise Failed(f'started ideas {started}, expected the two most urgent {smoke.ideas}')
    return f'runs {[r["id"] for r in both]} running'


def cancel(smoke: Smoke) -> str:
    """``piloop run cancel`` on the slow run: it closes as failed, its idea parks, its processes end."""
    time.sleep(CANCEL_AFTER)
    run = smoke.run_of('slow')
    if run is None or run['status'] != 'running':
        raise Failed(f'the slow run is not running after {CANCEL_AFTER}s: {run and dict(run)}')
    tokens = worker_tokens(smoke, run['id'])
    smoke.piloop('run', 'cancel', str(run['id']))
    closed = until(f'run {run["id"]} to close', 120, lambda: smoke.run_in('slow', OPEN, within=False))
    idea = smoke.sql('SELECT status FROM ideas WHERE id = ?', smoke.ideas['slow'])[0]['status']
    alive = [p for t in tokens for p in tagged(f'{TOKEN_ENV}={t}')]
    if closed['status'] != 'failed' or idea != 'parked' or alive:
        raise Failed(f'run {closed["status"]} ({closed["error"]}), idea {idea}, processes left {alive}')
    return f'run {run["id"]} failed ({closed["error"]}), idea parked, no process left'


def worker_tokens(smoke: Smoke, run_id: int | None = None) -> list[str]:
    """The process tokens of the run's workers, or of every agent."""
    rows = smoke.sql(
        "SELECT json_extract(handle, '$.token') AS token FROM workers WHERE token IS NOT NULL"
        + (' AND run_id = ?' if run_id is not None else ''),
        *([run_id] if run_id is not None else []),
    )
    return [r['token'] for r in rows]


def finish(smoke: Smoke) -> str:
    """The lzma run finishes on its own, and the bz2 run starts in the slot that the cancel freed."""
    done = until('the lzma run to finish', 900, lambda: smoke.run_in('lzma', OPEN, within=False))
    if done['status'] != 'succeeded':
        raise Failed(f'run {done["id"]} {done["status"]}: {done["error"]}')
    bz2 = until('the bz2 run to start', 300, lambda: smoke.run_in('bz2', {'running'}))
    return f'run {done["id"]} succeeded; run {bz2["id"]} running'


def judged(smoke: Smoke) -> str:
    """Every commit of the lzma run is a candidate with a verdict."""
    run = smoke.run_of('lzma')
    assert run is not None

    def verdicts() -> list[sqlite3.Row] | None:
        rows = smoke.sql('SELECT id, verdict, verdict_reason FROM candidates WHERE run_id = ?', run['id'])
        return rows if rows and all(r['verdict'] != 'pending' for r in rows) else None

    rows = until(f'the candidates of run {run["id"]} to be judged', 300, verdicts)
    return '; '.join(f'candidate#{r["id"]} {r["verdict"]}: {r["verdict_reason"]}' for r in rows)


def remeasure(smoke: Smoke) -> str:
    """``piloop candidate remeasure`` on the head: the daemon measures it again, and the keep stays."""
    run = smoke.run_of('lzma')
    assert run is not None
    head = smoke.sql("SELECT id FROM candidates WHERE run_id = ? AND verdict = 'keep'", run['id'])
    if not head:
        raise Failed(f'run {run["id"]} has no keep to measure again')
    candidate = head[0]['id']
    smoke.piloop('candidate', 'remeasure', str(candidate))

    def measured() -> list[sqlite3.Row] | None:
        rows = smoke.sql('SELECT metrics FROM measurements WHERE candidate_id = ? ORDER BY id', candidate)
        return rows if len(rows) >= 2 else None

    rows = until(f'a second measurement of candidate#{candidate}', 120, measured)
    verdict = smoke.sql('SELECT verdict FROM candidates WHERE id = ?', candidate)[0]['verdict']
    if verdict != 'keep':
        raise Failed(f'candidate#{candidate} is {verdict} after the remeasure')
    return f'candidate#{candidate}: {rows[0]["metrics"]} then {rows[-1]["metrics"]}, still keep'


def project_stop(smoke: Smoke) -> str:
    """``piloop project stop`` while the bz2 worker runs: it winds down, its commits are scored, nothing stays open."""
    bz2 = smoke.run_of('bz2')
    smoke.worker_at_stop = f'run {bz2["id"]} was {bz2["status"]}' if bz2 else 'no bz2 run'
    smoke.piloop('project', 'stop', PROJECT, '--reason', 'smoke: graceful stop')

    def settled() -> bool:
        open_ = smoke.sql(
            "SELECT (SELECT count(*) FROM runs WHERE status IN ('queued', 'running') OR scoring)"
            " + (SELECT count(*) FROM workers WHERE state IN ('requested', 'starting', 'running'))"
            " + (SELECT count(*) FROM candidates WHERE verdict = 'pending')"
            ' + (SELECT count(*) FROM evaluations) AS n'
        )
        return open_[0]['n'] == 0

    until('the stopped campaign to settle', 900, settled, every=5)
    project = smoke.sql('SELECT status, stopped_reason FROM projects WHERE name = ?', PROJECT)[0]
    scored = smoke.sql(
        'SELECT c.id, c.verdict FROM candidates c JOIN runs r ON r.id = c.run_id WHERE r.idea_id = ?',
        smoke.ideas['bz2'],
    )
    run = smoke.run_of('bz2')
    if project['status'] != 'stopped':
        raise Failed(f'project {dict(project)}')
    commits = ', '.join(f'candidate#{c["id"]} {c["verdict"]}' for c in scored) or 'no commit'
    return f'{smoke.worker_at_stop}; it ended {run and run["status"]}; {commits}; reason {project["stopped_reason"]!r}'


def override(smoke: Smoke) -> str:
    """``piloop candidate verdict`` overrides the bz2 verdict, and ``piloop measurement add`` records a measurement."""
    rows = smoke.sql(
        'SELECT c.id, m.metrics FROM candidates c JOIN runs r ON r.id = c.run_id'
        ' JOIN measurements m ON m.candidate_id = c.id WHERE r.idea_id = ? ORDER BY c.id DESC LIMIT 1',
        smoke.ideas['bz2'],
    )
    if not rows:
        raise Failed('the bz2 run has no measured candidate')
    candidate, metrics = rows[0]['id'], json.loads(rows[0]['metrics'])
    smoke.piloop('candidate', 'verdict', str(candidate), 'discard', '--reason', 'smoke: override')
    report = smoke.tmp / 'measurement.json'
    report.write_text(json.dumps({'metrics': metrics, 'seconds': 1}))
    said = smoke.piloop('measurement', 'add', str(candidate), '--report', str(report)).strip()
    row = smoke.sql('SELECT verdict, verdict_by, verdict_reason FROM candidates WHERE id = ?', candidate)[0]
    count = smoke.sql('SELECT count(*) AS n FROM measurements WHERE candidate_id = ?', candidate)[0]['n']
    if (row['verdict'], row['verdict_by'], row['verdict_reason']) != (
        'discard',
        'human',
        'smoke: override',
    ) or count < 2:
        raise Failed(f'candidate#{candidate} {dict(row)}, {count} measurements')
    return f'candidate#{candidate} discard by human, {count} measurements; {said}'


def eval_queue(smoke: Smoke) -> str:
    """``piloop eval queue`` showed the evaluations, within the one slot, and is empty once the campaign settled."""
    sample_queue(smoke)
    samples = smoke.queue_samples
    if not samples:
        raise Failed(f'no sample; errors {smoke.errors[-3:]}')
    busiest = max(r for r, _, _ in samples)
    waited = max(w for _, w, _ in samples)
    if (
        busiest < 1
        or busiest > MAX_EVALUATIONS
        or {s for _, _, s in samples} != {MAX_EVALUATIONS}
        or samples[-1][:2] != (0, 0)
    ):
        raise Failed(f'{len(samples)} samples, most running {busiest}, last {samples[-1]}')
    return (
        f'{len(samples)} samples: at most {busiest} running and {waited} waiting in {MAX_EVALUATIONS} slot; empty now'
    )


def events(smoke: Smoke) -> str:
    """The feed, read page by page from the saved cursor, gives every event after the snapshot once, in order."""
    while True:  # drain what the reader thread has not read yet
        before = smoke.cursor
        read_events(smoke)
        if smoke.cursor == before:
            break
    followed = smoke.feed(smoke.cursor, '--follow', '--timeout', '3')
    seen = [e['id'] for e in smoke.events]
    unique = sorted(set(seen))
    stored = smoke.sql('SELECT id FROM events WHERE id > ? AND id <= ? ORDER BY id', smoke.snapshot, smoke.cursor)
    stored_ids = [r['id'] for r in stored]
    kinds = {e['kind'] for e in smoke.events}
    missing = [k for k in REQUIRED_EVENTS if k not in kinds]
    if seen != unique or unique != stored_ids or missing or followed['events'] or smoke.event_reads < 2:
        raise Failed(
            f'{len(seen)} read ({len(unique)} distinct, in order: {seen == unique}), {len(stored_ids)} stored,'
            f' missing kinds {missing}, follow gave {len(followed["events"])}, {smoke.event_reads} reads,'
            f' errors {smoke.errors[-3:]}'
        )
    return f'{len(seen)} events in {smoke.event_reads} reads, all stored ones, in order; {len(kinds)} kinds'


def no_anomaly(smoke: Smoke) -> str:
    """The daemon saw no fault of the environment (``anomaly.*``): every agent started and worked."""
    rows = smoke.sql("SELECT id, kind, target_kind, target_id, payload FROM events WHERE kind LIKE 'anomaly.%'")
    if rows:
        raise Failed(
            '; '.join(f'event#{r["id"]} {r["kind"]} {r["target_kind"]}#{r["target_id"]} {r["payload"]}' for r in rows)
        )
    return 'none'


def daemon_stop(smoke: Smoke) -> str:
    """``piloop stop`` ends the daemon."""
    smoke.stop_threads.set()
    for thread in smoke.threads:
        thread.join(timeout=30)
    smoke.piloop('stop', '--timeout', '60')
    if (smoke.home / 'serve.pid').exists():
        raise Failed('serve.pid is still there')
    return 'stopped'


def orphans(smoke: Smoke) -> str:
    """No process of the campaign is left: none with its PILOOP_HOME, none with an agent's token."""
    markers = [f'PILOOP_HOME={smoke.home}', *(f'{TOKEN_ENV}={t}' for t in worker_tokens(smoke))]
    left = sorted({f'{pid} {cmd[:120]}' for m in markers for pid, _, cmd in tagged(m)})
    if left:
        raise Failed('; '.join(left))
    return f'none, {len(markers) - 1} agent tokens checked'


@dataclass(frozen=True)
class Check:
    """One line of the report."""

    name: str
    run: Callable[[Smoke], str]
    fatal: bool = False
    """The checks after it make no sense when it fails."""


CHECKS = (
    Check('install from the checkout', install, fatal=True),
    Check('init: copy the template, measure the baseline', init, fatal=True),
    Check('install-skills into temp dirs', skills),
    Check('serve --detach', serve, fatal=True),
    Check('doctor, daemon running', doctor),
    Check('status --json', status_json),
    Check('pause holds launches', pause, fatal=True),
    Check('resume launches runs', resume, fatal=True),
    Check('run cancel', cancel),
    Check('a run finishes, the next starts', finish),
    Check('commits are candidates with a verdict', judged),
    Check('candidate remeasure', remeasure),
    Check('project stop, graceful', project_stop),
    Check('candidate verdict, measurement add', override),
    Check('eval queue', eval_queue),
    Check('events --after --json across reconnects', events),
    Check('no anomaly event', no_anomaly),
    Check('stop the daemon', daemon_stop),
    Check('no orphan process', orphans),
    Check('doctor, daemon stopped', doctor),
)


@dataclass(frozen=True)
class Result:
    """The outcome of one check."""

    check: Check
    ok: bool | None
    """None when the check was skipped after a fatal failure."""
    seconds: float
    detail: str


def runs_table(smoke: Smoke) -> list[str]:
    """One row per agent: its run, how it ended, its tokens and the cost that the harness printed."""
    if not smoke.db.is_file():
        return []
    rows = smoke.sql(
        'SELECT w.id, w.kind, w.run_id, w.harness, w.model, w.state, w.reason, w.log_dir, w.usage,'
        ' r.status, r.error, i.title, (julianday(w.ended_at) - julianday(w.started_at)) * 86400 AS seconds'
        ' FROM workers w LEFT JOIN runs r ON r.id = w.run_id LEFT JOIN ideas i ON i.id = r.idea_id ORDER BY w.id'
    )
    lines = [
        '| agent | run | idea | ended | seconds | tokens (in/out/cache read/total) | cost USD (printed) |',
        '|---|---|---|---|---|---|---|',
    ]
    total_cost, total_tokens = 0.0, 0
    for r in rows:
        usage = json.loads(r['usage']) if r['usage'] else None
        cost = harness_cost(Path(r['log_dir'])) if r['log_dir'] else None
        total_cost += cost or 0
        total_tokens += usage['total'] if usage else 0
        tokens = f'{usage["input"]}/{usage["output"]}/{usage["cache_read"]}/{usage["total"]}' if usage else 'n/a'
        ended = f'{r["state"]}, run {r["status"]}' + (f' ({r["error"]})' if r['error'] else '')
        lines.append(
            f'| {r["kind"]}#{r["id"]} {r["harness"]} {r["model"]} | {r["run_id"]} | {r["title"]} | {ended} |'
            f' {r["seconds"] or 0:.0f} | {tokens} | {"n/a" if cost is None else f"{cost:.3f}"} |'
        )
    lines.append(f'| total | | | | | {total_tokens} | {total_cost:.3f} |')
    return lines


def harness_cost(log_dir: Path) -> float | None:
    """Sum the ``total_cost_usd`` that a headless Claude Code prints at the end of each turn."""
    out = log_dir / STDOUT
    if not out.is_file():
        return None
    found = re.findall(r'"total_cost_usd":\s*([0-9.]+)', out.read_text(errors='replace'))
    return sum(map(float, found)) if found else None


def report(smoke: Smoke, results: list[Result], seconds: float) -> str:
    """The markdown report."""
    passed = sum(r.ok is True for r in results)
    commit = subprocess.run(['git', '-C', str(ROOT), 'rev-parse', '--short', 'HEAD'], capture_output=True, text=True)
    lines = [
        f'# piloop smoke campaign: {passed}/{len(results)} passed',
        '',
        f'- commit `{commit.stdout.strip()}`, template `{TEMPLATE}`, harness `{smoke.harness}` model `{smoke.model}`',
        f'- {seconds / 60:.1f} minutes; temp dir `{smoke.tmp}`; daemon log `{smoke.home / "serve.log"}`',
        f'- graceful stop: {smoke.worker_at_stop or "not reached"}',
        '',
        '| check | result | seconds | detail |',
        '|---|---|---|---|',
    ]
    for r in results:
        mark = {True: 'pass', False: 'FAIL', None: 'skipped'}[r.ok]
        lines.append(f'| {r.check.name} | {mark} | {r.seconds:.0f} | {r.detail.replace("|", "/")} |')
    lines += [
        '',
        '## Agents',
        '',
        'The cost is what the harness printed at the end of each turn, at API prices; an agent that was killed printed'
        ' none.',
        '',
        *runs_table(smoke),
    ]
    return '\n'.join(lines) + '\n'


def cleanup(smoke: Smoke) -> None:
    """Stop the daemon if it runs, then end every process that carries the campaign's home or an agent's token."""
    smoke.stop_threads.set()
    if (smoke.home / 'serve.pid').exists():
        smoke.piloop('stop', '--timeout', '60', check=False, timeout=120)
    kill_tagged(f'PILOOP_HOME={smoke.home}')
    if smoke.db.is_file():
        for token in worker_tokens(smoke):
            kill_tagged(f'{TOKEN_ENV}={token}')


def terminate(signum: int, _frame: FrameType | None) -> None:
    """Turn SIGTERM into an exit, so that the cleanup runs."""
    raise SystemExit(128 + signum)


def main() -> int:
    """Run every check, print and write the report, and return 1 when a check failed."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--harness', default=HARNESS, help='the harness of every agent')
    parser.add_argument('--model', default=MODEL, help="the harness's model")
    parser.add_argument('--report', type=Path, help='also write the report here')
    args = parser.parse_args()
    signal.signal(signal.SIGTERM, terminate)
    smoke = Smoke(Path(tempfile.mkdtemp(prefix='piloop-smoke-')), args.harness, args.model)
    print(f'smoke campaign in {smoke.tmp}', flush=True)
    results: list[Result] = []
    started = time.monotonic()
    try:
        broken = False
        for check in CHECKS:
            if broken:
                results.append(Result(check, None, 0, 'skipped after a fatal failure'))
                continue
            began = time.monotonic()
            try:
                result = Result(check, True, 0, check.run(smoke))
            except Exception as e:  # every failure is a line of the report
                result = Result(check, False, 0, f'{type(e).__name__}: {e}' if not isinstance(e, Failed) else str(e))
                broken = check.fatal
            result = Result(result.check, result.ok, time.monotonic() - began, result.detail)
            results.append(result)
            print(
                f'{"pass" if result.ok else "FAIL"}  {check.name} ({result.seconds:.0f}s): {result.detail}', flush=True
            )
    finally:
        cleanup(smoke)
    text = report(smoke, results, time.monotonic() - started)
    (smoke.tmp / 'report.md').write_text(text)
    if args.report:
        args.report.write_text(text)
    print(text)
    print(f'report: {smoke.tmp / "report.md"}')
    return 0 if all(r.ok for r in results) else 1


if __name__ == '__main__':
    sys.exit(main())
