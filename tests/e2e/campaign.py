"""What every end-to-end scenario shares: a real campaign in a temporary directory, and the helpers that read it."""

import json
import re
import sqlite3
import subprocess
import threading
from collections.abc import Callable, Mapping
from contextlib import closing
from dataclasses import dataclass, field
from pathlib import Path
from types import FunctionType

import pytest
from pydantic import TypeAdapter

from piloop.api.views import Batch, CampaignState, Envelope, Status
from piloop.plugins.runtime.contract import TAG_ENV
from piloop.plugins.runtime.kit.tags import kill_tagged, tagged
from piloop.plugins.runtime.subprocess import STDOUT
from tests.fixtures.netfault import Proxy

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = ROOT / 'piloop' / 'campaign' / 'templates'
CHEAPEST_MODEL = {'claude': 'claude-haiku-4-5-20251001'}
"""The model of each harness when ``--e2e-model`` is not given."""
OPEN = {'queued', 'running'}
WORKER_LOG = 'worker.log'
EVENT_PAGE = 25
"""A small page, so the feed is read in many batches, each by a new process from the saved cursor."""

type Toml = Mapping[str, object]


STATUS = TypeAdapter(Status)
BATCH = TypeAdapter(Batch)


class CommandFailed(Exception):
    """A ``piloop`` command exited non-zero. A plain exception, so that the background readers keep it as evidence."""


def needs(*tests: FunctionType) -> pytest.MarkDecorator:
    """Skip the test unless each of ``tests``, earlier in the module, passed: it reads the state that they leave."""
    return pytest.mark.needs(*(test.__name__ for test in tests))


def merged(base: Toml, over: Toml) -> dict[str, object]:
    """``base`` with the keys of ``over``: tables merge key by key, every other value is replaced."""
    out = dict(base)
    for key, value in over.items():
        old = out.get(key)
        out[key] = merged(old, value) if isinstance(old, Mapping) and isinstance(value, Mapping) else value
    return out


@dataclass
class Elsewhere:
    """The machine of a worker elsewhere, in distributed mode: ``piloop worker`` in a home of its own, which reaches the
    daemon through a fault proxy and its CAS through ``piloop cas serve``."""

    home: Path
    worker: subprocess.Popen[bytes]
    cas: subprocess.Popen[str]
    proxy: Proxy

    def stop(self) -> None:
        """Stop the worker, as its operator does when the daemon is gone, then the CAS and the proxy."""
        for proc in (self.worker, self.cas):
            proc.terminate()
            try:
                proc.wait(WORKER_STOP)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        if self.cas.stdout is not None:
            self.cas.stdout.close()
        self.proxy.close()


WORKER_STOP = 60
"""Seconds that a worker elsewhere has to stop its jobs and end."""


@dataclass
class Smoke:
    """One campaign, with its own home and campaign repo, and what the tests learn of it as they go."""

    dir: Path
    name: str
    """The campaign's name, from its ``piloop.toml``."""
    env: dict[str, str] = field(repr=False)
    """The environment of every ``piloop`` command. Not in the repr: it holds the caller's secrets."""
    ideas: dict[str, int] = field(default_factory=dict)
    """The id of each idea that ``add_idea`` added, by key."""
    snapshot: int = 0
    """The cursor of the ``status --json`` snapshot, where the event reader starts."""
    cursor: int = 0
    events: list[Envelope] = field(default_factory=list)
    event_reads: int = 0
    queue_samples: list[tuple[int, int, int]] = field(default_factory=list)
    """``(running, waiting)`` from each ``piloop eval queue``, and the live agent leases at that moment."""
    errors: list[str] = field(default_factory=list)
    """What the background readers raised, as evidence for the tests that read their results."""
    stop_readers: threading.Event = field(default_factory=threading.Event)
    readers: list[threading.Thread] = field(default_factory=list)
    elsewhere: Elsewhere | None = None
    """The worker elsewhere that runs every job, in distributed mode. None: the daemon's own worker runs them."""

    @property
    def home(self) -> Path:
        return self.dir / 'home'

    @property
    def repo(self) -> Path:
        return self.dir / 'repo'

    @property
    def db(self) -> Path:
        return self.home / 'piloop.db'

    def piloop(self, *args: str, check: bool = True, timeout: float = 600) -> str:
        """Run the piloop installed for the session, and return its stdout."""
        proc = subprocess.run(
            ['piloop', *args], env=self.env, cwd=self.dir, capture_output=True, text=True, timeout=timeout
        )
        if check and proc.returncode != 0:
            raise CommandFailed(
                f'`piloop {" ".join(args)}` exited {proc.returncode}: {(proc.stderr or proc.stdout)[-800:]}'
            )
        return proc.stdout

    def sql(self, statement: str, *params: object) -> list[sqlite3.Row]:
        """Read the campaign's database directly, read-only."""
        with closing(sqlite3.connect(f'file:{self.db}?mode=ro', uri=True, timeout=30)) as db:
            db.row_factory = sqlite3.Row
            return db.execute(statement, params).fetchall()

    def feed(self, after: int, *args: str) -> Batch:
        """``piloop events --after <after> --json``, checked against its documented shape."""
        return BATCH.validate_json(self.piloop('events', '--after', str(after), *args, '--json'))

    # The campaign's names in the CLI, the schema and the feed are in these four methods, `Status`, and the event
    # kinds that a scenario requires: a rename of the noun changes only these.

    def status(self) -> Status:
        """``piloop status --json`` of the campaign, checked against its documented shape."""
        return STATUS.validate_json(self.piloop('status', self.name, '--json'))

    def campaign_state(self) -> CampaignState:
        """The campaign in ``piloop status --json``, the only one."""
        doc = self.status()
        assert [p.name for p in doc.campaigns] == [self.name], doc.campaigns
        return doc.campaigns[0]

    def campaign_row(self) -> sqlite3.Row:
        """The campaign's row in the database."""
        return self.sql('SELECT * FROM campaigns WHERE name = ?', self.name)[0]

    def stop_campaign(self, reason: str) -> None:
        """Stop the campaign gracefully: the running implementers commit and stop."""
        self.piloop('campaign', 'stop', self.name, '--reason', reason)

    def add_idea(self, key: str, title: str, plan: str, priority: int) -> int:
        """``piloop idea add``; ``ideas[key]`` is then its id."""
        self.piloop('idea', 'add', self.name, title, '--plan', plan, '--priority', str(priority))
        self.ideas[key] = self.sql('SELECT id FROM ideas WHERE title = ?', title)[0]['id']
        return self.ideas[key]

    def run_of(self, key: str) -> sqlite3.Row | None:
        """The latest run of an idea that ``add_idea`` added."""
        rows = self.sql('SELECT * FROM runs WHERE idea_id = ? ORDER BY id DESC LIMIT 1', self.ideas[key])
        return rows[0] if rows else None

    def run_in(self, key: str, statuses: set[str], within: bool = True) -> sqlite3.Row | None:
        """The latest run of the idea when its status is (``within``) or is not in ``statuses``, else None."""
        run = self.run_of(key)
        return run if run is not None and (run['status'] in statuses) == within else None

    def process_tags(self, run_id: int | None = None) -> list[str]:
        """The process tags of the run's implementers, or of every agent."""
        rows = self.sql(
            "SELECT json_extract(handle, '$.token') AS token FROM agent_sessions WHERE token IS NOT NULL"
            + (' AND run_id = ?' if run_id is not None else ''),
            *([run_id] if run_id is not None else []),
        )
        return [r['token'] for r in rows]

    @property
    def homes(self) -> list[Path]:
        """The daemon's home, and the home of the worker elsewhere."""
        return [self.home, *([self.elsewhere.home] if self.elsewhere is not None else [])]

    def stop(self) -> None:
        """``piloop stop``, then the worker elsewhere, which its operator stops."""
        self.piloop('stop', '--timeout', '60')
        if self.elsewhere is not None:
            self.elsewhere.stop()

    def processes(self) -> list[str]:
        """Every live process of the campaign: with one of its ``PILOOP_HOME``, or with an agent's process tag."""
        markers = [*(f'PILOOP_HOME={h}' for h in self.homes), *(f'{TAG_ENV}={t}' for t in self.process_tags())]
        return sorted({f'{pid} {cmd[:120]}' for m in markers for pid, _, cmd in tagged(m)})

    def start_readers(self) -> None:
        """Read the event feed and sample ``piloop eval queue`` every second, until ``join_readers``."""
        for name, target in (('events', self.read_events), ('eval queue', self.sample_queue)):
            thread = threading.Thread(target=self._watch, args=(name, target), daemon=True)
            thread.start()
            self.readers.append(thread)

    def join_readers(self) -> None:
        self.stop_readers.set()
        for thread in self.readers:
            thread.join(timeout=30)

    def _watch(self, name: str, target: Callable[[], None]) -> None:
        while not self.stop_readers.is_set():
            try:
                target()
            except Exception as e:  # kept as evidence: the tests that read the readers' results report it
                self.errors.append(f'{name}: {e}')
            self.stop_readers.wait(1)

    def read_events(self) -> None:
        """Read one page of the feed after the saved cursor, as a client that reconnects for every read does."""
        if not self.cursor:
            return
        batch = self.feed(self.cursor, '--limit', str(EVENT_PAGE))
        self.event_reads += 1
        self.events.extend(batch.events)
        self.cursor = batch.next

    def sample_queue(self) -> None:
        """Record what ``piloop eval queue`` shows, and how many agent leases are live."""
        out = self.piloop('eval', 'queue')
        agents = self.sql("SELECT count(*) FROM leases WHERE kind = 'agent' AND state IN ('acquired', 'running')")[0][0]
        if found := re.search(r'(\d+) running, (\d+) waiting', out):
            self.queue_samples.append((int(found[1]), int(found[2]), agents))

    def shut_down(self) -> None:
        """Stop the readers and the daemon, then end every process that carries the home or an agent's process tag."""
        self.join_readers()
        try:
            if (self.home / 'serve.pid').exists():
                self.piloop('stop', '--timeout', '60', check=False, timeout=120)
            if self.elsewhere is not None:
                self.elsewhere.stop()
        finally:
            for home in self.homes:
                kill_tagged(f'PILOOP_HOME={home}')
            if self.db.is_file():
                for token in self.process_tags():
                    kill_tagged(f'{TAG_ENV}={token}')

    def summary(self) -> list[str]:
        """Where the logs are, and one row per agent: its run, how it ended, its tokens and the printed cost."""
        lines = [f'{self.name}: daemon log {self.home / "serve.log"}; agent output under {self.home / "runs"}']
        if self.elsewhere is not None:
            lines.append(f'{self.name}: the worker elsewhere logs to {self.elsewhere.home / WORKER_LOG}')
        if not self.db.is_file():
            return lines
        rows = self.sql(
            'SELECT w.id, w.kind, w.run_id, w.state, w.log_dir, w.usage, r.status, r.error, i.title,'
            ' (julianday(w.ended_at) - julianday(w.started_at)) * 86400 AS seconds'
            ' FROM agent_sessions w LEFT JOIN runs r ON r.id = w.run_id LEFT JOIN ideas i ON i.id = r.idea_id ORDER BY w.id'
        )
        lines += [
            '',
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
                f'| {r["kind"]}#{r["id"]} | {r["run_id"]} | {r["title"]} | {ended} | {r["seconds"] or 0:.0f} |'
                f' {tokens} | {"n/a" if cost is None else f"{cost:.3f}"} |'
            )
        lines += [f'| total | | | | | {total_tokens} | {total_cost:.3f} |', '']
        return lines


def harness_cost(log_dir: Path) -> float | None:
    """Sum the last ``total_cost_usd`` of each session in a headless Claude Code agent's output.

    Claude Code prints its session's running total at the end of each process, and piloop resumes a session in a new
    process to deliver a note. A killed agent printed none.
    """
    out = log_dir / STDOUT
    if not out.is_file():
        return None
    last: dict[str, float] = {}
    for line in out.read_text(errors='replace').splitlines():
        try:
            result = json.loads(line)
        except json.JSONDecodeError:
            continue  # the harness's other output: only its result lines are JSON
        if isinstance(result, dict) and 'total_cost_usd' in result:
            last[str(result.get('session_id'))] = float(result['total_cost_usd'])
    return sum(last.values()) if last else None
