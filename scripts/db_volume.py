"""Record the SELECTs of piloop's hot paths on a populated database, for ``tests/test_plans.py``.

The hot paths are the real functions: the triggers and the take of a scheduler tick, the stop conditions, the
overview and history renderers, and the orchestrator's inbox wait. They run under a listener that records every
SELECT that the ORM sends. The example queries that ``sql_procedures.md`` gives the agents are added too. The test
runs ``EXPLAIN QUERY PLAN`` on each statement against a small populated file, and fails on a SCAN of a table that
grows. So the rule "index what the tick and the status filter on" is checked by a test.
"""

import json
import random
import re
import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

from rich import box
from sqlalchemy import Engine, event, select
from sqlalchemy.engine import Connection

from piloop.campaign import inbox
from piloop.campaign.project import BLANK
from piloop.campaign.stop import stop_reason
from piloop.config import ProjectConfig, config_of, load_project_config
from piloop.daemon.runner import RunExecutor
from piloop.daemon.scheduler import Scheduler
from piloop.db.models import Project, utcnow
from piloop.db.session import Database
from piloop.otel import Tracer
from piloop.reports import history, overview

ROOT = Path(__file__).resolve().parent.parent
PROCEDURES = ROOT / 'piloop' / 'agents' / 'prompts' / 'sql_procedures.md'
HOT_INDEXES = (
    'ix_ideas_project_status_priority',
    'ix_runs_status',
    'ix_candidates_run',
    'ix_candidates_parent',
    'ix_workers_project_kind_state',
    'ix_workers_run',
    'ix_comments_inbox',
)
"""The indexes that the hot paths need. ``tests/test_plans.py`` drops them to prove that its check finds a SCAN, and
checks that the models still declare each one."""
GROWING = frozenset({'ideas', 'runs', 'candidates', 'measurements', 'comments', 'workers', 'events'})
"""The tables that grow with the campaign. ``projects`` and ``evaluators`` stay small, so a scan of them is fine."""


@dataclass(frozen=True)
class Volume:
    """Row counts to populate."""

    projects: int
    ideas: int
    runs: int
    candidates: int
    measurements: int
    comments: int
    events: int
    workers: int


SMALL = Volume(projects=2, ideas=40, runs=60, candidates=80, measurements=160, comments=120, events=400, workers=70)
"""The volume that the test uses: enough rows for every code path to run. Without ``ANALYZE`` statistics, a query
plan does not depend on the row counts."""


@dataclass(frozen=True)
class Statement:
    """One SELECT as the driver got it: qmark placeholders and the bound values."""

    origin: str
    sql: str
    params: tuple[object, ...]


def populate(path: Path, volume: Volume, seed: int = 0) -> None:
    """Fill a migrated database at ``path`` with ``volume`` rows of plausible shape, deterministically.

    It uses raw ``executemany`` for speed. The server defaults and CHECK constraints of a real database apply. Every
    project gets one evaluator, a kept baseline candidate and a few live agents. The ideas, runs, candidates, comments,
    workers and events are spread over the projects, with statuses drawn at the frequencies of a real campaign.
    """
    rng = random.Random(seed)
    cfg = load_project_config(BLANK, runtime='subprocess', max_concurrent={'pi': 2})
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        con.execute('PRAGMA foreign_keys=OFF')  # parents are inserted first, and the check costs a scan per row
        con.execute('BEGIN')
        _projects(con, volume, cfg)
        _ideas(con, volume, rng)
        _runs(con, volume, rng)
        owners = _candidates(con, volume, rng)
        _measurements(con, volume, rng, owners)
        _comments(con, volume, rng)
        _workers(con, volume, rng)
        _events(con, volume, rng)
        con.execute('COMMIT')


def _projects(con: sqlite3.Connection, v: Volume, cfg: ProjectConfig) -> None:
    for p in range(1, v.projects + 1):
        config = json.dumps(cfg.model_copy(update={'name': f'project-{p}'}).model_dump(mode='json'))
        con.execute(
            'INSERT INTO projects (id, name, path, config, evaluator_id) VALUES (?, ?, ?, ?, ?)',
            (p, f'project-{p}', f'/tmp/project-{p}', config, p),
        )
        con.execute(
            "INSERT INTO evaluators (id, project_id, key, definition) VALUES (?, ?, ?, '{}')", (p, p, f'key{p}')
        )


def _ideas(con: sqlite3.Connection, v: Volume, rng: random.Random) -> None:
    statuses = ['done'] * 70 + ['ready'] * 12 + ['scheduled'] * 4 + ['parked'] * 8 + ['rejected'] * 4 + ['proposed'] * 2
    con.executemany(
        'INSERT INTO ideas (id, project_id, title, hypothesis, status, priority, created_by, track)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (
            (
                i,
                _project_of(i, v),
                f'idea {i}',
                f'hypothesis {i}',
                rng.choice(statuses),
                rng.randint(0, 4),
                'research#1',
                'score',
            )
            for i in range(1, v.ideas + 1)
        ),
    )


def _runs(con: sqlite3.Connection, v: Volume, rng: random.Random) -> None:
    statuses = ['succeeded'] * 70 + ['failed'] * 18 + ['timed_out'] * 8 + ['running'] * 3 + ['queued'] * 1
    rows = []
    for r in range(1, v.runs + 1):
        idea = (r - 1) % v.ideas + 1
        attempt = (r - 1) // v.ideas + 1
        st = rng.choice(statuses)
        finished = None if st in ('running', 'queued') else f'2026-01-{1 + r % 28:02d} 12:00:00'
        rows.append((r, idea, attempt, _project_of(idea, v), 'pi', 'm', st, finished))
    con.executemany(
        'INSERT INTO runs (id, idea_id, attempt, base_candidate_id, harness, model, status, finished_at)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        rows,
    )


def _candidates(con: sqlite3.Connection, v: Volume, rng: random.Random) -> dict[int, int]:
    """Insert the candidates, each chained to the previous one of its project; returns candidate id → project id."""
    verdicts = ['discard'] * 75 + ['keep'] * 12 + ['crash'] * 10 + ['interesting'] * 2 + ['pending'] * 1
    last: dict[int, int] = {}
    owners: dict[int, int] = {}
    rows = []
    for c in range(1, v.candidates + 1):
        if c <= v.projects:  # the baselines: no run, no parent, kept
            rows.append((c, c, None, None, f'tree{c}', f'commit{c}', 'baseline', 'keep'))
            last[c] = owners[c] = c
            continue
        run = rng.randint(1, v.runs)
        p = _project_of((run - 1) % v.ideas + 1, v)
        rows.append((c, p, run, last[p], f'tree{c}', f'commit{c}', f'summary {c}', rng.choice(verdicts)))
        last[p] = c
        owners[c] = p
    con.executemany(
        'INSERT INTO candidates (id, project_id, run_id, parent_id, tree_sha, commit_sha, summary, verdict)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        rows,
    )
    return owners


def _measurements(con: sqlite3.Connection, v: Volume, rng: random.Random, owners: dict[int, int]) -> None:
    con.executemany(
        'INSERT INTO measurements (candidate_id, evaluator_id, metrics, seconds) VALUES (?, ?, ?, ?)',
        (
            (c, owners[c], json.dumps({'score': rng.random()}), rng.uniform(1, 60))
            for c in (m % v.candidates + 1 for m in range(v.measurements))
        ),
    )


def _comments(con: sqlite3.Connection, v: Volume, rng: random.Random) -> None:
    kinds = ['note'] * 60 + ['finding'] * 36 + ['escalation'] * 2 + ['decision'] * 2
    targets = ['idea'] * 60 + ['candidate'] * 20 + ['project'] * 15 + ['run'] * 5
    limits = {'idea': v.ideas, 'candidate': v.candidates, 'project': v.projects, 'run': v.runs}
    authors = ['research#1', 'worker#2', 'watchdog#3', 'pi']

    def row(i: int) -> tuple[str, int, str, str, str, int | None, str | None]:
        target, kind = rng.choice(targets), rng.choice(kinds)
        # the inbox holds every escalation and decision; all but the last few are acknowledged
        priority = rng.randint(0, 4) if kind == 'escalation' or kind == 'decision' else None
        acked = '2026-01-02 12:00:00' if priority is not None and i < v.comments - 20 else None
        return target, rng.randint(1, limits[target]), rng.choice(authors), kind, f'body {i}', priority, acked

    con.executemany(
        'INSERT INTO comments (target_kind, target_id, author, kind, body, priority, acked_at)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?)',
        (row(i) for i in range(v.comments)),
    )


def _workers(con: sqlite3.Connection, v: Volume, rng: random.Random) -> None:
    rows = []
    for w in range(1, v.workers + 1):
        p = _project_of(w, v)
        if w <= 2 * v.projects:  # a few live agents per project, on the headless runtime so status reads a file
            kind, run = ('worker', w) if w % 2 else ('research', None)
            rows.append((w, kind, p, run, 'pi', 'm', 'subprocess', '{}', 'running', None))
            continue
        kind = rng.choice(['worker'] * 80 + ['watchdog'] * 10 + ['research'] * 10)
        run = rng.randint(1, v.runs) if kind == 'worker' else None
        state = rng.choice(['exited'] * 90 + ['killed'] * 10)
        rows.append((w, kind, p, run, 'pi', 'm', 'subprocess', '{}', state, f'2026-01-{1 + w % 28:02d} 12:00:00'))
    con.executemany(
        'INSERT INTO workers (id, kind, project_id, run_id, harness, model, runtime, handle, state, ended_at)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        rows,
    )


def _events(con: sqlite3.Connection, v: Volume, rng: random.Random) -> None:
    kinds = ['run.started', 'run.finished', 'measurement.recorded', 'candidate.judged', 'idea.status', 'worker.started']
    targets = {
        'run': v.runs,
        'measurement': v.measurements,
        'candidate': v.candidates,
        'idea': v.ideas,
        'worker': v.workers,
    }
    con.executemany(
        "INSERT INTO events (kind, actor, target_kind, target_id, payload) VALUES (?, 'daemon', ?, ?, '{}')",
        (
            (rng.choice(kinds), t, rng.randint(1, targets[t]))
            for t in (rng.choice(list(targets)) for _ in range(v.events))
        ),
    )


def _project_of(n: int, v: Volume) -> int:
    return (n - 1) % v.projects + 1


def record(engine: Engine, origin: str, body: Callable[[], object]) -> list[Statement]:
    """Every SELECT the ORM sends to the driver while ``body`` runs, in order, with its bound values."""
    seen: list[Statement] = []

    def listen(_conn: Connection, _cursor: object, sql: str, params: object, _ctx: object, _executemany: bool) -> None:
        if sql.lstrip().upper().startswith('SELECT'):
            seen.append(Statement(origin, sql, tuple(params) if isinstance(params, list | tuple) else ()))

    event.listen(engine, 'before_cursor_execute', listen)
    try:
        body()
    finally:
        event.remove(engine, 'before_cursor_execute', listen)
    return seen


class HotPaths:
    """The hot paths of the daemon and the CLI, as callables over one session that is rolled back.

    The rollback makes sure that nothing is requested or launched. Entering yields origin → body: the two phases of a
    scheduler tick (per project, rolled back after each, so every run of the body sees the same database), the stop
    conditions with all four configured, the status and history renderers for every project, and one inbox wait.
    """

    def __init__(self, db: Database, home: Path) -> None:
        tracer = Tracer(None)
        self._scheduler = Scheduler(db, RunExecutor(db, tracer, home), tracer, home)
        self._session = db.session()

    def __enter__(self) -> dict[str, Callable[[], object]]:
        s = self._session
        rows = [(p, config_of(p)) for p in s.scalars(select(Project).order_by(Project.id))]
        stopping = load_project_config(
            BLANK, stop={'target': 1.0, 'max_runs': 10**6, 'max_hours': 10**6, 'plateau': 10**6}
        )
        scheduler = self._scheduler

        def each(body: Callable[[Project, ProjectConfig], object]) -> Callable[[], object]:
            return lambda: [body(p, cfg) for p, cfg in rows]

        def tick(p: Project, cfg: ProjectConfig) -> None:
            scheduler._triggers(s, p, cfg)
            s.flush()
            scheduler._take(s, p, cfg)
            s.rollback()

        return {
            'scheduler.tick': each(tick),
            'stop.stop_reason': each(lambda p, _cfg: stop_reason(s, p, stopping)),
            'overview.overview': each(lambda p, cfg: overview.overview(s, p, cfg, utcnow(), lambda _w: [])),
            'history.renderables': each(lambda p, cfg: history.renderables(s, p, cfg, table_box=box.SIMPLE)),
            'inbox.news': lambda: inbox.news(s, 0, me='orchestrator'),
        }

    def __exit__(self, *_: object) -> None:
        self._session.rollback()
        self._session.close()
        self._scheduler._pool.shutdown(wait=False)


def hot_statements(db: Database, home: Path) -> list[Statement]:
    """Return the distinct SELECTs of every hot path, recorded from the real functions on ``db``, and the agents' ones.

    Each statement text is reported once, from the first origin that sent it.
    """
    recorded: list[Statement] = []
    with HotPaths(db, home) as paths:
        for origin, body in paths.items():
            recorded += record(db.engine, origin, body)
    recorded += [Statement(f'sql_procedures.md #{i}', sql, ()) for i, sql in enumerate(documented_queries(), 1)]
    distinct: dict[str, Statement] = {}
    for st in recorded:
        distinct.setdefault(re.sub(r'\s+', ' ', st.sql).strip(), st)
    return list(distinct.values())


def documented_queries(text: str | None = None) -> list[str]:
    """Return the SELECTs in the ``sql`` fences of ``sql_procedures.md``: the queries that agents are told to run."""
    text = PROCEDURES.read_text() if text is None else text
    fences = re.findall(r'```sql\n(.*?)```', text, flags=re.S)
    statements = (s.strip() for fence in fences for s in re.sub(r'^--.*$', '', fence, flags=re.M).split(';'))
    return [s for s in statements if s.upper().startswith('SELECT')]


def explain(con: sqlite3.Connection, st: Statement) -> list[str]:
    """Return the ``EXPLAIN QUERY PLAN`` of ``st`` on ``con``, one line per plan node, indented by depth."""
    rows = con.execute(f'EXPLAIN QUERY PLAN {st.sql}', st.params).fetchall()
    depth = {0: -1}
    lines = []
    for node_id, parent, _, detail in rows:
        depth[node_id] = depth[parent] + 1
        lines.append('  ' * depth[node_id] + detail)
    return lines


def scans(st: Statement, plan: list[str]) -> list[str]:
    """Return the growing tables that the plan reads in full (``SCAN x``), with each alias resolved to its table."""
    aliases: dict[str, str] = {}
    for table, alias in re.findall(
        r'\b(?:FROM|JOIN)\s+([a-z_]+)(?:\s+(?:AS\s+)?(?!ON\b|WHERE\b|LEFT\b|JOIN\b|GROUP\b|ORDER\b|LIMIT\b|INNER\b)'
        r'([a-z_]\w*))?',
        st.sql,
        flags=re.I,
    ):
        aliases[alias or table] = table
    found = []
    for line in plan:
        m = re.search(r'\bSCAN (\w+)', line)
        if m and aliases.get(m.group(1), m.group(1)) in GROWING:
            found.append(aliases.get(m.group(1), m.group(1)))
    return found


def drop_new_indexes(path: Path) -> None:
    """Drop the indexes of the hot paths from the database at ``path``."""
    with closing(sqlite3.connect(path, isolation_level=None)) as con:
        for name in HOT_INDEXES:
            con.execute(f'DROP INDEX IF EXISTS {name}')
