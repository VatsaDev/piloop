"""The project overview: the state of one project, computed from the database.

``piloop status`` prints it, the daemon puts it in the brief of each sweep, and ``piloop watch`` draws its parts
(``piloop/watch/panels.py``). So the three always say the same thing, and a sweep does not have to query the database
for basic facts. It shows the goal and the progress toward the stop conditions, the tracks, the trajectory of the
trunk, the runs, the queue and the live agents, the recent candidates, the spend, the anomalies, the inbox, the latest
decisions and the recent events. For an agent, it adds a summary of the tools and the schema.

``anomalies`` also drives the ``anomaly`` trigger of the watchdog. An incident anomaly that is not yet assigned to a
watchdog requests an incident sweep. An ``anomaly.reported`` event on the row of that watchdog records the assignment
(see ``reported`` and ``assigned``). So each anomaly is investigated once, and a daemon restart does not repeat it.
"""

import re
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from pathlib import Path
from statistics import median

from sqlalchemy import Select, func, select
from sqlalchemy.orm import Session

from piloop import git
from piloop.agents.launcher import STARTUP_ANOMALY, TRANSCRIPT
from piloop.agents.scout import scouting
from piloop.campaign import inbox, spend
from piloop.campaign.ideas import defined_by, links, runnable, superseded_by, yields
from piloop.campaign.inbox import project_of
from piloop.campaign.stop import attempt, hours, runs, runs_without_keep
from piloop.config import Direction, Metric, ProjectConfig, RuntimeKind, Track, harness_of
from piloop.db.events import ANOMALY_EVENT
from piloop.db.models import (
    ENDED,
    FAILED_RUNS,
    FINISHED_RUNS,
    LIVE,
    OPEN_RUNS,
    Base,
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaStatus,
    Measurement,
    Project,
    ProjectStatus,
    Run,
    TargetKind,
    Verdict,
    Worker,
    WorkerKind,
    WorkerState,
    count,
    latest_worker,
    project_runs,
)
from piloop.reports.history import block
from piloop.runtime.factory import handle_for, runtime_for
from piloop.scoring.ledger import (
    NOTE,
    baseline,
    branches,
    head,
    keeps,
    metrics,
    references,
    score,
    track_of,
    trunk_keeps,
)
from piloop.text import NONE, cell
from piloop.usage import human, project_tokens

STEPS_SHOWN = 8
ANOMALIES_SHOWN = 12
REFS_SHOWN = 8
BLOCKED_SHOWN = 5
QUEUE_SHOWN = 8
CANDIDATES_SHOWN = 8
COMMENTS_SHOWN = 8
EVENTS_SHOWN = 5
EVALS_IN_TREND = 5
"""How many of the first and of the latest measurements the evaluation-time trend compares."""
DETAIL_CHARS = 160
SILENT_SECONDS = 3600
"""How long a worker with jobs in flight can go without a comment or a commit before it is ``silent``.

One hour allows several progress checks on a long job, and is a small part of ``[roles.worker] timeout``.
"""
LIVE_LINES = 40
"""How many extra screen lines to read when the session file of an agent has no text yet.

The extra lines get past the input box and the status bar of the harness to the output above them.
"""
CHROME = re.compile(r'\s*([─━═]{3,}|[❯›](\s|$)|.*\besc to interrupt\b|Worked for \d)')
"""A line that the harness draws, not output of the agent.

It matches the rule of an input box (Claude Code, pi), the prompt (Claude Code ``❯``, Codex ``›``), the
``esc to interrupt`` of a spinner and the ``Worked for 3m 2s`` of Codex. Everything from the last such line down is
cut, which also removes the status bar under the prompt.
"""


class AnomalyKind(StrEnum):
    """A kind of anomaly that the daemon detects. The value follows the row reference: ``run#36 failed``."""

    RUN_FAILED = 'failed'
    DEAD_AT_STARTUP = 'dead at startup'
    WATCHDOG_DEAD_AT_STARTUP = 'dead at startup, a watchdog'
    SAME_AS_PARENT = 'scored exactly its parent'
    IDLE_QUEUE = 'idle'
    SILENT = 'silent while its jobs run'


INCIDENTS = frozenset(
    {
        AnomalyKind.RUN_FAILED,
        AnomalyKind.DEAD_AT_STARTUP,
        AnomalyKind.SILENT,
    }
)
"""Kinds that request an incident sweep. The progress review weighs the other kinds, which are costs, not failures.

A watchdog that died at startup is not an incident: the watchdog that it requests can die the same way, and so on.
The startup breaker of the scheduler escalates that fault instead.
"""

type JobsProbe = Callable[[Worker], list[str]]
"""Return the jobs that a running worker has in flight (``Runtime.jobs``). Tests pass their own function."""


@dataclass(frozen=True)
class Anomaly:
    """One problem that the daemon found, with the time it began and the evidence in one line."""

    kind: AnomalyKind
    ref: str
    """The row that the anomaly is about: ``run#36``, ``worker#12``, ``measurement#117``, ``project#3``."""
    at: datetime
    """When the anomaly began, for example the end of a failed run or the exit of an agent."""
    detail: str

    @property
    def key(self) -> str:
        """Identify the anomaly across ticks: the same row with the same kind is one anomaly."""
        return f'{self.ref} {self.kind}'

    @property
    def incident(self) -> bool:
        return self.kind in INCIDENTS

    def line(self, now: datetime) -> str:
        tag = '[incident] ' if self.incident else ''
        return f'- {tag}{self.key}, {age(self.at, now)} ago: {self.detail}'


def overview(
    s: Session, project: Project, cfg: ProjectConfig, now: datetime, jobs: JobsProbe, *, tools: bool = True
) -> str:
    """Render the overview as markdown, as of ``now``.

    ``tools`` adds the summary of tools and schema that an agent needs and a human does not.
    """
    sections = [
        f'## Lay of the land (computed at {now:%Y-%m-%d %H:%M} UTC)',
        headline(s, project, cfg, now),
        '### Goal',
        *_goal(s, project, cfg, now),
        *(['### Tracks', *tracks_lines(s, project, cfg, now)] if cfg.multitrack else []),
        '### Trajectory',
        *_trajectory(s, project, cfg, now),
        '### Now',
        *_now(s, project, cfg, now),
        *_candidates(s, project, cfg, now),
        '### Anomalies the daemon detected',
        *_anomalies(anomalies(s, project, cfg, now, jobs), now),
        *_comments(s, project, now),
        '### Recent events',
        _events(s, project, now),
    ]
    if tools:
        sections += ['### Tools', *TOOLS, *_schema()]
    return '\n'.join(sections) + '\n'


def age(then: datetime, now: datetime) -> str:
    """Format an age as ``12s``, ``3m``, ``2h`` or ``1d``, in the largest whole unit, rounded down.

    A clock skew gives ``0s``, not a negative age.
    """
    seconds = max(0, int((now - then).total_seconds()))
    for unit, size in (('d', 86400), ('h', 3600), ('m', 60)):
        if seconds >= size:
            return f'{seconds // size}{unit}'
    return f'{seconds}s'


def headline(s: Session, project: Project, cfg: ProjectConfig, now: datetime) -> str:
    """Return ``summary``, then the age of the evaluator and the stop reason."""
    evaluator = project.evaluator
    evaluator_age = f' · evaluator #{evaluator.id}, {age(evaluator.created_at, now)} old' if evaluator else ''
    return f'{summary(s, project, cfg)}{evaluator_age}{stopped(project)}'


def summary(s: Session, project: Project, cfg: ProjectConfig) -> str:
    """Return the one-line summary of the project.

    The format is ``<name> · head <metric> <score> (candidate#<id>, <delta> vs baseline) · <kept> kept / <total>
    candidates``. With several tracks, the head shows the primary of each track:
    ``head candidate#9: accuracy 0.9100 (+0.0200), seconds 3.1000 (-0.4000)``. Imported references are not counted
    as candidates. ``· 3 starting points`` follows when there are any. ``· 1.2M tok`` follows when the usage of an
    agent is known: it is the total for all roles so far.
    """
    at = head_scores(s, project, cfg)
    total = count(s, _produced(project))
    kept = count(s, keeps(project))
    imported = len(references(s, project))
    if at is None:
        head_text = 'no trunk keep'
    elif not cfg.multitrack:
        trunk, [(track, value, delta)] = at
        vs_baseline = f', {delta:+.4f} vs baseline' if delta is not None else ''
        head_text = f'head {track.primary.name} {_four(value)} (candidate#{trunk.id}{vs_baseline})'
    else:
        trunk, values = at
        head_text = f'head candidate#{trunk.id}: ' + ', '.join(
            f'{track.primary.name} {_four(value)}' + (f' ({delta:+.4f})' if delta is not None else '')
            for track, value, delta in values
        )
    spent = project_tokens(s, project.id)
    tokens = f' · {human(spent)} tok' if spent else ''
    starts = f' · {imported} starting point{"s" if imported != 1 else ""}' if imported else ''
    return f'{project.name} · {head_text} · {kept} kept / {total} candidates{starts}{tokens}'


def head_scores(
    s: Session, project: Project, cfg: ProjectConfig
) -> tuple[Candidate, list[tuple[Track, float | None, float | None]]] | None:
    """Return the head and, for each track, its primary score and its delta against the baseline.

    A score is None when the current evaluator has not measured it. A delta is also None when the head is the
    baseline. Returns None when there is no trunk keep.
    """
    trunk = head(s, project)
    if trunk is None:
        return None
    evaluator_id = project.evaluator_id or 0
    first = baseline(s, project)
    values: list[tuple[Track, float | None, float | None]] = []
    for track in cfg.all_tracks:
        value = score(trunk, evaluator_id, track.primary)
        base = score(first, evaluator_id, track.primary) if first.id != trunk.id else None
        values.append((track, value, None if value is None or base is None else value - base))
    return trunk, values


def stopped(project: Project) -> str:
    """Return `` · stopped: <reason>`` if the project stopped, else an empty string."""
    return f' · stopped: {project.stopped_reason}' if project.status is ProjectStatus.STOPPED else ''


def progress(s: Session, project: Project, cfg: ProjectConfig, now: datetime) -> str:
    """Return the runs and hours so far, with each ``[stop]`` condition against its limit: ``run 7/200 · 2.1h/12h``.

    If there is a ``[spend]`` budget, the spend follows (``$412.30/$1,000.00 spend``), with the reason when launches
    are held.
    """
    stop = cfg.stop
    parts = [
        f'run {runs(s, project)}{"" if stop.max_runs is None else f"/{stop.max_runs}"}',
        f'{hours(s, project, now) or 0:.1f}h{"" if stop.max_hours is None else f"/{stop.max_hours:g}h"}',
    ]
    if stop.max_tokens is not None:
        parts.append(f'{human(project_tokens(s, project.id))}/{human(stop.max_tokens)} tok')
    if stop.plateau is not None:
        parts.append(f'{runs_without_keep(s, project)}/{stop.plateau} runs without a keep')
    if stop.target is not None:  # A target is only allowed with one track.
        at = head_scores(s, project, cfg)
        value = at[1][0][1] if at is not None else None
        sign = '≥' if cfg.all_tracks[0].primary.direction is Direction.HIGHER else '≤'
        parts.append(f'target {sign} {stop.target:g} (head {_num(value)})')
    if (budget := spend.progress(project, cfg)) is not None:
        parts.append(budget)
    return ' · '.join(parts)


def _produced(project: Project) -> Select[Candidate]:
    """Select the candidates of the project: the baseline and the commits of its runs, not imported references."""
    return select(Candidate).where(Candidate.project_id == project.id, Candidate.verdict != Verdict.REFERENCE)


def live_workers(s: Session, project: Project) -> Sequence[Worker]:
    """Return the agents of the project that are starting or running, oldest first."""
    return s.scalars(
        select(Worker).where(Worker.project_id == project.id, Worker.state.in_(LIVE)).order_by(Worker.id)
    ).all()


def tab_of(w: Worker) -> str | None:
    """Return the Herdr tab id of a launched agent, or None for a headless agent or one not launched yet."""
    tab = w.handle.get('tab_id') if w.runtime == RuntimeKind.HERDR else None
    return None if tab is None else str(tab)


def on(s: Session, w: Worker) -> str:
    """Describe what the agent works on: the idea for a worker, the trigger reason for the other roles."""
    if w.kind is WorkerKind.WORKER and w.run_id is not None:
        idea = s.get_one(Run, w.run_id).idea
        return f'idea#{idea.id} {cell(idea.title)}'
    return cell(w.reason or NONE)


def agent_output(s: Session, w: Worker, project_path: Path, lines: int) -> list[str]:
    """Return the last ``lines`` non-empty lines of output of the agent.

    The first choice is the text in the session file of the harness, found in the same way as its usage
    (``UsageSpec``). If the harness has no usage spec, or the agent has written no text yet, the screen from the
    runtime is used, without the lines that the harness draws. The last choice is the transcript saved when the agent
    ended.
    """
    definition = harness_of(s, w)
    said = definition.usage.resolved.said(w.session_id, lines) if definition and definition.usage else []
    return (
        said
        or above_chrome(_live_output(s, w, project_path, lines + LIVE_LINES))[-lines:]
        or above_chrome(_transcript(w.log_dir))[-lines:]
    )


def _live_output(s: Session, w: Worker, project_path: Path, lines: int) -> str:
    try:
        return runtime_for(s, w, project_path).read(handle_for(w), lines=lines)
    except Exception:  # Not launched yet, herdr missing or the pane closed: the transcript is the fallback.
        return ''


def _transcript(log_dir: str | None) -> str:
    path = Path(log_dir) / TRANSCRIPT if log_dir else None
    return path.read_text(errors='replace') if path is not None and path.is_file() else ''


def above_chrome(text: str) -> list[str]:
    """Return the non-empty lines above the last ``CHROME`` line, without any other ``CHROME`` line."""
    lines = [line.rstrip() for line in text.splitlines() if line.strip()]
    furniture = [i for i, line in enumerate(lines) if CHROME.match(line)]
    if furniture:
        lines = [line for line in lines[: furniture[-1]] if not CHROME.match(line)]
    return lines


def queued_ideas(s: Session, project: Project) -> tuple[Sequence[Idea], Sequence[Idea]]:
    """Return every ``ready`` idea in scheduling order, and the ones not blocked by a prerequisite."""
    mine = select(Idea).where(Idea.project_id == project.id).order_by(Idea.priority, Idea.id)
    waiting = s.scalars(mine.where(Idea.status == IdeaStatus.READY)).all()
    ready = s.scalars(mine.where(runnable())).all()
    return waiting, ready


def scored(c: Candidate, evaluator_id: int, primary: Metric) -> tuple[str, str]:
    """Format the primary score of the candidate and its delta to its parent, each ``—`` when unknown."""
    value = score(c, evaluator_id, primary)
    parent = score(c.parent, evaluator_id, primary) if c.parent is not None else None
    delta = f'{value - parent:+.4f}' if value is not None and parent is not None else NONE
    return NONE if value is None else f'{value:.4f}', delta


def item_lines(s: Session, c: Comment, now: datetime) -> tuple[str, list[str]]:
    """Format one comment and its thread as ``piloop inbox show`` prints them: a title line and the lines below.

    The body is shown in full. The linked comments follow (the comments it cites as comment#<id>, the comments that
    cite it, and its replies), each cut by ``block``.
    """
    if c.priority is None:
        state = 'not in the inbox'
    elif c.acked_at is None:
        state = f'p{c.priority}, unacknowledged'
    else:
        state = f'p{c.priority}, acknowledged by {c.acked_by} {age(c.acked_at, now)} ago'
    title = f'comment#{c.id} {c.kind} on {c.target_kind}#{c.target_id} by {c.author}, {age(c.created_at, now)} ago · {state}'
    lines = c.body.strip().splitlines()
    rest = inbox.thread(s, c)
    if rest:
        lines += ['', 'linked: the comments it cites, those citing it, its replies']
    for other in rest:
        head = f'- comment#{other.id} {other.kind} by {other.author}, {age(other.created_at, now)} ago: '
        lines += (head + block(other.body).replace('\n', '\n  ')).splitlines()
    return title, lines


def anomalies(s: Session, project: Project, cfg: ProjectConfig, now: datetime, jobs: JobsProbe) -> list[Anomaly]:
    """Return every anomaly of ``project``, newest first. ``jobs`` is called only for silent workers."""
    found = [
        *_failed_runs(s, project),
        *_dead_at_startup(s, project),
        *_same_as_parent(s, project, cfg),
        *_idle_queue(s, project, cfg),
        *_silent(s, project, now, jobs),
    ]
    return sorted(found, key=lambda a: a.at, reverse=True)


def reported(s: Session, project_id: int) -> set[str]:
    """Return the keys of the anomalies that are already assigned to a watchdog of the project."""
    rows = s.scalars(
        select(Event.payload).where(
            Event.kind == ANOMALY_EVENT,
            Event.target_kind == TargetKind.WORKER,
            Event.target_id.in_(select(Worker.id).where(Worker.project_id == project_id)),
        )
    )
    return {str(payload['key']) for payload in rows}


def assigned(s: Session, worker_id: int) -> list[str]:
    """Return the anomaly lines assigned to a watchdog, in order. A progress sweep has none."""
    rows = s.scalars(
        select(Event.payload)
        .where(Event.kind == ANOMALY_EVENT, Event.target_kind == TargetKind.WORKER, Event.target_id == worker_id)
        .order_by(Event.id)
    )
    return [str(payload['anomaly']) for payload in rows]


def _goal(s: Session, project: Project, cfg: ProjectConfig, now: datetime) -> list[str]:
    primaries = {t.primary.name for t in cfg.all_tracks}
    others = ', '.join(f'{m.name} ({m.direction}, {m.role})' for m in cfg.metrics if m.name not in primaries)
    also = f'; also measured: {others}' if others else ''
    state = f'stopped — {project.stopped_reason}' if project.status is ProjectStatus.STOPPED else 'active'
    stop = cfg.stop
    limits = (stop.target, stop.max_runs, stop.max_hours, stop.max_tokens, stop.plateau)
    open_ended = '' if any(v is not None for v in limits) else ' (no [stop] condition: open-ended)'
    if cfg.multitrack:
        tracks = '; '.join(f'{t.name} ({_metric(t.primary)}, share {t.share:g})' for t in cfg.all_tracks)
        objective = f'- Tracks, each a primary its keeps improve while holding the others: {tracks}{also}'
    else:
        primary = cfg.all_tracks[0].primary
        objective = f'- Primary metric: {primary.name}, {primary.direction} is better{also}'
    return [
        objective,
        f'- Campaign: {state}. Progress: {progress(s, project, cfg, now)}{open_ended}.',
        "- The goal in words is the project's brief.md: weigh the metric against it.",
    ]


def _metric(m: Metric) -> str:
    """Format a primary metric as a guardrail of another track: ``accuracy, higher, margin 0.01, floor 0.8``."""
    margin = f', margin {m.margin:g}' if m.margin is not None else ''
    floor = f', floor {m.floor:g}' if m.floor is not None else ''
    return f'{m.name}, {m.direction}{margin}{floor}'


def tracks_lines(s: Session, project: Project, cfg: ProjectConfig, now: datetime) -> list[str]:
    """Return one line per track.

    Each line shows the primary at the head against the start and its floor, the trunk keeps of the track and the
    latest one, its share of the worker slots against its open runs, and its runnable ideas.
    """
    evaluator_id = project.evaluator_id or 0
    trunk = head(s, project)
    first = baseline(s, project)
    steps = s.scalars(trunk_keeps(project).where(Candidate.run_id.is_not(None)).order_by(Candidate.id)).all()
    open_runs = project_runs(project.id).where(Run.status.in_(OPEN_RUNS)).with_only_columns(Idea.track)
    running = Counter(s.scalars(open_runs))
    ready = Counter(s.scalars(select(Idea.track).where(Idea.project_id == project.id, runnable())))
    slots = cfg.roles.worker.max_concurrent
    lines = []
    for track in cfg.all_tracks:
        m = track.primary
        value = score(trunk, evaluator_id, m) if trunk is not None else None
        start = score(first, evaluator_id, m)
        gain = f' ({value - start:+g} on the start)' if value is not None and start is not None else ''
        floor = ''
        if m.floor is not None:
            floor = f'; floor {m.floor:g}' + (f', {m.gain(value, m.floor):g} to spare' if value is not None else '')
        mine = [c for c in steps if track_of(cfg, c).name == track.name]
        latest = f', the latest candidate#{mine[-1].id} {age(_known(mine[-1]), now)} ago' if mine else ''
        lines.append(
            f'- {track.name}: {m.name} {_num(value)} at the head{gain}{floor}; {len(mine)} trunk keeps{latest};'
            f' share {track.share:g} of {slots} slots, {running[track.name]} running; {ready[track.name]} runnable'
            ' ideas.'
        )
    return lines


def _trajectory(s: Session, project: Project, cfg: ProjectConfig, now: datetime) -> list[str]:
    """Show the trunk: the start, then each trunk keep in order, with its gain on its track."""
    evaluator_id = project.evaluator_id or 0
    steps = s.scalars(trunk_keeps(project).order_by(Candidate.id)).all()
    if not steps:
        return ["- No trunk keep: the baseline's verdict was overridden and nothing was kept since."]
    first, last = steps[0], steps[-1]
    gains = []
    for track in cfg.all_tracks:
        a, b = score(first, evaluator_id, track.primary), score(last, evaluator_id, track.primary)
        if a is None or b is None:
            gains.append(f'{track.primary.name} unmeasured')
            continue
        relative = f', {(b - a) / abs(a):+.1%}' if a else ''
        gains.append(f'{track.primary.name} {a:g} → {b:g} ({b - a:+g}{relative})')
    finished = project_runs(project.id).where(Run.status.in_(FINISHED_RUNS), attempt(), Run.finished_at > _known(last))
    lines = [
        f'- Start candidate#{first.id}, head candidate#{last.id}: {"; ".join(gains)}; {len(steps) - 1} trunk keeps.',
        f'- Last trunk keep {age(_known(last), now)} ago; {count(s, finished)} runs finished since.',
        f'- Trunk keeps (latest {STEPS_SHOWN}; when first measured, the gain on its track over the head before it):',
    ]
    before = dict(zip([c.id for c in steps[1:]], steps, strict=False))
    for c in steps[-STEPS_SHOWN:]:
        track = track_of(cfg, c)
        value, previous = score(c, evaluator_id, track.primary), before.get(c.id)
        prior = score(previous, evaluator_id, track.primary) if previous is not None else None
        if previous is None:
            delta = ' (start)'
        else:
            delta = f' ({value - prior:+g})' if value is not None and prior is not None else ''
        on = f' [{track.name}] {track.primary.name}' if cfg.multitrack and previous is not None else ''
        idea = f' idea#{c.run.idea_id}' if c.run is not None else ''
        lines.append(
            f'  - {_known(c):%m-%d %H:%M} candidate#{c.id}{on} {_num(value)}{delta}{idea}: {cell(c.summary or "—", 80)}'
        )
    return lines + _branches(s, project, cfg)


def _known(c: Candidate) -> datetime:
    """Return the time of the first successful measurement of the candidate, else its creation time."""
    return min((m.created_at for m in c.measurements if m.error is None), default=c.created_at)


def _values(c: Candidate, cfg: ProjectConfig, evaluator_id: int) -> str:
    """Format the primary of each track on ``c``: ``accuracy 0.91, seconds 3.1``. One track shows the score only."""
    if not cfg.multitrack:
        return _num(score(c, evaluator_id, cfg.all_tracks[0].primary))
    return ', '.join(f'{t.primary.name} {_num(score(c, evaluator_id, t.primary))}' for t in cfg.all_tracks)


def _branches(s: Session, project: Project, cfg: ProjectConfig) -> list[str]:
    """List the lineages of keeps if there is more than one, then the latest notes: other places a run can start."""
    found = branches(s, project)
    evaluator_id = project.evaluator_id or 0
    lines = []
    if len(found) > 1:
        lines.append('- Lineages (latest keep of each; a branch keep never moves the head):')
        lines += [
            f'  - {"branch" if b.branched else "trunk"} from candidate#{b.root.id} ({b.root.verdict}):'
            f' latest candidate#{b.latest.id} {_values(b.latest, cfg, evaluator_id)}, {b.keeps} kept'
            for b in found
        ]
    noted = s.scalars(
        select(Candidate)
        .where(
            Candidate.project_id == project.id,
            Candidate.verdict == Verdict.INTERESTING,
            Candidate.verdict_reason.startswith(NOTE),
        )
        .order_by(Candidate.id.desc())
        .limit(STEPS_SHOWN)
    ).all()
    if noted:
        lines.append('- Notes (regressed, kept in view as leads; pin one with base_candidate_id):')
        lines += [
            f'  - candidate#{c.id} {_num(score(c, evaluator_id, track_of(cfg, c).primary))} by {c.verdict_by}:'
            f' {cell((c.verdict_reason or "").removeprefix(NOTE), 100)}'
            for c in noted
        ]
    return lines


def _now(s: Session, project: Project, cfg: ProjectConfig, now: datetime) -> list[str]:
    return [
        *_runs_now(s, project, cfg, now),
        *_queue(s, project, now),
        *_agents(s, project, now),
        _evaluations(s, project),
    ]


def _runs_now(s: Session, project: Project, cfg: ProjectConfig, now: datetime) -> list[str]:
    open_runs = s.scalars(project_runs(project.id).where(Run.status.in_(OPEN_RUNS)).order_by(Run.id)).all()
    lines = [f'- Worker slots: {len(open_runs)} of {cfg.roles.worker.max_concurrent} busy.']
    for run in open_runs:
        worker = latest_worker(s, run.id)
        state = f'{run.status}'
        if run.started_at is not None:
            state += f' {age(run.started_at, now)}'
        if worker is not None and worker.ended_at is not None:
            state += f', worker#{worker.id} ended {age(worker.ended_at, now)} ago'
        lines.append(f'  - run#{run.id} idea#{run.idea_id} "{cell(run.idea.title, 60)}" ({run.harness}): {state}')
    return lines


def _queue(s: Session, project: Project, now: datetime) -> list[str]:
    """Show the idea counts by status, what blocks the blocked ideas, and the runnable ideas in queue order."""
    by_status = select(Idea.status, func.count()).where(Idea.project_id == project.id).group_by(Idea.status)
    counts: dict[IdeaStatus, int] = {status: n for status, n in s.execute(by_status)}
    waiting, ready = queued_ideas(s, project)
    blocked = [i for i in waiting if i not in ready]
    waits = [
        f'idea#{i.id} on ' + ', '.join(f'idea#{t.id} ({t.status})' for t in i.after if t.status is not IdeaStatus.DONE)
        for i in blocked[:BLOCKED_SHOWN]
    ]
    more = f', {len(blocked) - BLOCKED_SHOWN} more' if len(blocked) > BLOCKED_SHOWN else ''
    shown = f' ({"; ".join(waits)}{more})' if waits else ''
    rest = ', '.join(f'{counts.get(status, 0)} {status}' for status in IdeaStatus if status is not IdeaStatus.READY)
    newer = superseded_by(s, [i.id for i in ready[:QUEUE_SHOWN]])
    queue = [
        f'  - idea#{i.id} p{i.priority} "{cell(i.title, 60)}" by {i.created_by}, {age(i.created_at, now)} ago'
        + ('' if (linked := links(i, newer)) == NONE else f'; {linked}')
        for i in ready[:QUEUE_SHOWN]
    ]
    return [f'- Ideas: {len(ready)} runnable, {len(blocked)} blocked{shown}; {rest}.', *queue]


def _agents(s: Session, project: Project, now: datetime) -> list[str]:
    """Show the live agents with their last line, the agents per role, the yield of each source and the tokens."""
    rows = s.scalars(select(Worker).where(Worker.project_id == project.id).order_by(Worker.id)).all()
    path = Path(project.path)
    live = [
        f'  - {w.kind}#{w.id} {w.harness or NONE}/{w.model or NONE} {age(w.started_at, now)}'
        f'{f", tab {tab}" if (tab := tab_of(w)) else ""}, on {on(s, w)}: '
        + (cell(output[-1]) if (output := agent_output(s, w, path, 1)) else NONE)
        for w in rows
        if w.state in LIVE
    ]
    requested = [f'{w.kind}#{w.id}' for w in rows if w.state is WorkerState.REQUESTED]
    per_kind: list[str] = []
    spend: list[str] = []
    for kind in WorkerKind:
        mine = [w for w in rows if w.kind is kind]
        killed = sum(w.state is WorkerState.KILLED for w in mine)
        per_kind.append(f'{len(mine)} {kind}' + (f' ({killed} killed)' if killed else ''))
        spend.append(f'{kind} {human(sum(_tokens(w) for w in mine))}')
    defined = defined_by(s, project.id)
    scouts = set(s.scalars(select(Worker.id).where(Worker.project_id == project.id, scouting())))
    sweeps = [w for w in rows if w.kind is WorkerKind.RESEARCH and w.state in ENDED and w.id not in scouts]
    barren = sum(f'research#{w.id}' not in defined for w in sweeps)
    if scouts:
        per_kind.append(f'{len(scouts)} of the research in scout mode')
    hour = sum(_tokens(w) for w in rows if w.ended_at is not None and now - w.ended_at <= timedelta(hours=1))
    by_source = '; '.join(y.line() for y in yields(s, project.id)) or 'none yet'
    return [
        f'- Agents live: {len(live) or "none"}' + (f'; requested: {", ".join(requested)}' if requested else ''),
        *live,
        f'- Agents so far: {", ".join(per_kind)}; {barren} of {len(sweeps)} ended research sweeps defined no idea.',
        f'- Ideas by source: {by_source}.',
        f'- Tokens: {human(sum(_tokens(w) for w in rows))} ({", ".join(spend)}); {human(hour)} by agents that ended in'
        ' the last hour.',
    ]


def _evaluations(s: Session, project: Project) -> str:
    """Show the number of evaluations, their duration now against at first, and the candidates without a verdict."""
    evaluator_id = project.evaluator_id or 0
    mine = select(Candidate.id).where(Candidate.project_id == project.id)
    rows = s.scalars(
        select(Measurement)
        .where(Measurement.candidate_id.in_(mine), Measurement.evaluator_id == evaluator_id)
        .order_by(Measurement.id)
    ).all()
    crashed = sum(m.error is not None for m in rows)
    seconds = [m.seconds for m in rows if m.seconds is not None]
    waiting = count(
        s, select(Candidate).where(Candidate.project_id == project.id, Candidate.verdict == Verdict.PENDING)
    )
    timing = ''
    if seconds:
        timing = (
            f'; median {_minutes(median(seconds))}, first {EVALS_IN_TREND} {_minutes(median(seconds[:EVALS_IN_TREND]))},'
            f' latest {EVALS_IN_TREND} {_minutes(median(seconds[-EVALS_IN_TREND:]))}; {_minutes(sum(seconds))} in all'
        )
    return f'- Evaluations: {len(rows)} ({crashed} crashed){timing}; {waiting} candidates waiting for a verdict.'


def _candidates(s: Session, project: Project, cfg: ProjectConfig, now: datetime) -> list[str]:
    """List the last ``CANDIDATES_SHOWN`` candidates of the project, oldest first, scored against their parent."""
    total = count(s, _produced(project))
    recent = s.scalars(_produced(project).order_by(Candidate.id.desc()).limit(CANDIDATES_SHOWN)).all()[::-1]
    on = "each on its track's primary" if cfg.multitrack else cfg.all_tracks[0].primary.name
    lines = [f'### Recent candidates ({len(recent)} of {total}; {on}, vs parent)']
    for c in recent:
        track = track_of(cfg, c)
        value, delta = scored(c, project.evaluator_id or 0, track.primary)
        run = f'run#{c.run_id}' if c.run_id else 'no run'
        tag = f' [{track.name}]' if cfg.multitrack else ''
        lines.append(
            f'- candidate#{c.id} {c.verdict}{tag} {value} ({delta}), {run}, {age(c.created_at, now)} ago:'
            f' {cell(c.summary or NONE)}'
        )
    return lines


def _anomalies(found: list[Anomaly], now: datetime) -> list[str]:
    """List incidents one per line, newest first. Group each other kind on one line, because it is a cost."""
    if not found:
        return ['- None.']
    incidents = [a for a in found if a.incident]
    older = len(incidents) - ANOMALIES_SHOWN
    lines = [a.line(now) for a in incidents[:ANOMALIES_SHOWN]]
    if older > 0:
        lines.append(f'- … and {older} older incidents.')
    for kind in AnomalyKind:
        same = [a for a in found if a.kind is kind and not a.incident]
        if len(same) == 1:
            lines.append(same[0].line(now))
        elif same:
            refs = ', '.join(a.ref for a in same[:REFS_SHOWN]) + (', …' if len(same) > REFS_SHOWN else '')
            lines.append(f'- {len(same)} × {kind} ({refs}); the latest, {age(same[0].at, now)} ago: {same[0].detail}')
    return lines


def _comments(s: Session, project: Project, now: datetime) -> list[str]:
    """List the inbox and the latest decisions, escalations and watchdog findings, newest first.

    A sweep reads this so it does not repeat a finding, and a human sees what waits for an acknowledgement.
    """
    unacked = Comment.priority.is_not(None) & Comment.acked_at.is_(None)
    said = (
        unacked
        | Comment.kind.in_((CommentKind.DECISION, CommentKind.ESCALATION))
        | ((Comment.kind == CommentKind.FINDING) & Comment.author.like('watchdog#%'))
    )
    rows = s.scalars(
        select(Comment)
        .where(said, project_of(Comment.target_kind, Comment.target_id) == project.id)
        .order_by(Comment.id.desc())
        .limit(COMMENTS_SHOWN)
    ).all()
    waiting = len(inbox.items(s, project_id=project.id))
    lines = [
        f'- comment#{c.id} {c.kind}{f" (p{c.priority}, unacknowledged)" if c.priority is not None and c.acked_at is None else ""}'
        f' on {c.target_kind}#{c.target_id} by {c.author}, {age(c.created_at, now)} ago: {cell(c.body, DETAIL_CHARS)}'
        for c in rows
    ]
    return [
        f'### Inbox ({waiting} unacknowledged), decisions, escalations and watchdog findings',
        *(lines or ['- None.']),
    ]


def latest_events(s: Session, project: Project, limit: int) -> Sequence[Event]:
    """Return the last ``limit`` events of the project, newest first."""
    mine = select(Event).where(project_of(Event.target_kind, Event.target_id) == project.id)
    return s.scalars(mine.order_by(Event.id.desc()).limit(limit)).all()


def _events(s: Session, project: Project, now: datetime) -> str:
    latest = latest_events(s, project, EVENTS_SHOWN)
    return '- ' + (
        ' · '.join(f'{e.kind} {e.target_kind}#{e.target_id} {age(e.at, now)} ago' for e in latest) or 'None.'
    )


TOOLS = [
    '- `piloop sql "<one statement>"` (the full command is under Context) takes nothing else: no flags, no --help.'
    ' It prints a table cut to the terminal width, so prefix `COLUMNS=400` for wide rows, or select one'
    " concatenated column (`a || ' | ' || b`); at most 500 rows.",
    "- Scores: `json_extract(m.metrics, '$.<metric>')`; a candidate's score is the mean over its measurements with"
    ' `error IS NULL AND evaluator_id = projects.evaluator_id`. `piloop history` prints every idea with its outcome.',
    f'- Transcripts: `<runs.log_dir>/{TRANSCRIPT}` (a worker), `<workers.log_dir>/{TRANSCRIPT}` and `brief.md`'
    ' beside it (any agent); the whole session: `cd <repo> && entire checkpoint explain <workers.checkpoint_id>`.',
    '- Code: `git -C <repo> show <candidates.commit_sha>`; a candidate against its parent:'
    ' `git -C <repo> diff <parent commit_sha> <commit_sha>`.',
    "- Who changed what: `SELECT at, actor, kind, payload FROM events WHERE target_kind='idea' AND target_id=12`.",
    '- Full-text search: `ideas_fts` (title, hypothesis, plan), `comments_fts` (body); see the procedures above.',
    '- Tables and columns:',
]


def _schema() -> list[str]:
    """List every table with its columns, from the models, so that an agent does not query the schema."""
    return [f'  - {t.name}({", ".join(c.name for c in t.columns)})' for t in Base.metadata.sorted_tables]


def _failed_runs(s: Session, project: Project) -> list[Anomaly]:
    return [
        Anomaly(
            AnomalyKind.RUN_FAILED,
            f'run#{run.id}',
            run.finished_at or run.created_at,
            f'{run.status} on {run.harness} (idea#{run.idea_id}): {cell(run.error or "no error recorded", DETAIL_CHARS)}',
        )
        for run in s.scalars(project_runs(project.id).where(Run.status.in_(FAILED_RUNS), attempt()))
    ]


def _dead_at_startup(s: Session, project: Project) -> list[Anomaly]:
    """Find agents of any role that the launcher found dead at startup (``launcher.STARTUP_ANOMALY``)."""
    events = select(Event).where(
        Event.kind == STARTUP_ANOMALY,
        Event.target_kind == TargetKind.WORKER,
        Event.target_id.in_(select(Worker.id).where(Worker.project_id == project.id)),
    )
    found: list[Anomaly] = []
    for e in s.scalars(events):
        p = e.payload
        error = cell(str(p.get('error') or 'no error recorded'), DETAIL_CHARS)
        detail = f'{p.get("kind")} on {p.get("harness")} failed to start ({p.get("seconds")}s): {error}'
        kind = (
            AnomalyKind.WATCHDOG_DEAD_AT_STARTUP
            if p.get('kind') == WorkerKind.WATCHDOG
            else AnomalyKind.DEAD_AT_STARTUP
        )
        found.append(Anomaly(kind, f'worker#{e.target_id}', e.at, detail))
    return found


def _same_as_parent(s: Session, project: Project, cfg: ProjectConfig) -> list[Anomaly]:
    """Find candidates whose primary score is exactly the score of their parent.

    If the other metrics are also equal (except a timing metric), the change did not affect what the evaluation sees.
    """
    evaluator_id = project.evaluator_id or 0
    found: list[Anomaly] = []
    children = select(Candidate).where(Candidate.project_id == project.id, Candidate.parent_id.is_not(None))
    for c in s.scalars(children.order_by(Candidate.id)):
        assert c.parent is not None
        primary = track_of(cfg, c).primary.name
        mine, theirs = metrics(c, evaluator_id), metrics(c.parent, evaluator_id)
        if primary not in mine or mine[primary] != theirs.get(primary):
            continue
        rows = [m for m in c.measurements if m.evaluator_id == evaluator_id and m.error is None]
        others = [name for name in mine if name != primary]
        same = sum(mine[name] == theirs.get(name) for name in others)
        detail = (
            f'scored what candidate#{c.parent_id} did on {primary} and on {same} of {len(others)} other metrics;'
            f' {_minutes(sum(m.seconds or 0 for m in rows))} of evaluation'
        )
        found.append(Anomaly(AnomalyKind.SAME_AS_PARENT, f'candidate#{c.id}', min(m.created_at for m in rows), detail))
    return found


def _idle_queue(s: Session, project: Project, cfg: ProjectConfig) -> list[Anomaly]:
    """Find an active project with a free worker slot and no runnable idea, since the last run started or ended."""
    if project.status is not ProjectStatus.ACTIVE:
        return []
    open_runs = project_runs(project.id).where(Run.status.in_(OPEN_RUNS))
    free = cfg.roles.worker.max_concurrent - count(s, open_runs)
    if cfg.stop.max_runs is not None:
        free = min(free, cfg.stop.max_runs - runs(s, project))
    ready = count(s, select(Idea).where(Idea.project_id == project.id, runnable()))
    if free <= 0 or ready:
        return []
    moments = [project.created_at]
    for column in (Run.created_at, Run.finished_at):
        latest = s.scalar(project_runs(project.id).with_only_columns(func.max(column)))
        if latest is not None:
            moments.append(latest)
    research = s.scalars(
        select(Worker).where(
            Worker.project_id == project.id,
            Worker.kind == WorkerKind.RESEARCH,
            Worker.state.in_((WorkerState.REQUESTED, *LIVE)),
        )
    ).all()
    sweeps = ', '.join(f'research#{w.id} {w.state}' for w in research) or 'no research sweep pending'
    detail = f'{free} free worker slots and no runnable idea; {sweeps}'
    return [Anomaly(AnomalyKind.IDLE_QUEUE, f'project#{project.id}', max(moments), detail)]


def _silent(s: Session, project: Project, now: datetime, jobs: JobsProbe) -> list[Anomaly]:
    """Find running workers with jobs in flight that have not commented or committed for ``SILENT_SECONDS``.

    The silence starts at the latest of: the start of the run, a comment signed ``worker#<id>`` or ``run#<id>``, and
    the HEAD commit of its worktree. The commits of a worker become candidates only when it ends, so the worktree is
    checked directly. The runtime is asked for jobs last, and only for workers that are already silent. Each worker
    gets one anomaly. If a worker goes silent again later, the progress review notices it.
    """
    limit = timedelta(seconds=SILENT_SECONDS)
    rows = s.scalars(
        select(Worker)
        .where(
            Worker.project_id == project.id,
            Worker.kind == WorkerKind.WORKER,
            Worker.state == WorkerState.RUNNING,
            Worker.run_id.is_not(None),
        )
        .order_by(Worker.id)
    )
    found: list[Anomaly] = []
    for worker in rows:
        run = s.get_one(Run, worker.run_id)
        signed = (f'worker#{worker.id}', f'run#{run.id}')
        moments = [
            ('its run started', run.started_at or worker.started_at),
            ('its last comment', s.scalar(select(func.max(Comment.created_at)).where(Comment.author.in_(signed)))),
            ('its last commit', git.head_time(Path(run.log_dir) / 'worktree') if run.log_dir else None),
        ]
        what, last = max(((w, m) for w, m in moments if m is not None), key=lambda p: p[1])
        if now - last <= limit:
            continue
        running = jobs(worker)
        if not running:
            continue
        detail = (
            f'run#{run.id} idea#{run.idea_id}: no comment or commit for {age(last, now)} ({what});'
            f' {len(running)} jobs in flight, e.g. {cell(running[0], DETAIL_CHARS)}'
        )
        found.append(Anomaly(AnomalyKind.SILENT, f'worker#{worker.id}', last + limit, detail))
    return found


def _tokens(w: Worker) -> int:
    return w.usage['total'] if w.usage else 0


def _minutes(seconds: float) -> str:
    return f'{seconds / 60:.1f} min'


def _num(value: float | None) -> str:
    return NONE if value is None else f'{value:g}'


def _four(value: float | None) -> str:
    return 'unmeasured' if value is None else f'{value:.4f}'
