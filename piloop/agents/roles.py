"""The agent roles as data: what requests an agent, what its brief says and what it may write (https://piloop.autoresearch.inc/concepts/roles).

Every role shares one lifecycle (``piloop.agents.launcher``). A role differs only in its ``Role`` and in the content
of its brief, which this module composes from ``prompts/``.
"""

import shlex
import sys
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel
from sqlalchemy.orm import Session

from piloop.agents.scout import is_scout
from piloop.config import BRIEF_FILE, ProjectConfig, Track, config_of
from piloop.db.models import Project, Worker, WorkerKind

PROMPTS = Path(__file__).resolve().parent / 'prompts'
PROCEDURES = 'sql_procedures.md'
PILOOP_HINT = f'{shlex.quote(sys.executable)} -m piloop'
SQL_HINT = f'{PILOOP_HINT} sql'


class Trigger(StrEnum):
    """What makes the daemon request one more agent of a role. Checked for each project on each tick."""

    QUEUE = 'queue'
    """A ``ready`` idea and a free worker slot."""
    ANOMALY = 'anomaly'
    """An incident-class anomaly (``piloop.reports.overview``) that no agent of the role was given yet."""
    CRON = 'cron'
    """``interval`` seconds since the role's last routine row, one with no anomaly. 0 turns it off."""
    LOW_WATER = 'low_water'
    """Fewer runnable ideas than one per worker slot plus ``buffer``, and new results to read or the rest is over."""
    SEED = 'seed'
    """A seed filed after the role's last sweep started. Research develops it, however full the backlog is."""
    SCOUT = 'scout'
    """``[roles.research.scout] interval`` seconds since the last scout sweep started. Requests a scout sweep."""


class WatchdogMode(StrEnum):
    """What a watchdog sweep is for, set by what requested it. The brief adds ``prompts/watchdog_<mode>.md``."""

    INCIDENT = 'incident'
    """Requested for anomalies the daemon detected (``anomaly.reported`` events on its row): find their causes."""
    PROGRESS = 'progress'
    """Every other sweep (the timer, the campaign's stop, a request by hand): check that the campaign makes progress."""


class Role(BaseModel, frozen=True):
    """The part of an agent that changes with its role. ``Launcher.launch`` does everything else.

    Every role works in a detached worktree of the project repo. The worker gets its run's base candidate
    (``RunExecutor``). The sweeping roles get the head (``Scheduler._sweep``).
    """

    kind: WorkerKind
    name: str
    """The signature of its agents (``<name>#<worker id>``), and so the source of the ideas they file.

    It is the kind's name, except for the scout mode of the research role (``SCOUT``).
    """
    prompt: str
    """The file under ``piloop/agents/prompts/`` that starts the brief."""
    triggers: frozenset[Trigger]
    writes: frozenset[str]
    """What the role may write. The brief states it. The authorizer does not enforce it."""
    ideas: str
    """How this role gives work to the workers, as the context block of the brief says it.

    ``{piloop}`` is the CLI and ``{project}`` is the project's name.
    """


SEED_HINT = (
    '- File a seed: `{piloop} idea seed {project} "<hunch, one line>" --from "<pointers: ids, paths, numbers>"`'
    ' (`--base <candidate>` when it is about that tree): research checks it against the existing work and develops it'
    ' into an idea, or rejects it with the reason.'
)

ROLES: dict[WorkerKind, Role] = {
    WorkerKind.WORKER: Role(
        kind=WorkerKind.WORKER,
        name=WorkerKind.WORKER.value,
        prompt='worker.md',
        triggers=frozenset({Trigger.QUEUE}),
        writes=frozenset({'commits', 'comments(finding) on your idea', 'comments(note) on your run'}),
        ideas=(
            '- A hunch worth a run of its own goes in your finding, with pointers (ids, paths, the number): research'
            " reads the workers' findings. You file no ideas or seeds."
        ),
    ),
    WorkerKind.WATCHDOG: Role(
        kind=WorkerKind.WATCHDOG,
        name=WorkerKind.WATCHDOG.value,
        prompt='watchdog.md',
        triggers=frozenset({Trigger.ANOMALY, Trigger.CRON}),
        writes=frozenset(
            {
                'comments(escalation, finding, note)',
                'comments(lesson) on the project once it stopped',
                'seeds via idea seed',
            }
        ),
        ideas=SEED_HINT,
    ),
    WorkerKind.RESEARCH: Role(
        kind=WorkerKind.RESEARCH,
        name=WorkerKind.RESEARCH.value,
        prompt='research.md',
        triggers=frozenset({Trigger.SEED, Trigger.LOW_WATER, Trigger.SCOUT}),
        writes=frozenset(
            {
                'seeds via idea develop, or their status to rejected with a note',
                'ideas via idea add',
                'comments(finding, escalation) on the project',
                'comments(note) on an idea',
            }
        ),
        ideas=(
            '- Develop a seed: `{piloop} idea develop <seed id> --hypothesis "…" --evidence "<ids, paths>" --check'
            ' "<cheapest measurement that kills it, and the result that stops the run>" --metric "<what decides'
            ' success>" --effect "<expected size, in the metric\'s units, and why>" [--plan "…"] [--family "…"]'
            ' [--title "…"] [--base <candidate>]`.\n'
            '- Add an idea of your own: `{piloop} idea add {project} "<title>" --hypothesis "…" --plan "…" --family'
            ' "<mechanism>" --from "<the evidence, with ids>"` (`--help` for links, a base, a priority).'
        ),
    ),
}


SCOUT = Role(
    kind=WorkerKind.RESEARCH,
    name='scout',
    prompt='research_scout.md',
    triggers=frozenset({Trigger.SCOUT}),
    writes=frozenset(
        {
            'seeds via idea seed (at most the count under Context)',
            'techniques (the library), by INSERT',
            'one comments(finding) on the project',
        }
    ),
    ideas=(
        '- File a seed: `{piloop} idea seed {project} "<technique: its mechanism, one line>" --from "<link>; targets'
        ' <the measured loss it might reduce, with its size>; <its substance: a number, code, an ablation>"`: research'
        ' develops it into an idea or rejects it with the reason.'
        '\n- File a technique into the library: `{piloop} sql "INSERT INTO techniques (project_id, title, mechanism,'
        " url, created_by) VALUES (<project id>, '<technique>', '<mechanism and its substance, one line>', '<link>',"
        " '<your signature>')\"`."
    ),
)
"""The scout mode of the research role.

A research row requested with a ``SCOUT_EVENT`` (``piloop.agents.scout``) runs this brief and signs ``scout#<id>``. It
shares the research role's config table, rotation, concurrency and MCP servers.
"""


def role_of(s: Session, worker: Worker) -> Role:
    """Return the role of a row: its kind's role, or the scout mode for a research row requested as a scout."""
    return SCOUT if worker.kind is WorkerKind.RESEARCH and is_scout(s, worker.id) else ROLES[worker.kind]


def compose_brief(
    role: Role,
    project: Project,
    worker_id: int,
    *,
    reason: str,
    context: str,
    mode: WatchdogMode | None = None,
    track: Track | None = None,
    wake: str = '',
) -> str:
    """Compose a brief: the role prompt, the project brief, the role's addendum, the database procedures, the context.

    A watchdog also gets its mode's prompt after the role prompt. ``wake``, the lessons overview of the repo
    (``piloop.reports.wake``), follows the role prompt. A worker also gets its track's addendum after the role's
    addendum. With several tracks, the context lists them.
    """
    author = f'{role.name}#{worker_id}'
    repo = Path(project.path)
    cfg = config_of(project)
    addendum = cfg.role(role.kind).addendum
    parts = [
        (PROMPTS / role.prompt).read_text().rstrip(),
        (PROMPTS / f'{role.kind}_{mode}.md').read_text().rstrip() if mode is not None else '',
        wake.rstrip(),
        (repo / BRIEF_FILE).read_text().rstrip(),
        (repo / addendum).read_text().rstrip() if addendum else '',
        (repo / track.addendum).read_text().rstrip() if track is not None and track.addendum else '',
        (PROMPTS / PROCEDURES).read_text().rstrip(),
        '\n'.join(
            [
                '# Context',
                f'- You are worker#{worker_id}, the {role.name} for project#{project.id} ({project.name}).',
                f'- Project repo: {project.path} (every candidate commit and Entire checkpoint is in it).',
                f"- Sign what you write as '{author}' (comments.author, ideas.created_by).",
                f'- Trigger: {reason}',
                f'- SQL tool: `{SQL_HINT} "<sql>"`',
                *([tracks_line(cfg, track)] if cfg.multitrack else []),
                role.ideas.format(piloop=PILOOP_HINT, project=project.name),
                f'- You may write: {", ".join(sorted(role.writes))}.',
                context.rstrip(),
            ]
        ),
    ]
    return '\n\n'.join(p for p in parts if p) + '\n'


def tracks_line(cfg: ProjectConfig, track: Track | None) -> str:
    """Return the project's tracks in one line for a brief. Every idea and seed names one. A worker learns its own."""
    tracks = '; '.join(
        f'{t.name} (primary {t.primary.name}, {t.primary.direction} is better; share {t.share:g})'
        for t in cfg.all_tracks
    )
    mine = f' Your run is on track {track.name}.' if track is not None else ''
    return (
        f'- Tracks: {tracks}. Every idea and seed names its track (`--track <name>`); every keep must also hold the'
        f" other tracks' primaries within their margins and floors.{mine}"
    )
