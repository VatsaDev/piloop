"""The ``piloop`` command line. ``piloop serve`` runs the daemon. Every other command uses the database directly."""

import json
import os
import shlex
import sqlite3
import subprocess
import sys
import time
from collections.abc import Generator
from contextlib import closing, contextmanager
from datetime import timedelta
from functools import partial
from pathlib import Path
from typing import Annotated

import cyclopts
from rich import box
from rich.console import Console, Group
from rich.table import Table
from rich.text import Text
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from piloop import deprecated, git
from piloop.agents.roles import PROCEDURES, PROMPTS
from piloop.agents.scout import SCOUT_EVENT
from piloop.campaign import inbox
from piloop.campaign.ideas import Definition, NotASeed, develop, files_nothing, seeds_only
from piloop.campaign.project import (
    BLANK,
    EVALUATE_CONTRACT,
    EVALUATE_STUB,
    EXAMPLE_DATA,
    TEMPLATES,
    WORKSPACE,
    ProjectError,
    checked_out,
    create_project_repo,
    find,
    register,
    registered,
    stub_untouched,
    sync,
)
from piloop.cli import doctor as doctor_module, skills
from piloop.cli.upgrade import requirement, source
from piloop.cli.version import installed
from piloop.config import ConfigError, RuntimeKind, config_of
from piloop.daemon.main import Daemon
from piloop.daemon.serve import restart as restart_in_place, running_pid, service_unit, spawn_detached, terminate
from piloop.db import sql
from piloop.db.events import emit
from piloop.db.models import (
    Candidate,
    Comment,
    CommentKind,
    Event,
    Idea,
    IdeaLink,
    IdeaStatus,
    LinkKind,
    Project,
    ProjectStatus,
    TargetKind,
    Worker,
    WorkerKind,
    WorkerState,
    utcnow,
)
from piloop.db.session import Database, SchemaAhead, migrate, revision
from piloop.proc import child_env
from piloop.reports.overview import age, item_lines, overview
from piloop.reports.pack import renderables as pack_renderables
from piloop.runtime import NotSteerable
from piloop.runtime.factory import handle_for, runtime_for
from piloop.runtime.herdr import HERDR, HerdrError, focus_tab, session_socket
from piloop.runtime.subprocess import STDOUT
from piloop.scoring.ledger import ImportRefused, NoteRefused, follow_up, import_reference, note, track_of
from piloop.settings import settings
from piloop.text import cell
from piloop.watch.app import Watch

app = cyclopts.App(name='piloop', help='Run autoresearch campaigns with coding agents.', version=installed)
idea_app = cyclopts.App(name='idea', help='Propose work: file a seed for research to develop, or add a defined idea.')
app.command(idea_app)
candidate_app = cyclopts.App(
    name='candidate', help='Import a starting point, or keep a regressed result in view as a lead.'
)
app.command(candidate_app)
inbox_app = cyclopts.App(
    name='inbox', help='Read and acknowledge what the agents address to you: decisions, escalations and findings.'
)
app.command(inbox_app)
console = Console()

ProjectRef = Annotated[str, cyclopts.Parameter(help='The project name, or a path inside its repo.')]
WAIT_POLL_SECONDS = 1.0
"""How often ``piloop inbox wait`` reads the events feed. A read is one indexed query: a wait costs no model turns."""
PRIORITY_HELP = 'From 0 (urgent) to 4 (someday).'
HARNESS_HELP = 'Use this harness instead of the rotation.'
FAMILY_HELP = 'The mechanism family. Ideas that try the same mechanism share it.'
BASE_HELP = 'The candidate id to start from instead of the head. This makes a branch.'
TRACK_HELP = 'The track it works on. Required when piloop.toml has more than one track.'


@contextmanager
def _session() -> Generator[Session]:
    """Open and migrate the database, and yield one session for the command."""
    db = Database(settings.database_url, actor=settings.actor)
    _migrate(db)
    try:
        with db.session() as s:
            yield s
    finally:
        db.close()


def _project(s: Session, ref: str) -> Project:
    """Find the project that ``ref`` names, or exit with how to register it."""
    project = find(s, ref)
    if project is None:
        raise SystemExit(f'no project {ref!r}: register it with `piloop init <path>`')
    return project


def _projects(s: Session, ref: str | None) -> list[Project]:
    """The projects for a command without a ref: the one cwd is in, else the only active one, else every project."""
    if ref:
        return [_project(s, ref)]
    if project := checked_out(s, Path.cwd()):
        return [project]
    active = s.scalars(select(Project).where(Project.status == ProjectStatus.ACTIVE)).all()
    return list(active) if len(active) == 1 else list(s.scalars(select(Project)).all())


@app.command
def init(
    path: Annotated[Path, cyclopts.Parameter(help='The project directory.')] = Path(),
    /,
    *,
    example: Annotated[
        str | None,
        cyclopts.Parameter(help=f'Write a bundled example instead of the blank template: {", ".join(EXAMPLE_DATA)}.'),
    ] = None,
) -> None:
    """Make a directory a piloop project and register it.

    It is safe to run again. When needed, it runs `git init` and makes a baseline commit. When there is no
    piloop.toml, it writes piloop.toml, brief.md and evaluate.py from the template, and keeps the files that exist.
    While evaluate.py is still the template stub, it stops and says what to edit. Then it measures HEAD as the
    baseline candidate and registers the project. For a project that is registered already, it reads piloop.toml
    again.
    """
    if example is not None and example not in EXAMPLE_DATA:
        raise SystemExit(f'no example {example!r}; bundled: {", ".join(EXAMPLE_DATA)}')
    written = create_project_repo(path, TEMPLATES / example if example else BLANK, EXAMPLE_DATA.get(example or '', {}))
    path = path.resolve()
    for rel in written:
        console.print(f'wrote {rel}')
    if stub_untouched(path):
        console.print(EVALUATE_CONTRACT, style='dim', markup=False)
        console.print(
            f'next: {path / EVALUATE_STUB} is still the template stub. Make it measure this project and print the'
            f' METRIC lines that piloop.toml names. Describe the goal in brief.md. Then run `piloop init {path}` again.'
        )
        return
    with _session() as s:
        project = registered(s, path)
        if project is None:
            try:
                project, measurement = register(s, path)
            except ProjectError as e:
                raise SystemExit(str(e)) from e
            console.print(
                f'project [bold]{project.name}[/] #{project.id} registered; baseline candidate#{measurement.candidate_id}'
                f' {measurement.metrics}'
            )
            console.print(
                f'next: add ideas: `piloop idea add {project.name} "<title>"`; start the daemon: `piloop serve`'
            )
        else:
            stopped = project.status is ProjectStatus.STOPPED
            sync(s, project)
            s.commit()
            resumed = '; resumed: stop conditions changed' if stopped and project.status is ProjectStatus.ACTIVE else ''
            console.print(
                f'project {project.name} #{project.id} already registered; config re-read from piloop.toml{resumed}'
            )


@idea_app.command(name='add')
def idea_add(
    ref: ProjectRef,
    title: Annotated[str, cyclopts.Parameter(help='A short title for the idea.')],
    /,
    *,
    hypothesis: Annotated[
        str, cyclopts.Parameter(help='Why this should help, and by how much. Default: the title.', show_default=False)
    ] = '',
    plan: Annotated[str | None, cyclopts.Parameter(help='The steps for the worker.')] = None,
    from_: Annotated[
        str | None,
        cyclopts.Parameter(
            name='--from', help='What prompted the idea, with ids or a URL. Example: "finding comment#881: …".'
        ),
    ] = None,
    family: Annotated[str | None, cyclopts.Parameter(help=FAMILY_HELP)] = None,
    priority: Annotated[int, cyclopts.Parameter(help=PRIORITY_HELP)] = 2,
    harness: Annotated[str | None, cyclopts.Parameter(help=HARNESS_HELP)] = None,
    base: Annotated[int | None, cyclopts.Parameter(help=BASE_HELP)] = None,
    proposed: Annotated[
        bool, cyclopts.Parameter(help='Wait for a review before the daemon schedules the idea.')
    ] = False,
    track: Annotated[str | None, cyclopts.Parameter(help=TRACK_HELP)] = None,
    after: Annotated[
        tuple[int, ...],
        cyclopts.Parameter(help='An idea id to wait for. Repeatable. The idea runs from the head when all are done.'),
    ] = (),
    supersedes: Annotated[
        tuple[int, ...],
        cyclopts.Parameter(
            help='An idea id that this idea refines. Repeatable. The daemon rejects each one that never ran.'
        ),
    ] = (),
) -> None:
    """Add a defined idea, which the daemon schedules in priority order once it is ready.

    Research, the orchestrator and humans add ideas. A watchdog files a seed instead (`piloop idea seed`). A worker
    files neither. The idea is signed as PILOOP_ACTOR.
    """
    _refuse_worker()
    if seeds_only(settings.actor):
        raise SystemExit(
            f'{settings.actor} files seeds, not ideas: `piloop idea seed {ref} "<hunch>" --from "<pointers>"`.'
            ' Research develops each seed with the whole campaign in view.'
        )
    with _session() as s:
        project = _project(s, ref)
        _check_base(s, project, base)
        idea = Idea(
            project_id=project.id,
            track=_track(project, track),
            title=title,
            hypothesis=hypothesis or title,
            plan=plan,
            provenance=from_,
            family=family,
            priority=priority,
            harness=harness,
            base_candidate_id=base,
            status=IdeaStatus.PROPOSED if proposed else IdeaStatus.READY,
            created_by=settings.actor,
            links=[
                *(IdeaLink(target_id=t, kind=LinkKind.AFTER) for t in after),
                *(IdeaLink(target_id=t, kind=LinkKind.SUPERSEDES) for t in supersedes),
            ],
        )
        s.add(idea)
        s.flush()
        emit(s, 'idea.created', TargetKind.IDEA, idea.id, actor=settings.actor)
        s.commit()
        console.print(f'idea#{idea.id} {idea.status} p{priority}: {title}', markup=False)


@idea_app.command(name='seed')
def idea_seed(
    ref: ProjectRef,
    hunch: Annotated[str, cyclopts.Parameter(help='The hunch, in one line: what might move the metric.')],
    /,
    *,
    from_: Annotated[
        str,
        cyclopts.Parameter(
            name='--from',
            help='Pointers to what prompted the hunch: ids, paths or numbers. Example: "run#7 finding comment#81: …".',
        ),
    ],
    base: Annotated[
        int | None, cyclopts.Parameter(help='The candidate id to start from, when the hunch is about that tree.')
    ] = None,
    track: Annotated[str | None, cyclopts.Parameter(help=TRACK_HELP)] = None,
) -> None:
    """File a seed: a hunch with pointers, for research to develop into a defined idea or to reject.

    Every role except the worker can file a seed. Research reads the earlier work and checks for duplicates and
    negative results. Then it defines the seed (`piloop idea develop`) or rejects it with the reason. The seed is
    signed as PILOOP_ACTOR.
    """
    _refuse_worker()
    if not from_.strip():
        raise SystemExit('a seed needs pointers: --from "<ids, paths, numbers>"')
    with _session() as s:
        project = _project(s, ref)
        _check_base(s, project, base)
        idea = Idea(
            project_id=project.id,
            track=_track(project, track),
            title=hunch,
            hypothesis=hunch,
            provenance=from_,
            base_candidate_id=base,
            status=IdeaStatus.SEED,
            created_by=settings.actor,
        )
        s.add(idea)
        s.flush()
        emit(s, 'idea.created', TargetKind.IDEA, idea.id, actor=settings.actor)
        s.commit()
        console.print(f'idea#{idea.id} seed: {hunch}; research develops it next', markup=False)


@idea_app.command(name='develop')
def idea_develop(
    idea_id: Annotated[int, cyclopts.Parameter(help='The id of the seed.')],
    /,
    *,
    hypothesis: Annotated[str, cyclopts.Parameter(help='The mechanism, and why it should move the metric.')],
    evidence: Annotated[
        str, cyclopts.Parameter(help='What supports it, with ids and paths: runs, findings, candidates, loss tables.')
    ],
    check: Annotated[
        str,
        cyclopts.Parameter(help='The cheapest measurement that can prove it wrong, and the result that stops the run.'),
    ],
    metric: Annotated[
        str, cyclopts.Parameter(help='The metric that decides success: the primary metric, or the slice it targets.')
    ],
    effect: Annotated[str, cyclopts.Parameter(help="The expected size of the effect in the metric's units, and why.")],
    plan: Annotated[str | None, cyclopts.Parameter(help='More steps for the worker, after the check.')] = None,
    title: Annotated[str | None, cyclopts.Parameter(help="A clearer title. Default: the seed's title.")] = None,
    family: Annotated[str | None, cyclopts.Parameter(help=FAMILY_HELP)] = None,
    base: Annotated[int | None, cyclopts.Parameter(help=BASE_HELP)] = None,
    priority: Annotated[int | None, cyclopts.Parameter(help=f"{PRIORITY_HELP} Default: the seed's priority.")] = None,
    proposed: Annotated[
        bool, cyclopts.Parameter(help='Wait for a human review before the daemon schedules the idea.')
    ] = False,
) -> None:
    """Develop a seed into a defined idea that the daemon can schedule.

    The hypothesis, the evidence, the check, the metric and the effect are all required, because the worker needs
    each one. Reject a seed that does not hold up instead, with a note that names the earlier work (see
    `piloop schema`). The idea is signed as PILOOP_ACTOR.
    """
    _refuse_worker()
    if seeds_only(settings.actor):
        raise SystemExit(f'{settings.actor} files seeds; research develops them')
    with _session() as s:
        idea = s.get(Idea, idea_id)
        if idea is None:
            raise SystemExit(f'no idea#{idea_id}')
        _check_base(s, idea.project, base)
        definition = Definition(hypothesis, evidence, check, metric, effect, plan)
        try:
            develop(idea, definition, title=title, family=family, base=base, priority=priority, proposed=proposed)
        except (NotASeed, ValueError) as e:
            raise SystemExit(str(e)) from e
        s.commit()
        console.print(f'idea#{idea.id} {idea.status} p{idea.priority}: {idea.title}', markup=False)


def _refuse_worker() -> None:
    if files_nothing(settings.actor):
        raise SystemExit(
            f"{settings.actor} files no ideas or seeds: say the hunch in your run's finding, with pointers."
            ' Research reads the findings of the workers.'
        )


def _track(project: Project, track: str | None) -> str:
    """The track for an idea: ``track`` when the config names it, else the only track. Exits otherwise."""
    tracks = [t.name for t in config_of(project).all_tracks]
    if track is None and len(tracks) == 1:
        return tracks[0]
    if track not in tracks:
        given = 'name one with --track' if track is None else f'there is no track {track!r}'
        raise SystemExit(f'{project.name} has tracks {", ".join(tracks)}: {given}')
    return track


def _check_base(s: Session, project: Project, base: int | None) -> None:
    if base is not None and s.scalar(select(Candidate.project_id).where(Candidate.id == base)) != project.id:
        raise SystemExit(f'no candidate#{base} in project {project.name}')


@candidate_app.command(name='import')
def candidate_import(
    ref: ProjectRef,
    commit: Annotated[str, cyclopts.Parameter(help="A commit, branch or tag in the project's repo.")],
    /,
    *,
    summary: Annotated[
        str | None, cyclopts.Parameter(help="A summary of the candidate. Default: the commit's subject line.")
    ] = None,
) -> None:
    """Register the tree of a commit as a reference candidate: a starting point for an idea.

    The daemon measures it with the current evaluator. A reference is never kept, and it is never the head or a bar.
    It does not count in the kept total, the sparkline or a stop condition. To import a candidate of another project,
    fetch its `commit_sha` into this repo first (`git fetch <its repo> <sha>`).
    """
    with _session() as s:
        project = _project(s, ref)
        cfg = config_of(project)
        try:
            candidate = import_reference(
                s, project, cfg, git.open_repo(Path(project.path)), commit, summary=summary, actor=settings.actor
            )
        except ImportRefused as e:
            raise SystemExit(str(e)) from e
        s.commit()
        console.print(
            f'candidate#{candidate.id} reference: {candidate.summary}; the daemon measures it; start an idea from it'
            f' with `piloop idea add {project.name} "<title>" --base {candidate.id}`',
            markup=False,
        )


@candidate_app.command(name='note')
def candidate_note(
    candidate_id: Annotated[
        int, cyclopts.Parameter(help='The candidate: a measured discard, or one marked interesting already.')
    ],
    reason: Annotated[str, cyclopts.Parameter(help='Why the tree is worth starting from although it regressed.')],
    /,
    *,
    follow_ups: Annotated[
        tuple[str, ...],
        cyclopts.Parameter(
            name='--follow-up', help='A seed pinned to the candidate, for research to develop. Repeatable.'
        ),
    ] = (),
    track: Annotated[
        str | None,
        cyclopts.Parameter(
            help="The track of the follow-ups. Default: the track of the candidate's run, else the first track."
        ),
    ] = None,
) -> None:
    """Keep a regressed result in view as a lead, with follow-up seeds pinned to it.

    The verdict becomes `interesting`, with the reason `note: <reason>`. The candidate is not kept and sets no bar,
    but it stays listed and ideas can start from it. Each `--follow-up` is a seed whose `base_candidate_id` is the
    candidate. A worker files no follow-ups. When its candidate does not exist yet, a worker ends its commit message
    with a `Piloop-Note: <reason>` trailer instead, and the daemon applies the note when it judges the commit.
    """
    if follow_ups:
        _refuse_worker()
    with _session() as s:
        candidate = s.get(Candidate, candidate_id)
        if candidate is None:
            raise SystemExit(f'no candidate#{candidate_id}')
        try:
            note(candidate, reason, settings.actor)
        except NoteRefused as e:
            raise SystemExit(str(e)) from e
        project = s.get_one(Project, candidate.project_id)
        on = _track(project, track) if track is not None else track_of(config_of(project), candidate).name
        ideas = follow_up(s, candidate, follow_ups, reason, settings.actor, track=on)
        s.commit()
        console.print(f'candidate#{candidate.id} interesting: {candidate.verdict_reason}', markup=False)
        for idea in ideas:
            console.print(f'idea#{idea.id} seed, pinned to candidate#{candidate.id}: {idea.title}', markup=False)


KindFilter = Annotated[
    tuple[CommentKind, ...], cyclopts.Parameter(name='--kind', help='Only items of this kind. Repeatable.')
]


@inbox_app.default
def inbox_list(
    ref: ProjectRef | None = None,
    /,
    *,
    kind: KindFilter = (),
    all_: Annotated[bool, cyclopts.Parameter(name='--all', help='Show acknowledged items too.')] = False,
) -> None:
    """List the unacknowledged inbox items, most urgent first, then the seeds that wait for research.

    Priority 0 is urgent and 4 is someday. Without a project, it lists the items of every project.
    """
    with _session() as s:
        project_id = _project(s, ref).id if ref else None
        rows = inbox.items(s, project_id=project_id, kinds=kind, acked=all_)
        now = utcnow()
        table = Table('id', 'p', 'kind', 'project', 'on', 'by', 'age', *(['acked'] if all_ else []), 'first line')
        for c, project in rows:
            acked = [f'{c.acked_by}, {age(c.acked_at, now)} ago' if c.acked_at else '—'] if all_ else []
            table.add_row(
                str(c.id),
                str(c.priority),
                str(c.kind),
                project.name if project else '—',
                f'{c.target_kind}#{c.target_id}',
                c.author,
                age(c.created_at, now),
                *acked,
                Text(cell(c.body)),
            )
        waiting = inbox.seeds(s, project_id=project_id)
        seeds = [inbox.seed_line(idea, project) for idea, project in waiting]
    if rows:
        console.print(table)
    console.print(
        f'{len(rows)} item(s){"" if all_ else " unacknowledged"}; `piloop inbox show <id>`, `piloop inbox ack <id>`'
    )
    if seeds:
        console.print(f'{len(seeds)} seed(s) waiting for research:', markup=False)
        console.print('\n'.join(seeds), markup=False, highlight=False, soft_wrap=True)


@inbox_app.command(name='show')
def inbox_show(comment_id: Annotated[int, cyclopts.Parameter(help='The id of the item.')], /) -> None:
    """Show one item in full, then the comments that it cites as `comment#<id>`, that cite it, or reply to it."""
    with _session() as s:
        c = s.get(Comment, comment_id)
        if c is None:
            raise SystemExit(f'no comment#{comment_id}')
        title, lines = item_lines(s, c, utcnow())
    console.print(title, style='bold', markup=False)
    console.print('\n'.join(lines), markup=False, highlight=False)


@inbox_app.command(name='wait')
def inbox_wait(
    ref: ProjectRef | None = None,
    /,
    *,
    timeout: Annotated[float, cyclopts.Parameter(help='Seconds to wait before it returns with nothing new.')] = 1800,
    after: Annotated[
        int | None,
        cyclopts.Parameter(help='The event id to start after: the cursor that the last wait printed. Default: now.'),
    ] = None,
) -> None:
    """Wait until something needs the orchestrator, print it one line per item, then print the command to wait again.

    These events end the wait: a new unacknowledged inbox item that someone else wrote, a project that the daemon
    stopped, an anomaly that the daemon gave to a watchdog, and the daemon's verdict on a measured candidate. Your own
    decisions, as PILOOP_ACTOR, do not end it. At --timeout it returns with nothing. Set the timeout of your shell tool
    higher than --timeout. Without a project, it waits for every project.
    """
    deadline = time.monotonic() + timeout
    with _session() as s:
        project_id = _project(s, ref).id if ref else None
        cursor = after if after is not None else s.scalar(select(func.max(Event.id))) or 0
        while True:
            cursor, lines = inbox.news(s, cursor, project_id=project_id, me=settings.actor)
            s.rollback()  # end the read transaction: a WAL reader that stays open while it sleeps blocks checkpoints
            if lines or time.monotonic() >= deadline:
                break
            time.sleep(WAIT_POLL_SECONDS)
    again = shlex.join(['piloop', 'inbox', 'wait', *([ref] if ref else []), '--after', str(cursor)])
    console.print(
        '\n'.join(lines) or f'nothing new in {timedelta(seconds=round(timeout))}', markup=False, soft_wrap=True
    )
    console.print(f'next: {again}', markup=False, soft_wrap=True)


@inbox_app.command(name='ack')
def inbox_ack(
    *ids: Annotated[int, cyclopts.Parameter(help='The ids of the items.')],
    up_to: Annotated[
        int | None,
        cyclopts.Parameter(
            help='Acknowledge every unacknowledged item up to this id instead. --project and --kind narrow it.'
        ),
    ] = None,
    project: Annotated[str | None, cyclopts.Parameter(help='With --up-to: only the items of this project.')] = None,
    kind: KindFilter = (),
) -> None:
    """Acknowledge items to say that you have seen them.

    Nothing waits for an acknowledgement. To change a decision, write a `decision` comment on its target.
    """
    if bool(ids) == (up_to is not None):
        raise SystemExit('name the ids to acknowledge, or --up-to <id>')
    with _session() as s:
        if up_to is not None:
            project_id = _project(s, project).id if project else None
            ids = tuple(c.id for c, _ in inbox.items(s, project_id=project_id, kinds=kind) if c.id <= up_to)
        lines = inbox.ack(s, ids, settings.actor)
    console.print('\n'.join(lines) or 'nothing to acknowledge', markup=False)


@app.command
def launch(
    ref: ProjectRef,
    role: Annotated[WorkerKind, cyclopts.Parameter(help='The role of the agent.')],
    /,
    *,
    harness: Annotated[str | None, cyclopts.Parameter(help=HARNESS_HELP)] = None,
    model: Annotated[str | None, cyclopts.Parameter(help='The model id, as the harness names it.')] = None,
    reason: Annotated[
        str, cyclopts.Parameter(help="Why you request the agent. The agent's brief includes it.")
    ] = 'requested from the CLI',
    scout: Annotated[
        bool, cyclopts.Parameter(help='Research only: run a scout sweep that looks for outside techniques.')
    ] = False,
) -> None:
    """Ask the daemon for one more agent of a role.

    The daemon launches it on its next tick, within the role's `max_concurrent`.
    """
    if scout and role is not WorkerKind.RESEARCH:
        raise SystemExit('--scout is a mode of the research role: `piloop launch <project> research --scout`')
    with _session() as s:
        project = _project(s, ref)
        worker = Worker(kind=role, project_id=project.id, harness=harness, model=model, reason=reason)
        s.add(worker)
        s.flush()
        if scout:
            emit(s, SCOUT_EVENT, TargetKind.WORKER, worker.id, actor=settings.actor)
        s.commit()
        console.print(f'worker#{worker.id} {role}{" (scout)" if scout else ""} requested for {project.name}: {reason}')


@app.command(name='sql')
def sql_cmd(statement: Annotated[str, cyclopts.Parameter(help='One SQL statement.')], /) -> None:
    """Run one SQL statement on the piloop database.

    Statements that change the schema are denied.
    """
    database = settings.home / 'piloop.db'
    with closing(sql.connect(database, actor=settings.actor)) as con:
        try:
            result = sql.run(con, statement)
        except sqlite3.Error as e:
            holder = sql.lock_holder(e, database)
            raise SystemExit(f'sql error: {e}' if holder is None else f'sql error: {e} ({holder})') from e
    if result.columns:
        table = Table(*result.columns, show_lines=False)
        for row in result.rows:
            # Text, not str: a cell is data, so '[voting]' from snippet() or a '[wip]' title is not rich markup
            table.add_row(*(Text(_cell(v)) for v in row))
        console.print(table)
        if result.truncated:
            console.print(f'[dim]truncated to {sql.ROW_LIMIT} rows[/]')
    else:
        console.print(f'{result.changes} row(s) changed, last insert id {result.last_insert_rowid}')


@app.command
def schema() -> None:
    """Print the guide to the database: what `piloop sql` accepts, the conventions, and every table."""
    text = f'{(PROMPTS / PROCEDURES).read_text().strip()}\n\n## Schema\n\n```sql\n{sql.schema_ddl()}\n```'
    console.print(text, markup=False, highlight=False, soft_wrap=True)


def _cell(value: object) -> str:
    return '' if value is None else json.dumps(value) if isinstance(value, dict | list) else str(value)


@app.command
def history(ref: ProjectRef | None = None, /) -> None:
    """Print the research pack: what a research sweep reads before it proposes ideas.

    It shows the metrics, the ideas by source, the analysis artifacts, what was tried per mechanism family, every idea
    with its outcome, the kept lineage, the findings, the digests and the open escalations. Without a project, it uses
    the project that the current directory is in, else the only active project, else every project.
    """
    with _session() as s:
        for project in _projects(s, ref):
            console.rule(f'[bold]{project.name}[/] #{project.id}  {project.path}')
            workspace = settings.projects_dir / str(project.id) / WORKSPACE
            console.print(
                Group(*pack_renderables(s, project, config_of(project), table_box=box.SIMPLE_HEAD, workspace=workspace))
            )


@app.command
def status(ref: ProjectRef | None = None, /) -> None:
    """Print the state of the daemon, then an overview of each project.

    The overview is what a sweep reads in its brief, without the tool guide. It shows the head, the tracks, the
    trajectory, the progress against [stop], the open runs, the queue, the live agents with their last line, the
    recent candidates, the anomalies, the inbox, the latest decisions and the recent events. Without a project, it
    uses the project that the current directory is in, else the only active project, else every project.
    """
    console.print(_daemon_line())
    with _session() as s:
        for project in _projects(s, ref):
            console.rule(f'[bold]{project.name}[/] #{project.id}  {project.path}')
            text = overview(s, project, config_of(project), utcnow(), partial(_jobs, s), tools=False)
            console.print(text, markup=False, highlight=False, soft_wrap=True)


def _jobs(s: Session, worker: Worker) -> list[str]:
    """What a running worker has in flight, for the ``silent`` anomaly. Empty when its runtime cannot say."""
    try:
        return runtime_for(s, worker, settings.home).jobs(handle_for(worker))
    except Exception:  # herdr is missing or the pane is gone: status still prints, without this anomaly
        return []


@app.command(name='watch')
def watch_cmd(ref: ProjectRef | None = None, /) -> None:
    """Show the campaign on one screen, with keys to act on it.

    It shows every project, or one project when you name it. A project has four panels (inbox, agents, ideas and
    candidates, on the keys 1 to 4) and the detail of the selected row. `?` lists the keys of the focused panel, `/`
    filters it, esc goes back to every project and q quits. Writes are signed as PILOOP_ACTOR.
    """
    if not sys.stdin.isatty():
        raise SystemExit('piloop watch reads keys from a terminal; `piloop status` prints the same once')
    db = Database(settings.database_url, actor=settings.actor)
    _migrate(db)
    try:
        Watch(db, settings.home / 'piloop.db', ref, settings.actor).run()
    finally:
        db.close()


@app.command
def prompt(
    worker_id: Annotated[int, cyclopts.Parameter(help='The id of the agent, as `piloop status` lists it.')],
    text: Annotated[str, cyclopts.Parameter(help='The message.')],
    /,
) -> None:
    """Send a message to a running agent of any role (Herdr runtime only)."""
    with _session() as s:
        worker = s.scalars(select(Worker).where(Worker.id == worker_id, Worker.state == WorkerState.RUNNING)).first()
        if worker is None:
            raise SystemExit(f'worker#{worker_id} is not running')
        handle = handle_for(worker)
        runtime = runtime_for(s, worker, settings.home)
    try:
        runtime.prompt(handle, text)
    except NotSteerable as e:
        raise SystemExit(str(e)) from e
    console.print(f'prompted worker#{worker_id}')


@app.command
def attach(worker_id: Annotated[int, cyclopts.Parameter(help='The id of the agent.')], /) -> None:
    """Focus the tab of a running agent in Herdr, and attach this terminal to the piloop Herdr session."""
    with _session() as s:
        worker = s.scalars(select(Worker).where(Worker.id == worker_id, Worker.state == WorkerState.RUNNING)).first()
        if worker is None:
            raise SystemExit(f'worker#{worker_id} is not running')
        if worker.runtime != RuntimeKind.HERDR:
            console.print(
                f'worker#{worker.id} is headless ({worker.runtime} runtime): {worker.log_dir}/{STDOUT} while it runs,'
                f' transcript.txt once it ends'
            )
            return
        tab_id = str(handle_for(worker).data['tab_id'])
    try:
        focus_tab(tab_id)
        _attach(tab_id)
    except HerdrError as e:
        raise SystemExit(str(e)) from e


def _attach(tab_id: str) -> None:
    """Replace this process with the Herdr client of the piloop session, unless this terminal is in it already."""
    if not sys.stdout.isatty():
        console.print(
            f'not a terminal; would attach to Herdr session {settings.herdr_session} with tab {tab_id} focused'
        )
        return
    if settings.herdr_socket_path == session_socket():
        console.print(f'focused tab {tab_id}')
        return
    os.execvp(HERDR, [HERDR, '--session', settings.herdr_session])


@app.command
def serve(
    *,
    detach: Annotated[
        bool, cyclopts.Parameter(help='Start the daemon in the background. Its output goes to serve.log.')
    ] = False,
) -> None:
    """Run the daemon, which fires the triggers, launches the agents and reaps the agents that end.

    Only one daemon runs per PILOOP_HOME. The file serve.pid holds its lock.
    """
    if detach:
        pid = spawn_detached(settings)
        console.print(f'daemon started: pid {pid}, log {settings.log_file}')
        return
    try:
        daemon = Daemon(settings)
    except SchemaAhead as e:
        raise SystemExit(str(e)) from e
    daemon.serve()


@app.command
def stop(*, timeout: Annotated[float, cyclopts.Parameter(help='Seconds to wait for the daemon to exit.')] = 30) -> None:
    """Stop the running daemon with SIGTERM, and wait for it to exit.

    The daemon kills the agents that are running, and their runs fail.
    """
    pid = running_pid(settings)
    if pid is None:
        raise SystemExit('no daemon is running')
    if not terminate(settings, pid, timeout):
        raise SystemExit(f'daemon pid {pid} did not exit within {timeout:g}s')
    console.print(f'daemon pid {pid} stopped')


@app.command
def restart(
    *, timeout: Annotated[float, cyclopts.Parameter(help='Seconds to wait for the daemon to start again.')] = 30
) -> None:
    """Restart the running daemon on the installed code, in the same process.

    Running agents keep running. The daemon stops its ticks, then the same process execs `piloop serve`. The new start
    re-attaches to every agent and finishes their runs. The process keeps its pid and its environment, so systemd,
    launchd or the terminal that started it still holds it. To change the environment of a service unit, use the
    restart of the service manager.
    """
    pid = running_pid(settings)
    if pid is None:
        raise SystemExit('no daemon is running')
    console.print(f'daemon pid {_restart(pid, timeout)} restarted')


def _restart(pid: int, timeout: float) -> int:
    new = restart_in_place(settings, pid, timeout)
    if new is None:
        raise SystemExit(f'daemon pid {pid} did not restart within {timeout:g}s')
    return new


@app.command
def upgrade(
    *, timeout: Annotated[float, cyclopts.Parameter(help='Seconds to wait for a running daemon to restart.')] = 30
) -> None:
    """Upgrade the CLI, the skills and the database to the latest version, and restart a running daemon.

    It reads the source that uv installed the CLI from (the uv receipt), and runs
    `uv tool upgrade --reinstall-package piloop piloop`. Then the new code runs `piloop install-skills`. A running
    daemon restarts in place, as with `piloop restart`: its agents keep running, and the new code migrates the
    database and re-attaches to them. When no daemon runs, the new code migrates the database and the daemon stays
    stopped. A checkout installed with `uv sync` is not a uv tool: upgrade it with `git pull`.
    """
    req = requirement(settings.uv_tool_dir)
    before = installed().removeprefix('piloop ')
    console.print(f'CLI: {before}, installed by uv from {source(req)}')
    # --reinstall-package: without it, uv serves a directory install (PILOOP_SRC=<checkout>) from its cached build
    _run(['uv', 'tool', 'upgrade', '--reinstall-package', 'piloop', 'piloop'])
    after = _new_binary('--version').stdout.strip()
    console.print(f'CLI: {before} → {after.removeprefix("piloop ")}')
    console.print(_new_binary('install-skills').stdout.rstrip(), markup=False, highlight=False, soft_wrap=True)
    pid = running_pid(settings)
    if pid is not None:
        console.print(f'daemon: restarting pid {pid}; its running agents keep going and the new code re-attaches')
        console.print(f'daemon: pid {_restart(pid, timeout)} runs the new code')
    # this process runs the old code and knows only the old migrations, so the new code (or daemon) migrates
    console.print(_new_binary('migrate').stdout.rstrip())
    if pid is None:
        console.print('daemon: not running, left stopped')
    checked = _new_binary('doctor', check=False)
    # its stderr has the deprecation warnings of the piloop.toml files it loaded; they matter most after an upgrade
    console.print((checked.stdout + checked.stderr).rstrip(), markup=False, highlight=False, soft_wrap=True)
    if checked.returncode:
        raise SystemExit('piloop doctor found problems after the upgrade: each FAIL above says how to fix it')


def _new_binary(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run the newly installed piloop. uv rebuilds the venv in place, so ``sys.executable`` now imports the new code.

    With ``check``, a failure of the child exits this command too. For example, the database can still be ahead of
    the code that the upgrade fetched. The child sees the same homes as this process.
    """
    homes = {
        'PILOOP_HOME': settings.home,
        'UV_TOOL_DIR': settings.uv_tool_dir,
        'CLAUDE_CONFIG_DIR': settings.claude_config_dir,
        'PILOOP_AGENT_SKILLS_DIR': settings.agent_skills_dir,
    }
    try:
        return subprocess.run(
            [sys.executable, '-m', 'piloop', *args],
            capture_output=True,
            text=True,
            check=check,
            env=child_env() | {k: str(v) for k, v in homes.items()},
        )
    except subprocess.CalledProcessError as e:
        raise SystemExit(
            f'piloop {shlex.join(args)} on the new code failed with exit {e.returncode}: {e.stderr.strip()}'
        ) from e


@app.command(name='doctor')
def doctor_cmd() -> None:
    """Check the install without writing anything, and say how to fix each failure.

    It checks that the daemon runs the installed code, the database is at head, each piloop.toml loads, the MCP
    servers can start, the harnesses are on PATH and have the skills of this CLI, and Herdr answers.
    """
    results = doctor_module.checks(settings)
    console.print('\n'.join(doctor_module.report(results)), markup=False, highlight=False, soft_wrap=True)
    if any(c.fix is not None for c in results):
        raise SystemExit(1)


@app.command(name='install-skills')
def install_skills() -> None:
    """Install the orchestrator skills for every harness on PATH.

    Claude Code reads them from ~/.claude/skills (or $CLAUDE_CONFIG_DIR/skills). Codex, pi, Gemini CLI and Cursor read
    them from ~/.agents/skills. It replaces piloop's own skills there and nothing else. A running harness loads them
    at its next session. The installer and `piloop upgrade` run this command.
    """
    found = skills.targets(settings)
    for target, harnesses in found.items():
        skills.install(target)
        line = f'skills: {", ".join(skills.bundled())} → {target} ({", ".join(harnesses)})'
        console.print(line, markup=False, highlight=False, soft_wrap=True)
    if not found:
        console.print(f'skills: none of {", ".join(skills.SKILL_DIRS)} is on PATH, nothing installed', soft_wrap=True)


@app.command(name='migrate')
def migrate_cmd() -> None:
    """Migrate the database to the schema of this piloop, and print what changed.

    Every other command migrates without a message when it starts.
    """
    db = Database(settings.database_url, actor=settings.actor)
    try:
        old = revision(db.engine)
        _migrate(db)
        new = revision(db.engine)
    finally:
        db.close()
    console.print(f'schema: {new}, current' if old == new else f'schema: {old or "none"} → {new}')


def _migrate(db: Database) -> None:
    try:
        migrate(db.engine)
    except SchemaAhead as e:
        db.close()
        raise SystemExit(str(e)) from e


def _run(argv: list[str]) -> None:
    """Run a tool with its output on this terminal. Exit with an error when it fails."""
    console.print(f'$ {shlex.join(argv)}', style='dim')
    try:
        rc = subprocess.run(argv, env=child_env()).returncode
    except FileNotFoundError as e:
        raise SystemExit(f'{argv[0]} is not on PATH') from e
    if rc:
        raise SystemExit(f'{shlex.join(argv)} failed with exit {rc}')


@app.command(name='service-unit')
def service_unit_cmd() -> None:
    """Print a systemd user unit (a launchd plist on macOS) that keeps `piloop serve` running.

    It prints the unit only, and installs nothing.
    """
    console.print(service_unit(settings), end='', markup=False, highlight=False, soft_wrap=True)


def _daemon_line() -> str:
    pid = running_pid(settings)
    if pid is None:
        return 'daemon: not running'
    up = timedelta(seconds=int(time.time() - settings.pid_file.stat().st_mtime))
    return f'daemon: pid {pid}, up {up}'


def main() -> None:
    """Run the CLI. An error prints one message. PILOOP_DEBUG=1 shows the traceback of an unexpected error."""
    deprecated.show_in_cli()
    try:
        app()
    except ConfigError as e:
        raise SystemExit(str(e)) from e
    except Exception as e:
        if settings.debug:
            raise
        raise SystemExit(
            f'piloop: {type(e).__name__}: {e}\nrun with PILOOP_DEBUG=1 for the traceback; `piloop doctor` checks the install'
        ) from e
