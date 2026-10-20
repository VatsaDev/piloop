"""The commands about projects: ``init``, ``status``, ``history``, ``watch``, ``pause``, ``resume``, ``project stop``."""

import sys
from functools import partial
from pathlib import Path
from typing import Annotated

import cyclopts
from rich import box
from rich.console import Group
from sqlalchemy.orm import Session

from piloop.campaign import ops
from piloop.campaign.memory import sync_documents
from piloop.campaign.project import (
    BLANK,
    EVALUATE_CONTRACT,
    EVALUATE_STUB,
    EXAMPLE_DATA,
    TEMPLATES,
    ProjectError,
    create_project_repo,
    register,
    registered,
    stub_untouched,
    sync,
    workspace_dir,
)
from piloop.campaign.stop import request_stop
from piloop.cli import common
from piloop.cli.common import ProjectRef
from piloop.cli.daemon import daemon_line
from piloop.config import config_of
from piloop.db.models import ProjectStatus, Worker, utcnow
from piloop.db.session import Database
from piloop.reports.overview import overview, paused
from piloop.reports.pack import renderables as pack_renderables
from piloop.runtime.factory import handle_for, runtime_for
from piloop.watch.app import Watch

app = cyclopts.App(name='project', help='Act on a campaign as a whole.')


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
        common.console.print(f'wrote {rel}')
    if stub_untouched(path):
        common.console.print(EVALUATE_CONTRACT, style='dim', markup=False)
        common.console.print(
            f'next: {path / EVALUATE_STUB} is still the template stub. Make it measure this project and print the'
            f' METRIC lines that piloop.toml names. Describe the goal in brief.md. Then run `piloop init {path}` again.'
        )
        return
    with common.session() as s:
        project = registered(s, path)
        if project is None:
            try:
                project, measurement = register(s, path)
            except ProjectError as e:
                raise SystemExit(str(e)) from e
            common.console.print(
                f'project [bold]{project.name}[/] #{project.id} registered; baseline candidate#{measurement.candidate_id}'
                f' {measurement.metrics}'
            )
            common.console.print(
                f'next: add ideas: `piloop idea add {project.name} "<title>"`; start the daemon: `piloop serve`'
            )
        else:
            stopped = project.status is ProjectStatus.STOPPED
            sync(s, project)
            s.commit()
            resumed = '; resumed: stop conditions changed' if stopped and project.status is ProjectStatus.ACTIVE else ''
            common.console.print(
                f'project {project.name} #{project.id} already registered; config re-read from piloop.toml{resumed}'
            )
        synced = sync_documents(s, project, config_of(project), common.settings.actor)
        s.commit()
        if synced.added or synced.unchanged or synced.skipped:
            common.console.print(synced.line(), markup=False, highlight=False, soft_wrap=True)


def history(ref: ProjectRef | None = None, /) -> None:
    """Print the research pack: what a research sweep reads before it proposes ideas.

    It shows the metrics, the ideas by source, the analysis artifacts, what was tried per mechanism family, the latest
    60 ideas with their outcomes, the kept lineage, the findings, the digests and the open escalations. Without a
    project, it uses the project that the current directory is in, else the only active project, else every project.
    """
    with common.session() as s:
        for project in common.projects(s, ref):
            common.console.rule(f'[bold]{project.name}[/] #{project.id}  {project.path}')
            workspace = workspace_dir(common.settings.home, project.id)
            common.console.print(
                Group(*pack_renderables(s, project, config_of(project), table_box=box.SIMPLE_HEAD, workspace=workspace))
            )


def status(ref: ProjectRef | None = None, /) -> None:
    """Print the state of the daemon, then an overview of each project.

    The overview is what a sweep reads in its brief, without the tool guide. It shows the head, the tracks, the
    trajectory, the progress against [stop], the open runs, the queue, the live agents with their last line, the
    recent candidates, the anomalies, the inbox, the latest decisions and the recent events. Without a project, it
    uses the project that the current directory is in, else the only active project, else every project.
    """
    common.console.print(daemon_line())
    with common.session() as s:
        for project in common.projects(s, ref):
            common.console.rule(f'[bold]{project.name}[/] #{project.id}  {project.path}')
            text = overview(s, project, config_of(project), utcnow(), partial(_jobs, s), tools=False)
            common.console.print(text, markup=False, highlight=False, soft_wrap=True)


def _jobs(s: Session, worker: Worker) -> list[str]:
    """What a running worker has in flight, for the ``silent`` anomaly. Empty when its runtime cannot say."""
    try:
        return runtime_for(s, worker, common.settings.home).jobs(handle_for(worker))
    except Exception:  # the runtime is not installed or the agent is gone: status still prints, without this anomaly
        return []


def pause(
    ref: ProjectRef,
    /,
    *,
    reason: Annotated[str, cyclopts.Parameter(help='Why, with ids: what the pause waits for.')],
    evals: Annotated[
        bool,
        cyclopts.Parameter(help='Hold the measurements that the daemon starts too: after a new evaluator, or asked.'),
    ] = False,
) -> None:
    """Pause a project: nothing new launches, and nothing is escalated. `piloop resume` ends the pause.

    The daemon queues no run, requests no role and launches no requested row, so research files no ideas. A sweep that
    runs already files its ideas as proposed. Running work goes on, and its commits are scored. The daemon still
    measures again what a new evaluator or `piloop candidate remeasure` needs, so the pause can end on a measured head.
    `--evals` holds those measurements too. The pause is in the database: it holds wherever you run the command.
    """
    with common.refused(), common.session() as s:
        project = ops.pause(s, common.project(s, ref).id, evals=evals, reason=reason)
        s.commit()
        common.console.print(
            f'project {project.name} {paused(project)}; running work goes on; `piloop resume {project.name}` ends it',
            markup=False,
        )


def resume(ref: ProjectRef, /) -> None:
    """Resume a paused project. The daemon queues runs and requests roles again on its next tick."""
    with common.refused(), common.session() as s:
        project = common.project(s, ref)
        held = ops.resume(s, project.id)
        s.commit()
        common.console.print(f'project {project.name} resumed; it held {held}', markup=False)


def watch_cmd(ref: ProjectRef | None = None, /) -> None:
    """Show the campaign on one screen, with keys to act on it.

    It shows every project, or one project when you name it. A project has four panels (inbox, agents, ideas and
    candidates, on the keys 1 to 4) and the detail of the selected row. `?` lists the keys of the focused panel, `/`
    filters it, esc goes back to every project and q quits. Writes are signed as PILOOP_ACTOR.
    """
    if not sys.stdin.isatty():
        raise SystemExit('piloop watch reads keys from a terminal; `piloop status` prints the same once')
    db = Database(common.settings.database_url, actor=common.settings.actor)
    common.migrate(db)
    try:
        Watch(db, common.settings.home / 'piloop.db', ref, common.settings.actor).run()
    finally:
        db.close()


@app.command(name='stop')
def stop(
    ref: ProjectRef,
    /,
    *,
    kill: Annotated[
        bool, cyclopts.Parameter(help='End the running workers at once, without the wrap-up prompt.', negative='')
    ] = False,
    reason: Annotated[str, cyclopts.Parameter(help='Why, for `stopped_reason` and the closing review.')] = '',
) -> None:
    """Stop a campaign. The daemon stops it on its next tick, as when a `[stop]` condition holds.

    It drops the queued runs and the requested agents, and the `stopped` triggers request the closing review. Each
    running worker is told to commit and stop, and is ended 10 minutes later. What the workers committed is scored.
    With `--kill`, the workers are ended at once, and what they committed is still scored. On a stopped project,
    `--kill` ends the workers that are still winding down. The project's `piloop.toml` is not changed.
    """
    with common.session() as s:
        project = common.project(s, ref)
        if project.status is ProjectStatus.STOPPED and not kill:
            raise SystemExit(
                f'{project.name} is stopped already ({project.stopped_reason}); --kill ends its workers at once'
            )
        request_stop(s, project, common.settings.actor, reason=reason, kill=kill)
        s.commit()
        how = 'ends its workers at once' if kill else 'tells its workers to commit and stop'
        common.console.print(f'stop requested for {project.name}: the daemon {how} on its next tick', markup=False)
