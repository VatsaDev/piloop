"""The commands about projects: ``init``, ``status``, ``history`` and ``watch``."""

import sys
from functools import partial
from pathlib import Path
from typing import Annotated

import cyclopts
from rich import box
from rich.console import Group
from sqlalchemy.orm import Session

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
from piloop.cli import common
from piloop.cli.common import ProjectRef
from piloop.cli.daemon import daemon_line
from piloop.config import config_of
from piloop.db.models import ProjectStatus, Worker, utcnow
from piloop.db.session import Database
from piloop.reports.overview import overview
from piloop.reports.pack import renderables as pack_renderables
from piloop.runtime.factory import handle_for, runtime_for
from piloop.watch.app import Watch


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
