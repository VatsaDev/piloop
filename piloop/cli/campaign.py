"""The commands about campaigns: ``init``, ``status``, ``history``, ``watch``, ``pause``, ``resume``, ``campaign stop``."""

import sys
from functools import partial
from pathlib import Path
from typing import Annotated

import cyclopts
from rich import box
from rich.console import Group
from sqlalchemy.orm import Session

from piloop.campaign import ops
from piloop.campaign.campaigns import (
    BLANK,
    EVALUATE_CONTRACT,
    EVALUATE_STUB,
    EXAMPLE_DATA,
    TEMPLATES,
    CampaignError,
    create_campaign_repo,
    register,
    registered,
    stub_untouched,
    sync,
    tree as campaign_tree,
    workspace_dir,
)
from piloop.campaign.memory import sync_documents
from piloop.campaign.stop import request_stop
from piloop.cli import common, limits as limits_cli
from piloop.cli.common import CampaignRef, JsonFlag
from piloop.cli.daemon import daemon_line
from piloop.config import config_of
from piloop.daemon.serve import running_pid
from piloop.db.models import AgentSession, Campaign, CampaignStatus, utcnow
from piloop.db.session import Database
from piloop.plugins.limits import Origin
from piloop.reports import feed
from piloop.reports.overview import overview, paused
from piloop.reports.pack import renderables as pack_renderables
from piloop.runtime.factory import handle_for, runtime_for
from piloop.watch.app import Watch

app = cyclopts.App(name='campaign', help='Act on a campaign as a whole.')


def init(
    path: Annotated[Path, cyclopts.Parameter(help='The campaign directory.')] = Path(),
    /,
    *,
    example: Annotated[
        str | None,
        cyclopts.Parameter(help=f'Write a bundled example instead of the blank template: {", ".join(EXAMPLE_DATA)}.'),
    ] = None,
) -> None:
    """Make a directory a piloop campaign and register it.

    It is safe to run again. When needed, it runs `git init` and makes a baseline commit. When there is no
    piloop.toml, it writes piloop.toml, brief.md and evaluate.py from the template, and keeps the files that exist.
    While evaluate.py is still the template stub, it stops and says what to edit. Then it measures HEAD as the
    baseline candidate and registers the campaign. For a campaign that is registered already, it reads piloop.toml
    again.
    """
    if example is not None and example not in EXAMPLE_DATA:
        raise SystemExit(f'no example {example!r}; bundled: {", ".join(EXAMPLE_DATA)}')
    written = create_campaign_repo(path, TEMPLATES / example if example else BLANK, EXAMPLE_DATA.get(example or '', {}))
    path = path.resolve()
    for rel in written:
        common.console.print(f'wrote {rel}')
    if stub_untouched(path):
        common.console.print(EVALUATE_CONTRACT, style='dim', markup=False)
        common.console.print(
            f'next: {path / EVALUATE_STUB} is still the template stub. Make it measure this campaign and print the'
            f' METRIC lines that piloop.toml names. Describe the goal in brief.md. Then run `piloop init {path}` again.'
        )
        return
    with common.session(create=True) as s:
        campaign = registered(s, path)
        if campaign is None:
            try:
                campaign, measurement = register(s, path, common.settings.actor)
            except CampaignError as e:
                raise SystemExit(str(e)) from e
            common.console.print(
                f'campaign [bold]{campaign.name}[/] #{campaign.id} registered; baseline candidate#{measurement.candidate_id}'
                f' {measurement.metrics}'
            )
            common.console.print(
                f'next: add ideas: `piloop idea add {campaign.name} "<title>"`; start the daemon: `piloop serve`'
            )
        else:
            stopped = campaign.status is CampaignStatus.STOPPED
            sync(s, campaign)
            s.commit()
            resumed = (
                '; resumed: stop conditions changed' if stopped and campaign.status is CampaignStatus.ACTIVE else ''
            )
            common.console.print(
                f'campaign {campaign.name} #{campaign.id} already registered; config re-read from piloop.toml{resumed}'
            )
        synced = sync_documents(s, campaign, config_of(campaign), common.settings.actor)
        s.commit()
        if synced.added or synced.unchanged or synced.skipped:
            common.console.print(synced.line(), markup=False, highlight=False, soft_wrap=True)


def history(ref: CampaignRef | None = None, /) -> None:
    """Print the research pack: what a research sweep reads before it proposes ideas.

    It shows the metrics, the analysis artifacts, what was tried per mechanism family, the latest 60 ideas with their
    outcomes, the kept lineage, the findings and the digests. Without a campaign, it uses the campaign that the current
    directory is in, else the only active campaign, else every campaign.
    """
    with common.session() as s:
        for campaign in common.campaigns(s, ref):
            common.console.rule(f'[bold]{campaign.name}[/] #{campaign.id}  {campaign.path}')
            workspace = workspace_dir(common.settings.home, campaign.id)
            common.console.print(
                Group(
                    *pack_renderables(s, campaign, config_of(campaign), table_box=box.SIMPLE_HEAD, workspace=workspace)
                )
            )


def status(ref: CampaignRef | None = None, /, *, json_: JsonFlag = False) -> None:
    """Print the state of the daemon and the live keys that `config.toml` or a limit sets, then each campaign.

    The overview is what a sweep reads in its brief, without the tool guide. It shows the head, the tracks, the
    trajectory, the progress against [stop], the open runs, the queue, the live agents with their last line, the
    recent candidates, the anomalies, the inbox, the latest decisions and the recent events. Without a campaign, it
    uses the campaign that the current directory is in, else the only active campaign, else every campaign.

    With --json, it prints the daemon's pid and the main facts of each campaign, read at one cursor: follow them with
    `piloop events --after <cursor>`.
    """
    if json_:
        with common.session() as s, feed.snapshot(s) as cursor:
            states = feed.status_view(s, common.campaigns(s, ref))
        common.print_json({'cursor': cursor, 'daemon': {'pid': running_pid(common.settings)}, 'campaigns': states})
        return
    common.console.print(daemon_line())
    with common.session() as s:
        for line in limits_cli.lines(s, (Origin.CONFIG, Origin.LIMIT)):
            common.console.print(f'limit: {line}', markup=False)
        for campaign in common.campaigns(s, ref):
            parent = None if campaign.parent_id is None else s.get_one(Campaign, campaign.parent_id)
            under = '' if parent is None else f'  under {parent.name} #{parent.id}'
            common.console.rule(f'[bold]{campaign.name}[/] #{campaign.id}  {campaign.path}{under}')
            text = overview(s, campaign, config_of(campaign), utcnow(), partial(_jobs, s), tools=False)
            common.console.print(text, markup=False, highlight=False, soft_wrap=True)


def _jobs(s: Session, agent_session: AgentSession) -> list[str]:
    """What a running implementer has in flight, for the ``silent`` anomaly. Empty when its runtime cannot say."""
    try:
        return runtime_for(s, agent_session, common.settings.home).jobs(handle_for(agent_session))
    except Exception:  # the runtime is not installed or the agent is gone: status still prints, without this anomaly
        return []


def pause(
    ref: CampaignRef,
    /,
    *,
    reason: Annotated[str, cyclopts.Parameter(help='Why, with ids: what the pause waits for.')],
    evals: Annotated[
        bool,
        cyclopts.Parameter(help='Hold the measurements that the daemon starts too: after a new evaluator, or asked.'),
    ] = False,
) -> None:
    """Pause a campaign: nothing new launches, and nothing is escalated. `piloop resume` ends the pause.

    The daemon queues no run, requests no role and launches no requested row, so research files no ideas. A sweep that
    runs already files its ideas as proposed. Running work goes on, and its commits are scored. The daemon still
    measures again what a new evaluator or `piloop candidate remeasure` needs, so the pause can end on a measured head.
    `--evals` holds those measurements too. The pause is in the database: it holds wherever you run the command.
    """
    with common.refused(), common.session() as s:
        campaign = ops.pause(s, common.campaign(s, ref).id, evals=evals, reason=reason)
        s.commit()
        common.console.print(
            f'campaign {campaign.name} {paused(campaign)}; running work goes on; `piloop resume {campaign.name}` ends it',
            markup=False,
        )


def resume(ref: CampaignRef, /) -> None:
    """Resume a paused campaign. The daemon queues runs and requests roles again on its next tick."""
    with common.refused(), common.session() as s:
        campaign = common.campaign(s, ref)
        held = ops.resume(s, campaign.id)
        s.commit()
        common.console.print(f'campaign {campaign.name} resumed; it held {held}', markup=False)


def watch_cmd(ref: CampaignRef | None = None, /) -> None:
    """Show the campaign on one screen, with keys to act on it.

    It shows every campaign, or one campaign when you name it. A campaign has four panels (inbox, agents, ideas and
    candidates, on the keys 1 to 4) and the detail of the selected row. `?` lists the keys of the focused panel, `/`
    filters it, esc goes back to every campaign and q quits. Writes are signed as PILOOP_ACTOR.
    """
    if not sys.stdin.isatty():
        raise SystemExit('piloop watch reads keys from a terminal; `piloop status` prints the same once')
    common.refuse_other_schema()
    if ref is not None:
        common.known(ref)
    db = Database(common.settings.database_url, actor=common.settings.actor)
    try:
        Watch(db, common.settings.home / 'piloop.db', ref, common.settings.actor).run()
    finally:
        db.close()


@app.command(name='parent')
def parent(
    ref: CampaignRef,
    parent_ref: Annotated[str | None, cyclopts.Parameter(help='The campaign to put it under.')] = None,
    /,
    *,
    clear: Annotated[bool, cyclopts.Parameter(help='Make the campaign a root.', negative='')] = False,
) -> None:
    """Put a campaign under another, or make it a root with --clear.

    A campaign has at most one parent, in the same piloop home, and never itself or one of its descendants.
    `piloop campaign tree` shows the tree.
    """
    if (parent_ref is None) is not clear:
        raise SystemExit('name the parent or --clear, not both' if clear else 'name the parent, or --clear')
    with common.refused(), common.session() as s:
        above = None if parent_ref is None else common.campaign(s, parent_ref)
        campaign = ops.set_parent(s, common.campaign(s, ref).id, None if above is None else above.id)
        s.commit()
        where = 'a root' if above is None else f'under {above.name} #{above.id}'
        common.console.print(f'{campaign.name} #{campaign.id} is {where}', markup=False)


@app.command(name='tree')
def tree() -> None:
    """Print every campaign under its parent, with its type and its status."""
    with common.session() as s:
        for depth, campaign in campaign_tree(s):
            line = f'{"  " * depth}{campaign.name} #{campaign.id}  {campaign.type}  {campaign.status}'
            common.console.print(line, markup=False, highlight=False)


@app.command(name='stop')
def stop(
    ref: CampaignRef,
    /,
    *,
    kill: Annotated[
        bool, cyclopts.Parameter(help='End the running implementers at once, without the wrap-up prompt.', negative='')
    ] = False,
    reason: Annotated[str, cyclopts.Parameter(help='Why, for `stopped_reason` and the closing review.')] = '',
) -> None:
    """Stop a campaign. The daemon stops it on its next tick, as when a `[stop]` condition holds.

    This works also while the campaign is paused or held at its `[spend]` limit. A paused campaign gets its closing
    review; a held one as `[spend] closing_review` says. It drops the queued runs and the requested agents, and the
    `stopped` triggers request the closing review. Each
    running implementer is told to commit and stop, and is ended 10 minutes later. What the implementers committed is scored.
    With `--kill`, the implementers are ended at once, and what they committed is still scored. On a stopped campaign,
    `--kill` ends the implementers that are still winding down. The campaign's `piloop.toml` is not changed.
    """
    with common.session() as s:
        campaign = common.campaign(s, ref)
        if campaign.status is CampaignStatus.STOPPED and not kill:
            raise SystemExit(
                f'{campaign.name} is stopped already ({campaign.stopped_reason}); --kill ends its implementers at once'
            )
        request_stop(s, campaign, common.settings.actor, reason=reason, kill=kill)
        s.commit()
        how = 'ends its implementers at once' if kill else 'tells its implementers to commit and stop'
        common.console.print(f'stop requested for {campaign.name}: the daemon {how} on its next tick', markup=False)
