"""The commands about campaigns: ``init``, and those that open the database: ``history``, ``watch``, ``campaign parent|tree``."""

import sys
from pathlib import Path
from typing import Annotated

import cyclopts
from rich import box
from rich.console import Group

from piloop.api.client import Client, answers
from piloop.api.contract import MAX_POLL_SECONDS, Checkout, EventsQuery, Selection
from piloop.api.views import Registered
from piloop.campaign import ops
from piloop.campaign.campaigns import (
    BLANK,
    EVALUATE_CONTRACT,
    EVALUATE_STUB,
    EXAMPLE_DATA,
    TEMPLATES,
    create_campaign_repo,
    stub_untouched,
    tree as campaign_tree,
    workspace_dir,
)
from piloop.cli import common, daemon, local
from piloop.cli.client import daemon_http
from piloop.cli.common import CampaignRef
from piloop.db.models import EVENT
from piloop.db.session import Database
from piloop.db.triggers.changelog import CampaignPausedChanged, CampaignStatusChanged
from piloop.ids import CampaignStatus, checked_campaign_name
from piloop.reports.pack import renderables as pack_renderables
from piloop.scoring.ledger import MeasurementRecorded
from piloop.text import metric
from piloop.watch.app import Watch


def init(
    path: Annotated[Path, cyclopts.Parameter(help='The campaign directory.')] = Path(),
    /,
    *,
    example: Annotated[
        str | None,
        cyclopts.Parameter(help=f'Write a bundled example instead of the blank template: {", ".join(EXAMPLE_DATA)}.'),
    ] = None,
    name: Annotated[
        str | None, cyclopts.Parameter(help='Register the campaign under this name, not the `name` of piloop.toml.')
    ] = None,
) -> None:
    """Make a directory a piloop campaign and register it.

    It is safe to run again. When needed, it runs `git init` and makes a baseline commit. When there is no
    piloop.toml, it writes piloop.toml, brief.md and evaluate.py from the template, and keeps the files that exist.
    While evaluate.py is still the template stub, it stops and says what to edit. Then it starts the daemon, unless
    one runs, registers the campaign with HEAD as its baseline candidate, and waits while the daemon measures it. A
    baseline that fails to evaluate pauses the campaign, and init exits with why. Ctrl-C stops only the wait: the
    campaign stays registered. For a campaign that is registered already, it reads piloop.toml again.
    """
    if example is not None and example not in EXAMPLE_DATA:
        raise SystemExit(f'no example {example!r}; bundled: {", ".join(EXAMPLE_DATA)}')
    try:
        named = None if name is None else checked_campaign_name(name)
    except ValueError as e:
        raise SystemExit(str(e)) from e
    written = create_campaign_repo(path, TEMPLATES / example if example else BLANK, EXAMPLE_DATA.get(example or '', {}))
    path = path.resolve()
    for rel in written:
        common.console.print(f'wrote {rel}')
    if stub_untouched(path):
        common.console.print(EVALUATE_CONTRACT, style='dim', markup=False)
        common.console.print(
            f'next: {path / EVALUATE_STUB} is still the template stub. Make it measure this campaign and report the'
            f' metrics that piloop.toml names. Describe the goal in brief.md. Then run `piloop init {path}` again.'
        )
        return
    if common.settings.server is None and not answers(common.settings.socket_file):
        daemon.detached()
    with Client.connect(daemon_http()) as client:
        found = next((c for c in client.status(Selection()).campaigns if c.path == str(path)), None)
        if found is None:
            added = client.register_campaign(Checkout(path=str(path), name=named))
            campaign, documents = added.campaign, added.documents
            # the measurement waits for a free evaluation slot, which can be on a worker that has not connected yet
            common.console.print(
                f'campaign [bold]{campaign.name}[/] #{campaign.id} registered; measuring baseline'
                f' candidate#{added.baseline} (`piloop eval queue` shows it; Ctrl-C stops only the wait)'
            )
            metrics = _measured(client, added)
            common.console.print(
                f'baseline candidate#{added.baseline}:'
                f' {", ".join(f"{name} {metric(value)}" for name, value in metrics.items())}'
            )
            common.console.print(f'next: add ideas: `piloop idea add {campaign.name} "<title>"`')
        elif named not in (None, found.name):
            raise SystemExit(f'{path} is campaign {found.name} #{found.id} already: a campaign keeps its name')
        else:
            synced = client.sync_campaign(found.name)
            campaign, documents = synced.campaign, synced.documents
            resumed = (
                '; resumed: stop conditions changed'
                if synced.was is CampaignStatus.STOPPED and campaign.status is CampaignStatus.ACTIVE
                else ''
            )
            common.console.print(
                f'campaign {campaign.name} #{campaign.id} already registered; config re-read from piloop.toml{resumed}'
            )
    if documents.added or documents.unchanged or documents.skipped:
        common.console.print(documents.line(), markup=False, highlight=False, soft_wrap=True)


FOLLOWED = (*MeasurementRecorded.kinds(), *CampaignPausedChanged.kinds(), *CampaignStatusChanged.kinds())
"""What ``init`` follows on the feed after it registers a campaign: the baseline's measurement, the pause that a
failed one makes, or a stop, after which the daemon measures nothing unasked."""


def _measured(client: Client, added: Registered) -> dict[str, float]:
    """Follow the feed after the registration until the daemon measures the baseline, and return its metrics.

    Exit with the reason when the campaign is paused or stopped first: a baseline that fails to evaluate pauses it.
    """
    cursor = added.cursor
    while True:
        query = EventsQuery(after=cursor, campaign=added.campaign.name, kind=FOLLOWED, wait=MAX_POLL_SECONDS)
        batch = client.events(query)
        for event in batch.events:
            match EVENT.validate_python(event.payload):
                case MeasurementRecorded(candidate=added.baseline, error=None, metrics=metrics):
                    return metrics
                case CampaignPausedChanged(paused_reason=str(reason)):
                    raise SystemExit(reason)
                case CampaignStatusChanged(new=CampaignStatus.STOPPED, stopped_reason=reason):
                    raise SystemExit(reason or f'campaign {added.campaign.name} stopped')
                case _:
                    pass  # a failed measurement: its pause follows
        cursor = batch.next


def history(ref: CampaignRef | None = None, /) -> None:
    """Print the research pack: what a research sweep reads before it proposes ideas.

    It shows the metrics, the analysis artifacts, what was tried per mechanism family, the latest 60 ideas with their
    outcomes, the kept lineage, the findings and the digests. Without a campaign, it uses the campaign that the current
    directory is in, else the only active campaign, else every campaign.
    """
    with local.session() as s:
        for campaign in local.campaigns(s, ref):
            common.heading(f'[bold]{campaign.name}[/] #{campaign.id}  {campaign.path}')
            workspace = workspace_dir(common.settings.home, campaign.id)
            common.console.print(
                Group(*pack_renderables(s, campaign, campaign.config, table_box=box.SIMPLE_HEAD, workspace=workspace))
            )


def watch_cmd(
    ref: CampaignRef | None = None,
    /,
    *,
    mouse: Annotated[
        bool,
        cyclopts.Parameter(
            help='Take the mouse. --no-mouse leaves it to the terminal: a multiplexer can misread the mouse of the app.'
        ),
    ] = True,
) -> None:
    """Show the campaign on one screen, with keys to act on it.

    It shows every campaign, or one campaign when you name it. A campaign has four panels (inbox, agents, ideas and
    candidates, on the keys 1 to 4) and the detail of the selected row. `?` lists the keys of the focused panel, `/`
    filters it, esc goes back to every campaign and q quits. Writes are signed as PILOOP_ACTOR.
    """
    if not sys.stdin.isatty():
        raise SystemExit('piloop watch reads keys from a terminal; `piloop status` prints the same once')
    local.refuse_other_schema()
    if ref is not None:
        local.known(ref)
    db = Database(common.settings.database_url, actor=common.settings.actor)
    try:
        Watch(db, common.settings.home / 'piloop.db', ref, common.settings.actor, local.cas()).run(mouse=mouse)
    finally:
        db.close()


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
    with local.refused(), local.session() as s:
        above = None if parent_ref is None else local.campaign(s, parent_ref)
        campaign = ops.set_parent(s, local.campaign(s, ref).id, None if above is None else above.id)
        s.commit()
        where = 'a root' if above is None else f'under {above.name} #{above.id}'
        common.console.print(f'{campaign.name} #{campaign.id} is {where}', markup=False)


def tree() -> None:
    """Print every campaign under its parent, with its type and its status."""
    with local.session() as s:
        for depth, campaign in campaign_tree(s):
            line = f'{"  " * depth}{campaign.name} #{campaign.id}  {campaign.type}  {campaign.status}'
            common.console.print(line, markup=False, highlight=False)
