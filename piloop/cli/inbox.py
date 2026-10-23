"""The ``piloop inbox`` commands: list, show, wait for and acknowledge what the agents address to you."""

import shlex
from datetime import timedelta
from typing import Annotated

import cyclopts
from rich.table import Table
from rich.text import Text
from sqlalchemy.orm import Session

from piloop.campaign import inbox
from piloop.cli import common
from piloop.cli.common import CampaignRef, JsonFlag
from piloop.db import feed as db_feed
from piloop.db.models import Comment, CommentKind, utcnow
from piloop.ids import CommentId
from piloop.reports import feed
from piloop.reports.overview import age, item_lines
from piloop.text import cell

app = cyclopts.App(
    name='inbox', help='Read and acknowledge what the agents address to you: decisions, escalations and findings.'
)
KindFilter = Annotated[
    tuple[CommentKind, ...], cyclopts.Parameter(name='--kind', help='Only items of this kind. Repeatable.')
]


@app.default
def inbox_list(
    ref: CampaignRef | None = None,
    /,
    *,
    kind: KindFilter = (),
    all_: Annotated[bool, cyclopts.Parameter(name='--all', help='Show acknowledged items too.')] = False,
    json_: JsonFlag = False,
) -> None:
    """List the unacknowledged inbox items, most urgent first, then the seeds that wait for research.

    Priority 0 is urgent and 4 is someday. Without a campaign, it lists the items of every campaign. With --json, it
    prints them read at one cursor: follow them with `piloop events --after <cursor>`.
    """
    if json_:
        with common.session() as s, db_feed.snapshot(s) as cursor:
            campaign_id = common.campaign(s, ref).id if ref else None
            view = feed.inbox_view(s, campaign_id=campaign_id, kinds=kind, acked=all_)
        common.print_json({'cursor': cursor, **view})
        return
    with common.session() as s:
        campaign_id = common.campaign(s, ref).id if ref else None
        rows = inbox.items(s, campaign_id=campaign_id, kinds=kind, acked=all_)
        now = utcnow()
        table = Table('id', 'p', 'kind', 'campaign', 'on', 'by', 'age', *(['acked'] if all_ else []), 'first line')
        for c, campaign in rows:
            acked = [f'{c.acked_by}, {age(c.acked_at, now)} ago' if c.acked_at else '—'] if all_ else []
            table.add_row(
                str(c.id),
                str(c.priority),
                str(c.kind),
                campaign.name if campaign else '—',
                str(c.target),
                c.author,
                age(c.created_at, now),
                *acked,
                Text(cell(c.body)),
            )
        waiting = inbox.seeds(s, campaign_id=campaign_id)
        seeds = [inbox.seed_line(idea, campaign) for idea, campaign in waiting]
    if rows:
        common.console.print(table)
    common.console.print(
        f'{len(rows)} item(s){"" if all_ else " unacknowledged"}; `piloop inbox show <id>`, `piloop inbox ack <id>`'
    )
    if seeds:
        common.console.print(f'{len(seeds)} seed(s) waiting for research:', markup=False)
        common.console.print('\n'.join(seeds), markup=False, highlight=False, soft_wrap=True)


@app.command(name='show')
def inbox_show(comment_id: Annotated[CommentId, cyclopts.Parameter(help='The id of the item.')], /) -> None:
    """Show one item in full, then the comments that it cites as `comment#<id>`, that cite it, or reply to it."""
    with common.session() as s:
        c = s.get(Comment, comment_id)
        if c is None:
            raise SystemExit(f'no comment#{comment_id}')
        title, lines = item_lines(s, c, utcnow())
    common.console.print(title, style='bold', markup=False)
    common.console.print('\n'.join(lines), markup=False, highlight=False)


@app.command(name='wait')
def inbox_wait(
    ref: CampaignRef | None = None,
    /,
    *,
    timeout: Annotated[float, cyclopts.Parameter(help='Seconds to wait before it returns with nothing new.')] = 1800,
    after: Annotated[
        int | None,
        cyclopts.Parameter(help='The event id to start after: the cursor that the last wait printed. Default: now.'),
    ] = None,
) -> None:
    """Wait until something needs the orchestrator, print it one line per item, then print the command to wait again.

    These events end the wait: a new unacknowledged inbox item that someone else wrote, a seed that someone else
    filed, a campaign that the daemon stopped, an anomaly that the daemon gave to a watchdog, and the daemon's verdict
    on a measured candidate. Your own decisions, as PILOOP_ACTOR, do not end it. At --timeout it returns with nothing.
    Set the timeout of your shell tool higher than --timeout. Without a campaign, it waits for every campaign.
    """
    lines: list[str] = []
    with common.database() as db:
        with db.session() as s:
            campaign_id = common.campaign(s, ref).id if ref else None
            cursor = after if after is not None else db_feed.newest(s)

        def news(s: Session) -> bool:
            nonlocal cursor, lines
            cursor, lines = inbox.news(s, cursor, campaign_id=campaign_id, me=common.settings.actor)
            return bool(lines)

        db.wait_until(news, timeout=timeout)
    again = shlex.join(['piloop', 'inbox', 'wait', *([ref] if ref else []), '--after', str(cursor)])
    common.console.print(
        '\n'.join(lines) or f'nothing new in {timedelta(seconds=round(timeout))}', markup=False, soft_wrap=True
    )
    common.console.print(f'next: {again}', markup=False, soft_wrap=True)


@app.command(name='ack')
def inbox_ack(
    *ids: Annotated[CommentId, cyclopts.Parameter(help='The ids of the items.')],
    up_to: Annotated[
        CommentId | None,
        cyclopts.Parameter(
            help='Acknowledge every unacknowledged item up to this id instead. --campaign and --kind narrow it.'
        ),
    ] = None,
    campaign: Annotated[str | None, cyclopts.Parameter(help='With --up-to: only the items of this campaign.')] = None,
    kind: Annotated[
        tuple[CommentKind, ...],
        cyclopts.Parameter(name='--kind', help='With --up-to: only the items of this kind. Repeatable.'),
    ] = (),
) -> None:
    """Acknowledge items to say that you have seen them.

    Nothing waits for an acknowledgement. To change a decision, write a `decision` comment on its target.
    """
    if bool(ids) == (up_to is not None):
        raise SystemExit('name the ids to acknowledge, or --up-to <id>')
    if ids and (campaign or kind):
        raise SystemExit('--campaign and --kind work only with --up-to <id>')
    with common.session() as s:
        if up_to is not None:
            campaign_id = common.campaign(s, campaign).id if campaign else None
            ids = tuple(c.id for c, _ in inbox.items(s, campaign_id=campaign_id, kinds=kind) if c.id <= up_to)
        lines = inbox.ack(s, ids, common.settings.actor)
    common.console.print('\n'.join(lines) or 'nothing to acknowledge', markup=False)
