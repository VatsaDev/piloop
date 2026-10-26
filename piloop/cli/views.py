"""The commands that print the daemon's views: ``status``, ``inbox`` (list, show, wait) and ``events``, clients of the API."""

import json
import shlex
import time
from datetime import UTC, datetime, timedelta
from typing import Annotated, assert_never

import cyclopts
from cyclopts import validators
from rich.table import Table
from rich.text import Text

from piloop.api.client import Client, Refused
from piloop.api.contract import (
    LIMIT,
    MAX_LIMIT,
    MAX_POLL_SECONDS,
    EventsQuery,
    InboxQuery,
    NewsQuery,
    Problem,
    Selection,
    defaulted,
)
from piloop.api.views import (
    Batch,
    CampaignOverview,
    DaemonState,
    InvalidConfig,
    LiveKey,
    seed_line,
    thread_lines,
)
from piloop.cli import common
from piloop.cli.client import cwd, daemon_http
from piloop.cli.common import CampaignRef, JsonFlag
from piloop.ids import CommentId, CommentKind
from piloop.text import age, cell

KindFilter = Annotated[
    tuple[CommentKind, ...], cyclopts.Parameter(name='--kind', help='Only items of this kind. Repeatable.')
]


def status(ref: CampaignRef | None = None, /, *, json_: JsonFlag = False) -> None:
    """Print the state of the daemon and the live keys that `config.toml` or a limit sets, then each campaign.

    Each campaign shows the sections of its [status] sections: by default the head, the goal and the progress against
    the stop rule, the tracks, the trajectory, the open runs, the queue, the live agents with their last line, the
    evaluations, the recent candidates, the incidents that agents got, the inbox and the recent events. Without a campaign, it uses
    the campaign that the current directory is in, else the only active campaign, else every campaign.

    With --json, it prints the daemon's pid and the URL it listens on, and the main facts of each campaign, read at one
    cursor: follow them with `piloop events --after <cursor>`.
    """
    with Client.connect(daemon_http()) as client:
        status = client.status(Selection(campaign=ref, cwd=cwd()))
        status = status.model_copy(update={'campaigns': defaulted(status.campaigns)})
        if json_:
            common.print_json(status.model_dump(mode='json'))
            return
        names = [c.name for c in status.campaigns]
        shown = client.overview(Selection(campaign=names[0] if len(names) == 1 else None))
    common.console.print(daemon_line(shown.daemon), markup=False)
    for key in shown.limits:
        common.console.print(f'limit: {limit_line(key)}', markup=False)
    for campaign in shown.campaigns:
        parent = campaign.parent
        under = '' if parent is None else f'  under {parent.name} #{parent.id}'
        common.heading(f'[bold]{campaign.name}[/] #{campaign.id}  {campaign.path}{under}')
        match campaign:
            case CampaignOverview():
                blocks = [section.markdown() for section in campaign.sections]
            case InvalidConfig():
                blocks = [campaign.problem, f'fix: {campaign.fix}']
            case _:
                assert_never(campaign)
        for text in blocks:
            common.console.print(text + '\n', markup=False, highlight=False, soft_wrap=True)


def daemon_line(state: DaemonState) -> str:
    """One line on the daemon: its pid, its uptime, and the URL that other machines reach it on."""
    up = timedelta(seconds=int(time.time() - datetime.fromisoformat(state.started).timestamp()))
    listening = '' if state.listen is None else f', listening on {state.listen}'
    return f'daemon: pid {state.pid}, up {up}{listening}'


def limit_line(key: LiveKey) -> str:
    """One live key: its value, and where the value comes from."""
    source = key.origin if key.actor is None else f'limit by {key.actor}: {key.reason}'
    return f'{key.key} = {json.dumps(key.value)}  ({source})'


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
    with Client.connect(daemon_http()) as client:
        inbox = client.inbox(InboxQuery(campaign=ref, cwd=cwd(), kind=kind, acked=all_))
    if json_:
        common.print_json(inbox.model_dump(mode='json'))
        return
    now = clock()
    table = Table('id', 'p', 'kind', 'campaign', 'on', 'by', 'age', *(['acked'] if all_ else []), 'first line')
    for item in inbox.items:
        acked = [f'{item.acked_by}, {_age(item.acked_at, now)} ago' if item.acked_at else '—'] if all_ else []
        table.add_row(
            str(item.id),
            str(item.priority),
            item.kind,
            item.campaign_name or '—',
            f'{item.target.kind}#{item.target.id}',
            item.author,
            _age(item.created, now),
            *acked,
            Text(cell(item.body)),
        )
    if inbox.items:
        common.console.print(table)
    common.console.print(
        f'{len(inbox.items)} item(s){"" if all_ else " unacknowledged"}; `piloop inbox show <id>`,'
        ' `piloop inbox ack <id>`'
    )
    if seeds := [seed_line(seed) for seed in inbox.seeds]:
        common.console.print(f'{len(seeds)} seed(s) waiting for research:', markup=False)
        common.console.print('\n'.join(seeds), markup=False, highlight=False, soft_wrap=True)


def inbox_show(comment: Annotated[CommentId, cyclopts.Parameter(help='The id of the item.')], /) -> None:
    """Show one item in full, then the comments that it cites as `comment#<id>`, that cite it, or reply to it."""
    with Client.connect(daemon_http()) as client:
        title, lines = thread_lines(client.thread(comment), clock())
    common.console.print(title, style='bold', markup=False)
    common.console.print('\n'.join(lines), markup=False, highlight=False)


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
    deadline, cursor = time.monotonic() + timeout, after
    with Client.connect(daemon_http()) as client:
        while True:
            wait = min(MAX_POLL_SECONDS, max(0.0, deadline - time.monotonic()))
            found = client.news(NewsQuery(after=cursor, campaign=ref, cwd=cwd(), wait=wait))
            cursor = found.next
            if found.items or time.monotonic() >= deadline:
                break
    again = shlex.join(['piloop', 'inbox', 'wait', *([ref] if ref else []), '--after', str(cursor)])
    lines = '\n'.join(item.text() for item in found.items)
    common.console.print(lines or f'nothing new in {timedelta(seconds=round(timeout))}', markup=False, soft_wrap=True)
    common.console.print(f'next: {again}', markup=False, soft_wrap=True)


def events(
    *,
    after: Annotated[
        int,
        cyclopts.Parameter(
            help='The cursor to read after: the `next` of the last read, or the `cursor` of a snapshot.',
            validator=validators.Number(gte=0),
        ),
    ] = 0,
    campaign: Annotated[str | None, cyclopts.Parameter(help='Only the events about rows of this campaign.')] = None,
    kind: Annotated[
        tuple[str, ...], cyclopts.Parameter(name='--kind', help='Only events of this kind. Repeatable.')
    ] = (),
    limit: Annotated[
        int,
        cyclopts.Parameter(help='The most events to print.', validator=validators.Number(gte=1, lte=MAX_LIMIT)),
    ] = LIMIT,
    follow: Annotated[
        bool, cyclopts.Parameter(negative=(), help='Wait for an event when there is none yet, up to --timeout.')
    ] = False,
    timeout: Annotated[
        float, cyclopts.Parameter(help='With --follow: seconds to wait.', validator=validators.Number(gte=0))
    ] = 1800,
    json_: JsonFlag = False,
) -> None:
    """Print the events after a cursor, oldest first, then the cursor to read after next.

    The cursor moves over every event that the read examined, also those that --campaign and --kind leave out. Read a
    snapshot first (`piloop status --json` or `piloop inbox --json`), then the events after its cursor: nothing is
    missed in between. Save the cursor after you have handled the events, and read from it again after a disconnect.
    An event can come twice then: skip an id that you have handled.
    """
    deadline, cursor = time.monotonic() + (timeout if follow else 0), after
    with Client.connect(daemon_http()) as client:
        while True:
            wait = min(MAX_POLL_SECONDS, max(0.0, deadline - time.monotonic()))
            try:
                batch = _read(client, cursor, campaign, kind, limit, wait)
            except Refused as e:
                if json_ and e.error == Problem.CURSOR_AHEAD:
                    raise common.json_error(e.error, str(e)) from e
                raise
            if batch.events or time.monotonic() >= deadline:
                break
            cursor = batch.next
    if json_:
        common.print_json(batch.model_dump(mode='json'))
        return
    for e in batch.events:
        invalid = e.invalid or {}
        payload = json.dumps(invalid.get('payload', e.payload))
        common.console.out(
            f'event#{e.id} {e.time or invalid.get("time")} {e.kind} {e.subject.kind}#{e.subject.id} by {e.actor}: {payload}',
            highlight=False,
        )
    narrow = [*(['--campaign', campaign] if campaign else []), *(a for k in kind for a in ('--kind', k))]
    again = shlex.join(['piloop', 'events', '--after', str(batch.next), *narrow])
    common.console.print(f'next: {again}' + (' (more now)' if batch.more else ''), markup=False, soft_wrap=True)


def _read(client: Client, after: int, campaign: str | None, kind: tuple[str, ...], limit: int, wait: float) -> Batch:
    return client.events(EventsQuery(after=after, campaign=campaign, cwd=cwd(), kind=kind, limit=limit, wait=wait))


def clock() -> datetime:
    """The time that the ages count from: now, in UTC."""
    return datetime.now(UTC)


def _age(iso: str, now: datetime) -> str:
    return age(datetime.fromisoformat(iso), now)
