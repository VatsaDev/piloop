"""``piloop events``: read the event feed after a cursor, and wait for new events."""

import json
import shlex
from typing import Annotated

import cyclopts
from cyclopts import validators

from piloop.cli import common
from piloop.cli.common import JsonFlag
from piloop.db import feed as db_feed
from piloop.reports import feed


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
        cyclopts.Parameter(help='The most events to print.', validator=validators.Number(gte=1, lte=feed.MAX_LIMIT)),
    ] = feed.LIMIT,
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
    with common.database() as db, db.session() as s:
        campaign_id = common.campaign(s, campaign).id if campaign else None
        try:
            if follow:
                batch = feed.follow(db, after, campaign_id=campaign_id, kinds=kind, limit=limit, timeout=timeout)
            else:
                batch = feed.read(s, after, campaign_id=campaign_id, kinds=kind, limit=limit)
        except db_feed.CursorAhead as e:
            raise (common.json_error(e.code, str(e)) if json_ else SystemExit(str(e))) from e
    if json_:
        common.print_json(batch)
        return
    for e in batch['events']:
        subject, invalid = e['subject'], e.get('invalid', {})
        payload = json.dumps(invalid['payload'] if 'payload' in invalid else e['payload'])
        common.console.out(
            f'event#{e["id"]} {e["time"] or invalid.get("time")} {e["kind"]} {subject["kind"]}#{subject["id"]}'
            f' by {e["actor"]}: {payload}',
            highlight=False,
        )
    narrow = [*(['--campaign', campaign] if campaign else []), *(a for k in kind for a in ('--kind', k))]
    again = shlex.join(['piloop', 'events', '--after', str(batch['next']), *narrow])
    common.console.print(f'next: {again}' + (' (more now)' if batch['more'] else ''), markup=False, soft_wrap=True)
