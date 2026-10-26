"""``piloop inbox ack`` and ``piloop inbox reply``: acknowledge or answer what the agents address to you."""

from typing import Annotated

import cyclopts

from piloop.api.client import Client
from piloop.api.contract import AckBody, InboxQuery, ReplyBody
from piloop.cli import common
from piloop.cli.client import cwd, daemon_http
from piloop.ids import CommentId, CommentKind


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
    with Client.connect(daemon_http()) as client:
        if up_to is not None:
            listed = client.inbox(InboxQuery(campaign=campaign, cwd=cwd(), kind=kind)).items
            ids = tuple(item.id for item in listed if item.id <= up_to)
        results = client.ack(AckBody(ids=ids)).results
    common.console.print('\n'.join(r.line() for r in results) or 'nothing to acknowledge', markup=False)


def inbox_reply(
    comment: Annotated[CommentId, cyclopts.Parameter(help='The id of the item.')],
    body: Annotated[str, cyclopts.Parameter(help='The answer.')],
    /,
    *,
    priority: Annotated[int, cyclopts.Parameter(help='From 0 (urgent) to 4 (someday).')] = 2,
) -> None:
    """Answer an item with a `decision` on the root of its thread, signed as PILOOP_ACTOR.

    The answer is an inbox item of its own, so every orchestrator but you sees it in `piloop inbox wait`.
    """
    with Client.connect(daemon_http()) as client:
        answer = client.reply(comment, ReplyBody(body=body, priority=priority)).comment
    common.console.print(f'comment#{answer.id} decision on {answer.target}', markup=False)
