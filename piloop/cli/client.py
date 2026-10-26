"""What the commands that call the API share: the daemon to call, and where the command runs.

A command calls ``Client.connect(daemon_http())``, or the client of a capability's routes the same way. ``main.answered``
turns no daemon, and a refusal, into the message to exit with.

Each command that opens the database instead (``piloop.cli.local``) imports none of it.
"""

from pathlib import Path

import httpx2 as httpx

from piloop.api.client import Client, socket_http
from piloop.api.contract import Selection
from piloop.cli import common
from piloop.ids import CampaignStatus


def daemon_http() -> httpx.Client:
    """The HTTP client of the daemon that this command calls.

    It is the home's socket, else ``PILOOP_SERVER`` through the home's network, which carries the actor.

    Raises:
        NoDaemon: Nothing listens on the home's socket.
    """
    actor, server = common.settings.actor, common.settings.server
    if server is None:
        return socket_http(common.settings.socket_file, actor, common.settings.agent_token)
    from piloop.plugins.network.contract import NETWORKS  # only a remote call needs a network's client

    return NETWORKS.selected(common.settings.providers).client(server, actor)


def cwd() -> str | None:
    """Where this command runs, for the daemon of this home to resolve a relative path from. None for another server."""
    return str(Path.cwd()) if common.settings.server is None else None


def campaign_name(client: Client, ref: str) -> str:
    """The name of the campaign that ``ref`` names, to change it: a name, an id, or a path resolved from ``cwd``.

    A retired campaign changes no more, and another campaign may have its name.

    Raises:
        SystemExit: The campaign is retired.
    """
    [found] = client.status(Selection(campaign=ref, cwd=cwd())).campaigns
    if found.status is CampaignStatus.RETIRED:
        raise SystemExit(f'campaign #{found.id} ({found.name}) is retired')
    return found.name
