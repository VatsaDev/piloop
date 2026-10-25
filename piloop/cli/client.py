"""What the commands that call the API share: the client of the home's daemon, and where the command runs.

Each command that opens the database instead (``piloop.cli.local``) imports none of it.
"""

from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from piloop.api.client import Client, NoDaemon, Refused
from piloop.api.contract import Selection
from piloop.cli import common


@contextmanager
def api() -> Generator[Client]:
    """A client of the home's daemon. Exit with what to do when there is no daemon or it refuses the call."""
    actor, server = common.settings.actor, common.settings.server
    try:
        if server is None:
            with Client.on(common.settings.socket_file, actor) as client:
                yield client
        else:
            from piloop.plugins.network.contract import NETWORKS  # only a remote call needs a network's client

            with Client.at(NETWORKS.selected(common.settings.providers).client(server, actor)) as client:
                yield client
    except (NoDaemon, Refused) as e:
        raise SystemExit(str(e)) from e


def cwd() -> str | None:
    """Where this command runs, for the daemon of this home to resolve a relative path from. None for another server."""
    return str(Path.cwd()) if common.settings.server is None else None


def campaign_name(client: Client, ref: str) -> str:
    """The name of the campaign that ``ref`` names: a name, or a path that the daemon resolves from ``cwd``."""
    [found] = client.status(Selection(campaign=ref, cwd=cwd())).campaigns
    return found.name
