"""``piloop cas serve``: this home's CAS, served to workers on other machines, apart from ``piloop serve``."""

import contextlib
import sys
from typing import Annotated

import cyclopts
from pydantic import ValidationError

from piloop.cli import common, local
from piloop.db.session import Database
from piloop.net import cas_server
from piloop.net.server import Server
from piloop.net.tokens import Tokens
from piloop.plugins.network.contract import NETWORKS, Address, NetworkHost
from piloop.plugins.secrets.contract import SECRETS, HomeSecrets

app = cyclopts.App(name='cas')  # its help is where piloop.cli.main registers it


@app.command
def serve(
    *,
    listen: Annotated[
        str,
        cyclopts.Parameter(help='Serve on HOST:PORT. An address that is not loopback needs TLS ([plugins.network]).'),
    ],
) -> None:
    """Serve the CAS of `[plugins.cas]` until Ctrl-C or SIGTERM. Each caller sends a worker's token."""
    try:
        network = NETWORKS.selected(common.settings.providers).listening(Address.model_validate(listen))
    except ValidationError as e:
        raise SystemExit(f'--listen {listen}: {e}') from e
    local.refuse_other_schema()
    db = Database(common.settings.database_url, actor=common.settings.actor)
    try:
        tokens = Tokens(db)
        net = network.open(NetworkHost(tokens, HomeSecrets(db, SECRETS.selected(common.settings.providers))))
        try:
            listener = net.listener()
        except OSError as e:
            raise SystemExit(f'the CAS does not listen: {e}') from e
        assert listener is not None  # `listening` set the address
        server = Server(cas_server.app(tokens, net, local.cas(), common.settings.home / cas_server.TMP), listener)
        common.console.print(f'cas serving on {server.url}', markup=False, highlight=False, soft_wrap=True)
        sys.stdout.flush()  # a service manager or a test reads the URL from a pipe
        with contextlib.suppress(KeyboardInterrupt):  # uvicorn raises the Ctrl-C again once it has shut down
            server.run()
    finally:
        db.close()
