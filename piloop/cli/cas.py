"""``piloop cas``: ``get`` prints a file of this home's CAS, and ``serve`` serves the CAS to workers on other machines."""

import contextlib
import sys
from typing import Annotated

import cyclopts
from covalence import IntegrityError
from pydantic import TypeAdapter, ValidationError

from piloop.cli import common, local
from piloop.db.session import Database
from piloop.net import cas_server
from piloop.net.server import Server
from piloop.net.tokens import Tokens
from piloop.plugins.cas.contract import BlobAddress
from piloop.plugins.network.contract import NETWORKS, Address, NetworkHost
from piloop.plugins.secrets.contract import SECRETS, HomeSecrets
from piloop.store import chunks
from piloop.store.tree import Missing

app = cyclopts.App(name='cas', help="This home's CAS: print a blob, or serve the CAS to workers on other machines.")
ADDRESS: TypeAdapter[BlobAddress] = TypeAdapter(BlobAddress)


@app.command
def get(cid: Annotated[str, cyclopts.Parameter(help='The address of the file.')], /) -> None:
    """Print a file of the CAS of `[plugins.cas]` to stdout: a blob as stored, or a chunked file whole.

    An agent's session file (`agent_sessions.session_file`) is a chunked file.
    """
    try:
        address = ADDRESS.validate_python(cid)
    except ValidationError as e:
        raise SystemExit(f'{cid} is not a blob address: {e.errors()[0]["msg"]}') from e
    try:
        found = chunks.read(local.cas(), address)
    except IntegrityError as e:
        raise SystemExit(f'{address}: the bytes in the CAS do not match it') from e
    except (Missing, ValueError) as e:  # a part gone, or a value that is not a chunked file
        raise SystemExit(f'{address} is not a file that the CAS holds whole: {e}') from e
    if found is None:
        raise SystemExit(f'the CAS has no blob {address}')
    sys.stdout.buffer.write(found)


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
