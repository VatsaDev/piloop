"""``piloop token create|list|revoke``, clients of the API: the bearer tokens that other machines call the server with."""

from typing import Annotated

import cyclopts
from rich import box
from rich.table import Table

from piloop.api.client import Client
from piloop.api.contract import Access, TokenBody
from piloop.cli import common
from piloop.cli.client import daemon_http

app = cyclopts.App(name='token', help='The bearer tokens of `piloop serve` and `piloop cas serve`.')


@app.command
def create(
    name: Annotated[str, cyclopts.Parameter(help='The name of the caller that uses it, such as a worker.')],
    /,
    *,
    access: Annotated[Access, cyclopts.Parameter(help='What its caller may do.')] = Access.WORKER,
) -> None:
    """Make a token and print its secret. Only its hash is kept: copy the secret now."""
    with Client.connect(daemon_http()) as client:
        made = client.create_token(TokenBody(name=name, access=access))
    common.console.print(made.secret.get_secret_value(), markup=False, highlight=False, soft_wrap=True)


@app.command(name='list')
def list_tokens() -> None:
    """The tokens: name, access and when each was made. Never the secrets."""
    with Client.connect(daemon_http()) as client:
        listed = client.list_tokens().tokens
    table = Table('name', 'access', 'created', box=box.SIMPLE)
    for token in listed:
        table.add_row(token.name, token.access, f'{token.created_at:%Y-%m-%d %H:%M}')
    common.console.print(table)


@app.command
def revoke(name: Annotated[str, cyclopts.Parameter(help='The token to revoke.')], /) -> None:
    """Delete a token: the next call with it gets 401."""
    with Client.connect(daemon_http()) as client:
        client.revoke_token(name)
    common.console.print(f'token {name} revoked', markup=False, highlight=False)
