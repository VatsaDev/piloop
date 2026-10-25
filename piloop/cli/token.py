"""``piloop token create|list|revoke``: the bearer tokens that other machines call the server with."""

from typing import Annotated

import cyclopts
from rich import box
from rich.table import Table
from sqlalchemy import select

from piloop.api.contract import Access
from piloop.cli import common, local
from piloop.db.models import Token
from piloop.net import tokens

app = cyclopts.App(name='token')  # its help is where piloop.cli.main registers it


@app.command
def create(
    name: Annotated[str, cyclopts.Parameter(help='The name of the caller that uses it, such as a worker.')],
    /,
    *,
    access: Annotated[Access, cyclopts.Parameter(help='What its caller may do.')] = Access.WORKER,
) -> None:
    """Make a token and print its secret. Only its hash is kept: copy the secret now."""
    with local.session() as s:
        try:
            secret = tokens.create(s, name, access)
        except tokens.NameTaken as e:
            raise SystemExit(str(e)) from e
        s.commit()
    common.console.print(secret, markup=False, highlight=False, soft_wrap=True)


@app.command(name='list')
def list_tokens() -> None:
    """The tokens: name, access and when each was made. Never the secrets."""
    table = Table('name', 'access', 'created', box=box.SIMPLE)
    with local.session() as s:
        for token in s.scalars(select(Token).order_by(Token.name)):
            table.add_row(token.name, token.access, f'{token.created_at:%Y-%m-%d %H:%M}')
    common.console.print(table)


@app.command
def revoke(name: Annotated[str, cyclopts.Parameter(help='The token to revoke.')], /) -> None:
    """Delete a token: the next call with it gets 401."""
    with local.session() as s:
        try:
            tokens.revoke(s, name)
        except tokens.NoToken as e:
            raise SystemExit(str(e)) from e
        s.commit()
    common.console.print(f'token {name} revoked', markup=False, highlight=False)
