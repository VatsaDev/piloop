"""``piloop token create|list|revoke``: the bearer tokens that workers on other machines call the server with."""

from typing import Annotated

import cyclopts
from rich import box
from rich.table import Table
from sqlalchemy import select

from piloop.cli import common
from piloop.db.models import Token, TokenScope
from piloop.net import tokens

app = cyclopts.App(name='token', help='The bearer tokens of the server that `piloop serve --listen` runs.')


@app.command
def create(name: Annotated[str, cyclopts.Parameter(help='The name of the worker that uses it.')], /) -> None:
    """Make a worker's token and print its secret. Only its hash is kept: copy the secret now."""
    with common.session() as s:
        try:
            secret = tokens.create(s, name, TokenScope.WORKER)
        except tokens.NameTaken as e:
            raise SystemExit(str(e)) from e
        s.commit()
    common.console.print(secret, markup=False, highlight=False, soft_wrap=True)


@app.command(name='list')
def list_tokens() -> None:
    """The tokens: name, scope and when each was made. Never the secrets."""
    table = Table('name', 'scope', 'created', box=box.SIMPLE)
    with common.session() as s:
        for token in s.scalars(select(Token).order_by(Token.name)):
            table.add_row(token.name, token.scope, f'{token.created_at:%Y-%m-%d %H:%M}')
    common.console.print(table)


@app.command
def revoke(name: Annotated[str, cyclopts.Parameter(help='The token to revoke.')], /) -> None:
    """Delete a token: the next call with it gets 401."""
    with common.session() as s:
        try:
            tokens.revoke(s, name)
        except tokens.NoToken as e:
            raise SystemExit(str(e)) from e
        s.commit()
    common.console.print(f'token {name} revoked', markup=False, highlight=False)
