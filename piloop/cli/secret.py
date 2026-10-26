"""``piloop secret set|list|rm``: the secrets that agents get in their environment. No command shows a value."""

import getpass
import sys
from typing import Annotated

import cyclopts
from pydantic import SecretStr, TypeAdapter, ValidationError
from rich import box
from rich.table import Table
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.cli import common, local
from piloop.db.models import Campaign
from piloop.mcp import EnvName
from piloop.plugins.secrets import contract as secrets

app = cyclopts.App(name='secret', help='The secrets that agents get in their environment. No command shows a value.')

Name = Annotated[str, cyclopts.Parameter(help='The name of the secret: the variable that agents get it in.')]
CampaignOption = Annotated[
    str | None,
    cyclopts.Parameter(help="The campaign whose secret it is, over the home's of the same name. Unset: the home's."),
]
ENV_NAME: TypeAdapter[str] = TypeAdapter(EnvName)


@app.command(name='set')
def secret_set(name: Name, /, *, campaign: CampaignOption = None) -> None:
    """Set a secret, in place of any, from stdin: `printf %s "$KEY" | piloop secret set EXA_API_KEY`.

    On a terminal, it asks for the value and does not show it. Agents get the value from their next start. An agent
    sets only its own campaign's secrets.
    """
    variable = _name(name)
    value = getpass.getpass(f'{variable}: ') if sys.stdin.isatty() else sys.stdin.read().removesuffix('\n')
    if not value:
        raise SystemExit(f'{variable}: the value is empty')
    with local.session() as s:
        owner = _owner(s, campaign)
        try:
            secrets.set_secret(s, _provider(), common.settings.actor, owner, variable, SecretStr(value))
        except secrets.Denied as e:
            raise SystemExit(str(e)) from e
        s.commit()
        whose = _whose(s, owner)
    common.console.print(f'{variable} set for {whose}', markup=False, highlight=False)


@app.command(name='list')
def secret_list() -> None:
    """The secrets: the name, whose it is, and who set it last and when. Never a value."""
    table = Table('name', 'of', 'set', 'by', box=box.SIMPLE)
    with local.session() as s:
        names = {row.id: row.name for row in s.execute(select(Campaign.id, Campaign.name))}
        for found in secrets.listed(s, _provider()):
            owner = found.secret.owner
            at = f'{found.set_at:%Y-%m-%d %H:%M}' if found.set_at else ''
            table.add_row(
                found.secret.name,
                'home' if owner is None else names[owner],
                at,
                found.set_by.signature if found.set_by else '',
            )
    common.console.print(table)


@app.command(name='rm')
def secret_rm(name: Name, /, *, campaign: CampaignOption = None) -> None:
    """Delete a secret. An agent that starts after it no longer gets it."""
    variable = _name(name)
    with local.session() as s:
        owner = _owner(s, campaign)
        whose = _whose(s, owner)
        try:
            gone = secrets.remove_secret(s, _provider(), common.settings.actor, owner, variable)
        except secrets.Denied as e:
            raise SystemExit(str(e)) from e
        if not gone:
            raise SystemExit(f'{whose} has no secret {variable}: see `piloop secret list`')
        s.commit()
    common.console.print(f'{variable} of {whose} deleted', markup=False, highlight=False)


def _name(name: str) -> str:
    try:
        return ENV_NAME.validate_python(name)
    except ValidationError as e:
        raise SystemExit(
            f'{name!r} is not an environment variable name: letters, digits and _, not first a digit'
        ) from e


def _provider() -> secrets.SecretsProvider:
    return secrets.SECRETS.selected(common.settings.providers)


def _owner(s: Session, campaign: str | None) -> secrets.Owner:
    return None if campaign is None else local.campaign(s, campaign).id


def _whose(s: Session, owner: secrets.Owner) -> str:
    return 'the home' if owner is None else f'campaign {s.get_one(Campaign, owner).name}'
