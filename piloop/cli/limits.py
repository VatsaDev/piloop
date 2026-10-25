"""``piloop limits``: list the live limits, or set and unset one while the daemon runs."""

import json
from collections.abc import Collection
from typing import Annotated

import cyclopts
from pydantic import JsonValue
from sqlalchemy.orm import Session

from piloop.cli import common
from piloop.plugins import limits

app = cyclopts.App(name='limits', help='Change a live key of a provider of the home while the daemon runs.')


@app.default
def limits_list() -> None:
    """List each live key: its value, and where it comes from (the default, `config.toml`, or a limit)."""
    with common.session() as s:
        for line in lines(s):
            common.console.print(line, markup=False)


def lines(s: Session, origins: Collection[limits.Origin] = tuple(limits.Origin)) -> list[str]:
    """One line for each live key whose value comes from one of ``origins``: `piloop status` leaves the defaults out."""
    out = []
    for key, value, origin, limit in limits.sources(s, common.settings.providers):
        if origin not in origins:
            continue
        source = f'limit by {limit.actor}: {limit.reason}' if limit is not None else origin
        out.append(f'{key} = {json.dumps(value)}  ({source})')
    return out


@app.command(name='set')
def limits_set(
    assignment: Annotated[str, cyclopts.Parameter(help='`KEY=VALUE`: `executor.pools.serve.evaluations=1`.')],
    /,
    *,
    reason: Annotated[str, cyclopts.Parameter(help='Why: it shows next to the value.')],
) -> None:
    """Set a live key. It wins over `config.toml` at once, and holds until `piloop limits unset`.

    The value is JSON (`1`, `null`, `1.5`), or else text.
    """
    key, eq, text = assignment.partition('=')
    if not eq:
        raise SystemExit(f'{assignment!r}: write KEY=VALUE')
    with common.session() as s:
        try:
            limits.set_limit(
                s, common.settings.providers, key, _value(text), reason=reason, actor=common.settings.actor.signature
            )
        except limits.Refused as e:
            raise SystemExit(str(e)) from e
        s.commit()
    common.console.print(f'{key} = {text}: the daemon applies it at once', markup=False)


@app.command(name='unset')
def limits_unset(key: Annotated[str, cyclopts.Parameter(help='The key of the limit.')], /) -> None:
    """Remove a limit: `config.toml` holds again at once."""
    with common.session() as s:
        if not limits.unset_limit(s, key):
            raise SystemExit(f'{key} has no limit: see `piloop limits`')
        s.commit()
    common.console.print(f'{key}: the limit is gone', markup=False)


def _value(text: str) -> JsonValue:
    try:
        return json.loads(text)
    except json.JSONDecodeError:  # not JSON: the text itself, which the parameters then check
        return text
