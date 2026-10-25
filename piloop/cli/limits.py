"""``piloop limits``: list the live limits, or set and unset one while the daemon runs."""

import json
from typing import Annotated

import cyclopts
from pydantic import JsonValue

from piloop.cli import common, local
from piloop.cli.views import limit_line
from piloop.plugins import limits

app = cyclopts.App(name='limits')  # its help is where piloop.cli.main registers it


@app.default
def limits_list() -> None:
    """List each live key: its value, and where it comes from (the default, `config.toml`, or a limit)."""
    with local.session() as s:
        for key in limits.live_keys(s, common.settings.providers, tuple(limits.Origin)):
            common.console.print(limit_line(key), markup=False)


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
    with local.session() as s:
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
    with local.session() as s:
        if not limits.unset_limit(s, key):
            raise SystemExit(f'{key} has no limit: see `piloop limits`')
        s.commit()
    common.console.print(f'{key}: the limit is gone', markup=False)


def _value(text: str) -> JsonValue:
    try:
        return json.loads(text)
    except json.JSONDecodeError:  # not JSON: the text itself, which the parameters then check
        return text
