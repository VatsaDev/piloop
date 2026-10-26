"""``piloop limits``, a client of the API: list the live limits, or set and unset one while the daemon runs."""

import json
from typing import Annotated

import cyclopts
from pydantic import JsonValue

from piloop.api.client import Client
from piloop.api.contract import LimitBody
from piloop.cli import common
from piloop.cli.client import daemon_http
from piloop.cli.views import limit_line

app = cyclopts.App(name='limits', help='Change a live key of a provider of the home while the daemon runs.')


@app.default
def limits_list() -> None:
    """List each live key: its value, and where it comes from (the default, `config.toml`, or a limit)."""
    with Client.connect(daemon_http()) as client:
        keys = client.limits().keys
    for key in keys:
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
    with Client.connect(daemon_http()) as client:
        client.set_limit(key, LimitBody(value=_value(text), reason=reason))
    common.console.print(f'{key} = {text}: the daemon applies it at once', markup=False)


@app.command(name='unset')
def limits_unset(key: Annotated[str, cyclopts.Parameter(help='The key of the limit.')], /) -> None:
    """Remove a limit: `config.toml` holds again at once."""
    with Client.connect(daemon_http()) as client:
        client.unset_limit(key)
    common.console.print(f'{key}: the limit is gone', markup=False)


def _value(text: str) -> JsonValue:
    try:
        return json.loads(text)
    except json.JSONDecodeError:  # not JSON: the text itself, which the parameters then check
        return text
