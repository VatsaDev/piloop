"""What every command module shares: the console, the settings and the output. It opens no database.

Command modules read ``common.console`` and ``common.settings`` through this module, so that a test replaces each in
one place. What the commands that open the database share is in ``piloop.cli.local``.
"""

import json
import os
from collections.abc import Mapping
from typing import Annotated

import cyclopts
from rich.console import Console

from piloop.actor import Agent
from piloop.api.views import PROTOCOL
from piloop.settings import settings

UNWRAPPED = 1_000_000
"""The width of the output that is not a terminal, unless ``COLUMNS`` sets one: no cell is wrapped there. Agents and
scripts read it, and Rich's default of 80 columns wrapped a table's last column to a word a line. A program that needs
the structure reads ``--json``."""


def output_console() -> Console:
    """Return the console of the commands, on the current ``sys.stdout``, unwrapped when Rich finds no terminal."""
    found = Console()
    if not found.is_terminal and 'COLUMNS' not in os.environ:
        found.width = UNWRAPPED
    return found


def heading(title: str) -> None:
    """Print ``title`` as a rule across a terminal, or as its own line where the output has no width to fill."""
    if console.width == UNWRAPPED:
        console.print(title)
    else:
        console.rule(title)


console = output_console()

CampaignRef = Annotated[str, cyclopts.Parameter(help='The campaign name, or a path inside its repo.')]
HARNESS_HELP = 'Use this harness instead of the rotation.'
JsonFlag = Annotated[
    bool,
    cyclopts.Parameter(
        name='--json', negative=(), help='Print one JSON document on stdout, for a program. Messages go to stderr.'
    ),
]


def print_json(document: Mapping[str, object]) -> None:
    """Print ``document`` on one line of stdout, with the protocol version first. Rich neither wraps nor colors it.

    Raises:
        ValueError: A float in it is not finite, which JSON cannot say. The reader keeps them out of its documents.
    """
    console.out(json.dumps({'protocol': PROTOCOL, **document}, allow_nan=False), highlight=False)


def json_error(code: str, message: str) -> SystemExit:
    """Print an error document on stdout, and return the exit that also says ``message`` on stderr."""
    print_json({'error': {'code': code, 'message': message}})
    return SystemExit(message)


def refuse_agent(verb: str) -> None:
    """Exit when an agent that the daemon launched runs ``piloop <verb>``, which changes the install under it."""
    match settings.actor:
        case Agent():
            raise SystemExit(f'`piloop {verb}` is not for agents (PILOOP_ACTOR={settings.actor})')
