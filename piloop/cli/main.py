"""The ``piloop`` command line. ``piloop serve`` runs the daemon. ``status``, ``inbox`` and ``events`` ask it.

This module only assembles the commands. Each group of commands is its own module in this package. A command loads
by its import path when it runs (``COMMANDS``), so that the views (``piloop.cli.views``), which open no database, start
fast.
"""

import os
import sys
from collections.abc import Callable, Generator
from contextlib import contextmanager

import cyclopts
from pydantic import TypeAdapter

from piloop import deprecated
from piloop.cli import common, views
from piloop.plugins.host import PluginError, Slot
from piloop.plugins.runtime.contract import TAG_ENV
from piloop.version import installed

COMMANDS = Slot('commands', TypeAdapter(Callable[..., object]), 'command')
"""The commands that load by their import path when they run, piloop's own included: a cyclopts ``App`` for a group,
else a function. A package adds one with an entry point: ``<name> = "<module>:<attribute>"``."""

app = cyclopts.App(name='piloop', help='Run autoresearch campaigns with coding agents.', version=installed)
app.command(views.status)
app.command(views.events)
inbox = cyclopts.App(
    name='inbox', help='Read and acknowledge what the agents address to you: decisions, escalations and findings.'
)
inbox.default(views.inbox_list)
inbox.command(views.inbox_show, name='show')
inbox.command(views.inbox_wait, name='wait')
inbox.command('piloop.cli.inbox:inbox_ack', name='ack')
inbox.command('piloop.cli.inbox:inbox_reply', name='reply')
app.command(inbox)
campaign = cyclopts.App(name='campaign', help='Act on a campaign as a whole.')
campaign.command('piloop.cli.campaign:parent', name='parent')
campaign.command('piloop.cli.campaign:tree', name='tree')
campaign.command('piloop.cli.control:stop', name='stop')
campaign.command('piloop.cli.control:retire', name='retire')
app.command(campaign)


def add_commands(to: cyclopts.App) -> None:
    """Add each command of ``COMMANDS`` to ``to`` by its path, which imports nothing.

    Raises:
        PluginError: Two entry points have one name, or one has the name of a command of ``to``.
    """
    for name, path in COMMANDS.paths().items():
        if name in to:
            raise PluginError(
                f'entry point {name} = {path} in {COMMANDS.group}: {name!r} is a command of piloop already'
            )
        to.command(path, name=name)


add_commands(app)


def load_for_help(argv: list[str]) -> None:
    """Load the commands that a help lists: its line on each command is that command's docstring, in its module."""
    if argv and not set(argv) & set(app.help_flags):
        return
    listed = app
    for token in argv:
        if token in app.help_flags or token not in listed:
            break
        listed = listed[token]
    for name in listed:
        listed[name]


@contextmanager
def answered() -> Generator[None]:
    """Exit with the message of an error that says what to do: no daemon, a refusal of the daemon, a configuration."""
    try:
        yield
    except Exception as e:
        # only now: a view never loads the configuration, and a local command no client
        from piloop.api.client import NoDaemon, Refused
        from piloop.config import ConfigError

        if isinstance(e, NoDaemon | Refused | ConfigError):
            raise SystemExit(str(e)) from e
        raise


def main() -> None:
    """Run the CLI. An error prints one message. PILOOP_DEBUG=1 shows the traceback of an unexpected error."""
    deprecated.show_in_cli()
    load_for_help(sys.argv[1:])
    # what an agent's piloop command starts, a daemon or an evaluation, must not die with the agent. The command itself
    # does: a process's environment as the OS shows it, which the runtime's kill reads, is the one it started with.
    os.environ.pop(TAG_ENV, None)
    try:
        with answered():
            app()
    except Exception as e:
        if common.settings.debug:
            raise
        raise SystemExit(
            f'piloop: {type(e).__name__}: {e}\nrun with PILOOP_DEBUG=1 for the traceback; `piloop doctor` checks the install'
        ) from e
