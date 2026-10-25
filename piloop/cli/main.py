"""The ``piloop`` command line. ``piloop serve`` runs the daemon. ``status``, ``inbox`` and ``events`` ask it.

This module only assembles the commands. Each group of commands is its own module in this package. A command loads
by its import path when it runs, so that the views (``piloop.cli.views``), which open no database, start fast.
"""

import os
import sys

import cyclopts

from piloop import deprecated
from piloop.cli import common, views
from piloop.plugins.runtime.contract import TAG_ENV
from piloop.version import installed

GROUPS = (
    ('idea', 'idea:app', 'Propose work (a seed, a defined idea) and change an idea: its status, links, base or track.'),
    (
        'candidate',
        'candidate:app',
        'Import a starting point, measure a candidate again, keep a regressed result in view, or override a verdict.',
    ),
    ('measurement', 'measurement:app', 'Record a measurement made outside the daemon.'),
    ('eval', 'evaluation:app', 'List or cancel the evaluations that the daemon runs or that wait for a slot.'),
    (
        'store',
        'store:app',
        'The artifact store that archived workspace files live in: what it holds, eviction, checks.',
    ),
    ('token', 'token:app', 'The bearer tokens of `piloop serve` and `piloop cas serve`.'),
    ('agent', 'agents:agent_app', 'Act on one agent.'),
    ('run', 'agents:run_app', 'Act on one run.'),
    ('worker', 'worker:app', 'The workers that run jobs on leases.'),
    ('limits', 'limits:app', 'Change a live key of a provider of the home while the daemon runs.'),
    ('cas', 'cas:app', "This home's CAS: print a blob, or serve the CAS to workers on other machines."),
    ('secret', 'secret:app', 'The secrets that agents get in their environment. No command shows a value.'),
)
"""Each group of commands: its name, its ``App`` in this package, and its help."""
COMMANDS = (
    ('init', 'campaign:init'),
    ('history', 'campaign:history'),
    ('watch', 'campaign:watch_cmd'),
    ('pause', 'control:pause'),
    ('resume', 'control:resume'),
    ('restore', 'store:restore'),
    ('launch', 'agents:launch'),
    ('prompt', 'agents:prompt'),
    ('attach', 'agents:attach'),
    ('journal', 'memory:journal'),
    ('wake', 'memory:wake'),
    ('serve', 'daemon:serve'),
    ('stop', 'daemon:stop'),
    ('restart', 'daemon:restart'),
    ('service-unit', 'daemon:service_unit_cmd'),
    ('sql', 'database:sql_cmd'),
    ('schema', 'database:schema'),
    ('migrate', 'database:migrate_cmd'),
    ('upgrade', 'install:upgrade'),
    ('doctor', 'install:doctor_cmd'),
    ('plugins', 'plugins:plugins_cmd'),
    ('install-skills', 'install:install_skills'),
)
"""Each command outside a group: its name, and its function in this package."""

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
app.command(campaign)
for name, path, help_ in GROUPS:
    app.command(f'piloop.cli.{path}', name=name, help=help_)
for name, path in COMMANDS:
    app.command(f'piloop.cli.{path}', name=name)


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


def main() -> None:
    """Run the CLI. An error prints one message. PILOOP_DEBUG=1 shows the traceback of an unexpected error."""
    deprecated.show_in_cli()
    load_for_help(sys.argv[1:])
    # what an agent's piloop command starts, a daemon or an evaluation, must not die with the agent. The command itself
    # does: a process's environment as the OS shows it, which the runtime's kill reads, is the one it started with.
    os.environ.pop(TAG_ENV, None)
    try:
        app()
    except Exception as e:
        from piloop.config import ConfigError  # only now: a view never loads the configuration

        if isinstance(e, ConfigError):
            raise SystemExit(str(e)) from e
        if common.settings.debug:
            raise
        raise SystemExit(
            f'piloop: {type(e).__name__}: {e}\nrun with PILOOP_DEBUG=1 for the traceback; `piloop doctor` checks the install'
        ) from e
