"""The ``piloop`` command line. ``piloop serve`` runs the daemon. Every other command uses the database directly.

This module only assembles the commands. Each group of commands is its own module in this package.
"""

import os

import cyclopts

from piloop import deprecated
from piloop.cli import (
    agents,
    campaign,
    candidate,
    common,
    daemon,
    database,
    evaluation,
    events,
    idea,
    inbox,
    install,
    measurement,
    memory,
    plugins,
    store,
    token,
)
from piloop.config import ConfigError
from piloop.plugins.runtime.contract import OLD_TAG_ENV, TAG_ENV
from piloop.version import installed

app = cyclopts.App(name='piloop', help='Run autoresearch campaigns with coding agents.', version=installed)
for group in (
    idea.app,
    candidate.app,
    measurement.app,
    evaluation.app,
    inbox.app,
    store.app,
    token.app,
    campaign.app,
    agents.agent_app,
    agents.run_app,
):
    app.command(group)
app.command(campaign.init)
app.command(campaign.history)
app.command(campaign.status)
app.command(events.events)
app.command(campaign.watch_cmd, name='watch')
app.command(campaign.pause)
app.command(campaign.resume)
app.command(store.restore)
app.command(agents.launch)
app.command(agents.prompt)
app.command(agents.attach)
app.command(memory.journal)
app.command(memory.wake)
app.command(daemon.serve)
app.command(daemon.stop)
app.command(daemon.restart)
app.command(daemon.service_unit_cmd, name='service-unit')
app.command(database.sql_cmd, name='sql')
app.command(database.schema)
app.command(database.migrate_cmd, name='migrate')
app.command(install.upgrade)
app.command(install.doctor_cmd, name='doctor')
app.command(plugins.plugins_cmd, name='plugins')
app.command(install.install_skills, name='install-skills')


def main() -> None:
    """Run the CLI. An error prints one message. PILOOP_DEBUG=1 shows the traceback of an unexpected error."""
    deprecated.show_in_cli()
    # what an agent's piloop command starts, a daemon or an evaluation, must not die with the agent. The command itself
    # does: a process's environment as the OS shows it, which the runtime's kill reads, is the one it started with.
    for name in (TAG_ENV, OLD_TAG_ENV):
        os.environ.pop(name, None)
    try:
        app()
    except ConfigError as e:
        raise SystemExit(str(e)) from e
    except Exception as e:
        if common.settings.debug:
            raise
        raise SystemExit(
            f'piloop: {type(e).__name__}: {e}\nrun with PILOOP_DEBUG=1 for the traceback; `piloop doctor` checks the install'
        ) from e
