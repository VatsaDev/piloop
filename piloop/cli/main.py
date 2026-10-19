"""The ``piloop`` command line. ``piloop serve`` runs the daemon. Every other command uses the database directly.

This module only assembles the commands. Each group of commands is its own module in this package.
"""

import cyclopts

from piloop import deprecated
from piloop.cli import (
    agents,
    candidate,
    common,
    daemon,
    database,
    evaluation,
    idea,
    inbox,
    install,
    memory,
    project,
    store,
)
from piloop.cli.version import installed
from piloop.config import ConfigError

app = cyclopts.App(name='piloop', help='Run autoresearch campaigns with coding agents.', version=installed)
for group in (idea.app, candidate.app, evaluation.app, inbox.app, store.app):
    app.command(group)
app.command(project.init)
app.command(project.history)
app.command(project.status)
app.command(project.watch_cmd, name='watch')
app.command(project.pause)
app.command(project.resume)
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
app.command(install.install_skills, name='install-skills')


def main() -> None:
    """Run the CLI. An error prints one message. PILOOP_DEBUG=1 shows the traceback of an unexpected error."""
    deprecated.show_in_cli()
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
