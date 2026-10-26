"""The commands that run the daemon: ``serve``, ``stop``, ``restart`` and ``service-unit``."""

from typing import Annotated

import cyclopts
from pydantic import ValidationError
from sqlalchemy import select

from piloop.cli import common, local
from piloop.daemon.main import Daemon, preflight
from piloop.daemon.serve import (
    restart as restart_in_place,
    running_pid,
    service_unit,
    spawn_detached,
    terminate,
)
from piloop.db.models import Campaign
from piloop.db.session import BackupFailed, OlderDaemon, SchemaMismatch, unmigratable
from piloop.harnesses import DEFAULT_HARNESSES
from piloop.plugins.host import PluginError
from piloop.plugins.network.contract import Address
from piloop.proc import Inherited


def serve(
    *,
    detach: Annotated[
        bool, cyclopts.Parameter(help='Start the daemon in the background. Its output goes to serve.log.')
    ] = False,
    check: Annotated[
        bool,
        cyclopts.Parameter(
            help='Check that the daemon can start (its plugins, and what config.toml binds), and start nothing.'
        ),
    ] = False,
    listen: Annotated[
        str | None,
        cyclopts.Parameter(
            help='Serve other machines on HOST:PORT ([plugins.network] listen). An address that is not loopback needs'
            ' TLS. Port 0 takes a free port, which `piloop restart` keeps. `piloop status` shows the URL.'
        ),
    ] = None,
) -> None:
    """Run the daemon, which fires the triggers, launches the agents and reaps the agents that end.

    Only one daemon runs per PILOOP_HOME. The file serve.pid holds its lock.
    """
    try:
        address = Address.model_validate(listen) if listen is not None else None
    except ValidationError as e:
        raise SystemExit(f'--listen {listen}: {e}') from e
    if check:
        can_start(address)
        common.console.print('the daemon can start')
        return
    if detach:
        detached(address)
        return
    inherited = Inherited.now()  # before the daemon starts a child of its own
    try:
        daemon = Daemon(common.settings, inherited, address)
    except (OlderDaemon, SchemaMismatch, BackupFailed) as e:
        raise SystemExit(str(e)) from e
    daemon.serve()


def detached(address: Address | None = None) -> None:
    """Start the daemon in the background, and say where its log is. Exit with why when it does not start."""
    pid = spawn_detached(common.settings, address)
    common.console.print(
        f'daemon started: pid {pid}, log {common.settings.log_file}', markup=False, highlight=False, soft_wrap=True
    )


def stop(*, timeout: Annotated[float, cyclopts.Parameter(help='Seconds to wait for the daemon to exit.')] = 30) -> None:
    """Stop the running daemon with SIGTERM, and wait for it to exit.

    The daemon kills the agents that are running, and their runs fail. A run whose implementer already left its finding is
    finished instead: the next daemon scores its commits.
    """
    pid = running_pid(common.settings)
    if pid is None:
        raise SystemExit('no daemon is running')
    if not terminate(common.settings, pid, timeout):
        raise SystemExit(f'daemon pid {pid} did not exit within {timeout:g}s')
    common.console.print(f'daemon pid {pid} stopped')


def restart(
    *, timeout: Annotated[float, cyclopts.Parameter(help='Seconds to wait for the daemon to start again.')] = 30
) -> None:
    """Restart the running daemon on the installed code, in the same process.

    Running agents keep running. The daemon stops its loop, then the same process execs `piloop serve`. The new start
    re-attaches to every agent and finishes their runs. The process keeps its pid and its environment, so systemd,
    launchd or the terminal that started it still holds it. To change the environment of a service unit, use the
    restart of the service manager. A re-attached agent of the subprocess runtime takes no more notes (`piloop prompt`).
    """
    pid = running_pid(common.settings)
    if pid is None:
        raise SystemExit('no daemon is running')
    try:
        can_start()
    except SystemExit as e:
        raise SystemExit(f'daemon pid {pid} not restarted, and it keeps running: {e}') from e
    common.console.print(f'daemon pid {restarted(pid, timeout)} restarted')


def can_start(listen: Address | None = None) -> None:
    """Exit with why the daemon would not start on this code, before anything signals the running one.

    A database that this code cannot migrate (``unmigratable``) would make the restarted daemon exit, with its agents
    still running.
    """
    try:
        preflight(common.settings, listen)
    except PluginError as e:
        raise SystemExit(f'the daemon cannot start: {e}') from e
    if (refused := unmigratable(common.settings.home / 'piloop.db')) is not None:
        raise SystemExit(f'the daemon cannot start: {refused}') from refused


def restarted(pid: int, timeout: float) -> int:
    """Restart the daemon ``pid`` in place and return its pid, or exit when it does not restart in time."""
    new = restart_in_place(common.settings, pid, timeout)
    if new is None:
        raise SystemExit(f'daemon pid {pid} did not restart within {timeout:g}s')
    return new


def service_unit_cmd() -> None:
    """Print a systemd user unit (a launchd plist on macOS) that keeps `piloop serve` running.

    It prints the unit only, and installs nothing. The unit carries the variables that the usage globs of the built-in
    harnesses and of the registered campaigns' harnesses name.
    """
    harnesses = list(DEFAULT_HARNESSES.values())
    with local.session() as s:
        for campaign in s.scalars(select(Campaign)):
            harnesses += campaign.config.harness.values()
    unit = service_unit(common.settings, harnesses)
    common.console.print(unit, end='', markup=False, highlight=False, soft_wrap=True)
