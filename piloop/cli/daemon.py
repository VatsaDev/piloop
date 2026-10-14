"""The commands that run the daemon: ``serve``, ``stop``, ``restart`` and ``service-unit``."""

import time
from datetime import timedelta
from typing import Annotated

import cyclopts
from sqlalchemy import select

from piloop.cli import common
from piloop.config import config_of
from piloop.daemon.main import Daemon
from piloop.daemon.serve import restart as restart_in_place, running_pid, service_unit, spawn_detached, terminate
from piloop.db.models import Project
from piloop.db.session import SchemaAhead
from piloop.harnesses import DEFAULT_HARNESSES
from piloop.proc import Inherited


def serve(
    *,
    detach: Annotated[
        bool, cyclopts.Parameter(help='Start the daemon in the background. Its output goes to serve.log.')
    ] = False,
) -> None:
    """Run the daemon, which fires the triggers, launches the agents and reaps the agents that end.

    Only one daemon runs per PILOOP_HOME. The file serve.pid holds its lock.
    """
    if detach:
        pid = spawn_detached(common.settings)
        common.console.print(f'daemon started: pid {pid}, log {common.settings.log_file}')
        return
    inherited = Inherited.now()  # before the daemon starts a child of its own
    try:
        daemon = Daemon(common.settings, inherited)
    except SchemaAhead as e:
        raise SystemExit(str(e)) from e
    daemon.serve()


def stop(*, timeout: Annotated[float, cyclopts.Parameter(help='Seconds to wait for the daemon to exit.')] = 30) -> None:
    """Stop the running daemon with SIGTERM, and wait for it to exit.

    The daemon kills the agents that are running, and their runs fail. A run whose worker already left its finding is
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

    Running agents keep running. The daemon stops its ticks, then the same process execs `piloop serve`. The new start
    re-attaches to every agent and finishes their runs. The process keeps its pid and its environment, so systemd,
    launchd or the terminal that started it still holds it. To change the environment of a service unit, use the
    restart of the service manager.
    """
    pid = running_pid(common.settings)
    if pid is None:
        raise SystemExit('no daemon is running')
    common.console.print(f'daemon pid {restarted(pid, timeout)} restarted')


def restarted(pid: int, timeout: float) -> int:
    """Restart the daemon ``pid`` in place and return its pid, or exit when it does not restart in time."""
    new = restart_in_place(common.settings, pid, timeout)
    if new is None:
        raise SystemExit(f'daemon pid {pid} did not restart within {timeout:g}s')
    return new


def service_unit_cmd() -> None:
    """Print a systemd user unit (a launchd plist on macOS) that keeps `piloop serve` running.

    It prints the unit only, and installs nothing. The unit carries the variables that the usage globs of the built-in
    harnesses and of the registered projects' harnesses name.
    """
    harnesses = list(DEFAULT_HARNESSES.values())
    with common.session() as s:
        for project in s.scalars(select(Project)):
            harnesses += config_of(project).harness.values()
    unit = service_unit(common.settings, harnesses)
    common.console.print(unit, end='', markup=False, highlight=False, soft_wrap=True)


def daemon_line() -> str:
    """One line on the daemon: its pid and uptime, or that it is not running."""
    pid = running_pid(common.settings)
    if pid is None:
        return 'daemon: not running'
    up = timedelta(seconds=int(time.time() - common.settings.pid_file.stat().st_mtime))
    return f'daemon: pid {pid}, up {up}'
