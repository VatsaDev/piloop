"""Assemble and run the daemon: the database, the run executor and the scheduler."""

import os
import signal
import sys
from types import FrameType

from piloop.agents.capture import MIN_ENTIRE, AttachError, entire_version, usable
from piloop.daemon.archiver import ArchivePolicy, Archiving
from piloop.daemon.run_executor import RunExecutor
from piloop.daemon.scheduler import Scheduler
from piloop.daemon.serve import (
    RESTART_SIGNAL,
    SECRETS_COMMAND_ENV,
    SERVE_ARGV,
    SecretsError,
    acquire,
    running_pid,
    secrets,
)
from piloop.db.session import BackupFailed, Database, OlderDaemon, SchemaMismatch, migrate
from piloop.db.triggers import DAEMON_ACTOR
from piloop.otel import Span, tracer
from piloop.plugins import host as plugins
from piloop.proc import Inherited
from piloop.settings import CONFIG_FILE, Settings, settings
from piloop.store.config import open_store

ACTOR = DAEMON_ACTOR
"""The actor that the daemon's connections record in ``updated_by``. The change-log triggers use this name."""


def bound(cfg: Settings) -> list[plugins.Activatable]:
    """The capabilities that the daemon uses: every one, but one that ``config.toml`` selects only when it does.

    A broken package of a capability that nothing selects does not stop the daemon (``unbound_failures``).
    """
    home = cfg.model_dump(mode='json')
    return [
        c
        for c in plugins.declared()
        if c.selection is None or c.selection.scope == 'campaign' or plugins.lookup(home, c.selection.key) is not None
    ]


def unbound_failures(cfg: Settings) -> dict[plugins.Activatable, str]:
    """The capabilities that the daemon does not use and that do not load, with why. ``piloop doctor`` warns."""
    failed: dict[plugins.Activatable, str] = {}
    for capability in plugins.declared():
        if capability not in bound(cfg):
            try:
                plugins.compose(capability)
            except plugins.PluginError as e:
                failed[capability] = str(e)
    return failed


def preflight(cfg: Settings) -> None:
    """Compose the capabilities that the daemon uses (``bound``), then check what ``config.toml`` selects.

    ``piloop serve --check`` runs this, and ``piloop restart`` and ``piloop upgrade`` run it before they restart the
    daemon. ``piloop.toml`` is checked when it loads (``piloop init``).

    Raises:
        PluginError: A provider does not compose, or ``config.toml`` selects a provider that there is not, or one
            that refuses its parameters.
    """
    plugins.compose(*bound(cfg))
    home = cfg.model_dump(mode='json')
    for capability in bound(cfg):
        try:
            plugins.check_selection(capability, home, 'home')
        except ValueError as e:
            raise plugins.PluginError(f'{cfg.home / CONFIG_FILE}: {e}') from e


class Daemon:
    """Everything that a ``piloop serve`` process owns."""

    def __init__(self, cfg: Settings = settings, inherited: Inherited | None = None) -> None:
        try:
            preflight(cfg)
        except plugins.PluginError as e:
            raise SystemExit(f'the daemon does not start: {e}') from e
        self._inherited = inherited or Inherited(set())
        """The children an in-place restart handed over (``piloop.proc.Inherited``): reaped on every tick."""
        self.db = Database(cfg.database_url, actor=ACTOR)
        try:
            self._backup = migrate(self.db.engine, backups=cfg.migration_backups, daemon=running_pid(cfg))
        except OlderDaemon, SchemaMismatch, BackupFailed:
            self.db.close()
            raise
        self._cfg = cfg
        try:
            # Before the remote opens: its credentials can come from the secrets. And before the lock, because
            # `serve --detach` reports a start when the child holds it, so a failing secrets command must fail first.
            self._supplied = self._load_secrets()
        except SystemExit:
            self.db.close()
            raise
        self.tracer = tracer()
        self.executor = RunExecutor(self.db, self.tracer, cfg.home, max_evaluations=cfg.max_evaluations)
        self.scheduler = Scheduler(
            self.db,
            self.executor,
            self.tracer,
            cfg.home,
            archiving=Archiving(open_store(cfg), ArchivePolicy.of(cfg.storage)),
        )
        self._restart = False

    def serve(self) -> None:
        """Lock the pid file, reap, then tick until Ctrl-C or SIGTERM.

        On ``RESTART_SIGNAL``, stop the same way, then exec ``piloop serve``. The exec keeps the pid, the environment
        and the service manager's hold on the process. So ``piloop upgrade`` and ``piloop restart`` start the installed
        code under whatever started the daemon: systemd, launchd or a terminal.
        """
        try:
            # Lock before the main try block: shutdown() must never fail the runs of another daemon.
            lock = acquire(self._cfg)
        except SystemExit:
            self.db.close()  # Only the database is open at this point.
            raise
        previous = signal.signal(signal.SIGTERM, self._on_sigterm)
        previous_restart = signal.signal(RESTART_SIGNAL, self._on_restart)
        try:
            with self.tracer.span(
                'piloop.start',
                db=self._cfg.database_url,
                secrets=self._supplied,
                backup=None if self._backup is None else str(self._backup),
            ) as span:
                self._check_entire(span)
                self.scheduler.reap()
                span.set(inherited_reaped=self._inherited.reap())
            self.scheduler.serve_forever(self._cfg.tick_seconds, also=self._inherited.reap)
        except KeyboardInterrupt:
            pass
        finally:
            try:
                self.shutdown()
            finally:
                signal.signal(signal.SIGTERM, previous)
                signal.signal(RESTART_SIGNAL, previous_restart)
                if self._restart:
                    # Exec even after a failed shutdown. Otherwise its run threads keep this process, the old code
                    # and the lock alive. The reap in the new code fails the runs that this one could not stop.
                    # The exec releases the lock, because Python's descriptors are close-on-exec.
                    sys.stdout.flush()  # exec discards Python's buffers.
                    sys.stderr.flush()
                    os.execv(SERVE_ARGV[0], SERVE_ARGV)
            # Only for tidiness. The lock, not the file, tells which daemon runs.
            self._cfg.pid_file.unlink(missing_ok=True)
            os.close(lock)

    def _load_secrets(self) -> str | None:
        """Add the secrets command's output to this process's environment, and return the variable names.

        Every child process inherits the environment. A failing command stops the start. After an in-place restart,
        the environment still has the previous values. A changed key is overwritten, but a removed key stays until
        the service manager restarts the daemon.
        """
        if self._cfg.secrets_command is None:
            return None
        try:
            values = secrets(self._cfg.secrets_command)
        except SecretsError as e:
            raise SystemExit(f'{SECRETS_COMMAND_ENV} ({self._cfg.secrets_command}) failed: {e}') from e
        os.environ.update(values)
        return ','.join(sorted(values))

    def _check_entire(self, span: Span) -> None:
        """Record Entire's version on the start span, or an event that says why no session will be attached."""
        try:
            version = entire_version()
        except AttachError as e:
            span.event('entire.missing', error=str(e))
            return
        self.executor.launcher.entire_version = version
        if not usable(version):
            span.event('entire.too_old', version=version, min=MIN_ENTIRE)
            return
        span.set(entire=version)

    def _on_sigterm(self, signum: int, frame: FrameType | None) -> None:
        """Treat a service manager's stop as Ctrl-C: the loop ends and ``shutdown`` kills the running agents."""
        self.scheduler.interrupt()

    def _on_restart(self, signum: int, frame: FrameType | None) -> None:
        self._restart = True
        self.scheduler.interrupt()

    def shutdown(self, timeout: float = 30) -> None:
        """Stop the scheduler and flush traces. An in-place restart hands the agents over. Any other stop kills them."""
        with self.tracer.span('piloop.shutdown'):
            (self.scheduler.hand_over if self._restart else self.scheduler.stop)(timeout)
        self.tracer.shutdown()
        self.db.close()
        plugins.close()
