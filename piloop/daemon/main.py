"""Assemble and run the daemon: the database, the run executor and the scheduler."""

import os
import signal
import sys
from types import FrameType

from piloop.agents.capture import MIN_ENTIRE, AttachError, entire_version, usable
from piloop.daemon.runner import RunExecutor
from piloop.daemon.scheduler import Scheduler
from piloop.daemon.serve import RESTART_SIGNAL, SECRETS_COMMAND_ENV, SERVE_ARGV, SecretsError, acquire, secrets
from piloop.db.session import Database, SchemaAhead, migrate
from piloop.db.triggers import DAEMON_ACTOR
from piloop.otel import Span, tracer
from piloop.settings import Settings, settings

ACTOR = DAEMON_ACTOR
"""The actor that the daemon's connections record in ``updated_by``. The change-log triggers use this name."""


class Daemon:
    """Everything that a ``piloop serve`` process owns."""

    def __init__(self, cfg: Settings = settings) -> None:
        self.db = Database(cfg.database_url, actor=ACTOR)
        try:
            migrate(self.db.engine)
        except SchemaAhead:
            self.db.close()
            raise
        self.tracer = tracer()
        self.executor = RunExecutor(self.db, self.tracer, cfg.home)
        self.scheduler = Scheduler(self.db, self.executor, self.tracer, cfg.home)
        self._cfg = cfg
        self._restart = False

    def serve(self) -> None:
        """Lock the pid file, reap, then tick until Ctrl-C or SIGTERM.

        On ``RESTART_SIGNAL``, stop the same way, then exec ``piloop serve``. The exec keeps the pid, the environment
        and the service manager's hold on the process. So ``piloop upgrade`` and ``piloop restart`` start the installed
        code under whatever started the daemon: systemd, launchd or a terminal.
        """
        try:
            # Load secrets before the lock. `serve --detach` reports a start when the child holds the lock, so a
            # failing secrets command must fail before that.
            supplied = self._load_secrets()
            # Lock before the main try block: shutdown() must never fail the runs of another daemon.
            lock = acquire(self._cfg)
        except SystemExit:
            self.db.close()  # Only the database is open at this point.
            raise
        previous = signal.signal(signal.SIGTERM, self._on_sigterm)
        previous_restart = signal.signal(RESTART_SIGNAL, self._on_restart)
        try:
            with self.tracer.span('piloop.start', db=self._cfg.database_url, secrets=supplied) as span:
                self._check_entire(span)
                self.scheduler.reap()
            self.scheduler.serve_forever(self._cfg.tick_seconds)
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
        """Treat a service manager's stop as Ctrl-C: the loop ends and ``shutdown`` kills the running workers."""
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
