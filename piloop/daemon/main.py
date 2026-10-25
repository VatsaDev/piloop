"""Assemble and run the daemon: the database, the run controller and the scheduler."""

import os
import signal
import sys
from functools import partial
from types import FrameType

from piloop import deprecated
from piloop.actor import System
from piloop.campaign.archive import ManifestNotMigrated, migrate_manifests
from piloop.daemon.api import Handlers, daemon_state, with_api
from piloop.daemon.archiver import ArchivePolicy, Archiving
from piloop.daemon.run_controller import RunController
from piloop.daemon.scheduler import Scheduler
from piloop.daemon.serve import (
    RESTART_SIGNAL,
    SECRETS_COMMAND_ENV,
    SecretsError,
    acquire,
    running_pid,
    secrets,
    serve_argv,
)
from piloop.db.models import utcnow
from piloop.db.session import BackupFailed, Database, OlderDaemon, SchemaMismatch, migrate
from piloop.net.server import Server, app, local_listener, url
from piloop.net.tokens import Tokens
from piloop.otel import Span, tracer
from piloop.plugins import host as plugins
from piloop.plugins.cas import contract as cas
from piloop.plugins.executor.contract import EXECUTORS
from piloop.plugins.network import contract as network
from piloop.plugins.secrets.contract import SECRETS, HomeSecrets
from piloop.proc import Inherited
from piloop.reports.feed import iso
from piloop.runtime.factory import make_runtime, runtimes
from piloop.settings import CONFIG_FILE, Settings, settings
from piloop.version import code_digest

ACTOR = System.DAEMON
"""The actor that the daemon's connections record in ``updated_by``. The change-log triggers use this name."""


def bound(cfg: Settings) -> list[plugins.Capability[plugins.Parameters]]:
    """The capabilities that the daemon uses: every one, but one of the home only when ``config.toml`` selects it.

    A broken package of a capability that nothing selects does not stop the daemon (``unbound_failures``).
    """

    def used(c: plugins.Capability[plugins.Parameters]) -> bool:
        match c.choice:
            case plugins.Choice(scope=plugins.Scope.HOME, default=default):
                return default is not None or c.name in cfg.plugins
            case _:
                return True

    return [c for c in plugins.declared() if used(c)]


def unbound_failures(cfg: Settings) -> dict[plugins.Capability[plugins.Parameters], str]:
    """The capabilities that the daemon does not use and that do not load, with why. ``piloop doctor`` warns."""
    failed: dict[plugins.Capability[plugins.Parameters], str] = {}
    for capability in plugins.declared():
        if capability not in bound(cfg):
            try:
                plugins.compose(capability)
            except plugins.PluginError as e:
                failed[capability] = str(e)
    return failed


def preflight(cfg: Settings, listen: network.Address | None = None) -> network.NetworkConfig:
    """Compose the capabilities that the daemon uses (``bound``), check what ``config.toml`` selects, return the network.

    The network has ``listen`` (``piloop serve --listen``) in place of its own.

    ``piloop serve --check`` runs this, and ``piloop restart`` and ``piloop upgrade`` run it before they restart the
    daemon. ``piloop.toml`` is checked when it loads (``piloop init``).

    Raises:
        PluginError: A provider does not compose, or ``config.toml`` selects a provider that there is not, or one
            that refuses its parameters (``[plugins]``, ``[runtimes]``), or the network refuses ``listen``.
    """
    plugins.compose(*bound(cfg))
    try:
        runtimes(cfg)
        return network.NETWORKS.selected(cfg.providers).listening(listen)
    except ValueError as e:
        raise plugins.PluginError(f'{cfg.home / CONFIG_FILE}: {e}') from e


class Daemon:
    """Everything that a ``piloop serve`` process owns."""

    def __init__(
        self, cfg: Settings = settings, inherited: Inherited | None = None, listen: network.Address | None = None
    ) -> None:
        """``listen``: where the server listens (``piloop serve --listen``), in place of ``[plugins.network] listen``."""
        try:
            served = preflight(cfg, listen)
        except plugins.PluginError as e:
            raise SystemExit(f'the daemon does not start: {e}') from e
        self._inherited = inherited or Inherited(set())
        """The children an in-place restart handed over (``piloop.proc.Inherited``): reaped as each one exits."""
        self.db = Database(cfg.database_url, actor=ACTOR)
        self._cfg = cfg
        self._argv = serve_argv(listen)
        """What an in-place restart execs."""
        self._address = served.listen
        """Where the server listens, as asked."""
        self._tokens = Tokens(self.db)
        self.network = served.open(
            network.NetworkHost(self._tokens, HomeSecrets(self.db, SECRETS.selected(cfg.providers)))
        )
        self._servers: list[Server] = []
        try:
            # Before the remote opens: its credentials can come from the secrets. And before the lock, because
            # `serve --detach` reports a start when the child holds it, so a failing secrets command must fail first.
            self._supplied = self._load_secrets()
        except SystemExit:
            self.db.close()
            raise
        self._cas = cas.CAS.selected(cfg.providers).open(cfg.home)
        try:  # after the CAS opens: a revision may move data into it
            self._backup = migrate(
                self.db.engine, cas=self._cas, backups=cfg.migration_backups, daemon=running_pid(cfg)
            )
        except OlderDaemon, SchemaMismatch, BackupFailed:
            self.db.close()
            raise
        self.tracer = tracer()
        opened = partial(make_runtime, cfg=cfg)
        self.controller = RunController(
            self.db,
            self.tracer,
            cfg.home,
            executor=EXECUTORS.selected(cfg.providers),
            cas=self._cas,
            reserve=cfg.storage.reserve,
            runtime_factory=opened,
            network=self.network,
            secrets=SECRETS.selected(cfg.providers),
        )
        self.scheduler = Scheduler(
            self.db,
            self.controller,
            self.tracer,
            cfg.home,
            archiving=Archiving(self._cas, ArchivePolicy.of(cfg.storage)),
            runtime_factory=opened,
        )
        self._restart = False

    def serve(self) -> None:
        """Lock the pid file, reap, then run the loop until Ctrl-C or SIGTERM.

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
        self._cfg.code_file.write_text(code_digest())
        previous = signal.signal(signal.SIGTERM, self._on_sigterm)
        previous_restart = signal.signal(RESTART_SIGNAL, self._on_restart)
        try:
            with self.tracer.span(
                'piloop.start',
                db=self._cfg.database_url,
                secrets=self._supplied,
                backup=None if self._backup is None else str(self._backup),
            ) as span:
                self._listen(span)
                self._migrate_manifests(span)
                self.scheduler.reap()
                self._inherited.watch()
            self.scheduler.serve()
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
                    os.execv(self._argv[0], self._argv)
            # Only for tidiness. The lock, not the file, tells which daemon runs.
            self._cfg.pid_file.unlink(missing_ok=True)
            self._cfg.code_file.unlink(missing_ok=True)
            os.close(lock)

    @deprecated.until('0.5.0', 'secrets_command: agents take their secrets from [plugins.secrets]; delete this method')
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

    def _migrate_manifests(self, span: Span) -> None:
        """Store the archives' manifests of piloop 0.2 again in the new format, or record why not on the start span.

        A failure does not stop the start: an in-place restart must not leave its agents without a daemon, and the
        store still reads the old manifests. The next start, or ``piloop migrate``, tries again.
        """
        try:
            span.set(manifests_migrated=migrate_manifests(self.db, self._cas))
        except ManifestNotMigrated as e:
            span.error(e)

    def _listen(self, span: Span) -> None:
        """Serve the API on the home's Unix socket, and on the network's listener when it has one.

        The network's URL goes on the start span. An in-place restart listens on the port that port 0 took, so a worker
        that has the URL keeps its server.
        """
        try:
            local = local_listener(self._cfg.socket_file)
        except OSError as e:
            raise SystemExit(f'the daemon does not listen on {self._cfg.socket_file}: {e}') from e
        try:
            listener = self.network.listener()
        except OSError as e:
            local.socket.close()
            raise SystemExit(f'the daemon does not listen: {e}') from e
        listeners = [local] if listener is None else [local, listener]
        listen = None if listener is None else url(listener)
        api = Handlers(self.db, self._cfg, daemon_state(iso(utcnow()), listen))
        routes = self.controller.executor.routes()
        # one app for each server: a server's shutdown ends the long-polls of its own app
        self._servers = [Server(with_api(app(self._tokens, self.network, routes), api), each) for each in listeners]
        for server in self._servers:
            server.start()
        if listener is not None:
            assert self._address is not None  # a network binds nothing without ``listen``
            if self._address.port == 0:
                port = listener.socket.getsockname()[1]
                self._argv = serve_argv(self._address.model_copy(update={'port': port}))
            span.set(listen=listen)

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
            for server in self._servers:
                server.close()
            self._cfg.socket_file.unlink(missing_ok=True)
        self.controller.close()
        self.tracer.shutdown()
        self.db.close()
        plugins.close()
