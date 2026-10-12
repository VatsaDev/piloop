"""The daemon as a process: the locked pid file, the secrets, detached start, stop and the service unit."""

import errno
import fcntl
import os
import plistlib
import re
import shlex
import signal
import subprocess
import sys
import time
import warnings

from piloop.proc import child_env, kill_group
from piloop.settings import Settings

SERVE_ARGV = [sys.executable, '-m', 'piloop', 'serve']
"""The command that starts the daemon. It uses the current interpreter, however the CLI was invoked."""
POLL_SECONDS = 0.1
RESTART_SIGNAL = signal.SIGUSR1
"""Stop as on SIGTERM, then exec ``piloop serve`` in place.

Not SIGHUP, because a closing terminal sends SIGHUP to a daemon in the foreground."""
SECRETS_COMMAND_ENV = 'PILOOP_SECRETS_COMMAND'
"""The environment variable for ``Settings.secrets_command``. ``piloop doctor`` reads it from the daemon."""
SECRETS_TIMEOUT_SECONDS = 60
"""The seconds that the secrets command can take before the daemon refuses to start."""
SECRET_NAME = re.compile(r'[A-Za-z_][A-Za-z0-9_]*')
SYSTEMD_UNIT = """[Unit]
Description=piloop autoresearch daemon

[Service]
ExecStart={exec_start}
Restart=on-failure
{environment}

[Install]
WantedBy=default.target
"""


def _open_pid_file(cfg: Settings, flags: int) -> int:
    """Open the pid file and return its descriptor. Refuse a symlink or a file that another user owns.

    Otherwise, if the home directory has permissive modes, anyone can choose the pid that ``piloop stop`` sends
    SIGTERM to.
    """
    cfg.home.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        fd = os.open(cfg.pid_file, flags | os.O_NOFOLLOW, 0o600)
    except OSError as e:
        if e.errno == errno.ELOOP:
            raise SystemExit(f'{cfg.pid_file} is a symlink; refusing to trust it') from e
        raise
    if os.fstat(fd).st_uid != os.getuid():
        os.close(fd)
        raise SystemExit(f'{cfg.pid_file} is not owned by uid {os.getuid()}; refusing to trust it')
    return fd


def _locked(fd: int) -> bool:
    """Try to take the daemon's lock without blocking. Return True if another process holds it."""
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return True
    return False


def acquire(cfg: Settings) -> int:
    """Become the only daemon of this home: lock the pid file for the life of the process, then write the pid.

    The kernel releases the lock when the process dies. So a daemon runs if and only if the file is locked. There
    are no stale files, no pid reuse and no identity checks. The caller must keep the returned descriptor open.
    flock is advisory and works on one host only. A network filesystem may not support it, but ``PILOOP_HOME`` is
    local.
    """
    fd = _open_pid_file(cfg, os.O_RDWR | os.O_CREAT)
    if _locked(fd):
        pid = os.read(fd, 32).decode().strip()
        os.close(fd)
        raise SystemExit(_already_running(cfg, pid))
    os.ftruncate(fd, 0)
    os.write(fd, str(os.getpid()).encode())
    os.fsync(fd)
    return fd


def _already_running(cfg: Settings, pid: object) -> str:
    return f'a daemon is already running (pid {pid}, {cfg.pid_file}). Run `piloop stop` first.'


def running_pid(cfg: Settings) -> int | None:
    """Return the pid of the daemon that holds the lock, or None. The file's contents mean nothing without the lock."""
    try:
        fd = _open_pid_file(cfg, os.O_RDONLY)
    except FileNotFoundError:
        return None
    try:
        if not _locked(fd):
            return None
        text = os.read(fd, 32).decode().strip()
    finally:
        os.close(fd)
    return int(text) if text.isdigit() else None


class SecretsError(Exception):
    """The secrets command failed or printed something other than ``NAME=value`` lines. It never contains a value."""


def secrets(command: str) -> dict[str, str]:
    """Run the secrets command with ``sh -c`` and parse its output: one ``NAME=value`` line per variable.

    Blank lines are skipped. The command writes its stderr to the daemon's stderr. Its stdout is parsed here and
    never shown. Errors name the line or the variable, never a value.

    Raises:
        SecretsError: The command did not start, timed out, exited non-zero or printed nothing. Or it printed a line
            that is not ``NAME=value``, an empty value, a value with whitespace, or one name twice.
    """
    try:
        proc = subprocess.Popen(['sh', '-c', command], env=child_env(), stdout=subprocess.PIPE, start_new_session=True)
    except OSError as e:
        raise SecretsError(f'could not start: {e}') from e
    with proc:
        try:
            out, _ = proc.communicate(timeout=SECRETS_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired as e:
            # Kill the children too. A child that holds the pipe open blocks the read below.
            kill_group(proc)
            proc.communicate()
            raise SecretsError(f'timed out after {SECRETS_TIMEOUT_SECONDS}s') from e
    if proc.returncode != 0:
        raise SecretsError(f'exit {proc.returncode}')
    values: dict[str, str] = {}
    for number, line in enumerate(out.decode(errors='replace').split('\n'), 1):
        if not line:
            continue
        name, eq, value = line.partition('=')
        if not eq or not SECRET_NAME.fullmatch(name):
            raise SecretsError(f'line {number} is not NAME=value')
        if not value:
            raise SecretsError(f'{name} is empty')
        if any(c.isspace() for c in value):
            raise SecretsError(f'{name} contains whitespace')
        if name in values:
            raise SecretsError(f'{name} is printed twice')
        values[name] = value
    if not values:
        raise SecretsError('printed no NAME=value line')
    return values


def spawn_detached(cfg: Settings) -> int:
    """Start ``piloop serve`` in its own session, append its output to the log file, and return its pid.

    The child takes the lock itself (``acquire``). This function waits until the child holds the lock. If the child
    was refused or died, it exits with the reason. Nothing ever waits for the daemon process, and ``Popen``'s
    finaliser reports that as a leak. The handle is dropped on purpose, with that one warning silenced.
    """
    cfg.home.mkdir(mode=0o700, parents=True, exist_ok=True)
    with cfg.log_file.open('ab') as log:
        daemon = subprocess.Popen(
            SERVE_ARGV,
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env=child_env() | {'PILOOP_HOME': str(cfg.home)},
        )
    while running_pid(cfg) != daemon.pid:
        if daemon.poll() is not None:
            if (holder := running_pid(cfg)) is not None:
                raise SystemExit(_already_running(cfg, holder))
            raise SystemExit(
                f'daemon pid {daemon.pid} exited with {daemon.returncode} before taking {cfg.pid_file}; see {cfg.log_file}'
            )
        time.sleep(POLL_SECONDS)
    pid = daemon.pid
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', ResourceWarning)  # Nothing waits for the daemon, as intended.
        del daemon
    return pid


def terminate(cfg: Settings, pid: int, timeout: float) -> bool:
    """Send SIGTERM to the lock holder and wait up to ``timeout`` seconds for it to release the lock."""
    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + timeout
    while running_pid(cfg) is not None:
        if time.monotonic() > deadline:
            return False
        time.sleep(POLL_SECONDS)
    return True


def restart(cfg: Settings, pid: int, timeout: float) -> int | None:
    """Ask the lock holder to restart in place. Return the pid that holds the lock again, or None after ``timeout``.

    The stop takes as long as a stop on SIGTERM. A rewritten pid file under a held lock means the new daemon started.
    The pid is usually the same. It differs if an old daemon without the signal handler died of the signal and the
    service manager started a new one.
    """
    before = _mtime_ns(cfg)
    os.kill(pid, RESTART_SIGNAL)
    deadline = time.monotonic() + timeout
    while (holder := running_pid(cfg)) is None or _mtime_ns(cfg) == before:
        if time.monotonic() > deadline:
            return None
        time.sleep(POLL_SECONDS)
    return holder


def _mtime_ns(cfg: Settings) -> int | None:
    try:
        return cfg.pid_file.stat().st_mtime_ns
    except FileNotFoundError:
        return None


def service_unit(cfg: Settings, platform: str = sys.platform) -> str:
    """Return a systemd ``--user`` unit, or a launchd agent plist on macOS, that keeps the daemon running.

    The unit sets PATH, because the daemon finds harness CLIs on its PATH and a service manager's PATH is minimal.
    It also sets the secrets command, if there is one.
    """
    env = {'PILOOP_HOME': str(cfg.home), 'PATH': child_env()['PATH']}
    if cfg.secrets_command is not None:
        env[SECRETS_COMMAND_ENV] = cfg.secrets_command
    if platform == 'darwin':
        plist = {
            'Label': 'dev.piloop.serve',
            'ProgramArguments': SERVE_ARGV,
            'EnvironmentVariables': env,
            'RunAtLoad': True,
            'KeepAlive': {'SuccessfulExit': False},
            'StandardOutPath': str(cfg.log_file),
            'StandardErrorPath': str(cfg.log_file),
        }
        return plistlib.dumps(plist, sort_keys=False).decode()
    environment = '\n'.join(f'Environment={systemd_quote(f"{k}={v}")}' for k, v in env.items())
    return SYSTEMD_UNIT.format(exec_start=shlex.join(SERVE_ARGV), environment=environment)


def systemd_quote(value: str) -> str:
    """Quote a unit-file word. Escape backslashes and quotes, and double ``%`` so that specifiers stay literal."""
    escaped = value.replace('\\', '\\\\').replace('"', '\\"').replace('%', '%%')
    return f'"{escaped}"'
