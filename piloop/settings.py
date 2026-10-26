"""Process-wide settings. Every environment variable piloop reads is declared here, or named by a placeholder.

Also read from ``<PILOOP_HOME>/config.toml``, the global settings file (``[storage]``, say); an environment variable
wins over the file, and a nested key is ``PILOOP_<TABLE>__<KEY>`` (``PILOOP_STORAGE__MIN_FREE_GB``). A
``{env:NAME:default}`` placeholder in a value of ``piloop.toml`` (a usage glob) reads ``NAME`` when piloop fills it
in (``expand_env``).
"""

import hashlib
import os
import pwd
import re
import stat
import sys
import tomllib
from datetime import timedelta
from functools import cached_property
from pathlib import Path
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, SecretStr, TypeAdapter, model_validator
from pydantic_settings import (
    BaseSettings,
    InitSettingsSource,
    NoDecode,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from piloop import deprecated
from piloop.actor import Actor, System
from piloop.plugins.host import Plugins, PluginTables, Scope

DEFAULT_HOME = Path.home() / '.piloop'
REAL_HOME = Path(pwd.getpwuid(os.getuid()).pw_dir)
"""The user's home directory from the passwd database: a test that moves ``$HOME`` does not move it."""
UNDER_TEST_ENV = 'PILOOP_UNDER_TEST'
"""Set by piloop's test suite, and inherited by every piloop it starts: ``Settings`` refuses the home of a real
install, so a test can never open, let alone migrate, the database of a user's piloop."""
CONFIG_FILE = 'config.toml'
STORE_DIR = 'store'
UNITS_DIR = 'units'
INPUTS_DIR = 'inputs'
"""The read-only copies of the blobs of linked inputs that worktrees share (``piloop.store.tree.Copies``)."""
TMP_DIR = 'tmp'
"""``<home>/tmp``: files that wait to be moved into place, on the disk of the home."""
GB = 1024**3


class StorageSettings(BaseModel):
    """``[storage]``: how archiving uses the disk of this PILOOP_HOME. Where the blobs go is ``[plugins.cas]``."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    min_free_gb: float = Field(default=20, ge=0)
    """Free space, in GB, that archiving keeps on the disk of PILOOP_HOME. Storing a file writes its compressed copy
    first, so the daemon removes the copies of linked inputs that no job uses, then the workspace copies of archived
    files, oldest first, while the disk has less than this free, and it archives in batches that fit above it. A
    worker removes the copies of linked inputs that no job uses after each job, under the same rule."""

    @property
    def reserve(self) -> int:
        """``min_free_gb`` in bytes."""
        return int(self.min_free_gb * GB)


class TracingSettings(BaseModel):
    """``[tracing]``: what this home measures for the spans that it exports (``OTEL_EXPORTER_OTLP_ENDPOINT``)."""

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    slow_write: timedelta = Field(default=timedelta(milliseconds=1), ge=timedelta(0))
    """A write transaction that lasts this long or longer, waiting for the database's write lock and holding it,
    exports a ``piloop.db.write`` span. A faster one exports nothing: its span costs about 35 µs, a fifth of a small
    write. 0 exports every write. Seconds, or an ISO 8601 duration such as ``PT0.001S``."""


SUN_PATH = 104 if sys.platform == 'darwin' else 108
"""The bytes of a Unix socket's path, its NUL included (``sockaddr_un.sun_path``): 104 on macOS, 108 on Linux."""
SHORT_SOCKETS = Path('/tmp')
"""Where a socket goes whose path in its home is too long: ``/tmp/piloop-<uid>/``, as tmux keeps ``/tmp/tmux-<uid>/``."""
RELAY_SOCKET = 'relay/piloop.sock'
"""``<home>/relay/piloop.sock``: a worker's relay, in a directory that only the worker's user reaches."""


def socket_at(path: Path) -> Path:
    """``path``, or when it does not fit in a Unix socket address, a short path that only it maps to.

    The short one is ``SHORT_SOCKETS/piloop-<uid>/<hash of path>.sock``, so the server and its clients find it from the
    home alone, whatever their environment. ``socket_dir`` makes its directory.
    """
    if len(os.fsencode(path)) < SUN_PATH:
        return path
    digest = hashlib.blake2b(os.fsencode(path), digest_size=10).hexdigest()
    return SHORT_SOCKETS / f'piloop-{os.getuid()}' / f'{digest}.sock'


def socket_dir(path: Path) -> None:
    """Make the directory of the socket ``path`` (``socket_at``), and check a short one.

    ``SHORT_SOCKETS`` is shared with other users, so a short socket's directory must be this user's, reached by it only.

    Raises:
        PermissionError: The short directory is a link, another user's, or open to others: someone else could take
            the socket's place.
    """
    directory = path.parent
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    if directory.parent != SHORT_SOCKETS:
        return
    found = directory.lstat()
    if not stat.S_ISDIR(found.st_mode) or found.st_uid != os.getuid() or found.st_mode & 0o077:
        raise PermissionError(
            f'{directory} is not a directory that only this user reaches (mode {stat.filemode(found.st_mode)}, owner'
            f' {found.st_uid}): remove it, and piloop makes it again'
        )


class Settings(BaseSettings):
    """Settings of the daemon and the CLI. ``PILOOP_*`` environment variables override them."""

    model_config = SettingsConfigDict(
        env_prefix='PILOOP_', env_nested_delimiter='__', frozen=True, use_attribute_docstrings=True
    )

    home: Path = DEFAULT_HOME
    """The directory that holds the database, the artifact store and the directories of each run (worktrees, logs,
    workspaces)."""

    actor: Annotated[Actor, NoDecode] = System.HUMAN
    """Who this process is. Every row it updates records this value in ``updated_by``. The daemon launches each agent
    with ``<role>#<agent_session_id>``, for example ``implementer#12`` or ``scout#7``."""

    agent_token: SecretStr | None = None
    """The token of the agent that the daemon launched as ``actor``, which its worker sets at its start: its calls act
    as that agent with it, and as nobody else."""

    server: str | None = None
    """The daemon that the commands which call the API ask, such as ``https://piloop.example.com``: its network's
    client dials it, with the credential of this machine. Unset: the daemon of this home, on its socket."""

    messages: Path | None = None
    """The queue of messages for this agent's turn, set by a runtime that keeps one. ``piloop agent messages`` takes
    them."""

    plugins: dict[str, dict[str, JsonValue]] = {}
    """``[plugins.<capability>]``: the provider of each capability of this home that ``config.toml`` selects, and its
    parameters. ``providers`` validates them."""

    migration_backups: int = Field(default=3, ge=0)
    """The number of copies of the database to keep, one written before each migration: ``piloop.db.bak-<revision>-<UTC
    time>`` next to it, the database as that revision left it. A new copy removes the oldest beyond this number. 0
    writes none. Each copy is about the size of the database."""

    secrets_command: str | None = None
    """A shell command that prints ``NAME=value`` lines, which the daemon adds to its environment when it starts and on
    every ``piloop restart``. ``piloop secret set`` replaces it (``Daemon._load_secrets``): until then, a secret that the
    home lacks is read from that environment."""

    otel_exporter_otlp_endpoint: str | None = Field(default=None, validation_alias='OTEL_EXPORTER_OTLP_ENDPOINT')
    """The standard OTLP endpoint. piloop exports traces only when this is set."""

    uv_tool_dir: Path = Field(default=Path.home() / '.local' / 'share' / 'uv' / 'tools', validation_alias='UV_TOOL_DIR')
    """Where uv keeps its tools (uv's own ``UV_TOOL_DIR``). The default is uv's default when ``XDG_DATA_HOME`` is not
    set. ``piloop upgrade`` reads the ``uv-receipt.toml`` of the piloop tool there to learn where the CLI was installed
    from."""

    cli: str = 'piloop'
    """The command that runs this install. ``piloop doctor`` names it in what it tells you to run. A second install,
    such as a build of ``main`` next to a release, sets it in its wrapper (``PILOOP_CLI=piloop-canary``), so that an
    orchestrator restarts and upgrades that install, never the release."""

    auto_upgrade: bool = False
    """Whether the orchestrator may upgrade piloop by itself when ``piloop doctor`` reports a newer version at its
    source. Off, it asks you in the inbox: an upgrade migrates the database one way, and the runs after it are scored
    by the new code. A daemon that runs older code than the installed CLI is restarted either way."""

    debug: bool = False
    """Show the full traceback of an unexpected error in the CLI, instead of a short message."""

    agent_skills_dir: Path = Path.home() / '.agents' / 'skills'
    """The user directory of Agent Skills, which Codex, pi, Gemini CLI and Cursor read. piloop installs the orchestrator
    skills there for them."""

    claude_skills_dir: Path | None = None
    """The directory in which piloop installs the orchestrator skills for Claude Code. Unset, it is the one Claude Code
    reads: ``skills`` in its config directory (``CLAUDE_CONFIG_DIR``, by default ``~/.claude``). A second piloop on
    the same machine, such as a build of ``main`` next to a release, sets this and ``agent_skills_dir`` to directories
    of its own, so that its skills do not replace those of the release."""

    storage: StorageSettings = StorageSettings()
    """``[storage]`` in ``config.toml``: how archiving uses the disk of this home."""

    tracing: TracingSettings = TracingSettings()
    """``[tracing]`` in ``config.toml``: what this home measures for its exported spans."""

    runtimes: dict[str, dict[str, JsonValue]] = {}
    """``[runtimes.<name>]`` in ``config.toml``: the settings of each runtime on this machine, by its name.
    ``piloop.runtime.factory.runtimes`` validates them."""

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Arguments, then the environment, then ``config.toml`` under the home the arguments or environment name."""
        given = init_settings.init_kwargs.get('home') if isinstance(init_settings, InitSettingsSource) else None
        home = Path(given) if given else home_of_env()
        file = deprecated.max_evaluations_env(os.environ, config_file(home))
        return init_settings, env_settings, InitSettingsSource(settings_cls, file)

    @model_validator(mode='after')
    def _not_a_real_home_under_test(self) -> Self:
        """Under test, refuse ``~/.piloop`` and its siblings (``~/.piloop-canary``) in the real home directory."""
        home = self.home.resolve()
        if UNDER_TEST_ENV in os.environ and home.parent == REAL_HOME.resolve() and home.name.startswith('.piloop'):
            raise ValueError(f'{home} is the home of a real install, and {UNDER_TEST_ENV} is set: use a temporary home')
        return self

    @cached_property
    def providers(self) -> Plugins:
        """``plugins``, validated on first use: the plugins cannot load while this module loads, and ``settings`` does.

        Raises:
            ValidationError: A table names no capability of the home, no provider of it, or parameters that the
                provider refuses.
        """
        return HOME_PLUGINS.validate_python(self.plugins)

    @property
    def database_url(self) -> str:
        """The SQLAlchemy URL of the database."""
        return f'sqlite:///{self.home / "piloop.db"}'

    @property
    def pid_file(self) -> Path:
        """The file with the pid of the running daemon. ``piloop serve`` writes it and removes it when it exits."""
        return self.home / 'serve.pid'

    @property
    def socket_file(self) -> Path:
        """The Unix socket that the running daemon serves the API on, to this user only. Every client command calls it.

        ``<home>/piloop.sock``, or a short path for it when that is too long for a socket (``socket_at``).
        """
        return socket_at(self.home / 'piloop.sock')

    @property
    def relay_file(self) -> Path:
        """The Unix socket of this home's worker relay, which its agents' ``piloop`` calls go through (``socket_at``)."""
        return socket_at(self.home / RELAY_SOCKET)

    @property
    def code_file(self) -> Path:
        """The digest of the code that the running daemon started on (``code_digest``), for ``piloop doctor``."""
        return self.home / 'serve.code'

    @property
    def log_file(self) -> Path:
        """The file that ``piloop serve --detach`` appends the daemon's stdout and stderr to."""
        return self.home / 'serve.log'


HOME_PLUGINS: TypeAdapter[Plugins] = TypeAdapter(
    Annotated[Plugins, PluginTables(Scope.HOME)], config=ConfigDict(title='[plugins]')
)


def home_of_env() -> Path:
    """Return the piloop home that the environment names (``PILOOP_HOME``), else the default."""
    return Path(os.environ.get('PILOOP_HOME', DEFAULT_HOME))


def config_file(home: Path) -> dict[str, object]:
    """Return the parsed ``config.toml`` under ``home``, its deprecated keys bridged, or nothing without the file."""
    path = home / CONFIG_FILE
    if not path.is_file():
        return {}
    raw = tomllib.loads(path.read_text())
    deprecated.apply(path, raw, deprecated.CONFIG_BRIDGES)
    return raw


ENV_PLACEHOLDER = re.compile(r'\{env:(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::(?P<default>[^}]*))?\}')
"""``{env:NAME:default}``: the value of the environment variable ``NAME``, or ``default`` when it is unset or empty."""


def expand_env(text: str) -> str:
    """Fill in the ``{env:NAME:default}`` placeholders of ``text``. A default that starts with ``~`` is in the home."""

    def value(m: re.Match[str]) -> str:
        return os.environ.get(m['name']) or os.path.expanduser(m['default'] or '')

    return ENV_PLACEHOLDER.sub(value, text)


def env_names(text: str) -> list[str]:
    """Return the environment variables that the placeholders of ``text`` name, once each, in order."""
    return list(dict.fromkeys(m['name'] for m in ENV_PLACEHOLDER.finditer(text)))


settings = Settings()
