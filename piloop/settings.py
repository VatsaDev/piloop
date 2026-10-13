"""Process-wide settings. Every environment variable piloop reads is declared here.

Also read from ``<PILOOP_HOME>/config.toml``, the global settings file (``[storage]``, say); an environment variable
wins over the file, and a nested key is ``PILOOP_<TABLE>__<KEY>`` (``PILOOP_STORAGE__REMOTE``).
"""

import os
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import (
    BaseSettings,
    InitSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)

DEFAULT_HOME = Path.home() / '.piloop'
CONFIG_FILE = 'config.toml'
STORE_DIR = 'store'
KB = 1024
GB = 1024**3


class StorageSettings(BaseModel):
    """``[storage]``: the artifact store every project of this PILOOP_HOME archives its workspace into.

    Blobs live in ``<PILOOP_HOME>/store``, and in ``remote`` too when it is set. The artifact store page of the docs
    describes the modes.
    """

    model_config = ConfigDict(extra='forbid', frozen=True, use_attribute_docstrings=True)

    remote: str | None = None
    """``s3://<bucket>[/<prefix>]``: every blob is uploaded there as it is stored, with boto3's credentials. Unset,
    the store is the local directory alone and nothing is ever evicted."""
    storage_class: str = 'GLACIER_IR'
    """The S3 storage class of a blob of at least ``standard_below`` compressed bytes."""
    standard_below: int = Field(default=128 * KB, ge=0)
    """Compressed size under which a blob is stored as ``STANDARD``: Glacier IR bills every object as at least 128 KB."""
    evict_after_days: float | None = Field(default=None, ge=0)
    """Drop the local copy of a blob the remote holds once it is this old (since it was stored or fetched); 0 drops it
    as soon as it is uploaded, so the local directory is only a staging area and every read fetches. Unset: by age
    never."""
    local_max_gb: float | None = Field(default=None, ge=0)
    """Drop the local copies of the oldest blobs the remote holds until the local store is at most this size."""
    min_free_gb: float = Field(default=20, ge=0)
    """Free space, in GB, that archiving always leaves on the disk of the local store. Storing a file writes its
    compressed copy before the original goes, so the daemon archives in batches that fit above this, and waits when
    not one more file fits."""

    @model_validator(mode='after')
    def _check(self) -> Self:
        if self.remote is None and (self.evict_after_days is not None or self.local_max_gb is not None):
            raise ValueError('evict_after_days and local_max_gb need a remote: the local copy is the only one')
        if self.remote is not None and not self.remote.startswith('s3://'):
            raise ValueError(f'remote {self.remote!r}: only s3:// is supported')
        return self


class Settings(BaseSettings):
    """Settings of the daemon and the CLI. ``PILOOP_*`` environment variables override them."""

    model_config = SettingsConfigDict(
        env_prefix='PILOOP_', env_nested_delimiter='__', frozen=True, use_attribute_docstrings=True
    )

    home: Path = DEFAULT_HOME
    """The directory that holds the database, the artifact store and the directories of each run (worktrees, logs,
    workspaces)."""

    actor: str = 'human'
    """Who this process is. Every row it updates records this value in ``updated_by``. The daemon launches each agent
    with ``<role>#<worker_id>``, for example ``worker#12`` or ``scout#7``."""

    tick_seconds: float = 2
    """The number of seconds between two ticks. On each tick, the daemon checks the triggers of every project and
    launches the requested workers."""

    herdr_session: str = 'piloop'
    """The name of the Herdr session that piloop owns. It has one workspace for each project, and every agent that
    piloop starts runs there, never in the session you use for other work. ``piloop attach`` attaches a terminal to
    it. piloop starts its server when it first needs it."""

    herdr_socket_path: str | None = Field(default=None, validation_alias='HERDR_SOCKET_PATH')
    """The socket of the Herdr session that this process runs in. Herdr sets it in every pane it manages.
    ``piloop attach`` reads it to tell whether it already runs inside the piloop session."""

    herdr_server_start_seconds: float = 10
    """The number of seconds to wait for the Herdr server of the piloop session to answer after piloop starts it."""

    herdr_start_timeout_seconds: float = 60
    """The number of seconds Herdr may take to detect an agent that just started."""

    herdr_cli_timeout_seconds: float = 30
    """The timeout in seconds of one ``herdr`` CLI call, in addition to any wait that the call itself does."""

    herdr_prompt_pickup_seconds: float = 5
    """The number of seconds to wait for an agent to start working after a prompt is sent. If the agent has not started
    by then and the screen does not show the prompt, the prompt is treated as lost."""

    herdr_shell_ready_seconds: float = 10
    """The number of seconds to retry ``agent start`` while the shell of a new tab is still starting."""

    herdr_wait_chunk_seconds: float = 10
    """The longest ``agent wait`` call, in seconds. piloop notices a worker whose tab was closed within this time."""

    secrets_command: str | None = None
    """A shell command that prints ``NAME=value`` lines: the API keys that the ``[mcp.<name>]`` servers name, from
    wherever you keep them. The daemon runs it when it starts and on every ``piloop restart``. The keys go into the
    daemon's environment only, never into a file or a log. When unset, the daemon has only the environment that
    ``piloop serve`` started with."""

    otel_exporter_otlp_endpoint: str | None = Field(default=None, validation_alias='OTEL_EXPORTER_OTLP_ENDPOINT')
    """The standard OTLP endpoint. piloop exports traces only when this is set."""

    entire_attach_timeout_seconds: float = 120
    """The number of seconds that ``entire session attach`` may take to turn the session of an ended agent into a
    checkpoint. After that, it is stopped and the checkpoint stays NULL."""

    uv_tool_dir: Path = Field(default=Path.home() / '.local' / 'share' / 'uv' / 'tools', validation_alias='UV_TOOL_DIR')
    """Where uv keeps its tools (uv's own ``UV_TOOL_DIR``). The default is uv's default when ``XDG_DATA_HOME`` is not
    set. ``piloop upgrade`` reads the ``uv-receipt.toml`` of the piloop tool there to learn where the CLI was installed
    from."""

    debug: bool = False
    """Show the full traceback of an unexpected error in the CLI, instead of a short message."""

    claude_config_dir: Path = Field(default=Path.home() / '.claude', validation_alias='CLAUDE_CONFIG_DIR')
    """The config directory of Claude Code (its own ``CLAUDE_CONFIG_DIR``, with the same default). piloop installs the
    orchestrator skills in its ``skills/`` directory, and reads the token usage of Claude Code agents from its
    ``projects/`` directory."""

    codex_home: Path = Field(default=Path.home() / '.codex', validation_alias='CODEX_HOME')
    """The home directory of Codex (its own ``CODEX_HOME``, with the same default). piloop reads the token usage of
    Codex agents from its ``sessions/`` directory."""

    agent_skills_dir: Path = Path.home() / '.agents' / 'skills'
    """The user directory of Agent Skills, which Codex, pi, Gemini CLI and Cursor read. piloop installs the orchestrator
    skills there for them."""

    storage: StorageSettings = StorageSettings()
    """``[storage]`` in ``config.toml``: where archived workspace files go."""

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
        home = Path(given or os.environ.get('PILOOP_HOME', DEFAULT_HOME))
        return init_settings, env_settings, TomlConfigSettingsSource(settings_cls, toml_file=home / CONFIG_FILE)

    @property
    def store_dir(self) -> Path:
        """The artifact store's local directory, shared by every project (``piloop.store``)."""
        return self.home / STORE_DIR

    @property
    def database_url(self) -> str:
        """The SQLAlchemy URL of the database."""
        return f'sqlite:///{self.home / "piloop.db"}'

    @property
    def projects_dir(self) -> Path:
        """The parent of the directory of each project (its shared workspace)."""
        return self.home / 'projects'

    @property
    def units_dir(self) -> Path:
        """The per-unit results of the daemon's measurements (``piloop.scoring.ledger.units_file``).

        Never inside a worktree.
        """
        return self.home / 'units'

    @property
    def pid_file(self) -> Path:
        """The file with the pid of the running daemon. ``piloop serve`` writes it and removes it when it exits."""
        return self.home / 'serve.pid'

    @property
    def log_file(self) -> Path:
        """The file that ``piloop serve --detach`` appends the daemon's stdout and stderr to."""
        return self.home / 'serve.log'


settings = Settings()
