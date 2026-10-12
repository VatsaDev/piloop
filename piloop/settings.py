"""Process-wide settings. Every environment variable piloop reads is declared here."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings of the daemon and the CLI. ``PILOOP_*`` environment variables override them."""

    model_config = SettingsConfigDict(env_prefix='PILOOP_', frozen=True, use_attribute_docstrings=True)

    home: Path = Path.home() / '.piloop'
    """The directory that holds the database and the directories of each run (worktrees, logs, workspaces)."""

    actor: str = 'human'
    """Who this process is. Every row it updates records this value in ``updated_by``. The daemon launches each agent
    with ``<kind>#<worker_id>``."""

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
    """Where uv keeps its tools (uv's own ``UV_TOOL_DIR``, with the same default). ``piloop upgrade`` reads the
    ``uv-receipt.toml`` of the piloop tool there to learn where the CLI was installed from."""

    debug: bool = False
    """Show the full traceback of an unexpected error in the CLI, instead of a one-line message."""

    claude_config_dir: Path = Field(default=Path.home() / '.claude', validation_alias='CLAUDE_CONFIG_DIR')
    """The config directory of Claude Code (its own ``CLAUDE_CONFIG_DIR``, with the same default). piloop installs the
    orchestrator skills in its ``skills/`` directory."""

    agent_skills_dir: Path = Path.home() / '.agents' / 'skills'
    """The user directory of Agent Skills, which Codex, pi, Gemini CLI and Cursor read. piloop installs the orchestrator
    skills there for them."""

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
