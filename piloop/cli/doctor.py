"""``piloop doctor``: read-only checks of an install. Each failure comes with the command that fixes it.

The checks find what can break after an upgrade: a daemon that still runs the old code, a database behind the code, a
secrets command that fails, a ``piloop.toml`` that the new code refuses, an MCP server that cannot start, skills that
are missing or out of date, and a missing harness, Herdr or Herdr hook. Nothing here writes: the database is opened
read-only, and the Herdr server is never started. ``piloop upgrade`` runs the checks last.
"""

import json
import shutil
import sqlite3
import subprocess
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from alembic.script import ScriptDirectory
from pydantic import ValidationError

import piloop
from piloop import mcp
from piloop.cli import skills
from piloop.cli.version import installed
from piloop.config import ConfigError, ProjectConfig, RuntimeKind, explain, load_project_config
from piloop.daemon.serve import SECRETS_COMMAND_ENV, SecretsError, running_pid, secrets
from piloop.db.models import WorkerKind, utcnow
from piloop.db.session import MIGRATIONS
from piloop.proc import child_env, environment
from piloop.reports.overview import age
from piloop.runtime.herdr import HERDR, HerdrError, integrations, server_running
from piloop.settings import Settings


@dataclass(frozen=True)
class Check:
    """One line of the report: what was checked, what was found and, for a failure, the fix."""

    name: str
    found: str
    fix: str | None = None
    """None when the check passed."""


def checks(cfg: Settings) -> list[Check]:
    """Run every check, in the best order to debug a broken upgrade: daemon, database, configs, skills, tools."""
    projects = _projects(cfg.home / 'piloop.db')
    stored = [c for _, _, c in projects if isinstance(c, ProjectConfig)]
    whose, names, command = _daemon_env(cfg)
    found_secrets, supplied = _secrets(whose, command)
    env = whose, names | supplied
    return [
        _daemon(cfg),
        _database(cfg.home / 'piloop.db'),
        found_secrets,
        *(_project(name, path, c) for name, path, c in projects),
        *(_mcp(name, c, env) for name, _, c in projects if isinstance(c, ProjectConfig) and c.mcp),
        *_skills(cfg),
        _harnesses(stored),
        *(_herdr(herdr) if (herdr := [c for c in stored if c.runtime is RuntimeKind.HERDR]) else []),
    ]


def _daemon(cfg: Settings) -> Check:
    """A daemon that started before the installed code changed runs the old code, because uv swaps files in place."""
    pid = running_pid(cfg)
    if pid is None:
        return Check('daemon', 'not running')
    started = cfg.pid_file.stat().st_mtime
    changed = max(p.stat().st_mtime for p in Path(piloop.__file__).parent.rglob('*.py'))
    if changed > started:
        return Check(
            'daemon',
            f'pid {pid}, started {_ago(started)} ago, runs older code than the installed {installed()}'
            f' (changed {_ago(changed)} ago)',
            '`piloop restart` restarts it on the installed code',
        )
    return Check('daemon', f'pid {pid} runs the installed {installed()}')


def _database(path: Path) -> Check:
    if not path.is_file():
        return Check('database', f'none at {path}', '`piloop init <repo>` creates it with the first project')
    scripts = ScriptDirectory(str(MIGRATIONS))
    head, at = scripts.get_current_head(), _revision(path)
    if at == head:
        return Check('database', f'{path} at schema {head}, current')
    if at is None or at in {s.revision for s in scripts.walk_revisions()}:
        return Check('database', f'{path} at schema {at or "none"}, this piloop is at {head}', '`piloop migrate`')
    return Check('database', f'{path} at schema {at}, which a newer piloop wrote', '`piloop upgrade`')


def _project(name: str, path: str, stored: ProjectConfig | ValidationError) -> Check:
    """Check that the repo's ``piloop.toml`` loads, and that the stored copy (``projects.config``) validates."""
    what = f'project {name}'
    reread = f'fix {path}/piloop.toml, then `piloop init {path}` re-reads it'
    try:
        load_project_config(Path(path))
    except FileNotFoundError:
        return Check(what, f'no piloop.toml in {path}', f'restore it, then `piloop init {path}`')
    except ConfigError as e:
        return Check(what, str(e), reread)
    if isinstance(stored, ValidationError):
        return Check(
            what,
            explain('its stored copy (projects.config, what the daemon schedules from)', stored),
            f'`piloop init {path}`',
        )
    return Check(what, f'{path}/piloop.toml loads')


def _daemon_env(cfg: Settings) -> tuple[str, set[str], str | None]:
    """Return whose environment gives the agents their MCP variables, the names in it, and its secrets command.

    It is the environment of the running daemon. Without a daemon, or when its environment cannot be read, it is the
    environment of this shell, which a ``piloop serve`` started here inherits. It never returns values. The daemon's
    environment is the one it was exec'd with, so after a first start it lacks what the secrets command printed.
    """
    shell = 'this shell', set(child_env()), cfg.secrets_command
    pid = running_pid(cfg)
    if pid is None:
        return shell
    env = environment(pid)
    if env is None:  # the daemon ended a moment ago
        return shell
    return f'the daemon (pid {pid})', set(env), env.get(SECRETS_COMMAND_ENV)


def _secrets(whose: str, command: str | None) -> tuple[Check, set[str]]:
    """Check where the daemon's secrets come from. Run its secrets command here for the names; never show values."""
    if command is None:
        return Check('secrets', f'from the environment of {whose} only ({SECRETS_COMMAND_ENV} unset)'), set()
    try:
        names = set(secrets(command))
    except SecretsError as e:
        return Check(
            'secrets',
            f'{SECRETS_COMMAND_ENV} of {whose} ({command}) failed: {e}',
            'fix the command; `piloop serve` refuses to start while it fails',
        ), set()
    return Check('secrets', f'{SECRETS_COMMAND_ENV} of {whose} ({command}) prints {", ".join(sorted(names))}'), names


def _mcp(name: str, config: ProjectConfig, env: tuple[str, set[str]]) -> Check:
    """Check that the project's MCP servers can start: their commands are on PATH and their variables are set.

    The config load checked their form already. Variables are reported by name. Values are never shown.
    """
    what = f'mcp {name}'
    servers = config.mcp.values()
    missing_exe = sorted({s.command[0] for s in servers if s.command and shutil.which(s.command[0]) is None})
    if missing_exe:
        return Check(what, f'not on PATH: {", ".join(missing_exe)}', 'install them, or fix the [mcp.*] tables')
    whose, names = env
    if missing := mcp.missing_env(servers, names):
        return Check(
            what,
            f'{whose} lacks {", ".join(missing)}',
            f'print them from {SECRETS_COMMAND_ENV}, or set them in the environment `piloop serve` runs in; then'
            ' restart the daemon',
        )
    roles = '; '.join(f'{kind}: {", ".join(listed)}' for kind in WorkerKind if (listed := config.role(kind).mcp))
    variables = mcp.env_names(servers)
    return Check(
        what,
        f'{len(config.mcp)} server(s) ({roles or "no role lists one"}), {len(variables)} variable(s) set in {whose}',
    )


def _skills(cfg: Settings) -> list[Check]:
    """Check that each harness on PATH finds the orchestrator skills, the same as the ones this piloop ships."""
    found = skills.targets(cfg)
    if not found:
        return [Check('skills', f'none of {", ".join(skills.SKILL_DIRS)} is on PATH: nothing to check')]
    return [
        Check(f'skills ({", ".join(harnesses)})', f'{target}: {"; ".join(drift)}', '`piloop install-skills`')
        if (drift := skills.drift(target))
        else Check(f'skills ({", ".join(harnesses)})', f'current in {target}')
        for target, harnesses in found.items()
    ]


def _harnesses(configs: list[ProjectConfig]) -> Check:
    """Check that each harness that a role of a project can launch has its executable on PATH."""
    executables = {
        c.harness_for(h).executable: h for c in configs for kind in WorkerKind for h in c.role_harnesses(kind)
    }
    missing = sorted(f'{h} ({exe})' for exe, h in executables.items() if shutil.which(exe) is None)
    if missing:
        return Check(
            'harnesses',
            f'not on PATH: {", ".join(missing)}',
            'install them, or take them out of `harnesses` in piloop.toml and `piloop init` the project',
        )
    return Check('harnesses', f'on PATH: {", ".join(sorted(executables.values())) or "none configured"}')


def _herdr(configs: list[ProjectConfig]) -> list[Check]:
    """Check Herdr for projects that run agents in it: the binary, the piloop session's server, and Herdr's hooks."""
    if shutil.which(HERDR) is None:
        return [Check('herdr', 'a project uses the herdr runtime and herdr is not on PATH', 'install herdr')]
    try:
        running = server_running()
    except (HerdrError, subprocess.TimeoutExpired, OSError) as e:
        server = Check('herdr', f'the piloop session does not answer: {e}', '`herdr server stop`, then retry')
    else:
        server = Check('herdr', 'the piloop session answers' if running else 'server not running; started on first use')
    return [server, _herdr_hooks(configs)]


def _herdr_hooks(configs: list[ProjectConfig]) -> Check:
    """Check that Herdr's hooks are installed and current for each harness that a Herdr project launches.

    Without the hooks, Herdr reports no session for the agent. Then piloop cannot read its tokens, and a fast agent
    that committed can look dead at startup.
    """
    kinds = {c.harness_for(h).executable for c in configs for kind in WorkerKind for h in c.role_harnesses(kind)}
    try:
        states = integrations()
    except (subprocess.SubprocessError, OSError) as e:
        return Check('herdr hooks', f'`herdr integration status` failed: {e}', 'upgrade herdr')
    known = sorted(kinds & states.keys())
    if stale := [k for k in known if not states[k].startswith('current')]:
        return Check(
            'herdr hooks',
            ', '.join(f'{k}: {states[k]}' for k in stale),
            ', '.join(f'`herdr integration install {k}`' for k in stale),
        )
    return Check('herdr hooks', f'current for {", ".join(known) or "no harness Herdr has hooks for"}')


def _revision(path: Path) -> str | None:
    with closing(_read_only(path)) as con:
        try:
            row = con.execute('SELECT version_num FROM alembic_version').fetchone()
        except sqlite3.OperationalError:  # never migrated, so there is no alembic_version table
            return None
    return row[0] if row else None


def _projects(path: Path) -> list[tuple[str, str, ProjectConfig | ValidationError]]:
    """Return the name, repo path and stored config of each registered project, or why the config does not validate."""
    if not path.is_file():
        return []
    with closing(_read_only(path)) as con:
        try:
            rows = con.execute('SELECT name, path, config FROM projects ORDER BY id').fetchall()
        except sqlite3.OperationalError:  # never migrated, so there is no projects table
            return []
    return [(name, repo, _stored(config)) for name, repo, config in rows]


def _stored(config: str) -> ProjectConfig | ValidationError:
    try:
        return ProjectConfig.model_validate(json.loads(config))
    except ValidationError as e:
        return e


def _read_only(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f'file:{path}?mode=ro', uri=True)


def _ago(mtime: float) -> str:
    return age(datetime.fromtimestamp(mtime, UTC), utcnow())


def report(results: list[Check]) -> Iterator[str]:
    """Yield ``ok    <name>: <found>`` for a pass, or ``FAIL  <name>: <found>`` and an indented ``fix:`` line."""
    for c in results:
        yield f'{"ok  " if c.fix is None else "FAIL"}  {c.name}: {c.found}'
        if c.fix is not None:
            yield f'      fix: {c.fix}'
