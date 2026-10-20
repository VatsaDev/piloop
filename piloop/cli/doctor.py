"""``piloop doctor``: read-only checks of an install. Each failure says how to fix it, with the command when there is one.

The checks note a newer piloop at the source of the CLI, and find what can break after an upgrade: a daemon that still
runs the old code, a database behind the code, a secrets command that fails, a ``piloop.toml`` that the new code refuses,
an MCP server that cannot start, skills that are missing or out of date, a missing harness, and what each runtime in
use checks itself (``Runtime.checks``). Nothing here writes: the database is opened read-only, and no runtime starts
anything. The check of the source asks the network (``git ls-remote`` or the package index). The commands that the
lines name use ``Settings.cli``. ``piloop upgrade`` runs the checks last.
"""

import json
import shutil
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from alembic.script import ScriptDirectory
from pydantic import ValidationError

import piloop
from piloop import mcp
from piloop.cli import skills, upgrade
from piloop.config import CampaignConfig, ConfigError, explain, load_campaign_config
from piloop.daemon.main import preflight, unbound_failures
from piloop.daemon.serve import SECRETS_COMMAND_ENV, SecretsError, running_pid, secrets
from piloop.db.models import utcnow
from piloop.db.session import MIGRATIONS, SchemaAhead, ahead
from piloop.plugins.contracts.runtime import RUNTIMES
from piloop.plugins.host import PluginError, declared
from piloop.proc import child_env, environment
from piloop.reports.overview import age
from piloop.runtime import Check
from piloop.settings import Settings
from piloop.version import direct_url, installed

CHANGELOG = 'https://github.com/hiloopai/piloop/blob/main/CHANGELOG.md'


def checks(cfg: Settings) -> list[Check]:
    """Run every check, in the best order to debug a broken upgrade: daemon, database, plugins, configs, skills, tools.

    When the plugins do not compose, the checks of the campaigns, harnesses and runtimes, which use them, do not run.
    """
    composed = _plugins(cfg)
    whose, names, command = _daemon_env(cfg)
    found_secrets, supplied = _secrets(whose, command)
    first = [_cli(cfg), _daemon(cfg), _database(cfg.home / 'piloop.db', running_pid(cfg)), composed, found_secrets]
    if composed.fix is not None:
        found = [*first, *_skills(cfg)]
    else:
        campaigns = stored_campaigns(cfg.home / 'piloop.db')
        stored = [c for _, _, c in campaigns if isinstance(c, CampaignConfig)]
        env = whose, names | supplied
        found = [
            *first,
            *(_campaign(name, path, c) for name, path, c in campaigns),
            *(_mcp(name, c, env) for name, _, c in campaigns if isinstance(c, CampaignConfig) and c.mcp),
            *_skills(cfg),
            _harnesses(stored),
            *_runtimes(stored),
        ]
    # the commands that the lines name are this install's, which a second install (piloop-canary) runs under its name
    return [replace(c, found=_named(c.found, cfg), fix=c.fix and _named(c.fix, cfg)) for c in found]


def _named(text: str, cfg: Settings) -> str:
    return text.replace('`piloop ', f'`{cfg.cli} ')


def _cli(cfg: Settings) -> Check:
    """Check that the source that uv installed piloop from has nothing newer: a release, or a commit on the branch."""
    try:
        found = upgrade.newer(cfg.uv_tool_dir, direct_url())
    except upgrade.InstallError as e:
        return Check('cli', f'{installed()}; not checked for a newer one: {e}')
    if found is None:
        return Check('cli', f'{installed()}, the newest at its source')
    # a notice, not a failure: whether to upgrade mid-campaign is the human's call (Settings.auto_upgrade)
    policy = (
        'PILOOP_AUTO_UPGRADE is on: `piloop upgrade` installs it'
        if cfg.auto_upgrade
        else f'ask the human before `piloop upgrade` (PILOOP_AUTO_UPGRADE is off); changes: {CHANGELOG}'
    )
    return Check('cli', f'{installed()}; {found} is out: {policy}')


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


def _plugins(cfg: Settings) -> Check:
    """Check the plugins as the daemon does before it starts, and warn on a broken package that nothing selects."""
    try:
        preflight(cfg)
    except PluginError as e:
        return Check('plugins', str(e), 'fix or uninstall the package of that entry point, then `piloop plugins`')
    unbound = unbound_failures(cfg)
    loaded = [c for c in declared() if c not in unbound]
    sizes = [(len(c.sources()), c.noun) for c in loaded]
    counts = ', '.join(f'{n} {noun}{"s" if n != 1 else ""}' for n, noun in sizes)
    added = sorted({s.release for c in loaded for s in c.sources().values() if not s.built_in})
    found = f'{counts}; {"from " + ", ".join(added) if added else "all built in"}'
    warnings = ''.join(f'; warning: nothing selects a {c.noun}, and they do not load: {e}' for c, e in unbound.items())
    return Check('plugins', found + warnings)


def _database(path: Path, daemon: int | None) -> Check:
    if not path.is_file():
        return Check('database', f'none at {path}', '`piloop init <repo>` creates it with the first campaign')
    if (newer := schema_ahead(path)) is not None:
        return Check('database', str(newer), '`piloop upgrade`, or set PILOOP_HOME to the home of a newer piloop')
    head, at = ScriptDirectory(str(MIGRATIONS)).get_current_head(), _revision(path)
    if at == head:
        return Check('database', f'{path} at schema {head}, current')
    found = f'{path} at schema {at or "none"}, this piloop is at {head}; its campaigns are checked once it is migrated'
    if daemon is not None:  # it runs an older piloop, which a migration would break: piloop migrate refuses
        return Check(
            'database', found, f'`piloop restart` restarts the daemon (pid {daemon}) on this piloop, which migrates'
        )
    return Check('database', found, '`piloop migrate`')


def schema_ahead(path: Path) -> SchemaAhead | None:
    """Return the error for a database that a newer piloop migrated, else None. Reads it read-only.

    ``piloop sql`` checks this before it connects, because it does not migrate.
    """
    at = _revision(path) if path.is_file() else None
    if at is None or at in {s.revision for s in ScriptDirectory(str(MIGRATIONS)).walk_revisions()}:
        return None
    return ahead(path, at)


def _campaign(name: str, path: str, stored: CampaignConfig | ValidationError) -> Check:
    """Check that the repo's ``piloop.toml`` loads, and that the stored copy (``campaigns.config``) validates."""
    what = f'campaign {name}'
    reread = f'fix {path}/piloop.toml, then `piloop init {path}` re-reads it'
    try:
        load_campaign_config(Path(path))
    except FileNotFoundError:
        return Check(what, f'no piloop.toml in {path}', f'restore it, then `piloop init {path}`')
    except ConfigError as e:
        return Check(what, str(e), reread)
    if isinstance(stored, ValidationError):
        return Check(
            what,
            explain('its stored copy (campaigns.config, what the daemon schedules from)', stored),
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


def _mcp(name: str, config: CampaignConfig, env: tuple[str, set[str]]) -> Check:
    """Check that the campaign's MCP servers can start: their commands are on PATH and their variables are set.

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
    roles = '; '.join(
        f'{kind}: {", ".join(listed)}' for kind, role in config.roles.all().items() if (listed := role.mcp)
    )
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


def _harnesses(configs: list[CampaignConfig]) -> Check:
    """Check that each harness that a role of a campaign can launch has its executable on PATH."""
    executables = {
        c.harness_for(h).executable: h for c in configs for kind in c.roles.all() for h in c.role_harnesses(kind)
    }
    missing = sorted(f'{h} ({exe})' for exe, h in executables.items() if shutil.which(exe) is None)
    if missing:
        return Check(
            'harnesses',
            f'not on PATH: {", ".join(missing)}',
            'install them, or take them out of `harnesses` in piloop.toml and `piloop init` the campaign',
        )
    return Check('harnesses', f'on PATH: {", ".join(sorted(executables.values())) or "none configured"}')


def _runtimes(configs: list[CampaignConfig]) -> list[Check]:
    """Run the checks of each runtime that a campaign uses, with the harness executables of the campaigns that use it."""
    executables: dict[str, set[str]] = {}
    for c in configs:
        programs = {c.harness_for(h).executable for kind in c.roles.all() for h in c.role_harnesses(kind)}
        for kind in c.runtimes:
            executables.setdefault(kind, set()).update(programs)
    return [check for kind, found in sorted(executables.items()) for check in RUNTIMES.get(kind).checks(found)]


def _revision(path: Path) -> str | None:
    with closing(_read_only(path)) as con:
        try:
            row = con.execute('SELECT version_num FROM alembic_version').fetchone()
        except sqlite3.OperationalError:  # never migrated, so there is no alembic_version table
            return None
    return row[0] if row else None


def stored_campaigns(path: Path) -> list[tuple[str, str, CampaignConfig | ValidationError]]:
    """Return the name, repo path and stored config of each registered campaign, or why the config does not validate."""
    if not path.is_file():
        return []
    with closing(_read_only(path)) as con:
        try:
            rows = con.execute('SELECT name, path, config FROM campaigns ORDER BY id').fetchall()
        except sqlite3.OperationalError:  # never migrated, so there is no campaigns table
            return []
    return [(name, repo, _stored(config)) for name, repo, config in rows]


def _stored(config: str) -> CampaignConfig | ValidationError:
    try:
        return CampaignConfig.model_validate(json.loads(config))
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
