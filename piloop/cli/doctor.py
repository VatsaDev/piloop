"""``piloop doctor``: read-only checks of an install. Each failure says how to fix it, with the command when there is one.

The checks note a newer piloop at the source of the CLI, and find what can break after an upgrade: a daemon that still
runs the old code, a database behind the code, a secrets command that fails, a ``piloop.toml`` that the new code refuses,
an MCP server that cannot start, skills that are missing or out of date, a missing harness, and what each vcs provider
checks itself (``Vcs.checks``). Nothing here writes: the database is opened read-only, and nothing starts. The check of the source asks the network (``git ls-remote`` or the package index). The commands that the
lines name use ``Settings.cli``. ``piloop upgrade`` runs the checks last.
"""

import json
import shutil
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from piloop.actor import Actor
from piloop.campaign.campaigns import check_takes
from piloop.cli import skills, upgrade
from piloop.config import CampaignConfig, ConfigError, explain, load_campaign_config
from piloop.daemon.main import preflight, unbound_failures
from piloop.daemon.serve import SECRETS_COMMAND_ENV, SecretsError, running_pid, secrets
from piloop.db.models import utcnow
from piloop.db.session import described, heads, schema_mismatch, stamps
from piloop.ids import CampaignId, CampaignStatus
from piloop.plugins.host import Check, PluginError, Severity, declared
from piloop.plugins.secrets.contract import SECRETS, SecretKey
from piloop.plugins.vcs.contract import VCS
from piloop.proc import child_env, environment
from piloop.settings import Settings
from piloop.text import age
from piloop.version import code_digest, direct_url, installed

CHANGELOG = 'https://github.com/hiloopai/piloop/blob/main/CHANGELOG.md'


@dataclass(frozen=True)
class StoredCampaign:
    """A registered campaign as ``piloop doctor`` reads it: its repo, its status and its stored config."""

    id: CampaignId
    name: str
    path: str
    status: CampaignStatus
    config: CampaignConfig | ValidationError
    """The stored copy (``campaigns.config``), or why it does not validate."""

    @property
    def severity(self) -> Severity:
        """What a problem of the campaign's own means: a warning while it is stopped, as it runs nothing until then."""
        return Severity.WARN if self.status is CampaignStatus.STOPPED else Severity.FAIL


def checks(cfg: Settings) -> list[Check]:
    """Run every check, in the best order to debug a broken upgrade: daemon, database, plugins, configs, skills, tools.

    When the plugins do not compose, the checks of the campaigns, harnesses and vcs providers, which use them, do not
    run.
    """
    composed = _plugins(cfg)
    whose, names, command = _daemon_env(cfg)
    found_secrets, supplied = _secrets(whose, command)
    first = [
        _cli(cfg),
        _daemon(cfg),
        _database(cfg.home / 'piloop.db', running_pid(cfg), cfg.actor),
        composed,
        found_secrets,
    ]
    if composed.fails:
        found = [*first, *_skills(cfg)]
    else:
        campaigns = stored_campaigns(cfg.home / 'piloop.db')
        stored = [c.config for c in campaigns if isinstance(c.config, CampaignConfig)]
        env = whose, names | supplied
        held = _stored_secrets(cfg)
        found = [
            *first,
            *map(_campaign, campaigns),
            *(_mcp(c.name, c.config) for c in campaigns if isinstance(c.config, CampaignConfig) and c.config.mcp),
            *(
                _secrets_of(c, needed, held, env)
                for c in campaigns
                if isinstance(c.config, CampaignConfig) and (needed := _needed(c.config))
            ),
            *_skills(cfg),
            _harnesses(stored),
            *(check for provider in VCS.providers().values() for check in provider.model.checks()),
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
    """A daemon runs the code that it started on (``code_file``), and uv swaps the installed files in place."""
    pid = running_pid(cfg)
    if pid is None:
        return Check('daemon', 'not running')
    try:
        started_on = cfg.code_file.read_text()
    except FileNotFoundError:
        started_on = None  # a daemon from before the code file
    if started_on != code_digest():
        return Check(
            'daemon',
            f'pid {pid}, started {_ago(cfg.pid_file.stat().st_mtime)} ago, runs other code than the installed'
            f' {installed()}',
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


def _database(path: Path, daemon: int | None, actor: Actor) -> Check:
    """Report the schema as the commands that refuse it do (``schema_mismatch``). It never migrates."""
    if (e := schema_mismatch(path, daemon, actor)) is not None:
        return Check('database', e.found, e.fix)
    gone = [name for name in stamps(path) if name not in heads()]
    left = ''.join(
        f'; warning: the plugin {name} ({package}) is not installed: its tables stay'
        for name, package in _packages(path, gone)
    )
    return Check('database', f'{path} at schema {described(heads())}, current{left}')


def _packages(path: Path, names: list[str]) -> list[tuple[str, str]]:
    """Each of the schemas ``names`` with the package that declared it, as ``migrations`` recorded it."""
    if not names:
        return []
    with closing(sqlite3.connect(f'file:{path}?mode=ro', uri=True)) as con:
        found = dict(con.execute('SELECT name, package FROM migrations').fetchall())
    return [(name, found.get(name, 'an unknown package')) for name in names]


def _campaign(campaign: StoredCampaign) -> Check:
    """Check that the repo's ``piloop.toml`` loads, and that the stored copy (``campaigns.config``) validates.

    The daemon schedules from the stored copy, and reads the repo's file only at ``piloop init``: a stopped campaign's
    file that does not load is a warning (``StoredCampaign.severity``).
    """
    path, stored = campaign.path, campaign.config
    what = f'campaign {campaign.name}'
    reread = f'fix {path}/piloop.toml, then `piloop init {path}` re-reads it'
    severity = campaign.severity
    try:
        check_takes(Path(path), load_campaign_config(Path(path)))
    except FileNotFoundError:
        return Check(what, f'no piloop.toml in {path}', f'restore it, then `piloop init {path}`', severity)
    except ConfigError as e:
        return Check(what, str(e), reread, severity)
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
    """Check the deprecated secrets command. Run it here for the names; never show values."""
    if command is None:
        return Check('secrets', f'from `piloop secret set` ({SECRETS_COMMAND_ENV} unset)'), set()
    try:
        names = set(secrets(command))
    except SecretsError as e:
        return Check(
            'secrets',
            f'{SECRETS_COMMAND_ENV} of {whose} ({command}) failed: {e}',
            'fix the command; `piloop serve` refuses to start while it fails',
        ), set()
    return Check(
        'secrets',
        f'{SECRETS_COMMAND_ENV} of {whose} ({command}) prints {", ".join(sorted(names))}',
        f'`piloop secret set` each of them, then unset {SECRETS_COMMAND_ENV}: piloop 0.5.0 removes it',
        Severity.WARN,
    ), names


def _mcp(name: str, config: CampaignConfig) -> Check:
    """Check that the commands of the campaign's MCP servers are on PATH. ``_secrets_of`` checks their keys."""
    what = f'mcp {name}'
    servers = config.mcp.values()
    missing_exe = sorted({s.command[0] for s in servers if s.command and shutil.which(s.command[0]) is None})
    if missing_exe:
        return Check(what, f'not on PATH: {", ".join(missing_exe)}', 'install them, or fix the [mcp.*] tables')
    roles = '; '.join(
        f'{kind}: {", ".join(listed)}' for kind, role in config.roles.all().items() if (listed := role.mcp)
    )
    return Check(what, f'{len(config.mcp)} server(s) ({roles or "no role lists one"})')


def _needed(config: CampaignConfig) -> list[str]:
    """The secrets that the agents of the campaign's roles get, on each harness of their rotation."""
    return sorted(
        {
            secret
            for role in config.roles.all()
            for harness in config.role_harnesses(role)
            for secret in config.role_secrets(role, harness).values()
        }
    )


def _secrets_of(campaign: StoredCampaign, needed: list[str], held: list[SecretKey], env: tuple[str, set[str]]) -> Check:
    """Check that the secrets that the campaign's agents need are set, by name, never by value.

    A missing one of a stopped campaign is a warning (``StoredCampaign.severity``). A secret that only the environment
    of the daemon has works until 0.5.0.
    """
    what = f'secrets {campaign.name}'
    have = {k.name for k in held if k.owner in (None, campaign.id)}
    whose, environ = env
    missing = [n for n in needed if n not in have and n not in environ]
    if missing:
        return Check(what, f'not set: {", ".join(missing)}', '`piloop secret set <NAME>` for each', campaign.severity)
    if from_env := [n for n in needed if n not in have]:
        return Check(
            what,
            f'{", ".join(from_env)} from the environment of {whose}',
            '`piloop secret set` each of them: piloop 0.5.0 reads secrets from `piloop secret` only',
            Severity.WARN,
        )
    return Check(what, f'{len(needed)} set')


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


def stored_campaigns(path: Path) -> list[StoredCampaign]:
    """Return each registered campaign of the database at ``path``, oldest first."""
    if not path.is_file():
        return []
    with closing(_read_only(path)) as con:
        try:
            rows = con.execute('SELECT id, name, path, status, config FROM campaigns ORDER BY id').fetchall()
        except sqlite3.OperationalError:  # never migrated, so there is no campaigns table
            return []
    return [
        StoredCampaign(CampaignId(id_), name, repo, CampaignStatus(status), _stored(config))
        for id_, name, repo, status, config in rows
    ]


def _stored_secrets(cfg: Settings) -> list[SecretKey]:
    """The secrets of the home's provider, by name, read-only. None of a database without the table."""
    path = cfg.home / 'piloop.db'
    if not path.is_file():
        return []
    engine = create_engine(f'sqlite:///file:{path}?mode=ro&uri=true')
    try:
        with Session(engine) as s:
            return SECRETS.selected(cfg.providers).names(s)
    except OperationalError:  # a schema before the secrets: the database check says what to run
        return []
    finally:
        engine.dispose()


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
    """Yield ``ok    <name>: <found>`` for a pass, else ``FAIL`` or ``WARN`` in place of ``ok`` and a ``fix:`` line."""
    for c in results:
        yield f'{"ok  " if c.fix is None else c.severity}  {c.name}: {c.found}'
        if c.fix is not None:
            yield f'      fix: {c.fix}'
