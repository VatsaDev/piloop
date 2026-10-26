"""``piloop doctor``: every check passes on a healthy install and each failure names its fix; nothing is written."""

import json
import os
import shutil
import sqlite3
from collections.abc import Mapping
from contextlib import closing
from importlib.metadata import distribution
from pathlib import Path

import pytest
from alembic import command
from alembic.script import ScriptDirectory
from pydantic import SecretStr
from pytest_mock import MockerFixture

from piloop.actor import System
from piloop.cli import doctor, skills, upgrade
from piloop.cli.doctor import checks, report
from piloop.config import CampaignConfig
from piloop.db.models import Campaign
from piloop.db.session import Database
from piloop.ids import CampaignStatus
from piloop.plugins.host import Check, Severity
from piloop.plugins.secrets.contract import SECRETS, set_secret
from piloop.settings import Settings
from piloop.version import Version, code_digest, installed
from tests.cli.test_cli import git_check
from tests.conftest import hold_lock, reconfigured, store_config
from tests.db.test_migrations import alembic

HEAD = ScriptDirectory.from_config(alembic(Path('unused.db'))).get_current_head()


@pytest.fixture
def cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """The test's home, a uv receipt of a git install, and a PATH holding only what a test puts in ``bin``."""
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir(exist_ok=True)
    for tool in ('git', 'sh', 'wc'):  # what the campaign fixture's repo and evaluate command need
        (bin_dir / tool).symlink_to(shutil.which(tool) or tool)
    monkeypatch.setenv('PATH', str(bin_dir))
    receipt = tmp_path / 'uv' / 'piloop' / 'uv-receipt.toml'
    receipt.parent.mkdir(parents=True)
    receipt.write_text('[tool]\nrequirements = [{ name = "piloop", git = "https://github.com/hiloopai/piloop" }]\n')
    return Settings(home=tmp_path, uv_tool_dir=tmp_path / 'uv')


def on_path(tmp_path: Path, name: str, target: Path | None = None) -> None:
    link = tmp_path / 'bin' / name
    if target is None:
        link.write_text('#!/bin/sh\n')
        link.chmod(0o755)
    else:
        link.symlink_to(target)


def by_name(cfg: Settings) -> dict[str, Check]:
    return {c.name: c for c in checks(cfg)}


def test_a_healthy_install_passes_every_check(
    cfg: Settings, campaign: Campaign, tmp_path: Path, mocker: MockerFixture
) -> None:
    on_path(tmp_path, 'pi')
    skills.install(cfg.agent_skills_dir)
    mocker.patch.object(doctor, 'direct_url', return_value=json.dumps({'url': 'file:///src/piloop', 'dir_info': {}}))
    lines = list(report(checks(cfg)))
    assert lines == [
        f'ok    cli: {installed()}; not checked for a newer one: installed from file:///src/piloop: update that checkout',
        'ok    daemon: not running',
        f'ok    database: {tmp_path}/piloop.db at schema {HEAD}, current',
        'ok    plugins: 5 CAS providers, 1 executor, 3 fact sources, 2 networks, 1 provisioner, 4 roles, 1 runtime, 1 secrets provider, 19 sections, 1 stop rule, 9 triggers, 1 vcs provider, 1 verdict rule; all built in',
        'ok    secrets: from `piloop secret set` (PILOOP_SECRETS_COMMAND unset)',
        f'ok    campaign demo: {campaign.path}/piloop.toml loads',
        f'ok    skills (pi): current in {cfg.agent_skills_dir}',
        'ok    harnesses: on PATH: script',
        git_check(),
    ]


def test_a_git_older_than_piloop_needs_or_one_that_does_not_run_fails_with_the_fix(
    cfg: Settings, tmp_path: Path
) -> None:
    git = tmp_path / 'bin' / 'git'
    git.unlink()
    fix = 'install git 2.40 or later'
    missing = by_name(cfg)['git']
    assert missing.found.startswith('`git --version` failed: ') and missing.fix == fix
    git.write_text('#!/bin/sh\nexit 3\n')
    git.chmod(0o755)
    assert by_name(cfg)['git'].fix == fix, 'a git that fails'
    git.write_text('#!/bin/sh\n[ "$1" = --version ] && echo "git version 2.39.5 (Apple Git-154)"\n')
    assert by_name(cfg)['git'] == Check('git', 'git version 2.39.5 (Apple Git-154); piloop needs 2.40 or later', fix)
    git.write_text('#!/bin/sh\n[ "$1" = --version ] && echo "git version 2.40.0"\n')
    assert by_name(cfg)['git'] == Check('git', 'git version 2.40.0')


def test_skills_missing_or_stale_for_a_harness_on_path_fail_with_the_install_command(
    cfg: Settings, tmp_path: Path
) -> None:
    assert by_name(cfg)['skills'].fix is None, 'no harness, nothing to check'
    on_path(tmp_path, 'claude')
    claude = tmp_path / 'harnesses' / 'claude'  # CLAUDE_CONFIG_DIR of every test (conftest.no_real_skill_dirs)
    skills.install(claude / 'skills')
    (claude / 'skills' / 'piloop-review' / 'SKILL.md').write_text('an older review')
    assert by_name(cfg)['skills (claude)'] == Check(
        'skills (claude)', f'{claude}/skills: piloop-review out of date', '`piloop install-skills`'
    )


def test_a_daemon_started_before_the_code_changed_still_runs_the_old_code(cfg: Settings) -> None:
    """The daemon's code is what it started on, whatever the file times: a reinstall can finish after a restart."""
    for holder in hold_lock(cfg.pid_file):
        for started_on in (None, 'another digest'):
            if started_on is not None:
                cfg.code_file.write_text(started_on)
            stale = by_name(cfg)['daemon']
            assert (
                stale.found.startswith(f'pid {holder.pid}, started ')
                and 'runs other code than the installed' in stale.found
            ), 'a daemon from before the digest, or on other code'
            assert stale.fix == '`piloop restart` restarts it on the installed code'
        cfg.code_file.write_text(code_digest())
        os.utime(cfg.pid_file, (0, 0))
        assert by_name(cfg)['daemon'] == Check('daemon', f'pid {holder.pid} runs the installed {installed()}')


def test_a_database_missing_unmigrated_behind_or_ahead_says_which_command_brings_it_to_head(cfg: Settings) -> None:
    path = cfg.home / 'piloop.db'
    assert by_name(cfg)['database'] == Check(
        'database', f'{path} does not exist', '`piloop init <repo>` creates it with the first campaign'
    )
    path.touch()
    behind = f'{path} is at schema revision none, and this {installed()} knows {HEAD}'
    assert by_name(cfg)['database'] == Check('database', behind, 'run `piloop migrate`, which backs it up first')
    for daemon in hold_lock(cfg.pid_file):
        assert by_name(cfg)['database'] == Check(
            'database',
            behind,
            f'the daemon (pid {daemon.pid}) runs an older piloop. `piloop restart` restarts it on this piloop, which'
            ' migrates it',
        ), 'piloop migrate refuses under the daemon of an older piloop'
    command.upgrade(alembic(path), 'head')
    assert by_name(cfg)['database'] == Check('database', f'{path} at schema {HEAD}, current')
    with closing(sqlite3.connect(path)) as con:
        con.execute("UPDATE alembic_version SET version_num = '9999_from_the_future'")
        con.commit()
    before = path.read_bytes()
    ahead = by_name(cfg)['database']
    assert ahead == Check(
        'database',
        f'{path} was migrated by a different piloop build (revision 9999_from_the_future is not in the history of'
        f' this {installed()}, which knows up to {HEAD})',
        'install the piloop that migrated it (`piloop upgrade` for a newer release), or set PILOOP_HOME to another'
        ' directory',
    )
    assert path.read_bytes() == before and not (cfg.home / 'piloop.db-wal').exists(), 'opened read-only'


def test_a_campaign_whose_toml_is_gone_invalid_or_whose_stored_copy_no_longer_validates_fails(
    cfg: Settings, db: Database, campaign: Campaign
) -> None:
    toml = Path(campaign.path) / 'piloop.toml'
    text = toml.read_text()
    toml.write_text(text.replace('role = "primary"', 'role = "boss"'))
    broken = by_name(cfg)['campaign demo']
    assert broken.found.startswith(f'{toml} is not a valid piloop config (1 problem(s)):\n  metrics[0].role = ')
    assert broken.fix == f'fix {toml}, then `piloop init {campaign.path}` re-reads it'
    toml.unlink()
    assert by_name(cfg)['campaign demo'] == Check(
        'campaign demo', f'no piloop.toml in {campaign.path}', f'restore it, then `piloop init {campaign.path}`'
    )
    toml.write_text(text)
    store_config(db, campaign.id, {'name': 'demo'})
    assert by_name(cfg)['campaign demo'] == Check(
        'campaign demo',
        'its stored copy (campaigns.config, what the daemon schedules from) is not a valid piloop config (3 problem(s)):\n'
        '  metrics: Field required. Fix: add the key; https://piloop.autoresearch.inc/reference/configuration says what it takes\n'
        '  evaluate: Field required. Fix: add the key; https://piloop.autoresearch.inc/reference/configuration says what it takes\n'
        '  model: Field required. Fix: add the key; https://piloop.autoresearch.inc/reference/configuration says what it takes',
        f'`piloop init {campaign.path}`',
    )
    assert by_name(cfg)['harnesses'].found == 'on PATH: none configured', 'no valid stored config, no harness to check'


def test_the_toml_of_a_stopped_campaign_that_does_not_load_is_a_warning(
    cfg: Settings, db: Database, campaign: Campaign
) -> None:
    """The daemon schedules a campaign from its stored copy: a stopped one reads its file again only at ``piloop init``."""
    toml = Path(campaign.path) / 'piloop.toml'
    toml.write_text(toml.read_text().replace('role = "primary"', 'role = "boss"'))
    with db.session() as s:
        s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
        s.commit()
    found = by_name(cfg)['campaign demo']
    assert (found.severity, found.fails, found.fix) == (
        Severity.WARN,
        False,
        f'fix {toml}, then `piloop init {campaign.path}` re-reads it',
    )
    assert next(line for line in report([found])).startswith(
        f'WARN  campaign demo: {toml} is not a valid piloop config'
    )


def test_a_secret_that_a_stopped_campaign_lacks_is_a_warning(
    cfg: Settings, db: Database, campaign: Campaign, monkeypatch: pytest.MonkeyPatch
) -> None:
    """As its ``piloop.toml``: a stopped campaign runs no agent that needs the secret until ``piloop init`` resumes it."""
    give_watchdog(db, campaign, {}, {'GATEWAY_KEY': {'secret': 'TEAM_KEY'}})
    monkeypatch.delenv('TEAM_KEY', raising=False)
    assert by_name(cfg)['secrets demo'].fails, 'an active campaign needs it now'
    with db.session() as s:
        s.get_one(Campaign, campaign.id).status = CampaignStatus.STOPPED
        s.commit()
    found = by_name(cfg)['secrets demo']
    assert (found.found, found.severity, found.fails) == ('not set: TEAM_KEY', Severity.WARN, False)


def test_a_missing_harness_is_named(cfg: Settings, db: Database, campaign: Campaign) -> None:
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        row.config = reconfigured(row.config, harnesses=['script', 'codex'], model={'script': 'm', 'codex': 'm'})
        s.commit()
    assert by_name(cfg)['harnesses'] == Check(
        'harnesses',
        'not on PATH: codex (codex)',
        'install them, or take them out of `harnesses` in piloop.toml and `piloop init` the campaign',
    )


def test_mcp_servers_are_checked_for_their_commands_and_their_secrets_by_name_only(
    cfg: Settings, db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    servers = {
        'exa': {'url': 'https://mcp.exa.ai/mcp', 'headers_env': {'x-api-key': 'EXA_API_KEY'}},
        'xai': {'command': ['xai-server'], 'env': ['XAI_API_KEY', 'EXA_API_KEY']},
    }
    give_watchdog(
        db, campaign, servers, {'GATEWAY_URL': 'https://gw.example.com', 'GATEWAY_KEY': {'secret': 'TEAM_KEY'}}
    )
    for name in ('EXA_API_KEY', 'XAI_API_KEY', 'TEAM_KEY'):
        monkeypatch.delenv(name, raising=False)
    assert 'mcp demo' not in by_name(Settings(home=tmp_path / 'empty')), 'no campaigns, nothing to check'
    assert by_name(cfg)['mcp demo'] == Check(
        'mcp demo', 'not on PATH: xai-server', 'install them, or fix the [mcp.*] tables'
    )
    on_path(tmp_path, 'xai-server')
    assert by_name(cfg)['mcp demo'] == Check('mcp demo', '2 server(s) (watchdog: exa, xai)')
    fix = '`piloop secret set <NAME>` for each'
    assert by_name(cfg)['secrets demo'] == Check('secrets demo', 'not set: EXA_API_KEY, TEAM_KEY, XAI_API_KEY', fix)
    with db.session() as s:
        set_secret(s, SECRETS.selected({}), System.HUMAN, None, 'EXA_API_KEY', SecretStr('exa-value'))
        set_secret(s, SECRETS.selected({}), System.HUMAN, campaign.id, 'TEAM_KEY', SecretStr('team-value'))
        s.commit()
    for _holder in hold_lock(cfg.pid_file):
        monkeypatch.setenv('XAI_API_KEY', 'xai-value')
        assert by_name(cfg)['secrets demo'].found == 'not set: XAI_API_KEY', (
            "the daemon's environment, not this shell's"
        )
    assert by_name(cfg)['secrets demo'] == Check(
        'secrets demo',
        'XAI_API_KEY from the environment of this shell',
        '`piloop secret set` each of them: piloop 0.5.0 reads secrets from `piloop secret` only',
        Severity.WARN,
    )
    with db.session() as s:
        set_secret(s, SECRETS.selected({}), System.HUMAN, None, 'XAI_API_KEY', SecretStr('xai-value'))
        s.commit()
    assert by_name(cfg)['secrets demo'] == Check('secrets demo', '3 set')
    monkeypatch.setattr('piloop.cli.doctor.running_pid', lambda _cfg: 2**30)
    assert by_name(cfg)['secrets'].found == 'from `piloop secret set` (PILOOP_SECRETS_COMMAND unset)', (
        'no process with that pid: this shell'
    )
    assert 'value' not in ' '.join(report(checks(cfg)))


def test_the_secrets_command_is_deprecated_and_doctor_runs_it_reporting_names_only(
    cfg: Settings, db: Database, campaign: Campaign, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    give_watchdog(db, campaign, {'xai': {'command': ['sh'], 'env': ['XAI_API_KEY', 'EXA_API_KEY']}})
    for name in ('EXA_API_KEY', 'XAI_API_KEY'):
        monkeypatch.delenv(name, raising=False)
    shell = Settings(home=cfg.home, secrets_command="printf 'XAI_API_KEY=%s%s' xai value")
    found = by_name(shell)
    assert found['secrets'] == Check(
        'secrets',
        "PILOOP_SECRETS_COMMAND of this shell (printf 'XAI_API_KEY=%s%s' xai value) prints XAI_API_KEY",
        '`piloop secret set` each of them, then unset PILOOP_SECRETS_COMMAND: piloop 0.5.0 removes it',
        Severity.WARN,
    )
    assert found['secrets demo'].found == 'not set: EXA_API_KEY', 'what the command prints counts as set'

    monkeypatch.setenv('PILOOP_SECRETS_COMMAND', 'exit 7')
    for holder in hold_lock(cfg.pid_file):
        found = by_name(shell)
        assert found['secrets'] == Check(
            'secrets',
            f'PILOOP_SECRETS_COMMAND of the daemon (pid {holder.pid}) (exit 7) failed: exit 7',
            'fix the command; `piloop serve` refuses to start while it fails',
        ), "the daemon's command, not this shell's"
        assert found['secrets demo'].found == 'not set: EXA_API_KEY, XAI_API_KEY'
    assert 'xaivalue' not in ' '.join(report(checks(shell)))


def give_watchdog(
    db: Database, campaign: Campaign, servers: Mapping[str, object], env: Mapping[str, object] | None = None
) -> None:
    """Give the watchdog ``servers`` on a harness that takes them, with ``env``."""
    script = {'executable': 'script', 'headless': ['{mcp}', '{prompt}'], 'mcp': 'claude', 'env': dict(env or {})}
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        roles = CampaignConfig.model_validate(row.config).roles.model_dump(mode='json')
        roles['watchdog']['mcp'] = list(servers)
        row.config = reconfigured(row.config, mcp=dict(servers), harness={'script': script}, roles=roles)
        s.commit()


GIT_URL = 'https://example.com/piloop'
OLD, NEW = 'a' * 40, 'b' * 40


def fake(tmp_path: Path, name: str, body: str) -> Path:
    """A fake CLI in the test's ``bin`` that records its argv in ``<name>.argv`` and then runs ``body``."""
    exe = tmp_path / 'bin' / name
    exe.unlink(missing_ok=True)  # the cfg fixture links the real git: never write through the link
    exe.write_text(f'#!/bin/sh\necho "$@" > {tmp_path / f"{name}.argv"}\n{body}\n')
    exe.chmod(0o755)
    return tmp_path / f'{name}.argv'


def git_install(rev: str | None) -> str:
    """The ``direct_url.json`` that uv writes for a ``git+`` install of ``rev`` (the default branch without one)."""
    vcs = {'vcs': 'git', 'commit_id': OLD} | ({'requested_revision': rev} if rev else {})
    return json.dumps({'url': GIT_URL, 'vcs_info': vcs})


def test_a_git_install_is_behind_when_its_branch_has_moved(cfg: Settings, tmp_path: Path) -> None:
    argv = fake(tmp_path, 'git', f"printf '{NEW}\\trefs/heads/main\\n'")
    assert upgrade.newer(cfg.uv_tool_dir, git_install('main')) == f'commit {NEW[:7]} on main of {GIT_URL}'
    assert argv.read_text().split() == ['ls-remote', GIT_URL, 'main']
    fake(tmp_path, 'git', f"printf '{OLD}\\tHEAD\\n'")
    assert upgrade.newer(cfg.uv_tool_dir, git_install(None)) is None, 'the default branch, at the installed commit'
    fake(tmp_path, 'git', f"printf '{NEW}\\trefs/tags/v0.1.0\\n'")
    with pytest.raises(upgrade.InstallError, match=f'^v0.1.0 is not a branch of {GIT_URL}: a pinned install'):
        upgrade.newer(cfg.uv_tool_dir, git_install('v0.1.0'))
    fake(tmp_path, 'git', 'echo "could not resolve host" >&2; exit 128')
    with pytest.raises(upgrade.InstallError, match=f'^git ls-remote {GIT_URL} main failed: could not resolve host$'):
        upgrade.newer(cfg.uv_tool_dir, git_install('main'))


def test_an_index_install_is_behind_when_uv_lists_it_as_outdated(cfg: Settings, tmp_path: Path) -> None:
    argv = fake(tmp_path, 'uv', "printf 'piloop v0.1.0 [latest: 99.0.0]\\n- piloop\\nruff v0.5.0 [latest: 0.17.1]\\n'")
    assert upgrade.newer(cfg.uv_tool_dir, None) == 'piloop 99.0.0'
    assert argv.read_text().split() == ['tool', 'list', '--outdated']
    fake(tmp_path, 'uv', "printf 'ruff v0.5.0 [latest: 0.17.1]\\n'")
    assert upgrade.newer(cfg.uv_tool_dir, None) is None
    (tmp_path / 'bin' / 'uv').unlink()
    with pytest.raises(
        upgrade.InstallError, match=r"^uv tool list --outdated failed: .*No such file or directory: 'uv'$"
    ):
        upgrade.newer(cfg.uv_tool_dir, None)


def test_an_index_install_is_behind_only_when_the_latest_release_is_newer_than_the_running_piloop(
    cfg: Settings, tmp_path: Path, mocker: MockerFixture
) -> None:
    """uv reports the piloop of its tool dir, which can be another install than the one running, such as an older one."""
    running = Version.parse(distribution('piloop').version)
    mocker.patch.object(doctor, 'direct_url', return_value=None)
    for latest in (running, Version(0, 1, 0)):
        fake(tmp_path, 'uv', f"printf 'piloop v0.1.0 [latest: {latest}]\\n'")
        assert by_name(cfg)['cli'] == Check('cli', f'{installed()}, the newest at its source')
    ahead = Version(running.major, running.minor, running.patch + 1)
    fake(tmp_path, 'uv', f"printf 'piloop v0.1.0 [latest: {ahead}]\\n'")
    assert by_name(cfg)['cli'].found.startswith(f'{installed()}; piloop {ahead} is out:')


def test_a_directory_install_or_no_uv_tool_is_not_checked_and_says_why(cfg: Settings, tmp_path: Path) -> None:
    with pytest.raises(upgrade.InstallError, match='^installed from file:///src/piloop: update that checkout$'):
        upgrade.newer(cfg.uv_tool_dir, json.dumps({'url': 'file:///src/piloop', 'dir_info': {'editable': True}}))
    with pytest.raises(upgrade.InstallError, match='^piloop is not installed as a uv tool'):
        upgrade.newer(tmp_path / 'elsewhere', None)


def test_a_newer_piloop_at_the_source_is_a_notice_that_says_who_upgrades_with_this_installs_command(
    cfg: Settings, tmp_path: Path, mocker: MockerFixture
) -> None:
    """Not a failure: an upgrade mid-campaign is one-way, so by default the orchestrator asks the human."""
    fake(tmp_path, 'git', f"printf '{NEW}\\trefs/heads/main\\n'")
    mocker.patch.object(doctor, 'direct_url', return_value=git_install('main'))
    newer = f'{installed()}; commit {NEW[:7]} on main of {GIT_URL} is out:'
    assert by_name(cfg)['cli'] == Check(
        'cli',
        f'{newer} ask the human before `piloop upgrade` (PILOOP_AUTO_UPGRADE is off); changes: {doctor.CHANGELOG}',
    )
    canary = Settings(home=cfg.home, uv_tool_dir=cfg.uv_tool_dir, cli='piloop-canary', auto_upgrade=True)
    assert by_name(canary)['cli'] == Check(
        'cli', f'{newer} PILOOP_AUTO_UPGRADE is on: `piloop-canary upgrade` installs it'
    )
    assert by_name(canary)['database'].fix == '`piloop-canary init <repo>` creates it with the first campaign'
    fake(tmp_path, 'git', f"printf '{OLD}\\trefs/heads/main\\n'")
    assert by_name(cfg)['cli'] == Check('cli', f'{installed()}, the newest at its source')
    fake(tmp_path, 'git', 'exit 2')
    assert by_name(cfg)['cli'] == Check(
        'cli', f'{installed()}; not checked for a newer one: git ls-remote {GIT_URL} main failed: exit 2'
    )


def test_a_pinned_index_install_is_not_reported_behind_because_uv_tool_upgrade_keeps_the_pin(
    cfg: Settings, tmp_path: Path
) -> None:
    (cfg.uv_tool_dir / 'piloop' / 'uv-receipt.toml').write_text(
        '[tool]\nrequirements = [{ name = "piloop", specifier = "==0.1.0" }]\n'
    )
    fake(tmp_path, 'uv', "printf 'piloop v0.1.0 [latest: 0.2.0]\\n'")
    with pytest.raises(upgrade.InstallError, match=r'^installed as piloop==0\.1\.0, which `uv tool upgrade` keeps$'):
        upgrade.newer(cfg.uv_tool_dir, None)


def test_the_source_check_never_waits_for_a_password(cfg: Settings, tmp_path: Path) -> None:
    """No stdin, no git prompt, ssh in batch mode: a private repo without credentials fails at once."""
    seen = tmp_path / 'seen'
    fake(
        tmp_path,
        'git',
        f'read answer; echo "stdin=[$answer] prompt=$GIT_TERMINAL_PROMPT ssh=$GIT_SSH_COMMAND" > {seen}; exit 128',
    )
    with pytest.raises(upgrade.InstallError, match='failed: exit 128'):
        upgrade.newer(cfg.uv_tool_dir, git_install('main'))
    assert seen.read_text().strip() == 'stdin=[] prompt=0 ssh=ssh -o BatchMode=yes'
