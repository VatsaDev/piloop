"""``piloop doctor``: every check passes on a healthy install and each failure names its fix; nothing is written."""

import json
import os
import shutil
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest
from alembic import command
from alembic.script import ScriptDirectory

from piloop.cli import skills
from piloop.cli.doctor import Check, checks, report
from piloop.cli.version import installed
from piloop.config import ProjectConfig
from piloop.db.models import Project
from piloop.db.session import Database
from piloop.settings import Settings
from tests.conftest import FAKE_HERDR, hold_lock
from tests.db.test_migrations import alembic
from tests.runtime.test_herdr import scripted

HEAD = ScriptDirectory.from_config(alembic(Path('unused.db'))).get_current_head()


@pytest.fixture
def cfg(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    """The test's home, a uv receipt of a git install, and a PATH holding only what a test puts in ``bin``."""
    bin_dir = tmp_path / 'bin'
    bin_dir.mkdir(exist_ok=True)
    for tool in ('git', 'sh', 'wc'):  # what the project fixture's repo and evaluate command need
        (bin_dir / tool).symlink_to(shutil.which(tool) or tool)
    (bin_dir / 'python3').symlink_to(sys.executable)  # the fake herdr's interpreter
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


def test_a_healthy_install_passes_every_check(cfg: Settings, project: Project, tmp_path: Path) -> None:
    on_path(tmp_path, 'pi')
    skills.install(cfg.agent_skills_dir)
    lines = list(report(checks(cfg)))
    assert lines == [
        'ok    daemon: not running',
        f'ok    database: {tmp_path}/piloop.db at schema {HEAD}, current',
        'ok    secrets: from the environment of this shell only (PILOOP_SECRETS_COMMAND unset)',
        f'ok    project demo: {project.path}/piloop.toml loads',
        f'ok    skills (pi): current in {cfg.agent_skills_dir}',
        'ok    harnesses: on PATH: script',
    ], 'a subprocess-runtime project needs no Herdr check'


def test_skills_missing_or_stale_for_a_harness_on_path_fail_with_the_install_command(
    cfg: Settings, tmp_path: Path
) -> None:
    assert by_name(cfg)['skills'].fix is None, 'no harness, nothing to check'
    on_path(tmp_path, 'claude')
    skills.install(cfg.claude_config_dir / 'skills')
    (cfg.claude_config_dir / 'skills' / 'piloop-review' / 'SKILL.md').write_text('an older review')
    assert by_name(cfg)['skills (claude)'] == Check(
        'skills (claude)', f'{cfg.claude_config_dir}/skills: piloop-review out of date', '`piloop install-skills`'
    )


def test_a_daemon_started_before_the_code_changed_still_runs_the_old_code(cfg: Settings) -> None:
    for holder in hold_lock(cfg.pid_file):
        os.utime(cfg.pid_file, (0, 0))
        stale = by_name(cfg)['daemon']
        assert (
            stale.found.startswith(f'pid {holder.pid}, started ')
            and 'runs older code than the installed' in stale.found
        )
        assert stale.fix == '`piloop restart` restarts it on the installed code'
        os.utime(cfg.pid_file, (4e9, 4e9))
        assert by_name(cfg)['daemon'] == Check('daemon', f'pid {holder.pid} runs the installed {installed()}')


def test_a_database_missing_unmigrated_behind_or_ahead_says_which_command_brings_it_to_head(cfg: Settings) -> None:
    path = cfg.home / 'piloop.db'
    assert by_name(cfg)['database'] == Check(
        'database', f'none at {path}', '`piloop init <repo>` creates it with the first project'
    )
    path.touch()
    assert by_name(cfg)['database'] == Check(
        'database', f'{path} at schema none, this piloop is at {HEAD}', '`piloop migrate`'
    )
    command.upgrade(alembic(path), 'head')
    assert by_name(cfg)['database'] == Check('database', f'{path} at schema {HEAD}, current')
    with closing(sqlite3.connect(path)) as con:
        con.execute("UPDATE alembic_version SET version_num = '9999_from_the_future'")
        con.commit()
    before = path.read_bytes()
    assert by_name(cfg)['database'].fix == '`piloop upgrade`'
    assert path.read_bytes() == before and not (cfg.home / 'piloop.db-wal').exists(), 'opened read-only'


def test_a_project_whose_toml_is_gone_invalid_or_whose_stored_copy_no_longer_validates_fails(
    cfg: Settings, db: Database, project: Project
) -> None:
    toml = Path(project.path) / 'piloop.toml'
    text = toml.read_text()
    toml.write_text(text.replace('role = "primary"', 'role = "boss"'))
    broken = by_name(cfg)['project demo']
    assert broken.found.startswith(f'{toml} is not a valid piloop config (1 problem(s)):\n  metrics[0].role = ')
    assert broken.fix == f'fix {toml}, then `piloop init {project.path}` re-reads it'
    toml.unlink()
    assert by_name(cfg)['project demo'] == Check(
        'project demo', f'no piloop.toml in {project.path}', f'restore it, then `piloop init {project.path}`'
    )
    toml.write_text(text)
    with db.session() as s:
        stale: dict[str, object] = {'name': 'demo'}
        s.get_one(Project, project.id).config = stale
        s.commit()
    assert by_name(cfg)['project demo'] == Check(
        'project demo',
        'its stored copy (projects.config, what the daemon schedules from) is not a valid piloop config (3 problem(s)):\n'
        '  metrics: Field required. Fix: add the key; https://piloop.autoresearch.inc/reference/configuration says what it takes\n'
        '  evaluate: Field required. Fix: add the key; https://piloop.autoresearch.inc/reference/configuration says what it takes\n'
        '  model: Field required. Fix: add the key; https://piloop.autoresearch.inc/reference/configuration says what it takes',
        f'`piloop init {project.path}`',
    )
    assert by_name(cfg)['harnesses'].found == 'on PATH: none configured', 'no valid stored config, no harness to check'


def test_a_missing_harness_and_herdr_are_named(
    cfg: Settings, db: Database, project: Project, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with db.session() as s:
        row = s.get_one(Project, project.id)
        row.config = {
            **row.config,
            'runtime': 'herdr',
            'harnesses': ['script', 'codex'],
            'model': {'script': 'm', 'codex': 'm'},
        }
        s.commit()
    found = by_name(cfg)
    assert found['harnesses'] == Check(
        'harnesses',
        'not on PATH: codex (codex)',
        'install them, or take them out of `harnesses` in piloop.toml and `piloop init` the project',
    )
    assert found['herdr'] == Check(
        'herdr', 'a project uses the herdr runtime and herdr is not on PATH', 'install herdr'
    )
    on_path(tmp_path, 'herdr', FAKE_HERDR)
    monkeypatch.setenv('FAKE_HERDR_LOG', str(tmp_path / 'herdr.log'))
    assert by_name(cfg)['herdr'] == Check('herdr', 'the piloop session answers')
    scripted(monkeypatch, workspace_list=[{'error': {'code': 'server_not_running', 'message': 'no server'}}])
    assert by_name(cfg)['herdr'] == Check('herdr', 'server not running; started on first use')
    calls = [json.loads(line) for line in (tmp_path / 'herdr.log').read_text().splitlines()]
    assert calls[-2][2:] == ['workspace', 'list'] and calls[-1] == ['integration', 'status'], (
        'asked, never started: no `server` call'
    )
    scripted(monkeypatch, workspace_list=[{'error': {'code': 'boom', 'message': 'socket refused'}}])
    assert by_name(cfg)['herdr'] == Check(
        'herdr', 'the piloop session does not answer: boom: socket refused', '`herdr server stop`, then retry'
    )


def test_missing_herdr_hooks_for_a_harness_of_a_herdr_project_fail(
    cfg: Settings, db: Database, project: Project, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Herdr reports no session for an agent whose harness lacks Herdr's hooks, so doctor names the install command."""
    with db.session() as s:
        row = s.get_one(Project, project.id)
        row.config = {**row.config, 'runtime': 'herdr', 'harnesses': ['codex'], 'model': {'codex': 'm'}}
        s.commit()
    on_path(tmp_path, 'herdr', FAKE_HERDR)
    monkeypatch.setenv('FAKE_HERDR_LOG', str(tmp_path / 'herdr.log'))
    assert by_name(cfg)['herdr hooks'] == Check('herdr hooks', 'current for codex')
    status = 'claude: current (v8) (/h/c.sh)\ncodex: not installed (/h/.codex/herdr-agent-state.sh)\nnoise\n'
    scripted(monkeypatch, integration_status=[{'text': status}])
    assert by_name(cfg)['herdr hooks'] == Check(
        'herdr hooks', 'codex: not installed', '`herdr integration install codex`'
    )
    scripted(monkeypatch, integration_status=[{'error': {'code': 'unknown_command', 'message': 'integration'}}])
    assert by_name(cfg)['herdr hooks'] == Check(
        'herdr hooks',
        "`herdr integration status` failed: Command '['herdr', 'integration', 'status']' returned non-zero exit status 1.",
        'upgrade herdr',
    )
    scripted(monkeypatch, integration_status=[{'text': ''}])
    assert by_name(cfg)['herdr hooks'] == Check('herdr hooks', 'current for no harness Herdr has hooks for')


def test_mcp_servers_are_checked_for_their_commands_and_the_variables_they_name_by_name_only(
    cfg: Settings, db: Database, project: Project, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    servers = {
        'exa': {'url': 'https://mcp.exa.ai/mcp', 'headers_env': {'x-api-key': 'EXA_API_KEY'}},
        'xai': {'command': ['xai-server'], 'env': ['XAI_API_KEY', 'EXA_API_KEY']},
    }
    with db.session() as s:
        row = s.get_one(Project, project.id)
        row.config = {**row.config, 'mcp': servers}
        s.commit()
    assert 'mcp demo' not in by_name(Settings(home=tmp_path / 'empty')), 'no projects, nothing to check'
    assert by_name(cfg)['mcp demo'] == Check(
        'mcp demo', 'not on PATH: xai-server', 'install them, or fix the [mcp.*] tables'
    )
    on_path(tmp_path, 'xai-server')
    fix = (
        'print them from PILOOP_SECRETS_COMMAND, or set them in the environment `piloop serve` runs in;'
        ' then restart the daemon'
    )
    assert by_name(cfg)['mcp demo'] == Check('mcp demo', 'this shell lacks EXA_API_KEY, XAI_API_KEY', fix)
    monkeypatch.setenv('EXA_API_KEY', 'exa-value')
    for holder in hold_lock(cfg.pid_file):
        found = by_name(cfg)['mcp demo']
        assert found == Check('mcp demo', f'the daemon (pid {holder.pid}) lacks XAI_API_KEY', fix), (
            "the daemon's environment, not this shell's"
        )
        monkeypatch.setenv('XAI_API_KEY', 'xai-value')
        assert by_name(cfg)['mcp demo'].found == f'the daemon (pid {holder.pid}) lacks XAI_API_KEY'
    monkeypatch.setattr('piloop.cli.doctor.running_pid', lambda _cfg: 2**30)
    with db.session() as s:
        row = s.get_one(Project, project.id)
        roles = ProjectConfig.model_validate(row.config).roles.model_dump(mode='json')
        roles['watchdog']['mcp'] = ['exa', 'xai']
        script = {'executable': 'script', 'headless': ['{mcp}', '{prompt}'], 'mcp': 'claude'}
        row.config = {**row.config, 'harness': {'script': script}, 'roles': roles, 'harnesses': ['script']}
        s.commit()
    found = by_name(cfg)['mcp demo']
    assert found == Check('mcp demo', '2 server(s) (watchdog: exa, xai), 2 variable(s) set in this shell'), (
        'no process with that pid: this shell'
    )
    assert 'value' not in ' '.join(report(checks(cfg)))


def test_secrets_come_from_the_daemons_secrets_command_which_doctor_runs_reporting_names_only(
    cfg: Settings, db: Database, project: Project, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with db.session() as s:
        row = s.get_one(Project, project.id)
        row.config = {**row.config, 'mcp': {'xai': {'command': ['sh'], 'env': ['XAI_API_KEY', 'EXA_API_KEY']}}}
        s.commit()
    shell = Settings(home=cfg.home, secrets_command="printf 'XAI_API_KEY=%s%s' xai value")
    found = by_name(shell)
    assert found['secrets'] == Check(
        'secrets', "PILOOP_SECRETS_COMMAND of this shell (printf 'XAI_API_KEY=%s%s' xai value) prints XAI_API_KEY"
    )
    assert found['mcp demo'].found == 'this shell lacks EXA_API_KEY', 'what the command prints counts as set'

    monkeypatch.setenv('PILOOP_SECRETS_COMMAND', 'exit 7')
    for holder in hold_lock(cfg.pid_file):
        found = by_name(shell)
        assert found['secrets'] == Check(
            'secrets',
            f'PILOOP_SECRETS_COMMAND of the daemon (pid {holder.pid}) (exit 7) failed: exit 7',
            'fix the command; `piloop serve` refuses to start while it fails',
        ), "the daemon's command, not this shell's"
        assert found['mcp demo'].found == f'the daemon (pid {holder.pid}) lacks XAI_API_KEY, EXA_API_KEY'
    assert 'xaivalue' not in ' '.join(report(checks(shell)))
