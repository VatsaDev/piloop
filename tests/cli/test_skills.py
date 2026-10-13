"""The orchestrator skills: an Agent Skills package at the repo root, copied into the skills directory of each
harness on PATH, with drift reported."""

import os
import re
from pathlib import Path

import pytest
from rich.console import Console

from piloop.cli import main as cli, skills
from piloop.settings import Settings


@pytest.fixture
def harness_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    settings = Settings(
        home=tmp_path / 'piloop', claude_config_dir=tmp_path / 'claude', agent_skills_dir=tmp_path / 'agents/skills'
    )
    monkeypatch.setattr(cli, 'settings', settings)
    monkeypatch.setattr(cli, 'console', Console(record=True, width=300, force_terminal=False))
    monkeypatch.setenv('PATH', str(tmp_path / 'bin'))
    (tmp_path / 'bin').mkdir()
    return settings


def on_path(*names: str) -> None:
    for name in names:
        exe = Path(os.environ['PATH']) / name
        exe.write_text('#!/bin/sh\n')
        exe.chmod(0o755)


def printed() -> str:
    return cli.console.export_text(clear=True)


ROOT = Path(__file__).parent.parent.parent
COMMAND = re.compile(r'\bpiloop ([a-z][a-z-]*)(?: ([a-z][a-z-]*))?')
"""``piloop <command> [<subcommand>]`` as a skill or AGENTS.md writes it."""


def test_every_skill_is_an_agent_skill_named_after_its_directory() -> None:
    shipped = skills.bundled()
    assert list(shipped) == ['piloop-operate', 'piloop-review', 'piloop-setup']
    for name, path in shipped.items():
        text = (path / 'SKILL.md').read_text()
        front = re.match(r'---\nname: (.+)\ndescription: (.+)\n---\n', text)
        assert front is not None and front[1] == name and len(front[2]) <= 1024, 'the frontmatter every harness reads'
        assert len(text.splitlines()) <= 500, 'the Agent Skills soft cap on a SKILL.md'
        assert path == ROOT / 'skills' / name, 'a checkout ships its own skills/'


def test_every_piloop_command_the_skills_name_exists() -> None:
    """A renamed or deleted command fails here, not in the shell of an orchestrator."""
    files = [*sorted((ROOT / 'skills').glob('*/SKILL.md')), ROOT / 'AGENTS.md']
    code = [part for f in files for part in f.read_text().split('`')[1::2]]  # inline spans and fenced blocks
    named = {m.groups() for part in code for m in COMMAND.finditer(part)}
    missing = sorted(
        f'piloop {" ".join(filter(None, (command, sub)))}'
        for command, sub in named
        if command not in cli.app or (sub and _subcommands(command) and sub not in cli.app[command])
    )
    assert missing == []
    assert ('inbox', 'wait') in named, 'the scan reads the skills at all'


def _subcommands(command: str) -> set[str]:
    return {name for name in cli.app[command] if not name.startswith('-')}


def test_a_wheel_ships_the_skills_inside_the_package(tmp_path: Path) -> None:
    package = tmp_path / 'piloop' / 'cli'
    (tmp_path / 'skills').mkdir()
    package.mkdir(parents=True)
    assert skills.source(package) == tmp_path / 'skills', 'a checkout: the repo root'
    (package / 'agent_skills').mkdir()
    assert skills.source(package) == package / 'agent_skills', "a wheel: pyproject's force-include"


def test_install_skills_copies_into_each_harness_dir_once_and_replaces_only_piloops_own(
    harness_home: Settings, tmp_path: Path
) -> None:
    cli.install_skills()
    assert printed().strip() == 'skills: none of claude, codex, pi, gemini, cursor-agent is on PATH, nothing installed'
    on_path('claude', 'codex', 'pi', 'gemini', 'cursor-agent')
    agents = harness_home.agent_skills_dir
    claude = harness_home.claude_config_dir / 'skills'
    assert skills.drift(agents) == ['piloop-operate missing', 'piloop-review missing', 'piloop-setup missing']
    (agents / 'piloop-old').mkdir(parents=True)
    (agents / 'theirs').mkdir()
    (agents / 'piloop-setup').symlink_to(tmp_path / 'checkout')
    (claude / 'piloop-operate').mkdir(parents=True)
    (claude / 'piloop-operate' / 'SKILL.md').write_text('an older operate')
    assert skills.drift(claude) == ['piloop-operate out of date', 'piloop-review missing', 'piloop-setup missing']
    assert skills.drift(agents)[-1] == 'piloop-old no longer shipped'
    cli.install_skills()
    assert printed().splitlines() == [
        f'skills: piloop-operate, piloop-review, piloop-setup → {claude} (claude)',
        f'skills: piloop-operate, piloop-review, piloop-setup → {agents} (codex, pi, gemini, cursor-agent)',
    ]
    assert skills.drift(claude) == skills.drift(agents) == []
    assert sorted(p.name for p in agents.iterdir()) == ['piloop-operate', 'piloop-review', 'piloop-setup', 'theirs']
    assert not (agents / 'piloop-setup').is_symlink()
