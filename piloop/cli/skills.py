"""Install the orchestrator skills (``piloop-setup``, ``piloop-operate``, ``piloop-review``) for every harness.

The repo's ``skills/`` directory is the only source. It is an Agent Skills package (agentskills.io). Each
``skills/<name>/SKILL.md`` drives piloop through its CLI only, so every harness with a shell reads the same text.
``install`` copies the skills into the Agent Skills directory, which any harness that follows agentskills.io reads,
and into the user skills directory of each harness on PATH (``Harness.skills_dir``). ``drift`` finds a copy that is
missing or out of date, for ``piloop doctor``. The installed CLI copies its own skills, so the skills always describe
the commands of that CLI.
"""

import shutil
from pathlib import Path

from piloop.plugins.harness.contract import each_provider
from piloop.settings import Settings

PREFIX = 'piloop-'
"""Every skill that piloop ships is named ``piloop-*``. A directory with this prefix in a skills directory is piloop's.
"""


def source(package: Path = Path(__file__).parent) -> Path:
    """Return the directory of the skills that this piloop ships: the wheel's copy, else the repo's ``skills/``.

    The wheel carries the repo's ``skills/`` as ``piloop/cli/agent_skills`` (the force-include in pyproject.toml).
    """
    wheel = package / 'agent_skills'
    return wheel if wheel.is_dir() else package.parent.parent / 'skills'


def bundled() -> dict[str, Path]:
    """Return the skills that this piloop ships, by name."""
    return {p.parent.name: p.parent for p in sorted(source().glob('*/SKILL.md'))}


def targets(s: Settings) -> dict[Path, list[str]]:
    """Return the Agent Skills directory and that of each harness on PATH, each with the harnesses on PATH that read it."""
    found: dict[Path, list[str]] = {s.agent_skills_dir: []}
    for name, harness in each_provider().items():
        if shutil.which(harness.executable):
            found.setdefault(harness.skills_dir(s), []).append(name)
    return found


def readers(harnesses: list[str]) -> str:
    """Who reads a skills directory, for a line of output: its harnesses on PATH, else the Agent Skills standard."""
    return ', '.join(harnesses) or 'Agent Skills'


def drift(target: Path) -> list[str]:
    """Return how the piloop skills in ``target`` differ from the bundled ones. Empty when they are current."""
    shipped = bundled()
    problems = []
    for name, src in shipped.items():
        if not (target / name).is_dir():
            problems.append(f'{name} missing')
        elif _files(target / name) != _files(src):
            problems.append(f'{name} out of date')
    return problems + [
        f'{p.name} no longer shipped' for p in sorted(target.glob(f'{PREFIX}*')) if p.name not in shipped
    ]


def install(target: Path) -> None:
    """Make the ``piloop-*`` skills in ``target`` the bundled ones. Skills that piloop no longer ships are removed."""
    target.mkdir(parents=True, exist_ok=True)
    for existing in target.glob(f'{PREFIX}*'):
        _remove(existing)
    for name, src in bundled().items():
        shutil.copytree(src, target / name)


def _files(root: Path) -> dict[Path, bytes]:
    return {f.relative_to(root): f.read_bytes() for f in root.rglob('*') if f.is_file()}


def _remove(path: Path) -> None:
    """Remove a copy of a skill, or a symlink to a checkout that someone made."""
    if path.is_symlink() or not path.is_dir():
        path.unlink()
    else:
        shutil.rmtree(path)
