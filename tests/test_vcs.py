"""The vcs seam: core reaches git only through the contract, so the ``git`` provider is the only code that runs it."""

import ast
import hashlib
from pathlib import Path

import pytest

import piloop
from piloop.agents.roles import PROMPTS
from piloop.plugins.vcs.contract import PART, VCS, Part, prompted
from piloop.plugins.vcs.git import PROMPTS as GIT_PROMPTS
from piloop.reports.overview import TOOLS

ROOT = Path(piloop.__file__).parent
PROVIDER = ROOT / 'plugins' / 'vcs' / 'git'
ALLOWED = {
    'capabilities.py': 'it re-exports every built-in provider for plugins, and nothing in piloop imports it',
    'db/migrations/versions/0006_research_memory.py': 'a migration is frozen at its revision, when every campaign was git',
    'cli/upgrade.py': "``git ls-remote`` of piloop's own install source, which is no campaign's",
}
"""What may reach git outside the provider, and why."""


def _git(module: str) -> bool:
    """Whether ``module`` is GitPython or a module of the ``git`` provider."""
    return module == 'git' or module.startswith(('git.', 'piloop.plugins.vcs.git'))


def _outside() -> list[Path]:
    return [p for p in ROOT.rglob('*.py') if not p.is_relative_to(PROVIDER) and str(p.relative_to(ROOT)) not in ALLOWED]


def _imports(tree: ast.AST) -> list[str]:
    found: list[str] = []
    for node in ast.walk(tree):
        match node:
            case ast.Import(names=names):
                found += [alias.name for alias in names]
            case ast.ImportFrom(module=str(module), level=0):
                found.append(module)
    return found


def _git_argv(tree: ast.AST) -> list[int]:
    """The lines of a list or tuple whose first item is ``'git'``: an argv that runs git."""
    return [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.List | ast.Tuple)
        and node.elts
        and isinstance(first := node.elts[0], ast.Constant)
        and first.value == 'git'
    ]


def test_only_the_git_provider_imports_git_or_runs_it() -> None:
    """``piloop/plugins/vcs/git/`` is the one place that imports GitPython or the provider's modules, or runs git."""
    imports: list[str] = []
    argv: list[str] = []
    for path in _outside():
        tree = ast.parse(path.read_text(), str(path))
        where = path.relative_to(ROOT)
        imports += [f'{where}: {m}' for m in _imports(tree) if _git(m)]
        argv += [f'{where}:{line}' for line in _git_argv(tree)]
    assert imports == [], 'core reaches git through piloop.plugins.vcs.contract'
    assert argv == [], 'core runs no git: the provider does'


def test_the_allowed_files_still_need_their_exception() -> None:
    """An exception whose reason is gone goes with it."""
    for rel in ALLOWED:
        tree = ast.parse((ROOT / rel).read_text())
        assert any(map(_git, _imports(tree))) or _git_argv(tree), f'{rel} no longer reaches git: delete it from ALLOWED'


BEFORE = {
    'TOOLS.txt': '66fdaa7a94533bed170a1cf9205102d2fcd8565f8d6ec4e8b252ce3cc4233d4e',
    'implementer.md': '5d886da05bcee55fb5c87b48df3f69e3b7e08973213322a7f1874c2623de1b2d',
    'research.md': '9e6c8f3071daf8cd35475ce2bc3645d7161650acd1591828328a7b30a012e680',
    'scout.md': '93b669faacc78e1333b5bd71e875e1369923781ff820d30ec5e7bbcd48f38947',
    'sql_orchestrator.md': 'cf764a8e7accf6894f2aa24690ad45f70a3f5958aa330bf2edbb5fe946a2e4d0',
    'sql_procedures.md': '75c0974dca5fd2fbad83384b186a3bf29ccabb93a918112140a26bda50ebd7c1',
    'watchdog.md': '2278c57e18f1bbfb79f4a4a0f567465141f755acdefd6e38e22a14f4d18fbad1',
    'watchdog_incident.md': '9b3ed86a60bbef091f7327f5c21e1d70be5ab8d97fe6170289b797a35c3d0870',
    'watchdog_progress.md': '3a717f95296438d6efe4dc5fd995293bd05bcdc69fdafadb6c6334abf3c5aa53',
}
"""The SHA-256 of each built-in prompt, and of the tools of the context, before the ``git`` provider wrote its parts."""

BUILT_IN = sorted(
    [*PROMPTS.glob('*.md'), *(Path(piloop.__file__).parent / 'plugins' / 'role' / 'prompts').glob('*.md')]
)
"""The built-in prompts: the briefs' own, and those of the built-in roles."""


def test_the_git_provider_writes_the_built_in_prompts_byte_for_byte_as_they_were() -> None:
    """Moving the git parts into the provider changed no brief. Edit a prompt on purpose: update its line."""
    vcs = VCS.selected({})
    rendered = {p.name: prompted(p.read_text(), vcs) for p in BUILT_IN}
    rendered['TOOLS.txt'] = '\n'.join(prompted(line, vcs) for line in TOOLS)
    assert {name: hashlib.sha256(text.encode()).hexdigest() for name, text in rendered.items()} == BEFORE


def test_each_part_is_in_a_built_in_prompt_and_the_git_provider_writes_each() -> None:
    texts = '\n'.join([*(p.read_text() for p in BUILT_IN), *TOOLS])
    assert set(PART.findall(texts)) == set(Part), 'a part that no prompt has is dead'
    assert {p.stem for p in GIT_PROMPTS.glob('*.md')} == set(Part)


def test_a_prompt_that_names_a_part_that_there_is_not_is_refused() -> None:
    with pytest.raises(ValueError, match="'nope' is not a valid Part"):
        prompted('{vcs:nope}', VCS.selected({}))
