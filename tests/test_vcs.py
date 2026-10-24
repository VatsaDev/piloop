"""The vcs seam: core reaches git only through the contract, so the ``git`` provider is the only code that runs it."""

import ast
from pathlib import Path

import piloop

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
