"""The vcs seam: core reaches git only through the contract, so the ``git`` provider is the only code that runs it."""

import ast
import re
from pathlib import Path

import pytest

import piloop
from piloop.agents.roles import PROMPTS
from piloop.plugins.section.tools import TOOLS
from piloop.plugins.vcs.contract import PART, VCS, Part, prompted
from piloop.plugins.vcs.git import PROMPTS as GIT_PROMPTS

ROOT = Path(piloop.__file__).parent
PROVIDER = ROOT / 'plugins' / 'vcs' / 'git'
GITPYTHON = ('git', 'git.exc')
"""What migration 0006 imports of GitPython."""
ALLOWED: dict[str, tuple[tuple[str, ...], str]] = {
    'capabilities.py': (
        ('piloop.plugins.vcs.git',),
        'it re-exports every built-in provider for plugins, and nothing in piloop imports it',
    ),
    'db/migrations/versions/0006_research_memory.py': (
        GITPYTHON,
        'a migration is frozen at its revision, when every campaign was git',
    ),
    'cli/upgrade.py': ((), "``git ls-remote`` of piloop's own install source, which is no campaign's"),
}
"""What may reach git outside the provider: the modules that it may import (none: it may run git), and why."""


def _git(module: str) -> bool:
    """Whether ``module`` is GitPython or a module of the ``git`` provider."""
    return module == 'git' or module.startswith(('git.', 'piloop.plugins.vcs.git'))


def _outside() -> list[Path]:
    return [p for p in ROOT.rglob('*.py') if not p.is_relative_to(PROVIDER)]


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
        where = str(path.relative_to(ROOT))
        allowed = ALLOWED.get(where)
        imports += [f'{where}: {m}' for m in _imports(tree) if _git(m) and (allowed is None or m not in allowed[0])]
        if allowed is None or allowed[0]:
            argv += [f'{where}:{line}' for line in _git_argv(tree)]
    assert imports == [], 'core reaches git through piloop.plugins.vcs.contract'
    assert argv == [], 'core runs no git: the provider does'


def test_the_allowed_files_still_need_their_exception() -> None:
    """An exception whose reason is gone goes with it."""
    for rel, (modules, _) in ALLOWED.items():
        tree = ast.parse((ROOT / rel).read_text())
        needed = set(modules) <= set(_imports(tree)) if modules else bool(_git_argv(tree))
        assert needed, f'{rel} no longer reaches git so: change or delete it in ALLOWED'


BUILT_IN = sorted(
    [*PROMPTS.glob('*.md'), *(Path(piloop.__file__).parent / 'plugins' / 'role' / 'prompts').glob('*.md')]
)
"""The built-in prompts: the briefs' own, and those of the built-in roles."""


def test_every_built_in_provider_writes_every_part() -> None:
    """A provider that left a part out would leave a hole in a brief."""
    for name, provider in VCS.builtins().items():
        vcs = provider()
        assert [part for part in Part if not vcs.prompt(part).strip()] == [], f'{name} writes every part'


GIT_WORDS = re.compile(r'\bgit\b|commit|worktree|cherry-pick|\bsha\b|trailer', re.IGNORECASE)
"""Words that only a provider's parts may say: git's own, and its name for a version."""
ALLOWED_WORDS = {
    'scout.md': (
        'its recent commits, issues and releases (`gh api repos/<owner>/<repo>/commits`',
        "a GitHub repo that the scout watches, which is no campaign's",
    ),
}
"""A phrase of a core prompt that may say one of ``GIT_WORDS``, and why."""


def git_words(text: str) -> list[str]:
    """The lines of ``text`` that say one of ``GIT_WORDS`` outside a ``{vcs:<part>}``."""
    return [line for line in PART.sub('', text).splitlines() if GIT_WORDS.search(line)]


def test_no_core_prompt_says_git_or_commit_outside_the_providers_parts() -> None:
    texts = {p.name: p.read_text() for p in BUILT_IN} | {'TOOLS': '\n'.join(TOOLS)}
    for name, (phrase, _) in ALLOWED_WORDS.items():
        assert phrase in texts[name], f'{name} no longer says it: delete it from ALLOWED_WORDS'
        texts[name] = texts[name].replace(phrase, '')
    assert {name: found for name, text in texts.items() if (found := git_words(text))} == {}


def test_each_part_is_in_a_built_in_prompt_and_the_git_provider_writes_each() -> None:
    texts = '\n'.join([*(p.read_text() for p in BUILT_IN), *TOOLS])
    assert set(PART.findall(texts)) == set(Part), 'a part that no prompt has is dead'
    assert {p.stem for p in GIT_PROMPTS.glob('*.md')} == set(Part)


def test_a_prompt_that_names_a_part_that_there_is_not_is_refused() -> None:
    with pytest.raises(ValueError, match="'nope' is not a valid Part"):
        prompted('{vcs:nope}', VCS.selected({}))
