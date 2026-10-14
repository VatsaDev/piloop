"""No module outside the leaves names an entry of a registry: a built-in harness, runtime, role, trigger or reader.

A module that needs a property of an entry asks the registry for it, so that an entry that a package or a project adds
gets the same treatment. Only the module that defines an entry, and the files listed here with their reason, may name it.
"""

import ast
from pathlib import Path

from piloop.config import Roles
from piloop.harnesses import DEFAULT_HARNESSES
from piloop.mcp import RENDERERS
from piloop.runtime.factory import BUILTIN_RUNTIMES
from piloop.triggers import BUILTIN_TRIGGERS
from piloop.usage import READERS
from piloop.usage.jsonl import TALLIES

PACKAGE = Path(__file__).parent.parent / 'piloop'
NAMES = {
    *DEFAULT_HARNESSES,
    *BUILTIN_RUNTIMES,
    *Roles.model_fields,
    *BUILTIN_TRIGGERS,
    *READERS,
    *TALLIES,
    *RENDERERS,
}
LEAVES: dict[str, frozenset[str] | None] = {
    'harnesses.py': None,
    'triggers.py': None,
    'db/models.py': None,
    'db/migrations/versions/*.py': None,
    'mcp/claude.py': None,
    'mcp/codex.py': None,
    'mcp/pi.py': None,
    'usage/__init__.py': frozenset({'jsonl'}),
    'usage/jsonl.py': frozenset({'jsonl'}),
    'usage/tallies/*.py': None,
    'runtime/subprocess.py': frozenset({'subprocess'}),
    'runtime/herdr.py': frozenset({'herdr', 'claude', 'codex'}),
    'config.py': frozenset({'subprocess', 'pi', 'scout'}),
    'agents/roles.py': frozenset({'scout'}),
    'cli/skills.py': frozenset({'claude', 'codex', 'pi'}),
    'cli/idea.py': frozenset({'seed'}),
    'cli/main.py': frozenset({'sql'}),
    'campaign/inbox.py': frozenset({'anomaly'}),
    'reports/overview.py': frozenset({'anomaly'}),
    'daemon/runner.py': frozenset({'worker'}),
    'daemon/archiver.py': frozenset({'research'}),
}
"""The files that may name entries, and which ones (None: any). The reasons:

- ``harnesses.py``, ``triggers.py``, ``db/models.py`` (``WorkerKind``), the MCP renderers and the usage readers and
  tallies define the entries. The migrations are history.
- ``usage/jsonl.py`` fills the ``{claude_config_dir}`` and ``{codex_home}`` placeholders, which this test cannot see:
  the folders of two harnesses are core settings. TODO: move them to the harness definitions.
- ``runtime/subprocess.py`` and ``runtime/herdr.py`` define their runtime. TODO: the screens and the queue key of each
  harness in ``runtime/herdr.py`` move to fields of ``Harness``.
- ``config.py``: the defaults of ``runtime`` and ``harnesses``, and ``scout``, a reserved name.
- ``agents/roles.py``: the scout mode of research, which signs as ``scout``.
- ``cli/skills.py``: the skill folder of each agent CLI, which is not the harness registry.
- ``cli/idea.py`` and ``cli/main.py``: the commands ``piloop idea seed`` and ``piloop sql``.
- ``campaign/inbox.py``, ``reports/overview.py`` and ``daemon/runner.py``: keys of event payloads (``anomaly`` of
  ``anomaly.reported``, ``worker`` of ``run.relaunched``), not entries.
- ``daemon/archiver.py``: ``research/`` is a folder of the shared workspace, not the role.
"""


def named(path: Path) -> set[str]:
    """Return the entries that the string constants of the module at ``path`` name."""
    tree = ast.parse(path.read_text())
    return {
        n.value for n in ast.walk(tree) if isinstance(n, ast.Constant) and isinstance(n.value, str) and n.value in NAMES
    }


def allowed(path: Path) -> frozenset[str] | None:
    """Return the entries that ``LEAVES`` lets the module at ``path`` name: None for any, empty for none."""
    rel = path.relative_to(PACKAGE)
    found = [names for pattern, names in LEAVES.items() if rel.match(pattern)]
    if not found:
        return frozenset()
    return None if None in found else frozenset().union(*(n for n in found if n is not None))


def test_only_the_leaves_name_an_entry_of_a_registry() -> None:
    wrong = {}
    for path in sorted(PACKAGE.rglob('*.py')):
        ok = allowed(path)
        if ok is not None and (extra := named(path) - ok):
            wrong[str(path.relative_to(PACKAGE))] = sorted(extra)
    assert wrong == {}, 'ask the registry instead, or add the file to LEAVES with its reason'


def test_every_leaf_entry_is_still_needed() -> None:
    """An allowance that no module uses any more goes, so the list shows what is left to clean up."""
    for pattern, names in LEAVES.items():
        paths = list(PACKAGE.glob(pattern))
        assert paths, f'{pattern} matches no file'
        if names is not None:
            used = set().union(*(named(p) for p in paths))
            assert names <= used, f'{pattern} no longer names {sorted(names - used)}'
