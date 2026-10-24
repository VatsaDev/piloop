"""No module outside the leaves names an entry of a registry: a built-in harness, runtime, role, trigger or reader.

And every capability of the plugin host states its tier, and the contract of a pure one is data in, data out.

A module that needs a property of an entry asks the registry for it, so that an entry that a package or a campaign adds
gets the same treatment. Only the module that defines an entry, and the files listed here with their reason, may name it.
"""

import ast
from pathlib import Path

from piloop.config import Roles
from piloop.harnesses import DEFAULT_HARNESSES
from piloop.mcp import RENDERERS
from piloop.plugins.host import Tier, data_errors, declared
from piloop.plugins.runtime.contract import RUNTIMES
from piloop.plugins.trigger.contract import TRIGGERS
from piloop.usage import READERS
from piloop.usage.jsonl import TALLIES

PACKAGE = Path(__file__).parent.parent / 'piloop'
NAMES = {
    *DEFAULT_HARNESSES,
    *RUNTIMES.builtins(),
    *Roles.model_fields,
    *TRIGGERS.builtins(),
    *READERS,
    *TALLIES,
    *RENDERERS,
}
LEAVES: dict[str, frozenset[str] | None] = {
    'harnesses.py': None,
    'plugins/trigger/*.py': None,
    'db/models.py': None,
    'ids.py': None,
    'db/migrations/versions/*.py': None,
    'mcp/claude.py': None,
    'mcp/codex.py': None,
    'mcp/pi.py': None,
    'usage/__init__.py': frozenset({'jsonl'}),
    'usage/jsonl.py': frozenset({'jsonl'}),
    'usage/tallies/*.py': None,
    'config.py': frozenset({'subprocess', 'pi'}),
    'plugins/role/research.py': frozenset({'claude', 'codex'}),
    'deprecated.py': None,
    'cli/skills.py': frozenset({'claude', 'codex', 'pi'}),
    'cli/idea.py': frozenset({'seed'}),
    'cli/main.py': frozenset({'sql'}),
    'plugins/executor/kit/relay.py': frozenset({'seed', 'sql'}),
    'daemon/archiver.py': frozenset({'research'}),
}
"""The files that may name entries, and which ones (None: any). The reasons:

- ``harnesses.py``, the triggers (``plugins/trigger/``), ``ids.py`` (the built-in roles), the MCP renderers and the
  usage readers and tallies define the entries. The migrations are history.
- ``db/models.py``: the states ``seed`` and ``stopped``, which share a name with a trigger.
- ``config.py``: the defaults of ``runtime`` and ``harnesses``.
- ``plugins/role/research.py``: the default ``model`` of research.
- ``deprecated.py``: the bridges rewrite the keys of earlier releases, which named built-in roles and triggers.
- ``cli/skills.py``: the skill folder of each agent CLI, which is not the harness registry.
- ``cli/idea.py``, ``cli/main.py`` and ``plugins/executor/kit/relay.py``: the commands ``piloop idea seed`` and
  ``piloop sql``.
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


HOST_CONTRACTS = {
    'runtime': 'starts, watches, steers and stops agent processes',
    'CAS provider': 'opens a store that reads and writes files, a directory or a bucket',
    'trigger': 'fire(tick) reads the database through the session of a Tick',
    'role': 'a role holds trigger objects and names a runtime',
    'executor': 'runs jobs on machines, and applies their results to the database through Results',
    'network': 'binds a socket, and makes HTTP clients',
    'vcs provider': 'runs the version control tool on checkouts, and moves versions through the CAS',
}
"""The capabilities whose contract passes live objects of the host, and why. Every other one is pure."""


def test_every_capability_has_a_tier_and_every_pure_contract_is_data_in_data_out() -> None:
    for capability in declared():
        if capability.tier is Tier.PURE:
            assert data_errors(capability.contract) == [], capability.noun
    assert {c.noun for c in declared() if c.tier is Tier.HOST} == set(HOST_CONTRACTS)
