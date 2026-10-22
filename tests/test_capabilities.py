"""``piloop.capabilities``: the one module that a plugin imports, complete and without stale names.

Complete: every capability that piloop declares, its built-in providers, and every type of piloop that a contract
names. A host capability's contract is the return type of its check, a pure one's is its protocol. The types of
``piloop.db`` are the database's own contract, so the search stops there.
"""

import ast
import inspect
import re
import subprocess
import sys
import types
import typing
from collections.abc import Iterable
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from typing import get_type_hints

import piloop_dir_store
import piloop_inline
import piloop_min_gain
import piloop_nice
import piloop_no_unit_regression
import piloop_stop_after_keeps
import piloop_verdict_checks
import piloop_verdict_checks.contract

from piloop import capabilities
from piloop.plugins import metric
from piloop.plugins.host import BUILT_IN, CAPABILITIES, Capability, PureCapability
from piloop.plugins.runtime.contract import NotAttachable, NotSteerable, RuntimeFailure, StartFailed

HELPERS: set[object] = {
    capabilities.MissingProvider,
    RuntimeFailure,
    StartFailed,
    NotSteerable,
    NotAttachable,
    capabilities.runtime_settings,
    capabilities.check_storage_backend,
    capabilities.number,
    capabilities.GO,
    capabilities.key_of,
    capabilities.Settled,
    capabilities.JobKey,  # a type alias, which the search of classes does not find by itself
    capabilities.Control,
    capabilities.run_agent,
    capabilities.run_evaluation,
    capabilities.remove,
    capabilities.kill_group,
    capabilities.Leases,  # the kit: helpers that an executor may use
    capabilities.Timing,
    capabilities.Grant,
    capabilities.Answer,
    capabilities.StaleLease,
    capabilities.NoParameters,
}
"""What a provider raises, calls or returns that no signature of a contract names."""


def _own(group: str) -> list[EntryPoint]:
    return [ep for ep in entry_points(group=group) if ep.dist is not None and ep.dist.name == BUILT_IN]


def _declared() -> list[Capability[object]]:
    loaded = [ep.load() for ep in _own(CAPABILITIES)]
    found = [c for c in loaded if isinstance(c, Capability)]
    assert len(found) == len(loaded)
    return found


def _types(hint: object) -> set[type]:
    """The classes of piloop that a type hint names, outside ``piloop.db``."""
    if isinstance(hint, typing.TypeAliasType):  # ``type Artifact = ...``: what it stands for
        return _types(hint.__value__)
    if isinstance(hint, type):
        ours = hint.__module__.startswith('piloop.') and not hint.__module__.startswith('piloop.db')
        return {hint} if ours else set()
    parts = [*typing.get_args(hint), *filter(None, [typing.get_origin(hint)])]
    return {t for part in parts for t in _types(part)}


def _named(cls: type) -> set[type]:
    """The classes that ``cls`` names in its fields, and in the signatures of its own methods and properties."""
    hints = list(get_type_hints(cls).values())
    for name, member in vars(cls).items():
        if name.startswith('_') and name != '__init__':
            continue
        function = member.fget if isinstance(member, property) else getattr(member, '__func__', member)
        if inspect.isfunction(function):
            hints += get_type_hints(function).values()
    return {t for hint in hints for t in _types(hint)}


def _contracts() -> set[type]:
    """Every type of piloop that the contract of one of its capabilities names, and what those types name."""
    found: set[type] = set()
    for capability in _declared():
        found |= {type(capability)} | ({type(capability.selection)} if capability.selection else set())
        if isinstance(capability, PureCapability):
            found.add(capability.contract)
        else:
            found |= _types(get_type_hints(capability._check)['return'])
    queue = list(found)
    while queue:
        new = _named(queue.pop()) - found
        found |= new
        queue += new
    return found


def _exports() -> dict[str, object]:
    """The public names of the module: its re-exports."""
    return {
        n: v for n, v in vars(capabilities).items() if not n.startswith('_') and not isinstance(v, types.ModuleType)
    }


def _ids(objects: Iterable[object]) -> set[int]:
    return {id(o) for o in objects}


def test_every_capability_of_piloop_and_its_built_in_providers_are_exported_by_the_name_of_the_entry_point() -> None:
    for ep in [*_own(CAPABILITIES), *(p for c in _own(CAPABILITIES) for p in _own(c.load().group))]:
        assert getattr(capabilities, ep.attr, None) is ep.load(), f'{ep.group} {ep.name} = {ep.value}'
    exported = [v for v in _exports().values() if isinstance(v, Capability)]
    assert _ids(exported) == _ids(_declared()), 'an exported capability that piloop no longer declares is stale'


def test_every_type_that_a_contract_names_is_exported_and_nothing_else_is() -> None:
    exported = _exports().values()
    contracts = _contracts()
    missing = sorted(f'{t.__module__}.{t.__qualname__}' for t in contracts if id(t) not in _ids(exported))
    assert missing == []
    providers = [p.load() for c in _own(CAPABILITIES) for p in _own(c.load().group)]
    known = _ids(contracts) | _ids(providers) | _ids(_declared()) | _ids(HELPERS)
    assert [n for n, v in _exports().items() if id(v) not in known] == [], 'stale exports'


def test_a_fresh_interpreter_imports_the_module_first_without_a_cycle() -> None:
    """piloop's own code never imports the module, so nothing that it imports can import it back half done."""
    imported = subprocess.run([sys.executable, '-c', 'import piloop.capabilities'], capture_output=True, text=True)
    assert (imported.returncode, imported.stderr) == (0, '')
    root = Path(capabilities.__file__).parent
    importers = [p for p in root.rglob('*.py') if 'piloop.capabilities' in _imports(p) and p.stem != 'capabilities']
    assert importers == []


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text())
    return {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | {
        n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module is not None
    }


def test_each_capability_is_a_package_of_its_contract_its_built_in_providers_and_its_kit() -> None:
    """``plugins/<capability>/``: ``contract.py`` declares the capability, every other module is a built-in provider
    by the name of its entry point, and ``kit/`` holds what any provider of the capability may use."""
    plugins = Path(capabilities.__file__).parent / 'plugins'
    declared = _own(CAPABILITIES)
    assert {p.name for p in plugins.iterdir() if p.is_dir() and p.name != '__pycache__'} == {c.name for c in declared}
    for capability in declared:
        assert capability.module == f'piloop.plugins.{capability.name}.contract'
        providers = _own(capability.load().group)
        assert [p.module for p in providers] == [f'piloop.plugins.{capability.name}.{p.name}' for p in providers]
        others = {'__init__.py', '__pycache__', 'contract.py', 'kit'}
        modules = {p.name.removesuffix('.py') for p in (plugins / capability.name).iterdir() if p.name not in others}
        assert modules == {p.name for p in providers}, capability.name


def test_core_imports_no_helper_of_a_kit() -> None:
    """``plugins/<capability>/kit`` holds helpers that providers may use: only providers, and this module, import it."""
    root = Path(capabilities.__file__).parent
    plugins = root / 'plugins'
    providers = [p for p in plugins.glob('*/*') if p.name not in {'__init__.py', 'contract.py'}]
    core = [p for p in root.rglob('*.py') if not any(p.is_relative_to(provider) for provider in providers)]
    importers = [
        p
        for p in core
        if any(re.match(r'piloop\.plugins\.\w+\.kit\b', m) for m in _imports(p)) and p.stem != 'capabilities'
    ]
    assert importers == []


def test_the_example_plugins_import_piloop_only_from_the_module() -> None:
    plugins = [
        piloop_dir_store,
        piloop_inline,
        piloop_min_gain,
        piloop_nice,
        piloop_no_unit_regression,
        piloop_stop_after_keeps,
        piloop_verdict_checks,
        piloop_verdict_checks.contract,
    ]
    for plugin in plugins:
        own = plugin.__name__.split('.')[0]
        outside = {m for m in _imports(Path(plugin.__file__ or '')) if m.split('.')[0] not in sys.stdlib_module_names}
        assert {m for m in outside if m.split('.')[0] != own} == {'piloop.capabilities'}, plugin.__name__


def test_a_pure_example_plugin_uses_only_pure_contracts_and_pure_providers() -> None:
    """A pure provider still imports only data and other pure providers through the one module: a pure contract, the
    metric types, or a pure built-in provider."""
    pure = [c for c in _declared() if isinstance(c, PureCapability)]
    modules = {c.contract.__module__ for c in pure} | {p.__module__ for c in pure for p in c.builtins().values()}
    modules |= {PureCapability.__module__, metric.__name__}
    plugins = [
        piloop_min_gain,
        piloop_no_unit_regression,
        piloop_stop_after_keeps,
        piloop_verdict_checks,
        piloop_verdict_checks.contract,
    ]
    for plugin in plugins:
        tree = ast.parse(Path(plugin.__file__ or '').read_text())
        imports = [n for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module == 'piloop.capabilities']
        used = {getattr(capabilities, a.name).__module__ for n in imports for a in n.names}
        assert used <= modules, plugin.__name__
