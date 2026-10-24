"""Generate ``docs/snippets/generated/capabilities.mdx``: the capabilities, by their names in ``piloop.capabilities``."""

from importlib.metadata import EntryPoint, entry_points

from piloop import capabilities
from piloop.plugins.host import BUILT_IN, CAPABILITIES, Capability, Choice, Scope
from scripts.generate import SNIPPETS, header

PATH = SNIPPETS / 'capabilities.mdx'


def _own(group: str) -> list[EntryPoint]:
    """Return piloop's own entry points of ``group``, by name."""
    found = [ep for ep in entry_points(group=group) if ep.dist is not None and ep.dist.name == BUILT_IN]
    return sorted(found, key=lambda ep: ep.name)


def _exported(ep: EntryPoint) -> str:
    """Return the name in ``piloop.capabilities`` of what ``ep`` names: the name of its attribute."""
    assert getattr(capabilities, ep.attr, None) is ep.load(), f'piloop.capabilities does not export {ep.value}'
    return f'`{ep.attr}`'


def generate() -> str:
    """Return a table of the capabilities that piloop declares: where each is selected, and its built-in providers."""
    rows = ['| Capability | Tier | Selected by | Built-in providers |', '|---|---|---|---|']
    for declaration in _own(CAPABILITIES):
        capability = declaration.load()
        assert isinstance(capability, Capability)
        match capability.choice:
            case Choice(scope=scope):
                key = f'`[plugins.{capability.name}] provider`'
            case None:
                scope, key = Scope.CAMPAIGN, 'its name, where the configuration uses it'
        key += ' in `config.toml`' if scope is Scope.HOME else ''
        providers = '<br/>'.join(f'`{ep.name}`: {_exported(ep)}' for ep in _own(capability.group))
        concept = declaration.name.replace('_', ' ')
        rows.append(
            f'| {concept}: {_exported(declaration)} (`{capability.group}`) | {capability.tier} | {key} | {providers} |'
        )
    source = 'the `piloop.capabilities` entry points and the provider entry points in `pyproject.toml`'
    return header(source) + '\n' + '\n'.join(rows) + '\n'
