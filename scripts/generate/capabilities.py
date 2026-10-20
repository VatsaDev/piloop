"""Generate ``docs/snippets/generated/capabilities.mdx``: piloop's capabilities and the import path of each provider."""

from importlib.metadata import EntryPoint, entry_points

from piloop.plugins.host import BUILT_IN, CAPABILITIES, Capability
from scripts.generate import SNIPPETS, header

PATH = SNIPPETS / 'capabilities.mdx'


def _own(group: str) -> list[EntryPoint]:
    """Return piloop's own entry points of ``group``, by name."""
    found = [ep for ep in entry_points(group=group) if ep.dist is not None and ep.dist.name == BUILT_IN]
    return sorted(found, key=lambda ep: ep.name)


def generate() -> str:
    """Return a table of the capabilities that piloop declares: where each is selected, and its built-in providers."""
    rows = ['| Capability | Tier | Selected by | Built-in providers, and where to import them |', '|---|---|---|---|']
    for declaration in _own(CAPABILITIES):
        capability = declaration.load()
        assert isinstance(capability, Capability)
        selection = capability.selection
        key = 'its name, where the configuration uses it'
        if selection is not None:
            leaf = selection.key.rpartition('.')[2]
            key = f'`[{selection.table}] {leaf}`' if selection.table else f'`{selection.key}`'
            key += ' in `config.toml`' if selection.scope == 'home' else ''
        providers = '<br/>'.join(f'`{ep.name}`: `{ep.value}`' for ep in _own(capability.group))
        concept = declaration.name.replace('_', ' ')
        rows.append(f'| {concept} (`{capability.group}`) | {capability.tier} | {key} | {providers} |')
    source = 'the `piloop.capabilities` entry points and the provider entry points in `pyproject.toml`'
    return header(source) + '\n' + '\n'.join(rows) + '\n'
