"""Generate ``docs/snippets/generated/cas-providers.mdx``: the built-in CAS providers."""

from piloop.plugins.cas.contract import CAS
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell, lead

PATH = SNIPPETS / 'cas-providers.mdx'


def generate() -> str:
    """Return a table of the built-in CAS providers: the name, what it stores into, and its keys."""
    rows = ['| Provider | What it is | Keys |', '|---|---|---|']
    for name, provider in CAS.builtins().items():
        what = cell(lead(provider, provider.__name__)).split(': ', 1)[-1]
        what = what[:1].upper() + what[1:]
        keys = ', '.join(f'`{k}`' for k in provider.model_fields) or 'none'
        rows.append(f'| `{name}` | {what} | {keys} |')
    source = (
        'the docstrings of the providers of the `piloop.cas` entry points in `pyproject.toml`, `piloop/plugins/cas/`'
    )
    return header(source) + '\n' + '\n'.join(rows) + '\n'
