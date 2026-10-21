"""Generate ``docs/snippets/generated/storage-backends.mdx``: the built-in storage backends."""

from piloop.plugins.contracts.store import STORAGE_BACKENDS
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell, lead

PATH = SNIPPETS / 'storage-backends.mdx'


def generate() -> str:
    """Return a table of the built-in storage backends: the ``kind``, what it stores into, and its keys."""
    rows = ['| Backend | What it is | Keys |', '|---|---|---|']
    for kind, backend in STORAGE_BACKENDS.builtins().items():
        what = cell(lead(backend, backend.__name__)).split(': ', 1)[-1]
        what = what[:1].upper() + what[1:]
        keys = ', '.join(f'`{k}`' for k in backend.model_fields if k != 'kind')
        rows.append(f'| `{kind}` | {what} | {keys} |')
    source = 'the docstrings of the backends of the `piloop.stores` entry points in `pyproject.toml`, `piloop/store/`'
    return header(source) + '\n' + '\n'.join(rows) + '\n'
