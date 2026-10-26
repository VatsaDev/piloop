"""Generate ``docs/snippets/generated/harnesses.mdx``: the built-in harnesses."""

from piloop.plugins.harness.contract import HARNESSES
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell, lead

PATH = SNIPPETS / 'harnesses.mdx'


def generate() -> str:
    """Return a table of the built-in harnesses: what each one runs, its program, and whether it is on by default."""
    rows = ['| Harness | What it runs | Executable | By default |', '|---|---|---|---|']
    for name, harness in HARNESSES.builtins().items():
        default = 'yes' if harness.DEFAULT else 'no'
        executable = harness.model_fields['executable'].default
        rows.append(f'| `{name}` | {cell(lead(harness, harness.__name__))} | `{executable}` | {default} |')
    source = 'the docstrings of the harnesses of the `piloop.harnesses` entry points in `pyproject.toml`, `piloop/plugins/harness/`'
    return header(source) + '\n' + '\n'.join(rows) + '\n'
