"""Generate ``docs/snippets/generated/runtimes.mdx``: the built-in runtimes."""

from piloop.plugins.runtime.contract import RUNTIMES
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell, lead, own, told

PATH = SNIPPETS / 'runtimes.mdx'
COMMANDS = {'prompt': '`piloop prompt`'}
"""The ``Runtime`` methods that a user calls, and how. The table of the built-in runtimes has a column for each."""


def generate() -> str:
    """Return a table of the built-in runtimes: what each one is, and what each command does in it."""
    head = ['Runtime', 'What it is', *COMMANDS.values()]
    rows = ['| ' + ' | '.join(head) + ' |', '|' + '---|' * len(head)]
    for kind, runtime in sorted(RUNTIMES.builtins().items()):
        cells = [cell(told(own(runtime, m), f'{runtime.__name__}.{m}')) for m in COMMANDS]
        what = cell(lead(runtime, runtime.__name__))
        rows.append(f'| `{kind}` | {what} | {" | ".join(cells)} |')
    source = (
        'the docstrings of the runtimes of the `piloop.runtimes` entry points in `pyproject.toml`, `piloop/runtime/`'
    )
    return header(source) + '\n' + '\n'.join(rows) + '\n'
