"""Generate ``docs/snippets/generated/runtime-protocol.mdx``: the methods of the ``Runtime`` protocol."""

import inspect

from piloop.runtime import Runtime
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell, told

PATH = SNIPPETS / 'runtime-protocol.mdx'


def generate() -> str:
    """Return a table of the methods of the ``Runtime`` protocol, from their docstrings."""
    rows = ['| Method | What it does |', '|---|---|']
    for name, member in vars(Runtime).items():
        fn = member.__func__ if isinstance(member, classmethod) else member
        if name.startswith('_') or not inspect.isfunction(fn):
            continue
        params = ', '.join(list(inspect.signature(fn).parameters)[1:])
        kind = ' (class method)' if isinstance(member, classmethod) else ''
        rows.append(f'| `{name}({params})`{kind} | {cell(told(fn, f"Runtime.{name}"))} |')
    return header('the docstrings of `Runtime` in `piloop/runtime/`') + '\n' + '\n'.join(rows) + '\n'
