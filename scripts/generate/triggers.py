"""Generate ``docs/snippets/generated/triggers.mdx``: the built-in triggers."""

from piloop.plugins.trigger.contract import TRIGGERS
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell, lead

PATH = SNIPPETS / 'triggers.mdx'


def generate() -> str:
    """Return a table of the built-in triggers: when each one fires, and its parameters."""
    rows = ['| Trigger | Fires when | Parameters |', '|---|---|---|']
    for name, trigger in TRIGGERS.builtins().items():
        params = ', '.join(f'`{field}`' for field in trigger.model_fields) or 'none'
        rows.append(f'| `{name}` | {cell(lead(trigger, trigger.__name__))} | {params} |')
    source = 'the docstrings of the triggers of the `piloop.triggers` entry points in `pyproject.toml`, `piloop/plugins/trigger/`'
    return header(source) + '\n' + '\n'.join(rows) + '\n'
