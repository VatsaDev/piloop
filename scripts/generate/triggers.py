"""Generate ``docs/snippets/generated/triggers.mdx``: the built-in triggers."""

from piloop.triggers import BUILTIN_TRIGGERS
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell, lead

PATH = SNIPPETS / 'triggers.mdx'


def generate() -> str:
    """Return a table of the built-in triggers: when each one fires, and its parameters."""
    rows = ['| Trigger | Fires when | Parameters |', '|---|---|---|']
    for kind, trigger in BUILTIN_TRIGGERS.items():
        params = ', '.join(f'`{name}`' for name in trigger.model_fields if name != 'kind') or 'none'
        rows.append(f'| `{kind}` | {cell(lead(trigger, trigger.__name__))} | {params} |')
    source = 'the docstrings of the triggers in `BUILTIN_TRIGGERS`, `piloop/triggers/`'
    return header(source) + '\n' + '\n'.join(rows) + '\n'
