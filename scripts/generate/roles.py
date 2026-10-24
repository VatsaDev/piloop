"""Generate ``docs/snippets/generated/roles.mdx``: the built-in roles."""

from piloop.config import Roles
from piloop.plugins.role.contract import ImplementerRole, SweepRole
from piloop.plugins.trigger.contract import TRIGGERS
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import cell, lead

PATH = SNIPPETS / 'roles.mdx'


def generate() -> str:
    """Return a table of the built-in roles: whether each does runs, its triggers, what it files, what it is."""
    rows = ['| Role | Does runs | Triggers | Files | What it is |', '|---|---|---|---|---|']
    for name, role in Roles().all().items():
        fired = role.triggers if isinstance(role, SweepRole) else []
        triggers = ', '.join(f'`{TRIGGERS.name_of(t)}`' for t in fired) or 'the run queue'
        what = cell(lead(type(role), type(role).__name__)).split(': ', 1)[-1]
        what = what[:1].upper() + what[1:]
        rows.append(
            f'| `{name}` | {"yes" if isinstance(role, ImplementerRole) else "no"} | {triggers} | `{role.files}` | {what} |'
        )
    return header('the defaults and docstrings of `Roles` in `piloop/config.py`') + '\n' + '\n'.join(rows) + '\n'
