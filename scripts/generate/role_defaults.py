"""Generate ``docs/snippets/generated/role-defaults.mdx``: the built-in roles with their default values."""

import json

from piloop.config import Roles
from scripts.generate import SNIPPETS, header

PATH = SNIPPETS / 'role-defaults.mdx'
UNSET = (None, [], {})
"""The values of a role's key that mean that it is not set. The table of the role defaults leaves them out."""


def generate() -> str:
    """Return the tables of the built-in roles with their default values, the keys that are unset left out."""
    tables = [
        '\n'.join(
            [f'[roles.{name}]']
            + [f'{k} = {_toml(v)}' for k, v in role.model_dump(mode='json').items() if k != 'runs' and v not in UNSET]
        )
        for name, role in Roles().all().items()
    ]
    return header('the defaults of `Roles` in `piloop/config.py`') + '\n```toml\n' + '\n\n'.join(tables) + '\n```\n'


def _toml(value: object) -> str:
    """Return ``value`` as a TOML value: the scalars, lists and inline tables that a role's defaults hold."""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, list):
        return f'[{", ".join(_toml(v) for v in value)}]'
    if isinstance(value, dict):
        return '{ ' + ', '.join(f'{k} = {_toml(v)}' for k, v in value.items()) + ' }'
    return json.dumps(value)
