"""Generate ``docs/snippets/generated/run-role-keys.mdx``: the keys that a role that does runs adds."""

from piloop.config import ImplementerRole, RoleConfig
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import meanings

PATH = SNIPPETS / 'run-role-keys.mdx'


def generate() -> str:
    """Return the table of the keys that ``ImplementerRole`` adds to ``RoleConfig``."""
    keys = [k for k in ImplementerRole.model_fields if k not in RoleConfig.model_fields]
    return header('the fields of `ImplementerRole` in `piloop/config.py`') + '\n' + meanings(ImplementerRole, keys)
