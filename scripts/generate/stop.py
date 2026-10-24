"""Generate ``docs/snippets/generated/stop.mdx``: the limits of the built-in stop rule ``limits``."""

from piloop.plugins.stop_rule.limits import Limits
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import meanings

PATH = SNIPPETS / 'stop.mdx'


def generate() -> str:
    """Return the table of the keys of ``[plugins.stop_rule]`` for ``limits``, from the fields of ``Limits``."""
    return header('the fields of `Limits` in `piloop/plugins/stop_rule/limits.py`') + '\n' + meanings(Limits)
