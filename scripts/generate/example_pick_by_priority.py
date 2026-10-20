"""Generate ``docs/snippets/generated/example-pick-by-priority.mdx``: the example plugin ``examples/plugins/pick-by-priority``."""

from scripts.generate import SNIPPETS
from scripts.generate.plugin_examples import snippet

PATH = SNIPPETS / 'example-pick-by-priority.mdx'


def generate() -> str:
    """Return the entry points and the code of the example."""
    return snippet('pick-by-priority', 'piloop_pick_by_priority/contract.py', 'piloop_pick_by_priority/__init__.py')
