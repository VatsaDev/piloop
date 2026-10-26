"""Generate ``docs/snippets/generated/example-keep-cost.mdx``: the example plugin ``examples/plugins/keep-cost``."""

from scripts.generate import SNIPPETS
from scripts.generate.plugin_examples import snippet

PATH = SNIPPETS / 'example-keep-cost.mdx'


def generate() -> str:
    """Return the entry points and the code of the example."""
    return snippet('keep-cost', 'piloop_keep_cost/__init__.py')
