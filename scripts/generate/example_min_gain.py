"""Generate ``docs/snippets/generated/example-min-gain.mdx``: the example plugin ``examples/plugins/min-gain``."""

from scripts.generate import SNIPPETS
from scripts.generate.plugin_examples import snippet

PATH = SNIPPETS / 'example-min-gain.mdx'


def generate() -> str:
    """Return the entry points and the code of the example."""
    return snippet('min-gain', 'piloop_min_gain/__init__.py')
