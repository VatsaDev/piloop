"""Generate ``docs/snippets/generated/example-nice-runtime.mdx``: the example plugin ``examples/plugins/nice-runtime``."""

from scripts.generate import SNIPPETS
from scripts.generate.plugin_examples import snippet

PATH = SNIPPETS / 'example-nice-runtime.mdx'


def generate() -> str:
    """Return the entry points and the code of the example."""
    return snippet('nice-runtime', 'piloop_nice/__init__.py')
