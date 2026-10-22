"""Generate ``docs/snippets/generated/example-inline-executor.mdx``: the example plugin ``examples/plugins/inline-executor``."""

from scripts.generate import SNIPPETS
from scripts.generate.plugin_examples import snippet

PATH = SNIPPETS / 'example-inline-executor.mdx'


def generate() -> str:
    """Return the entry points and the code of the example."""
    return snippet('inline-executor', 'piloop_inline/__init__.py')
