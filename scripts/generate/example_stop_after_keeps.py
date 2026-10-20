"""Generate ``docs/snippets/generated/example-stop-after-keeps.mdx``: the example plugin ``examples/plugins/stop-after-keeps``."""

from scripts.generate import SNIPPETS
from scripts.generate.plugin_examples import snippet

PATH = SNIPPETS / 'example-stop-after-keeps.mdx'


def generate() -> str:
    """Return the entry points and the code of the example."""
    return snippet('stop-after-keeps', 'piloop_stop_after_keeps/__init__.py')
