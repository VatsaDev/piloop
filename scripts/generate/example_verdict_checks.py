"""Generate ``docs/snippets/generated/example-verdict-checks.mdx``: the example plugin ``examples/plugins/verdict-checks``."""

from scripts.generate import SNIPPETS
from scripts.generate.plugin_examples import snippet

PATH = SNIPPETS / 'example-verdict-checks.mdx'


def generate() -> str:
    """Return the entry points and the code of the example."""
    return snippet('verdict-checks', 'piloop_verdict_checks/contract.py', 'piloop_verdict_checks/__init__.py')
