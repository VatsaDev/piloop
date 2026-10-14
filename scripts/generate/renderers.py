"""Generate ``docs/snippets/generated/renderers.mdx``: the built-in MCP renderers."""

import sys

from piloop.mcp import RENDERERS
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import registry_table

PATH = SNIPPETS / 'renderers.mdx'


def generate() -> str:
    """Return the table of the built-in MCP renderers, from the docstrings of their modules."""
    modules: dict[str, object] = {n: sys.modules[r.render.__module__] for n, r in RENDERERS.items()}
    return (
        header('the docstrings of the modules of `RENDERERS`, `piloop/mcp/`')
        + '\n'
        + registry_table('Renderer', modules)
    )
