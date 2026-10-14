"""Generate ``docs/snippets/generated/mcp-keys.mdx``: the keys of an MCP server."""

from piloop.mcp import McpServer
from scripts.generate import SNIPPETS, header
from scripts.generate.markdown import meanings

PATH = SNIPPETS / 'mcp-keys.mdx'


def generate() -> str:
    """Return the table of the keys of an MCP server, from the fields of ``McpServer``."""
    return header('the fields of `McpServer` in `piloop/mcp/`') + '\n' + meanings(McpServer)
