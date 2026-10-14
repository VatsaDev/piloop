"""Generate ``docs/snippets/generated/harnesses.mdx``: the built-in harnesses."""

from piloop.harnesses import DEFAULT_HARNESSES, Invocation
from scripts.generate import SNIPPETS, header

PATH = SNIPPETS / 'harnesses.mdx'


def generate() -> str:
    """Return a table of the built-in harnesses: program, usage reader, MCP renderer, where it takes MCP servers."""
    head = ['Harness', 'Executable', 'Usage reader', 'MCP renderer', *(f'MCP servers, {i}' for i in Invocation)]
    rows = ['| ' + ' | '.join(head) + ' |', '|' + '---|' * len(head)]
    for name, h in DEFAULT_HARNESSES.items():
        usage = f'`{h.usage.reader}` (tally `{h.usage.tally}`)' if h.usage else 'none'
        servers = ['yes' if h.refuses_servers(i) is None else 'no' for i in Invocation]
        rows.append(f'| `{name}` | `{h.executable}` | {usage} | `{h.mcp or "none"}` | {" | ".join(servers)} |')
    return header('`DEFAULT_HARNESSES` in `piloop/harnesses.py`') + '\n' + '\n'.join(rows) + '\n'
