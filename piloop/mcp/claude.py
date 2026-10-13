"""Claude Code: ``--strict-mcp-config --mcp-config '<inline JSON>'``.

Strict mode gives the agent the role's servers and none of the user's. API keys are ``${NAME}`` references, which
Claude expands from its own environment when it loads the config. ``--mcp-config`` takes several values, so another
flag must follow ``{mcp}`` in the argv, not the prompt.
"""

import json

from piloop.mcp import Launch, McpServer, Rendered, renderer


def server(s: McpServer) -> dict[str, object]:
    """One entry of ``mcpServers``."""
    if s.url is not None:
        headers = s.expanded_headers()
        return {'type': 'http', 'url': s.url, **({'headers': headers} if headers else {})}
    assert s.command is not None, 'a stdio server'
    env = s.expanded_env()
    return {'type': 'stdio', 'command': s.command[0], 'args': s.command[1:], **({'env': env} if env else {})}


@renderer('claude')
def render(launch: Launch) -> Rendered:
    """Strict mode also without servers, so that the agent then has no servers at all."""
    if not launch.servers:
        return Rendered(['--strict-mcp-config'])
    config = {'mcpServers': {name: server(s) for name, s in launch.servers.items()}}
    return Rendered(['--strict-mcp-config', '--mcp-config', json.dumps(config)])
