"""pi: through pi-mcp-adapter, in exclusive mode.

pi's built-in MCP has no config for one launch. So the renderer writes the role's servers to ``mcp.json`` in the
agent's log directory (with variable names only), and loads the adapter on that file in exclusive mode. In this mode
the adapter reads that file and nothing else.

Only an argv that turns pi's own extensions off can take servers. With them on, a user who installed the adapter gets
it twice (and pi does not start), and pi's built-in MCP loads the user's ``mcp.json``. piloop's headless pi turns the
extensions off. The interactive pi (Herdr runtime) keeps the user's extensions, because Herdr reads pi's state
through one of them.

API keys are ``${NAME}`` references in the headers and in a stdio server's env. The adapter expands them from pi's
environment.
"""

import json

from piloop.mcp import Launch, McpServer, Rendered, renderer

ADAPTER = 'npm:pi-mcp-adapter@4.0.0'
"""Pinned, because version 5.0 turns pi's built-in MCP off in the user's own pi settings when it first starts: an
agent would change how the user's pi works. The exclusive mode is in the adapter's code, not in its README.

TODO: move the pin when a release leaves the user's settings alone, or drop the adapter when pi's built-in MCP takes
a config for one launch."""
CONFIG_FILE = 'mcp.json'
NO_EXTENSIONS = ('--no-extensions', '-ne')


def server(s: McpServer) -> dict[str, object]:
    """One entry of the adapter's ``mcpServers``."""
    if s.url is not None:
        headers = s.expanded_headers()
        return {'url': s.url, **({'headers': headers} if headers else {})}
    assert s.command is not None, 'a stdio server'
    env = s.expanded_env()
    return {'command': s.command[0], 'args': s.command[1:], **({'env': env} if env else {})}


def refuses(argv: list[str]) -> str | None:
    """Refuse an argv that keeps pi's extensions on: it cannot take servers."""
    if any(flag in argv for flag in NO_EXTENSIONS):
        return None
    return (
        "pi takes MCP servers only with its own extensions off (--no-extensions): with them on, pi's built-in MCP loads"
        " the user's servers and an installed pi-mcp-adapter loads twice. Run pi agents of this role under the"
        ' subprocess runtime, or leave pi out of its harnesses'
    )


@renderer('pi', refuses=refuses)
def render(launch: Launch) -> Rendered:
    """Nothing without servers: piloop's headless pi then has no MCP at all."""
    if not launch.servers:
        return Rendered([])
    config = launch.log_dir / CONFIG_FILE
    config.write_text(json.dumps({'mcpServers': {name: server(s) for name, s in launch.servers.items()}}, indent=2))
    return Rendered(['-e', ADAPTER, '--mcp-config', str(config)], {'PI_MCP_CONFIG_MODE': 'exclusive'})
