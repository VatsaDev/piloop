"""A stdio MCP server with one tool, ``x_search``: it searches recent posts on X with xAI's Responses API.

Run it with the xAI key in the environment::

    XAI_API_KEY=… python3 scripts/xai_search_mcp.py <model> [--max-usd 1] [--max-tool-calls 10]

In ``piloop.toml``, mount it as an ``[mcp.xai]`` table with ``env = ["XAI_API_KEY"]``. The server wraps the
server-side ``x_search`` tool of ``POST /v1/responses`` (https://docs.x.ai/developers/tools/x-search). It uses only
the standard library.

Each call costs money: xAI charges for the model's tokens and for each post that the call fetches. The server caps
the spend of one session. Each reply states the cost of the call and the total of the session. When the total
reaches ``--max-usd``, the server refuses every further call. The cost of a call is known only when the call returns,
so a session can end up to one call over the cap. A call that times out counts as the most expensive call so far
(``UNKNOWN_CALL_USD`` before any call returned), because xAI can still charge for it. ``--max-tool-calls`` limits
the searches that one call can run, and so its cost.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

ENDPOINT = 'https://api.x.ai/v1/responses'
TIMEOUT_SECONDS = 420
"""A call that runs several searches can take more than 3 minutes."""
UNKNOWN_CALL_USD = 0.75
"""What a call without a reply counts against the cap, before any call returned a cost. xAI can still charge for it,
so an unknown cost is never $0. A call with about 30 searches costs about this much."""
TICKS_PER_USD = 10_000_000_000
MAX_HANDLES = 20
"""xAI's limit on ``allowed_x_handles``."""
TOOL = {
    'name': 'x_search',
    'description': (
        'Search recent posts on X (Twitter) with Grok and get a cited summary: practitioner discussion, new results'
        ' announced by their authors, tools people actually use. Returns the summary, the cited post URLs and what the'
        " call cost; refused once this session's budget is spent."
    ),
    'inputSchema': {
        'type': 'object',
        'properties': {
            'query': {'type': 'string', 'description': 'What to find out, in plain words.'},
            'from_date': {'type': 'string', 'description': 'Earliest post date, YYYY-MM-DD.'},
            'to_date': {'type': 'string', 'description': 'Latest post date, YYYY-MM-DD.'},
            'handles': {
                'type': 'array',
                'items': {'type': 'string'},
                'maxItems': MAX_HANDLES,
                'description': 'Only posts by these X handles (without @), at most 20.',
            },
        },
        'required': ['query'],
    },
}


@dataclass
class Session:
    """One server process: the model, the key, the caps, and what the session has spent so far."""

    model: str
    key: str
    max_usd: float
    max_tool_calls: int
    spent: float = 0.0
    dearest: float = 0.0
    """The highest cost of one call so far. A call that times out counts as this much."""

    def x_search(
        self, query: str, from_date: str | None = None, to_date: str | None = None, handles: list[str] | None = None
    ) -> str:
        """Make one Responses API call with the ``x_search`` tool. Return the answer, its cited URLs and its cost."""
        tool: dict[str, object] = {'type': 'x_search'}
        if from_date:
            tool['from_date'] = from_date
        if to_date:
            tool['to_date'] = to_date
        if handles:
            tool['allowed_x_handles'] = [h.removeprefix('@') for h in handles][:MAX_HANDLES]
        body = {
            'model': self.model,
            'input': [{'role': 'user', 'content': query}],
            'tools': [tool],
            'max_tool_calls': self.max_tool_calls,
        }
        request = urllib.request.Request(
            ENDPOINT,
            json.dumps(body).encode(),
            {'Content-Type': 'application/json', 'Authorization': f'Bearer {self.key}'},
        )
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            data = json.load(response)
        cost = data.get('usage', {}).get('cost_in_usd_ticks', 0) / TICKS_PER_USD
        self.spent += cost
        self.dearest = max(self.dearest, cost)
        texts = [
            c for o in data['output'] if o['type'] == 'message' for c in o['content'] if c['type'] == 'output_text'
        ]
        urls = dict.fromkeys(a['url'] for c in texts for a in c.get('annotations', []) if a.get('url'))
        sources = '\n'.join(f'- {u}' for u in urls) or '- (none)'
        answer = '\n\n'.join(c['text'] for c in texts)
        return f'{answer}\n\nSources:\n{sources}\n\n{self.cost_line(cost)}'

    def cost_line(self, cost: float) -> str:
        return f'Cost: ${cost:.2f} this call, ${self.spent:.2f} this session of its ${self.max_usd:.2f} cap.'

    def call(self, arguments: dict[str, Any]) -> tuple[str, bool]:
        """Return the tool's text, and whether it is an error: a call refused over the cap, or a failed API call."""
        if self.spent >= self.max_usd:
            return f'x_search refused: this session spent ${self.spent:.2f} of its ${self.max_usd:.2f} cap.', True
        try:
            return self.x_search(**arguments), False
        except TimeoutError as e:
            assumed = self.dearest or UNKNOWN_CALL_USD
            self.spent += assumed
            unknown = (
                f'its cost is unknown, counted as ${assumed:.2f}: now ${self.spent:.2f} of the ${self.max_usd:.2f} cap'
            )
            return f'x_search failed: TimeoutError: {e}; {unknown}. Ask a narrower question.', True
        except (urllib.error.URLError, KeyError, TypeError) as e:
            detail = e.read().decode(errors='replace') if isinstance(e, urllib.error.HTTPError) else ''
            return f'x_search failed: {type(e).__name__}: {e} {detail}'.strip(), True


def handle(session: Session, message: dict[str, Any]) -> dict[str, Any] | None:
    """Return the JSON-RPC response to one message, or None for a notification."""
    method, params = message.get('method'), message.get('params') or {}
    if 'id' not in message:
        return None
    reply: dict[str, Any] = {'jsonrpc': '2.0', 'id': message['id']}
    if method == 'initialize':
        reply['result'] = {
            'protocolVersion': params.get('protocolVersion', '2025-06-18'),
            'capabilities': {'tools': {}},
            'serverInfo': {'name': 'xai-search', 'version': '2'},
        }
    elif method == 'ping':
        reply['result'] = {}
    elif method == 'tools/list':
        reply['result'] = {'tools': [TOOL]}
    elif method == 'tools/call' and params.get('name') == TOOL['name']:
        text, failed = session.call(params.get('arguments', {}))
        reply['result'] = {'content': [{'type': 'text', 'text': text}], 'isError': failed}
    else:
        reply['error'] = {'code': -32601, 'message': f'unsupported method {method!r}'}
    return reply


def main() -> None:
    """Serve MCP on stdin/stdout until stdin closes."""
    parser = argparse.ArgumentParser(description='An MCP server for xAI x_search. It reads the key from XAI_API_KEY.')
    parser.add_argument('model', help='xAI model id, e.g. grok-4.7')
    parser.add_argument(
        '--max-usd', type=float, default=1.0, help='the most that one session can spend, in USD (default 1)'
    )
    parser.add_argument(
        '--max-tool-calls', type=int, default=10, help='the most searches that one call can run (default 10)'
    )
    args = parser.parse_args()
    key = os.environ.get('XAI_API_KEY')
    if not key:
        raise SystemExit('XAI_API_KEY is not set')
    if key != key.strip():  # http.client rejects the header with a traceback that prints the key
        raise SystemExit('XAI_API_KEY starts or ends with whitespace (a trailing newline?): supply the bare key')
    session = Session(args.model, key, args.max_usd, args.max_tool_calls)
    for line in sys.stdin:
        if line.strip() and (reply := handle(session, json.loads(line))) is not None:
            sys.stdout.write(json.dumps(reply) + '\n')
            sys.stdout.flush()


if __name__ == '__main__':
    main()
