"""scripts/xai_search_mcp.py: what it asks xAI for, the cost that it reports for a call, and the session cap."""

import io
import json
import urllib.error
import urllib.request
from typing import Any

import pytest

from scripts.xai_search_mcp import UNKNOWN_CALL_USD, Session, handle

REPLY = {
    'output': [
        {
            'type': 'message',
            'content': [
                {
                    'type': 'output_text',
                    'text': 'CTC beam search: -18% CER',
                    'annotations': [{'url': 'https://x.com/a/status/1'}],
                }
            ],
        }
    ],
    'usage': {'cost_in_usd_ticks': 6_000_000_000},
}


@pytest.fixture
def sent(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    bodies: list[dict[str, Any]] = []

    def urlopen(request: urllib.request.Request, timeout: float) -> io.BytesIO:
        assert isinstance(request.data, bytes)
        bodies.append(json.loads(request.data))
        return io.BytesIO(json.dumps(REPLY).encode())

    monkeypatch.setattr(urllib.request, 'urlopen', urlopen)
    return bodies


def call(session: Session, **arguments: object) -> dict[str, Any]:
    message = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'tools/call',
        'params': {'name': 'x_search', 'arguments': arguments},
    }
    reply = handle(session, message)
    assert reply is not None
    return reply['result']


def test_a_call_reports_its_cost_and_the_session_refuses_past_its_cap(sent: list[dict[str, Any]]) -> None:
    session = Session('grok-4.7', 'k', max_usd=1.0, max_tool_calls=8)
    first = call(session, query='ctc', from_date='2026-09-01', handles=['@someone', 'other'])
    assert not first['isError']
    assert first['content'][0]['text'] == (
        'CTC beam search: -18% CER\n\nSources:\n- https://x.com/a/status/1\n\n'
        'Cost: $0.60 this call, $0.60 this session of its $1.00 cap.'
    )
    assert sent[0]['max_tool_calls'] == 8
    assert sent[0]['tools'] == [
        {'type': 'x_search', 'from_date': '2026-09-01', 'allowed_x_handles': ['someone', 'other']}
    ]
    assert not call(session, query='again')['isError'], 'under the cap before the call: it runs, and may end over'
    refused = call(session, query='third')
    assert refused['isError'] and refused['content'][0]['text'] == (
        'x_search refused: this session spent $1.20 of its $1.00 cap.'
    )
    assert len(sent) == 2, 'a refused call never reaches xAI'


def test_a_call_that_times_out_counts_at_the_dearest_call_so_far(
    sent: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    session = Session('grok-4.7', 'k', max_usd=1.0, max_tool_calls=8)
    timed_out = (
        'x_search failed: TimeoutError: slow; its cost is unknown, counted as ${:.2f}: now ${:.2f} of the $1.00 cap.'
    )

    def slow(request: urllib.request.Request, timeout: float) -> io.BytesIO:
        raise TimeoutError('slow')

    with monkeypatch.context() as m:
        m.setattr(urllib.request, 'urlopen', slow)
        result = call(session, query='q', to_date='2026-10-01')
    assert result['isError']
    assert (
        result['content'][0]['text']
        == timed_out.format(UNKNOWN_CALL_USD, UNKNOWN_CALL_USD) + ' Ask a narrower question.'
    )
    call(session, query='measured')
    monkeypatch.setattr(urllib.request, 'urlopen', slow)
    assert session.spent == pytest.approx(1.35) and session.dearest == pytest.approx(0.6)
    assert call(session, query='q')['content'][0]['text'].startswith('x_search refused'), 'past the cap'


def test_a_failed_call_is_an_error_result(monkeypatch: pytest.MonkeyPatch) -> None:
    def urlopen(request: urllib.request.Request, timeout: float) -> io.BytesIO:
        raise urllib.error.URLError('no route')

    monkeypatch.setattr(urllib.request, 'urlopen', urlopen)
    result = call(Session('grok-4.7', 'k', max_usd=1.0, max_tool_calls=8), query='q')
    assert result == {
        'content': [{'type': 'text', 'text': 'x_search failed: URLError: <urlopen error no route>'}],
        'isError': True,
    }


def test_the_server_lists_its_tool_and_ignores_notifications() -> None:
    session = Session('grok-4.7', 'k', max_usd=1.0, max_tool_calls=8)
    assert handle(session, {'jsonrpc': '2.0', 'method': 'notifications/initialized'}) is None
    listed = handle(session, {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'})
    assert listed is not None
    assert [t['name'] for t in listed['result']['tools']] == ['x_search']
    assert 'handles' in listed['result']['tools'][0]['inputSchema']['properties']
