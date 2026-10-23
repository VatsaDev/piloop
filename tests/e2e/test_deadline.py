"""An implementer that works past its time is told to wrap up in its turn, and finishes before its timeout.

    uv run pytest -m e2e tests/e2e/test_deadline.py -rA [--e2e-harness claude] [--e2e-model <model>]

The role timeout is 5 minutes, so the wrap-up comes 75 seconds before it. The idea asks for more one-minute steps
than fit, all in one turn. Claude Code refuses a long ``sleep``, so a step sleeps in Python. A Claude agent reads the
wrap-up after its next tool call, commits and leaves its finding, and the daemon ends it a minute later. About 6
minutes and 0.1 USD.
"""

from collections.abc import Iterator

import pytest

from piloop.plugins.runtime.contract import TAG_ENV
from piloop.plugins.runtime.kit.tags import tagged
from tests.conftest import wait_until
from tests.e2e.campaign import OPEN, Smoke, needs
from tests.e2e.conftest import MakeCampaign

pytestmark = pytest.mark.e2e

TIMEOUT = 300
CONFIG = {
    'roles': {
        'implementer': {'max_concurrent': 1, 'timeout': TIMEOUT},
        'research': {'triggers': []},
        'watchdog': {'triggers': []},
    },
    'stop': {'max_runs': 3, 'max_hours': 1, 'max_tokens': 5_000_000},
}
PLAN = (
    'This idea tests the time limit, not the compressor. Do ten steps in this one turn. Each step is one Bash tool'
    ' call of its own: `python3 -c "import time; time.sleep(60)" && echo "step n" >> workspace/steps.txt`. Never put'
    ' two steps in one command, and never run a step in the background. Change and commit nothing until a [piloop]'
    ' message tells you to wrap up. Then set the zlib level in compressor.py to 9, commit, and leave your finding.'
)


@pytest.fixture(scope='module')
def campaign(make_campaign: MakeCampaign) -> Iterator[Smoke]:
    yield make_campaign(config=CONFIG)


def test_the_brief_states_the_deadline(campaign: Smoke) -> None:
    campaign.add_idea('long', 'Work past the time limit', PLAN, 0)
    wait_until(lambda: campaign.run_in('long', {'running'}) is not None, timeout=120)
    run = campaign.run_of('long')
    assert run is not None
    assert '- Your session ends 5 minutes after it starts, at `$PILOOP_DEADLINE`' in run['brief']


@needs(test_the_brief_states_the_deadline)
def test_the_wrap_up_reaches_the_turn_and_the_run_finishes_before_its_timeout(campaign: Smoke) -> None:
    wait_until(lambda: campaign.run_in('long', OPEN, within=False) is not None, timeout=TIMEOUT + 120)
    run = campaign.run_of('long')
    assert run is not None
    [implementer] = campaign.sql('SELECT * FROM agent_sessions WHERE run_id = ?', run['id'])
    delivered = campaign.sql(
        "SELECT kind FROM events WHERE target_kind = 'agent_session' AND target_id = ? AND kind LIKE 'comment.%'",
        implementer['id'],
    )
    assert [e['kind'] for e in delivered] == ['comment.queued', 'comment.delivered'], 'the wrap-up note, read'
    assert (run['status'], run['end']) == ('succeeded', 'finished'), run['error']
    ended = campaign.sql(
        "SELECT json_extract(payload, '$.reason') AS reason FROM events WHERE kind = 'agent_session.stop_ended'"
        ' AND target_id = ?',
        implementer['id'],
    )
    assert [e['reason'] for e in ended] in ([], ['left its finding']), 'it ended by itself, or after its finding'
    queue = campaign.home / 'runs' / str(run['id']) / 'queued-messages'
    assert not queue.exists(), 'taken in the turn, not left for a turn of its own'
    assert 'timeout' not in (implementer['error'] or ''), implementer['error']
    assert [p for t in campaign.process_tags(run['id']) for p in tagged(f'{TAG_ENV}={t}')] == []
    print(
        f'run {run["id"]} {run["end"]} in {implementer["ended_at"]} - {implementer["started_at"]}; ended {[e["reason"] for e in ended]}'
    )
