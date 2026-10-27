"""Three campaigns on one host never run more agents at once than its pool allows.

    uv run pytest -m e2e tests/e2e/test_agent_slots.py -rA [--e2e-harness claude] [--e2e-model <model>]

Each campaign takes four implementers (``max_concurrent``), and the pool ``serve`` takes two agents. Two small ideas
each: six runs, at most two at a time. About 8 minutes and 0.4 USD.
"""

import shutil
import tomllib
from collections.abc import Iterator

import pytest
import tomli_w

from tests.conftest import wait_until
from tests.e2e.campaign import OPEN, TEMPLATES, Smoke, merged, needs
from tests.e2e.conftest import MakeCampaign

pytestmark = pytest.mark.e2e

AGENTS = 2
"""The agent slots of the pool ``serve``."""
CONFIG = {
    'plugins': {
        'role': {
            'implementer': {'max_concurrent': 4, 'timeout': 600},
            'research': {'triggers': []},
            'watchdog': {'triggers': []},
        },
        'stop_rule': {'max_runs': 5, 'max_hours': 1, 'max_tokens': 20_000_000},
    },
}
OTHERS = ('enwik8-b', 'enwik8-c')
"""The two campaigns that join the first one's home."""
LEVELS = (1, 9)
"""One idea for each zlib level, in each campaign."""


@pytest.fixture(scope='module')
def host(make_campaign: MakeCampaign) -> Iterator[Smoke]:
    """The first campaign, its daemon with two agent slots, and two more campaigns in its home."""
    first = make_campaign(
        config=CONFIG,
        home_config={'plugins': {'executor': {'provider': 'workers', 'pools': {'serve': {'agents': AGENTS}}}}},
    )
    first.piloop('pause', first.name, '--reason', 'e2e: the ideas of all three campaigns go in first')
    toml = tomllib.loads((first.repo / 'piloop.toml').read_text())
    for name in OTHERS:
        repo = first.dir / name
        shutil.copytree(TEMPLATES / 'enwik8', repo, ignore=shutil.ignore_patterns('data', '.venv', '__pycache__'))
        (repo / 'piloop.toml').write_text(tomli_w.dumps(merged(toml, {'name': name})))
        first.piloop('init', str(repo), '--example', 'enwik8')
        first.piloop('pause', name, '--reason', 'e2e: the ideas of all three campaigns go in first')
    first.start_readers()
    yield first


def campaigns(host: Smoke) -> list[str]:
    return [host.name, *OTHERS]


def test_the_ideas_wait_while_the_campaigns_are_paused(host: Smoke) -> None:
    for name in campaigns(host):
        for level in LEVELS:
            host.piloop(
                'idea',
                'add',
                name,
                f'Compress at zlib level {level}',
                '--plan',
                f'In compressor.py, set the zlib level to {level}. Run `uv run python evaluate.py` once, then commit.'
                ' Change nothing else.',
            )
    assert host.sql('SELECT id FROM runs') == []


@needs(test_the_ideas_wait_while_the_campaigns_are_paused)
def test_six_runs_go_two_at_a_time_across_the_campaigns(host: Smoke) -> None:
    for name in campaigns(host):
        host.piloop('resume', name)
    wait_until(lambda: len(host.sql('SELECT id FROM runs')) == len(LEVELS) * 3, timeout=900)

    def settled() -> bool:
        statuses = [r['status'] for r in host.sql('SELECT status FROM runs')]
        return not OPEN.intersection(statuses)

    wait_until(settled, timeout=900)
    host.join_readers()
    agents = [a for _, _, a in host.queue_samples]
    assert agents, f'no sample; reader errors {host.errors[-3:]}'
    assert max(agents) == AGENTS, f'live agent leases at each sample: {agents}'
    print(f'{len(agents)} samples: at most {max(agents)} live agent leases under {AGENTS} slots')
