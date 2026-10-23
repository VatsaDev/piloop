"""The helpers of the e2e summary, which run without a campaign."""

import json
from pathlib import Path

from piloop.plugins.runtime.subprocess import STDOUT
from tests.e2e.campaign import harness_cost


def test_the_cost_of_an_agent_is_the_last_running_total_of_each_of_its_sessions(tmp_path: Path) -> None:
    """Claude Code prints its session's running total at the end of each process, and a resume is a new process."""
    assert harness_cost(tmp_path) is None, 'no output'
    results = [('a', 0.0807), ('a', 0.0917), ('b', 0.5)]
    lines = [json.dumps({'session_id': s, 'total_cost_usd': c}) for s, c in results]
    (tmp_path / STDOUT).write_text('\n'.join(['not a result', *lines]) + '\n')
    assert harness_cost(tmp_path) == 0.0917 + 0.5
    (tmp_path / STDOUT).write_text('killed before its result\n')
    assert harness_cost(tmp_path) is None
