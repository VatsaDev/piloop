"""A role with ``continuity = "resume"``: each agent continues the last agent's harness session, from the CAS."""

from dataclasses import replace
from pathlib import Path

import pytest
from covalence import Cid
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import select

from piloop.actor import Agent
from piloop.agents.launcher import resumable
from piloop.agents.results import AgentSessionStarted, SessionNotResumed, Unresumed
from piloop.agents.roles import compose_brief, compose_resumed, role
from piloop.db.events import in_order
from piloop.db.models import AgentSession, AgentSessionState, Campaign, Event
from piloop.db.session import Database
from piloop.ids import AgentSessionId, TargetKind, TargetRef
from piloop.otel import Tracer
from piloop.plugins.cas.disk import DiskCas
from piloop.plugins.executor.contract import RepoId, Resume
from piloop.plugins.executor.kit import jobs
from piloop.plugins.harness.claude import Claude
from piloop.plugins.harness.pi import Pi
from piloop.plugins.host import Instances
from piloop.plugins.role.contract import Continuity
from piloop.plugins.runtime.contract import AgentStart
from piloop.plugins.vcs.contract import Repository
from piloop.usage import Usage
from tests.agents.test_launcher import agent_session, checkout, controller, request, use_harness
from tests.conftest import WATCHDOG, ScriptRuntime, events, home_cas, reconfigured, role_timeout
from tests.test_lease import agent_job
from tests.usage.test_usage import SESSIONS

SESSION = SESSIONS / 'claude.jsonl'
"""Two responses: the first agent writes the head with the first, and the resumed one appends the second."""
HOME = '~/claude-home'
HARNESS = {
    'provider': 'claude',
    'home': HOME,
    'headless': ['--session-id', '{session_id}', '{prompt}'],
    'resume': ['--resume', '{session_id}', '{prompt}'],
}
"""The ``claude`` provider over the test script: it reads and restores Claude session files under ``HOME``."""


def harness(tmp_path: Path) -> str:
    """Starts the session with the head of ``SESSION``, or resumes it: its file must be there, and it gets the rest.

    Each start keeps the brief that it read, by its mode, so the test reads what the agent got.
    """
    lines = SESSION.read_bytes().splitlines(keepends=True)
    (tmp_path / 'head.jsonl').write_bytes(b''.join(lines[:5]))
    (tmp_path / 'rest.jsonl').write_bytes(b''.join(lines[5:]))
    return (
        '#!/bin/sh\n'
        'D="$HOME/claude-home/projects/$(pwd | sed \'s/[^a-zA-Z0-9]/-/g\')"\n'
        'mkdir -p "$D"\n'
        f'cp ../brief.md "{tmp_path}/brief$1.md"\n'
        'case "$1" in\n'
        f'  --session-id) cp {tmp_path / "head.jsonl"} "$D/$2.jsonl" ;;\n'
        f'  --resume) [ -f "$D/$2.jsonl" ] || exit 3; cat {tmp_path / "rest.jsonl"} >> "$D/$2.jsonl" ;;\n'
        'esac\n'
    )


def resuming(db: Database, campaign: Campaign) -> None:
    """Make the watchdog a role that resumes its sessions."""
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        row.config = reconfigured(row.config, plugins={'role': {WATCHDOG: {'continuity': Continuity.RESUME.value}}})
        s.commit()


def wake(db: Database, runtime: ScriptRuntime, tmp_path: Path, campaign: Campaign) -> AgentSessionId:
    """Launch one watchdog with both briefs, as the scheduler launches a sweep of a resuming role."""
    agent_session_id = request(db, campaign)
    with db.session() as s:
        row = s.get_one(Campaign, campaign.id)
        agent = Agent(role=WATCHDOG, session=agent_session_id)
        watchdog = role(row.config, WATCHDOG, Path(row.path))
        brief = compose_brief(watchdog, row, agent, reason='news', context='- the whole brief')
        resumed = compose_resumed(watchdog, row, agent, reason='news', context=f'- news for {agent}')
    ctl = controller(db, runtime, tmp_path, Tracer(None))
    ctl.launcher.launch(
        agent_session_id,
        brief=ctl.cas.put(brief.encode()),
        place=f'sweeps/{agent_session_id}',
        artifact=checkout(campaign),
        timeout=role_timeout(db, agent_session_id),
        resumed_brief=ctl.cas.put(resumed.encode()),
    )
    ctl.stop(5)
    return agent_session_id


def not_resumed(db: Database, agent_session_id: AgentSessionId) -> list[SessionNotResumed]:
    with db.session() as s:
        on = Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session_id)
        return [e for _, e in in_order(s, SessionNotResumed, on)]


def started(db: Database, agent_session_id: AgentSessionId) -> AgentSessionStarted:
    with db.session() as s:
        on = Event.target == TargetRef(TargetKind.AGENT_SESSION, agent_session_id)
        [(_, event)] = in_order(s, AgentSessionStarted, on)
        return event


def unread(_: RepoId) -> Repository:
    raise AssertionError('restoring a session opens no repo of record')


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / 'home'
    monkeypatch.setenv('HOME', str(home))
    return home


def test_each_agent_resumes_the_last_session_with_only_the_new_brief_and_counts_only_its_own_tokens(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, home: Path
) -> None:
    use_harness(db, campaign, harness(tmp_path), **HARNESS)
    resuming(db, campaign)
    first = wake(db, runtime, tmp_path, campaign)
    assert not_resumed(db, first) == [
        SessionNotResumed(reason=Unresumed.NO_SESSION, detail='no earlier watchdog left a session')
    ]
    assert '- the whole brief' in (tmp_path / 'brief--session-id.md').read_text()
    one = agent_session(db, first)
    assert one.state is AgentSessionState.EXITED and one.session_id is not None and one.session_file is not None
    assert one.usage is not None and one.usage.turns == 1

    (folder,) = (home / 'claude-home' / 'projects').iterdir()
    for left in folder.iterdir():
        left.unlink()  # the next worker is another machine: only the CAS has the session
    second = wake(db, runtime, tmp_path, campaign)
    two = agent_session(db, second)
    assert (two.state, two.error, not_resumed(db, second)) == (AgentSessionState.EXITED, None, [])
    assert started(db, second).resumes == first
    assert two.session_id == one.session_id, 'the same harness session'
    brief = (tmp_path / 'brief--resume.md').read_text()
    assert brief.startswith(f'You continue this session as a new agent, watchdog#{second}.')
    assert f'- news for watchdog#{second}' in brief and '- the whole brief' not in brief
    assert two.usage == Usage(input=2, output=1649, cache_read=52633, cache_write=3969, total=58253, turns=1), (
        'the tokens after the resume: the whole file less what it held'
    )
    assert two.session_file is not None and home_cas(db).get(two.session_file) is not None
    third = wake(db, runtime, tmp_path, campaign)
    assert (started(db, third).resumes, not_resumed(db, third)) == (second, []), 'a resume that ended well goes on'


def home_cas_disk(db: Database) -> DiskCas:
    cas = home_cas(db)
    assert isinstance(cas, DiskCas)
    return cas


def test_a_session_that_the_worker_cannot_write_back_starts_anew_with_the_whole_brief_and_says_why(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, home: Path
) -> None:
    use_harness(db, campaign, harness(tmp_path), **HARNESS)
    resuming(db, campaign)
    first = wake(db, runtime, tmp_path, campaign)
    session_file = agent_session(db, first).session_file
    assert session_file is not None
    home_cas_disk(db).release(session_file)
    second = wake(db, runtime, tmp_path, campaign)
    [why] = not_resumed(db, second)
    assert why.reason is Unresumed.UNRESTORED and str(session_file) in why.detail
    two = agent_session(db, second)
    assert two.state is AgentSessionState.EXITED and two.session_id != agent_session(db, first).session_id
    assert '- the whole brief' in (tmp_path / 'brief--session-id.md').read_text()


def test_no_session_is_resumed_after_a_resume_that_failed_or_on_a_harness_that_cannot(
    campaign: Campaign, db: Database, runtime: ScriptRuntime, tmp_path: Path, home: Path
) -> None:
    use_harness(db, campaign, harness(tmp_path), **HARNESS)
    resuming(db, campaign)
    first = wake(db, runtime, tmp_path, campaign)
    with db.session() as s:
        cfg = s.get_one(Campaign, campaign.id).config
        later = s.get_one(AgentSession, request(db, campaign))
        found = resumable(s, later, cfg, Path('w'))
        assert not isinstance(found, SessionNotResumed) and found.agent_session == first
        other = s.get_one(AgentSession, request(db, campaign, harness='other'))
        assert resumable(s, other, cfg, Path('w')) == SessionNotResumed(
            reason=Unresumed.NO_SESSION, detail='no earlier watchdog left a session on other'
        )
        harnesses = cfg.plugins['harness']
        assert isinstance(harnesses, Instances)
        bare = cfg.harness['script'].model_copy(update={'resume': []})
        with_bare = replace(harnesses, instances={'script': bare})
        assert resumable(
            s, later, cfg.model_copy(update={'plugins': {**cfg.plugins, 'harness': with_bare}}), Path('w')
        ) == (
            SessionNotResumed(
                reason=Unresumed.HARNESS, detail='script has no resume argv, or its provider restores no session file'
            )
        )
        without = replace(harnesses, instances={})
        assert resumable(
            s, later, cfg.model_copy(update={'plugins': {**cfg.plugins, 'harness': without}}), Path('w')
        ) == SessionNotResumed(reason=Unresumed.HARNESS, detail='piloop.toml no longer names script')

    use_harness(db, campaign, harness(tmp_path).replace('[ -f "$D/$2.jsonl" ] || exit 3', 'exit 3'), **HARNESS)
    resuming(db, campaign)
    broken = wake(db, runtime, tmp_path, campaign)
    assert started(db, broken).resumes == first and agent_session(db, broken).error is not None
    with db.session() as s:
        s.get_one(AgentSession, broken).state = AgentSessionState.KILLED
        s.commit()
    after = wake(db, runtime, tmp_path, campaign)
    assert not_resumed(db, after) == [
        SessionNotResumed(reason=Unresumed.FAILED, detail=f'agent_session#{broken} resumed a session and ended killed')
    ]
    with db.session() as s:
        assert s.scalar(select(AgentSession.state).where(AgentSession.id == after)) is AgentSessionState.EXITED


def test_a_worker_that_cannot_restore_says_why_and_a_session_without_a_response_yet_counts_every_token(
    db: Database, campaign: Campaign, tmp_path: Path, home: Path, tracer: Tracer, spans: InMemorySpanExporter
) -> None:
    cas = home_cas(db)
    site = jobs.local_site(tmp_path / 'worker', cas, lambda _: Path(campaign.path), unread, 0)
    session = cas.put(b'{"type":"session"}\n')
    resume = Resume(session_id='s1', file=session, brief=cas.put(b'# new'), args=['--resume', 's1'])
    claude = Claude(home=HOME)
    job = agent_job(db, campaign, tmp_path).model_copy(update={'harness': claude, 'resume': resume})
    start = AgentStart(executable='x', args=['--session-id', 'fresh'])
    with tracer.span('test') as span:
        (site.log_dir(job)).mkdir(parents=True)
        restored, resumed_start, outcome = jobs.restore_session(job, start, site, span)
        assert (restored.session_id, resumed_start.args, outcome) == ('s1', ['--resume', 's1'], jobs.Restored(None))
        cases = {
            'the harness has no path': job.model_copy(update={'harness': Pi()}),
            'the CAS lacks the resumed brief': job.model_copy(
                update={'resume': resume.model_copy(update={'brief': Cid.of(b'never put')})}
            ),
        }
        for why, broken in cases.items():
            kept, same, failed = jobs.restore_session(broken, start, site, span)
            assert (kept, same) == (broken, start)
            assert isinstance(failed, jobs.Unrestored) and why in failed.reason
    assert len(events(spans, jobs.SESSION_UNRESTORED)) == 2
