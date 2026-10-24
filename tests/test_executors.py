"""The executor contract: every executor runs a job the same way for core. Each case runs on ``workers`` (built in, on
leases) and on ``inline`` (the example ``examples/plugins/inline-executor``, a thread per job).

A run brings its commits back and is scored, an evaluation records its measurement, a cancel stops a running agent
and it still reports, and a stop closes a running agent as stopped.
"""

import subprocess
import threading
from collections.abc import Iterator
from dataclasses import is_dataclass
from pathlib import Path, PurePath
from typing import get_args, get_type_hints

import pytest
from git import Repo
from pydantic import BaseModel, ValidationError
from sqlalchemy import func, select

from piloop.actor import System
from piloop.agents.results import DaemonStopped, Ended, PromptOutcome, Ran
from piloop.campaign.campaigns import Sources, checkout_of
from piloop.daemon.main import Daemon
from piloop.daemon.run_controller import RunController
from piloop.db.events import in_order
from piloop.db.models import (
    LIVE,
    AgentSession,
    AgentSessionState,
    Campaign,
    Candidate,
    Event,
    Lease,
    Measurement,
    RunEnd,
)
from piloop.db.session import Database
from piloop.ids import WATCHDOG, AgentSessionId, CampaignId, CommentId
from piloop.otel import Tracer
from piloop.plugins import host as plugins
from piloop.plugins.executor.contract import (
    EXECUTORS,
    JOB,
    AgentJob,
    AgentKey,
    Capacity,
    EvaluationJob,
    Prompt,
    RepoId,
    Source,
)
from piloop.plugins.runtime.contract import Handle
from piloop.plugins.vcs.git import Git
from piloop.scoring.ledger import request_remeasure
from piloop.settings import Settings
from tests.conftest import (
    HARNESS_SLEEPS,
    ScriptRuntime,
    baseline,
    disk,
    example,
    factory,
    package,
    serving,
    wait_until,
    write_toml,
)
from tests.daemon.test_run_controller import execute, make_run, reload_campaign, run_row


@pytest.fixture(params=['workers', 'inline'])
def provider(request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """The name of the executor under the contract, installed and selectable."""
    if request.param == 'inline':
        example(tmp_path / 'site', 'inline-executor')
        monkeypatch.syspath_prepend(tmp_path / 'site')
    plugins.close()  # the registry loads again, with the example's entry point
    yield request.param
    plugins.close()


def controller(db: Database, runtime: ScriptRuntime, tmp_path: Path, provider: str) -> RunController:
    executor = EXECUTORS.get(provider).model()
    return serving(RunController(db, Tracer(None), tmp_path, factory(runtime), executor=executor))


def launch_in_thread(
    ex: RunController, db: Database, campaign: Campaign, tmp_path: Path
) -> tuple[AgentSessionId, list[Ended]]:
    with db.session() as s:
        w = AgentSession(kind=WATCHDOG, campaign_id=campaign.id, reason='contract')
        s.add(w)
        s.commit()
        agent_session_id = w.id
    source = ex.sources.source(campaign, Repo(campaign.path).head.commit.hexsha)
    ended: list[Ended] = []

    def launch() -> None:
        ended.append(ex.launcher.launch(agent_session_id, brief='b', place='w', artifact=source))

    threading.Thread(target=launch, daemon=True).start()
    return agent_session_id, ended


def test_a_run_brings_its_commits_back_and_is_scored(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    run_id = make_run(db, campaign)
    ex = controller(db, runtime, tmp_path, provider)
    execute(ex, db, run_id)
    assert run_row(db, run_id).status.value == 'succeeded'
    with db.session() as s:
        [candidate] = s.scalars(select(Candidate).where(Candidate.run_id == run_id)).all()
        assert candidate.summary == 'add a line' and candidate.measurements, 'its commit came back and was measured'
    pinned = subprocess.run(
        ['git', 'for-each-ref', 'refs/piloop/agents/'], cwd=campaign.path, capture_output=True, text=True, check=True
    )
    assert pinned.stdout == '', "the agent's ref goes once the run pinned its commits"


def test_an_evaluation_records_its_measurement(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    ex = controller(db, runtime, tmp_path, provider)
    candidate = baseline(db, campaign.id).id
    ex.remeasure(candidate)
    with db.session() as s:
        count = s.scalar(select(func.count()).select_from(Measurement).where(Measurement.candidate_id == candidate))
    assert count == 2, 'the baseline and the new measurement'


def test_a_paired_evaluation_records_each_sample_with_its_pair(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    ex = controller(db, runtime, tmp_path, provider)
    execute(ex, db, make_run(db, campaign))
    with db.session() as s:
        candidate = s.scalars(select(Candidate).where(Candidate.run_id.is_not(None))).one()
        request_remeasure(s, candidate, System.ORCHESTRATOR, repeats=2)
        s.commit()
        candidate_id = candidate.id
    ex.remeasure(candidate_id)
    with db.session() as s:
        pairs = [(m.metrics, m.pair.metrics) for m in s.get_one(Candidate, candidate_id).measurements if m.pair]
    assert pairs == [({'score': 2.0, 'size': 10.0}, {'score': 1.0, 'size': 5.0})] * 2


def test_a_cancelled_agent_is_stopped_and_still_reports(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    write_toml(Path(campaign.path), HARNESS_SLEEPS)
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path, provider)
    agent_session_id, ended = launch_in_thread(ex, db, campaign, tmp_path)
    wait_for_agent_session_handle_of(db, agent_session_id)
    ex.executor.cancel(AgentKey(agent_session_id))
    wait_until(lambda: ended)
    assert isinstance(ended[0], Ran), 'its result closed the row'
    with db.session() as s:
        assert s.get_one(AgentSession, agent_session_id).state not in LIVE


def test_a_stop_closes_a_running_agent_as_stopped(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    write_toml(Path(campaign.path), HARNESS_SLEEPS)
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path, provider)
    agent_session_id, ended = launch_in_thread(ex, db, campaign, tmp_path)
    wait_for_agent_session_handle_of(db, agent_session_id)
    ex.stop(30)
    wait_until(lambda: ended)
    assert ended[0] == DaemonStopped()
    with db.session() as s:
        w = s.get_one(AgentSession, agent_session_id)
        assert (w.state, w.error) == (AgentSessionState.KILLED, RunEnd.DAEMON_STOPPED)


class Steerable(ScriptRuntime):
    """A subprocess runtime that takes prompts, and records them."""

    def __init__(self) -> None:
        super().__init__()
        self.prompts: list[str] = []

    def prompt(self, handle: Handle, text: str) -> None:
        self.prompts.append(text)


def test_a_prompt_reaches_the_running_agent_and_its_outcome_is_recorded(
    db: Database, campaign: Campaign, tmp_path: Path, provider: str
) -> None:
    runtime = Steerable()
    write_toml(Path(campaign.path), HARNESS_SLEEPS)
    reload_campaign(db, campaign)
    ex = controller(db, runtime, tmp_path, provider)
    agent_session_id, ended = launch_in_thread(ex, db, campaign, tmp_path)
    wait_for_agent_session_handle_of(db, agent_session_id)
    ex.executor.prompt(AgentKey(agent_session_id), Prompt(CommentId(41), 'try lzma'))
    wait_until(lambda: outcomes(db, agent_session_id) == [PromptOutcome(kind='comment.queued', comment=CommentId(41))])
    assert runtime.prompts == ['try lzma']
    ex.executor.cancel(AgentKey(agent_session_id))
    wait_until(lambda: ended)
    ex.executor.prompt(AgentKey(agent_session_id), Prompt(CommentId(42), 'too late'))
    wait_until(lambda: len(outcomes(db, agent_session_id)) == 2)
    assert outcomes(db, agent_session_id)[1].kind == 'comment.undelivered' and runtime.prompts == ['try lzma']


def outcomes(db: Database, agent_session_id: AgentSessionId) -> list[PromptOutcome]:
    with db.session() as s:
        return [settled for _, settled in in_order(s, PromptOutcome, Event.target_id == agent_session_id)]


def wait_for_agent_session_handle_of(db: Database, agent_session_id: AgentSessionId) -> None:
    def running() -> bool:
        with db.session() as s:
            w = s.get_one(AgentSession, agent_session_id)
            return w.state is AgentSessionState.RUNNING and bool(w.handle)

    wait_until(running)


def test_config_toml_selects_the_executor_and_the_daemon_refuses_a_parameter_it_does_not_take(
    tmp_path: Path, provider: str
) -> None:
    (tmp_path / 'config.toml').write_text(f'[plugins.executor]\nprovider = "{provider}"\n')
    daemon = Daemon(Settings(home=tmp_path))
    try:
        assert (
            type(daemon.controller.executor).__name__
            == {'workers': 'WorkersExecutor', 'inline': 'InlineExecutor'}[provider]
        )
    finally:
        daemon.shutdown(timeout=5)
    (tmp_path / 'config.toml').write_text(f'[plugins.executor]\nprovider = "{provider}"\nslots = 3\n')
    with pytest.raises(SystemExit, match='slots'):
        Daemon(Settings(home=tmp_path))


def test_a_cancel_of_a_job_that_the_executor_does_not_have_changes_nothing(
    db: Database, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    controller(db, runtime, tmp_path, provider).executor.cancel(AgentKey(AgentSessionId(999)))


def test_an_entry_point_that_is_not_an_executor_class_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package(tmp_path / 'bad', 'piloop-bad', '1.0', '[piloop.executors]\nbad = tests.fixtures.plugin:Typed\n')
    monkeypatch.syspath_prepend(tmp_path / 'bad')
    plugins.close()
    try:
        with pytest.raises(
            plugins.PluginError, match='is not a subclass of piloop.plugins.executor.contract.ExecutorConfig'
        ):
            EXECUTORS.get('bad')
    finally:
        plugins.close()


def test_the_capacity_is_the_slots_of_the_executor_now(
    db: Database, runtime: ScriptRuntime, tmp_path: Path, provider: str
) -> None:
    """``workers``: the defaults of the pool ``serve``, once its worker registered. ``inline``: no bound."""
    expected = {'workers': Capacity(agents=None, evaluations=2), 'inline': Capacity(agents=None, evaluations=None)}
    assert controller(db, runtime, tmp_path, provider).executor.capacity() == expected[provider]


def test_config_toml_refuses_a_plugins_table_that_no_capability_of_the_home_declares(tmp_path: Path) -> None:
    """``[plugins.<name>]`` of ``config.toml`` is generic: the plugin host checks it, as ``piloop.toml``'s."""
    (tmp_path / 'config.toml').write_text('[plugins.verdict_rule]\nprovider = "bootstrap"\n')
    with pytest.raises(
        SystemExit, match=r'verdict_rule\n  Value error, no capability has that name; there is cas, executor'
    ):
        Daemon(Settings(home=tmp_path))


def path_types(annotation: object, seen: set[type]) -> list[str]:
    """The fields under ``annotation`` that hold a ``Path``, by dotted name: a walk of the type, not of a value."""
    found: list[str] = []
    if annotation is Path or (isinstance(annotation, type) and issubclass(annotation, PurePath)):
        return ['']
    if (
        isinstance(annotation, type)
        and annotation not in seen
        and (issubclass(annotation, BaseModel) or is_dataclass(annotation))
    ):
        seen.add(annotation)
        hints = get_type_hints(annotation)
        for name, hint in hints.items():
            found += [f'{name}{"." + rest if rest else ""}' for rest in path_types(hint, seen)]
        return found
    for arg in get_args(getattr(annotation, '__value__', annotation)):
        found += path_types(arg, seen)
    return found


def test_no_field_of_a_job_holds_a_path() -> None:
    """A job names its tree, its inputs and its place by address and by name: no path of the machine that made it."""
    assert path_types(AgentJob, set()) == [] and path_types(EvaluationJob, set()) == []


def strings(value: object) -> Iterator[str]:
    match value:
        case str():
            yield value
        case dict():
            for item in value.values():
                yield from strings(item)
        case list():
            for item in value:
                yield from strings(item)
        case _:
            pass


def test_a_job_that_the_daemon_queues_names_no_path_under_its_home(
    db: Database, campaign: Campaign, runtime: ScriptRuntime, tmp_path: Path
) -> None:
    """The jobs of a run and of its measurement, as they are queued: no string of theirs names the daemon's home."""
    run_id = make_run(db, campaign)
    home = tmp_path / 'home'
    ex = controller(db, runtime, home, 'workers')
    execute(ex, db, run_id)
    with db.session() as s:
        jobs = [JOB.validate_python(lease.job) for lease in s.scalars(select(Lease).order_by(Lease.id))]
    assert {job.kind for job in jobs} == {'agent', 'evaluation'}
    assert (
        run_row(db, run_id).status.value == 'succeeded' and (home / 'runs' / str(run_id) / 'transcript.txt').is_file()
    )
    for job in jobs:
        dumped = JOB.dump_python(job, mode='json')
        assert [line for text in strings(dumped) for line in text.splitlines() if str(home) in line] == [], job.kind


@pytest.mark.parametrize('place', ['', '/home/me/.piloop/runs/1', 'runs/../../etc'])
def test_a_place_is_a_relative_path_that_stays_inside_a_home(place: str) -> None:
    with pytest.raises(ValidationError, match='is not a relative path inside a home'):
        EvaluationJob(
            evaluation=1,
            candidate=1,
            runner='process',
            artifact=Source(vcs=Git(), repo=RepoId(CampaignId(1)), revision='c'),
            place=place,
            command=['true'],
            timeout=1,
            required=[],
        )


def test_two_campaigns_on_clones_of_one_repo_get_their_own_checkouts(
    db: Database, campaign: Campaign, tmp_path: Path
) -> None:
    """git's identity (a root commit) is one for every clone and fork: the repo of record is the campaign's."""
    clone = tmp_path / 'clone'
    subprocess.run(['git', 'clone', '-q', campaign.path, str(clone)], check=True)
    with db.session() as s:
        other = Campaign(name='other', path=str(clone), repo='other', config=campaign.config)
        s.add(other)
        s.commit()
        sources = Sources(disk(tmp_path), tmp_path, db)
        first = sources.source(s.get_one(Campaign, campaign.id), Repo(campaign.path).head.commit.hexsha)
        second = sources.source(other, Repo(clone).head.commit.hexsha)
    assert first.repo != second.repo
    assert (checkout_of(db, first.repo), checkout_of(db, second.repo)) == (Path(campaign.path), clone)
    with pytest.raises(KeyError, match='is gone'):
        checkout_of(db, RepoId(CampaignId(999)))
