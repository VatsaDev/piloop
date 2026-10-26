"""A small campaign on a piloop server that runs elsewhere, driven only through its API and the CLI, as its users drive it.

    PILOOP_HTTPS_TOKEN=<token> uv run pytest -m e2e tests/e2e/test_remote.py -rA --e2e-server https://piloop.example.com \
        [--e2e-secret ANTHROPIC_API_KEY] [--e2e-campaign <name>] [--e2e-harness claude] [--e2e-model <model>]

The server is any ``piloop serve --listen`` that this machine reaches with a bearer token in ``PILOOP_HTTPS_TOKEN``, of
``act`` access. A person registers a toy repository by its URL, files a seed and writes to the orchestrator. piloop
launches the orchestrator on the server's workers, and it answers the message. The tests also act as an orchestrator from
the CLI (``PILOOP_SERVER``): they add an idea, develop the seed, pause, resume and stop the campaign. Two implementers run
on the server's workers, and their candidates are judged.

The toy is published as a secret gist (``gh``, logged in with the ``gist`` scope), which the server clones: the server
needs to reach github.com. Its ``piloop.toml`` gives the harness each ``--e2e-secret`` from the server's secret of that
name. Its campaign has the same name on each run (``--e2e-campaign``, default ``e2e-remote``): the scenario retires a
campaign of that name that an earlier run left, and retires its own at the end. The gist is deleted at the end. About
3 minutes and 0.2 USD on ``claude-haiku-4-5``, mostly cache reads.
"""

import os
import shutil
import subprocess
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from unittest import mock

import pytest
import tomli_w
from pydantic import JsonValue

from piloop.actor import ORCHESTRATING, PERSON, SERVICE, System
from piloop.api.client import Client, Refused
from piloop.api.contract import (
    MAX_LIMIT,
    MAX_POLL_SECONDS,
    AckBody,
    Clone,
    EventsQuery,
    MessageBody,
    Remote,
    RetireBody,
    SeedBody,
    Selection,
    StopBody,
)
from piloop.api.views import AckOutcome, CampaignState, Envelope
from piloop.ids import CampaignId, Ending, RoleName
from piloop.plugins.network.https import TOKEN_ENV, Https
from tests.e2e.campaign import CHEAPEST_MODEL, CommandFailed, needs
from tests.e2e.conftest import Install
from tests.isolation import CALLER_ENV

pytestmark = pytest.mark.e2e

ROLE_TIMEOUT = 300
CONFIG = {
    'evaluate': ['python3', 'evaluate.py'],
    'evaluate_timeout': 120,
    'runtime': 'subprocess',
    'roles': {
        'implementer': {'timeout': ROLE_TIMEOUT, 'max_concurrent': 2},
        'research': {'triggers': [], 'max_ideas': 0},
        'watchdog': {'triggers': [], 'interval': 0},
        'orchestrator': {
            'triggers': [{'provider': 'news', 'quiet_seconds': 5, 'routine_every': 86_400}],
            'addendum': 'orchestrator.md',
            'timeout': ROLE_TIMEOUT,
        },
    },
    'plugins': {'stop_rule': {'max_runs': 3, 'max_tokens': 4_000_000, 'hold_at': {'tokens': 3_000_000}}},
    'metrics': [{'name': 'bytes', 'direction': 'lower', 'role': 'primary'}],
}
"""The toy's ``piloop.toml``, without its name and harness: no research or watchdog sweeps, an orchestrator that piloop
launches on a message or an escalation (and on routine news once), and limits only as a backstop on what it costs. Two
ideas run, so ``max_runs`` never stops it."""
ORCHESTRATOR = """# This campaign is a test

Answer each message and escalation, and acknowledge it. Change nothing else: add, develop or change no idea, override no
verdict, and launch, pause or stop nothing. The tests do that themselves.
"""
BRIEF = """# Make primes.py smaller

`primes.py` prints the first 50 prime numbers, one per line. Make the file as small as you can, in bytes, while it prints
exactly the same output. Edit only `primes.py`. `python3 evaluate.py` checks the output and prints the size. Keep it plain
Python 3 with no imports.
"""
EVALUATE = """import json, os, pathlib, subprocess, sys

def primes(n):
    out, k = [], 2
    while len(out) < n:
        if all(k % d for d in range(2, int(k ** 0.5) + 1)):
            out.append(k)
        k += 1
    return out

run = subprocess.run([sys.executable, 'primes.py'], capture_output=True, text=True, timeout=60)
if run.returncode != 0 or [int(line) for line in run.stdout.split()] != primes(50):
    sys.exit('primes.py does not print the first 50 primes')
line = json.dumps({'metrics': {'bytes': len(pathlib.Path('primes.py').read_bytes())}})
if path := os.environ.get('PILOOP_RESULT_FILE'):
    with open(path, 'a') as result:
        result.write(line + '\\n')
else:
    print(line)
"""
PRIMES = """# Prints the first 50 prime numbers, one per line.
def is_prime(candidate_number):
    if candidate_number < 2:
        return False
    divisor = 2
    while divisor < candidate_number:
        if candidate_number % divisor == 0:
            return False
        divisor = divisor + 1
    return True


found_primes = []
current_number = 2
while len(found_primes) < 50:
    if is_prime(current_number):
        found_primes.append(current_number)
    current_number = current_number + 1

for prime in found_primes:
    print(prime)
"""
WAIT = 25.0
"""Seconds that the CLI's ``inbox wait`` holds a request."""
BASELINE_WAIT = 900
"""Seconds to wait for the baseline: a provisioned evaluation worker can take minutes to start."""
RUNS_WAIT = 3 * ROLE_TIMEOUT + 600
"""Seconds to wait for both runs to end and be judged: their workers start, they work, then they are measured."""
RETIRE_WAIT = ROLE_TIMEOUT + 300
"""Seconds to wait for a stop to be over before a campaign retires: its closing review and runs end."""


@dataclass
class Server:
    """The campaign on the server, and what the tests learn of it as they go."""

    url: str
    name: str
    repo: str
    """The URL that the server clones."""
    api: Client
    """The person's client of the API."""
    env: dict[str, str] = field(repr=False)
    """The environment of the CLI, as the orchestrator. Not in the repr: it holds the token."""
    id: CampaignId | None = None
    """The campaign's id, once it registered: its name may name another campaign once it is retired."""
    again: CampaignId | None = None
    """The id of the campaign that registers the URL again, under the same name."""
    cursor: int = 0
    """The feed is read after it."""
    events: list[Envelope] = field(default_factory=list)
    baseline: int = 0
    ideas: dict[str, int] = field(default_factory=dict)
    """The id of each idea that the tests made, by key."""

    def piloop(self, *args: str) -> str:
        """Run the CLI against the server as the orchestrator, and return its stdout."""
        proc = subprocess.run(['piloop', *args], env=self.env, capture_output=True, text=True, timeout=300)
        if proc.returncode != 0:
            raise CommandFailed(
                f'`piloop {" ".join(args)}` exited {proc.returncode}: {(proc.stderr or proc.stdout)[-800:]}'
            )
        return proc.stdout

    def state(self) -> CampaignState:
        """The campaign as ``GET /v1/status`` shows it."""
        [found] = self.api.status(Selection(campaign=str(self.id))).campaigns
        assert isinstance(found, CampaignState), found
        return found

    def seen(self, match: Callable[[Envelope], bool], what: str, timeout: float) -> Envelope:
        """The first event of the campaign that ``match`` takes, read from the feed with long-polls until ``timeout``."""
        deadline = time.monotonic() + timeout
        read = 0
        while True:
            if found := next((e for e in self.events[read:] if match(e)), None):
                return found
            read = len(self.events)
            left = deadline - time.monotonic()
            assert left > 0, f'timed out after {timeout:.0f}s waiting for {what}; last events: {self.tail()}'
            query = EventsQuery(
                campaign=str(self.id), after=self.cursor, limit=MAX_LIMIT, wait=min(left, MAX_POLL_SECONDS)
            )
            batch = self.api.events(query)
            self.events += batch.events
            self.cursor = batch.next

    def none(self, match: Callable[[Envelope], bool], what: str, seconds: float) -> None:
        """Read the feed for ``seconds`` with long-polls, and fail on the first new event that ``match`` takes."""
        deadline = time.monotonic() + seconds
        while (left := deadline - time.monotonic()) > 0:
            query = EventsQuery(
                campaign=self.name, after=self.cursor, limit=MAX_LIMIT, wait=min(left, MAX_POLL_SECONDS)
            )
            batch = self.api.events(query)
            self.events += batch.events
            self.cursor = batch.next
            found = next((e for e in batch.events if match(e)), None)
            assert found is None, f'{what}: {found}'

    def of(self, kind: str) -> list[Envelope]:
        """The events of ``kind`` read so far."""
        return [e for e in self.events if e.kind == kind]

    def tail(self) -> list[str]:
        return [f'{e.id} {e.kind} {e.subject.kind}#{e.subject.id} by {e.actor}' for e in self.events[-8:]]


def payload(event: Envelope) -> dict[str, JsonValue]:
    assert isinstance(event.payload, dict), event
    return event.payload


def kind_on(kind: str, subject: str, subject_id: int) -> Callable[[Envelope], bool]:
    return lambda e: e.kind == kind and e.subject.kind == subject and e.subject.id == subject_id


def publish(directory: Path, files: dict[str, str]) -> str:
    """Write ``files`` in ``directory``, publish them as a secret gist, and return its URL. ``gh`` runs in the caller's
    environment, where it is logged in."""
    if shutil.which('gh') is None:
        raise RuntimeError('the toy is published as a gist: install gh and run `gh auth login`')
    for name, text in files.items():
        (directory / name).write_text(text)
    out = subprocess.run(
        ['gh', 'gist', 'create', '--desc', f'piloop e2e toy {directory.name}', *files],
        cwd=directory,
        env=CALLER_ENV,
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip()


@pytest.fixture(scope='module')
def server(request: pytest.FixtureRequest) -> Iterator[Server]:
    """The toy's campaign on the server of ``--e2e-server``, as ``--e2e-campaign``. It installs piloop only then."""
    url = request.config.getoption('e2e_server')
    if url is None:
        pytest.skip('pass --e2e-server <url> to run the scenario against a server')
    if not (token := CALLER_ENV.get(TOKEN_ENV)):
        raise pytest.UsageError(f'set {TOKEN_ENV} to a token of the server with act access')
    piloop_install: Install = request.getfixturevalue('piloop_install')
    harness = request.config.getoption('e2e_harness')
    model = request.config.getoption('e2e_model') or CHEAPEST_MODEL[harness]
    secrets = request.config.getoption('e2e_secret') or []
    name = request.config.getoption('e2e_campaign') or 'e2e-remote'
    directory = piloop_install.root / name
    directory.mkdir()
    toml = CONFIG | {
        'name': name,
        'harnesses': [harness],
        'model': {harness: model},
        'harness': {harness: {'env': {s: {'secret': s} for s in secrets}}},
    }
    files = {
        'piloop.toml': tomli_w.dumps(toml),
        'brief.md': BRIEF,
        'evaluate.py': EVALUATE,
        'primes.py': PRIMES,
        'orchestrator.md': ORCHESTRATOR,
        '.gitignore': 'workspace\n__pycache__\n',
    }
    gist = publish(directory, files)
    with mock.patch.dict(os.environ, {TOKEN_ENV: token}):
        http = Https().client(url)
    env = piloop_install.env(directory / 'home') | {
        'PILOOP_SERVER': url,
        TOKEN_ENV: token,
        'PILOOP_ACTOR': System.ORCHESTRATOR.signature,
    }
    try:
        with Client.connect(http) as api:
            for left in (c for c in api.status(Selection()).campaigns if c.name == name):
                retire(api, str(left.id), 'e2e: an earlier run left it')
            found = Server(url, name, f'{gist}.git', api, env)
            yield found
            for campaign in (found.id, found.again):
                if campaign and api.status(Selection(campaign=str(campaign))).campaigns[0].status != 'retired':
                    retire(api, str(campaign), 'e2e: the scenario ended')
    finally:
        subprocess.run(['gh', 'gist', 'delete', gist.rpartition('/')[2], '--yes'], env=CALLER_ENV, check=True)


def retire(api: Client, ref: str, reason: str) -> CampaignState:
    """Stop the campaign that ``ref`` names unless it is stopped, and retire it once its stop is over.

    Raises:
        Refused: ``ref`` names no campaign, or the stop is not over within ``RETIRE_WAIT``.
    """
    found = api.status(Selection(campaign=ref))
    [state] = found.campaigns
    campaign, cursor, deadline = str(state.id), found.cursor, time.monotonic() + RETIRE_WAIT
    pending = ['is still stopping']
    if state.status == 'active':
        api.stop(campaign, StopBody(reason=reason, ending=Ending.NOW))
        pending.append('is active')  # until the daemon acts on the stop that it was asked for
    while True:
        try:
            retired = api.retire(campaign, RetireBody(reason=reason)).campaign
        except Refused as e:
            if not any(p in str(e) for p in pending) or time.monotonic() > deadline:
                raise
            cursor = api.events(EventsQuery(campaign=campaign, after=cursor, wait=MAX_POLL_SECONDS)).next
            continue
        assert isinstance(retired, CampaignState), retired
        return retired


def test_registers_a_repository_by_its_url(server: Server) -> None:
    """``POST /v1/campaigns`` with ``{clone: {url}, name}``: the server clones the toy and adds its campaign."""
    registered = server.api.register_campaign(Clone(clone=Remote(url=server.repo), name=server.name))
    assert isinstance(registered.campaign, CampaignState), registered.campaign
    assert registered.campaign.name == server.name
    assert registered.campaign.status == 'active'
    server.id, server.cursor, server.baseline = registered.campaign.id, registered.cursor, registered.baseline
    print(f'{server.name} from {server.repo}: baseline candidate#{server.baseline}, cursor {server.cursor}')


@needs(test_registers_a_repository_by_its_url)
def test_measures_the_baseline(server: Server) -> None:
    def baseline(e: Envelope) -> bool:
        return e.kind == 'measurement.recorded' and payload(e)['candidate'] == server.baseline

    measured = payload(server.seen(baseline, 'the measurement of the baseline', BASELINE_WAIT))
    assert measured['error'] is None, measured
    assert measured['metrics'] == {'bytes': len(PRIMES.encode())}, measured
    print(f'baseline: {measured["metrics"]}')


@needs(test_measures_the_baseline)
def test_the_orchestrator_adds_an_idea(server: Server) -> None:
    title = 'Strip the comments and the long names from primes.py'
    plan = (
        'In primes.py, delete the comment, rename every variable to one letter, and remove the blank lines. Keep the'
        ' same algorithm. Run `python3 evaluate.py`, then commit. Change nothing else.'
    )
    server.piloop('idea', 'add', server.name, title, '--plan', plan, '--priority', '1')
    created = server.seen(lambda e: e.kind == 'idea.created' and e.actor.startswith(ORCHESTRATING), 'idea.created', 60)
    server.ideas['added'] = created.subject.id
    print(f'idea#{created.subject.id} by {created.actor}')


@needs(test_measures_the_baseline)
def test_a_person_files_a_seed_that_the_orchestrator_develops(server: Server) -> None:
    """The person's seed wakes the orchestrator's ``inbox wait``, which develops it into an idea for the implementers."""
    after = server.cursor
    hunch = 'A sieve in a list comprehension is shorter than the trial division loop'
    seed = server.api.file_seed(server.name, SeedBody(hunch=hunch, provenance=f'candidate#{server.baseline}'))
    assert seed.idea.status == 'seed', seed.idea
    assert seed.idea.created_by.startswith((PERSON, SERVICE)), seed.idea  # the token's caller: a person or a service
    woke = server.piloop('inbox', 'wait', server.name, '--after', str(after), '--timeout', str(WAIT))
    assert f'idea#{seed.idea.id}' in woke, woke
    server.piloop(
        'idea', 'develop', str(seed.idea.id),
        '--hypothesis', 'One expression replaces the function and both loops.',
        '--evidence', f'candidate#{server.baseline}: primes.py is a long trial division loop.',
        '--check', 'python3 evaluate.py prints a smaller bytes value.',
        '--metric', 'bytes',
        '--effect', 'About 300 bytes less.',
        '--plan', 'Rewrite primes.py as a short comprehension that prints the first 50 primes. Run'
        ' `python3 evaluate.py`, then commit. Change nothing else.',
        '--priority', '1',
    )  # fmt: skip
    developed = server.seen(kind_on('idea.status', 'idea', seed.idea.id), 'the seed developed', 60)
    assert payload(developed)['old'] == 'seed', payload(developed)
    server.ideas['seed'] = seed.idea.id
    print(f'seed idea#{seed.idea.id} by {seed.idea.created_by}: {payload(developed)}')


@needs(test_measures_the_baseline)
def test_a_message_wakes_the_orchestrator_that_piloop_launches_and_it_answers(server: Server) -> None:
    """``POST /v1/campaigns/{campaign}/messages`` reaches an orchestrator on a worker, whose reply reaches the person.

    piloop launches one for the message, or the news reaches one that already works. Each acknowledges what the other
    wrote: the author of a comment cannot.
    """
    assert server.state().roles[RoleName('orchestrator')].triggered
    sent = server.api.message(
        server.name, MessageBody(body='e2e: how many bytes is primes.py now? Reply with the number.')
    )

    def answered(e: Envelope) -> bool:
        return e.kind == 'comment.decision' and e.actor.startswith('orchestrator#')

    agent = int(server.seen(answered, "the orchestrator's reply", 600 + ROLE_TIMEOUT).actor.partition('#')[2])
    running = server.seen(kind_on('agent_session.running', 'agent_session', agent), f'orchestrator#{agent} ran', 60)
    thread = server.api.thread(sent.comment.id)
    [reply, *_] = [c for c in thread.linked if c.author.startswith('orchestrator#')]
    assert reply.in_reply_to == sent.comment.id, thread
    server.seen(
        kind_on('comment.acked_at', 'comment', sent.comment.id), 'the orchestrator acknowledges the message', 600
    )
    assert server.api.thread(sent.comment.id).comment.acked_by == reply.author
    [acked] = server.api.ack(AckBody(ids=(reply.id,))).results
    assert acked.outcome is AckOutcome.ACKED, acked
    print(
        f'comment#{sent.comment.id} by {sent.comment.author}, answered on {payload(running)["machine"]} by {reply.author}:'
        f' {reply.body[:200]}'
    )


@needs(test_the_orchestrator_adds_an_idea, test_a_person_files_a_seed_that_the_orchestrator_develops)
def test_implementers_run_on_the_servers_workers(server: Server) -> None:
    """Each idea gets a run, whose implementer names its harness session and runs on a machine of the server."""
    for key, idea in server.ideas.items():
        queued = server.seen(lambda e, i=idea: e.kind == 'run.queued' and payload(e)['idea'] == i, f'run of {key}', 300)
        run = queued.subject.id
        identified = server.seen(
            lambda e, r=run: e.kind == 'agent_session.identified' and payload(e)['run'] == r, f'run#{run} agent', 600
        )
        agent = identified.subject.id
        running = server.seen(kind_on('agent_session.running', 'agent_session', agent), f'agent#{agent} running', 900)
        print(f'run#{run} of idea#{idea}: agent#{agent} on {payload(running)["machine"]}')


@needs(test_implementers_run_on_the_servers_workers)
def test_candidates_are_measured_and_judged(server: Server) -> None:
    """Both runs end, and at least one commit of theirs is measured and judged."""
    runs = {e.subject.id for e in server.of('run.queued')}
    for run in runs:
        ended = server.seen(kind_on('run.finished', 'run', run), f'run#{run} finished', RUNS_WAIT)
        print(f'run#{run}: {payload(ended)}')

    def from_runs(e: Envelope) -> bool:
        return e.kind == 'measurement.recorded' and payload(e)['run'] in runs

    measured = server.seen(from_runs, 'a measurement of a run', 600)
    candidate = payload(measured)['candidate']
    assert isinstance(candidate, int)
    judged = payload(server.seen(kind_on('candidate.judged', 'candidate', candidate), 'its verdict', 600))
    print(f'candidate#{candidate} of run#{payload(measured)["run"]}: {payload(measured)["metrics"]}, {judged}')


@needs(test_candidates_are_measured_and_judged)
def test_every_agent_saved_its_session_file(server: Server) -> None:
    """Each agent that named its harness session stored the file in the server's CAS (``agent_session.session_saved``)."""
    identified = {e.subject.id: payload(e)['session_id'] for e in server.of('agent_session.identified')}
    for agent in identified:
        server.seen(kind_on('agent_session.finished', 'agent_session', agent), f'agent#{agent} finished', 600)
    saved = {e.subject.id: payload(e) for e in server.of('agent_session.session_saved')}
    assert identified.keys() <= saved.keys(), f'no session file of agents {identified.keys() - saved.keys()}'
    for agent, session in identified.items():
        assert saved[agent]['session_id'] == session, saved[agent]
        print(f'agent#{agent} session {session}: {saved[agent]["session_file"]}')


@needs(test_measures_the_baseline)
def test_pause_and_resume(server: Server) -> None:
    server.piloop('pause', server.name, '--reason', 'e2e: pause and resume')
    paused = server.seen(lambda e: e.kind == 'campaign.paused' and payload(e)['new'] is not None, 'the pause', 60)
    assert server.state().paused == payload(paused)['new']
    server.piloop('resume', server.name)
    server.seen(lambda e: e.kind == 'campaign.paused' and payload(e)['new'] is None, 'the resume', 60)
    assert server.state().paused is None


@needs(test_registers_a_repository_by_its_url)
def test_stop(server: Server) -> None:
    """``piloop campaign stop``: the campaign stops and closes. The stop wakes the orchestrator once, to report; after that
    wake ends, no agent is left and none starts."""
    server.piloop('campaign', 'stop', server.name, '--reason', 'e2e: the scenario ends')
    stopped = server.seen(lambda e: e.kind == 'campaign.status' and payload(e)['new'] == 'stopped', 'the stop', 120)
    server.seen(lambda e: e.kind == 'campaign.closed', 'the campaign closed', ROLE_TIMEOUT + 300)

    def reports(e: Envelope) -> bool:
        return e.id > stopped.id and e.kind == 'agent_session.started' and payload(e)['role'] == 'orchestrator'

    report = server.seen(reports, 'the orchestrator woken by the stop', 600).subject.id
    finished = server.seen(kind_on('agent_session.finished', 'agent_session', report), f'orchestrator#{report}', 900)
    server.none(
        lambda e: e.id > finished.id and e.kind == 'agent_session.started', 'an agent started after the report', 90
    )
    state = server.state()
    assert state.status == 'stopped', state
    assert state.agents == [], state.agents
    orchestrator = state.roles[RoleName('orchestrator')]
    assert (orchestrator.requested, orchestrator.live) == (0, 0), orchestrator
    print(f'{server.name}: {state.tokens} tokens, head {state.head}; orchestrator#{report} reported the stop')


@needs(test_stop)
def test_retire_frees_the_name_and_the_url_registers_again(server: Server) -> None:
    """``piloop campaign retire``: the stopped campaign leaves the listing and its name.

    A second registration of the URL under that name is a new campaign, in a checkout of its own.
    """
    server.piloop('campaign', 'retire', server.name, '--reason', 'e2e: the scenario ends')
    state = server.state()
    assert state.status == 'retired', state
    assert server.name not in [c.name for c in server.api.status(Selection()).campaigns]
    again = server.api.register_campaign(Clone(clone=Remote(url=server.repo), name=server.name)).campaign
    assert isinstance(again, CampaignState), again
    server.again = again.id
    assert (again.name, again.status) == (server.name, 'active'), again
    assert again.id != server.id and again.path != state.path, again
    print(f'{server.name}: retired #{server.id} at {state.path}; registered again as #{again.id} at {again.path}')
