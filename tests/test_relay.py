"""The ``piloop`` calls of an agent on a worker elsewhere: relayed by its worker, run on the server as the agent."""

import io
import re
import socket
import subprocess
import sys
import threading
from functools import partial
from pathlib import Path

import httpx2 as httpx
import pytest
from pytest_mock import MockerFixture
from sqlalchemy import select

from piloop.actor import Agent, System
from piloop.agents.roles import IDEAS, PROMPTS, SQL_HINT
from piloop.campaign.campaigns import checkout_of
from piloop.cli.database import ORCHESTRATOR_PROCEDURES
from piloop.cli.main import app
from piloop.db.models import Campaign, Comment, Event
from piloop.db.session import Database
from piloop.ids import AgentSessionId
from piloop.plugins.executor.kit import door as door_module, relay as relay_module
from piloop.plugins.executor.kit.client import Dismissed
from piloop.plugins.executor.kit.door import SOCKET, AgentCall, Call, Relayed, main
from piloop.plugins.executor.kit.leases import StaleLease
from piloop.plugins.executor.kit.relay import RELAYED, Relay, Runner
from piloop.plugins.runtime.contract import MESSAGES_ENV, QUEUE, put
from piloop.proc import ACTOR_ENV
from piloop.settings import Settings
from tests.test_lease import agent_job

AGENT = AgentSessionId(7)
SPAN = re.compile(r'`([^`]+)`')
FENCE = re.compile(r'```[a-z]*\n(.*?)```', re.DOTALL)
CALLER = re.compile(r'^(?:piloop|"\$PILOOP_BIN"|\{piloop\})\s')


def commands(text: str) -> set[tuple[str, ...]]:
    """The ``piloop`` commands that ``text`` names in its code spans and code blocks."""
    blocks = FENCE.findall(text)
    spans = SPAN.findall(FENCE.sub('', text))
    snippets = [*spans, *(line for block in blocks for line in block.splitlines())]
    found: set[tuple[str, ...]] = set()
    for snippet in snippets:
        if CALLER.match(snippet.strip()):
            tokens = [t for t in snippet.split()[1:] if re.fullmatch(r'[a-z][a-z-]*', t)]
            if chain := app.parse_commands(tokens)[0]:
                found.add(tuple(chain))
    return found


def test_an_agent_on_another_machine_may_run_the_commands_that_the_role_prompts_name_and_no_other() -> None:
    """The allow-list is the prompts': a command that a prompt adds is relayed, and one that it drops is not."""
    prompts = [p.read_text() for p in PROMPTS.glob('*.md') if p.name != ORCHESTRATOR_PROCEDURES]
    briefs = [f'`{SQL_HINT} -`', *(line.format(piloop='piloop', campaign='demo') for line in IDEAS.values())]
    named = set().union(*(commands(text) for text in [*prompts, *briefs]))
    assert named == RELAYED


@pytest.fixture
def runner(db: Database, tmp_path: Path) -> Runner:
    return Runner(db, tmp_path, partial(checkout_of, db))


def test_a_relayed_call_runs_on_the_server_as_the_agent_in_its_campaigns_checkout(
    db: Database, campaign: Campaign, tmp_path: Path, runner: Runner
) -> None:
    job = agent_job(db, campaign, tmp_path)
    actor = Agent(role='watchdog', session=job.agent_session).signature
    note = f"INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES ('campaign', 1, '{actor}', 'note', 'hi')"
    done = runner(job, Call(argv=['sql', '-'], stdin=note))
    assert done.exit_code == 0, done.stderr
    with db.session() as s:
        [written] = s.scalars(select(Comment).where(Comment.body == 'hi')).all()
        [event] = s.scalars(select(Event).where(Event.kind == 'comment.note')).all()
        assert event.actor == actor, 'signed as the agent of the lease'
    assert runner(job, Call(argv=['sql', 'SELECT 41 + 1'])).stdout.count('42') == 1, 'no stdin: none is read'


def test_a_command_that_is_not_relayed_is_refused_with_what_is(
    db: Database, campaign: Campaign, tmp_path: Path, runner: Runner
) -> None:
    done = runner(agent_job(db, campaign, tmp_path), Call(argv=['campaign', 'stop', 'demo']))
    assert done.exit_code == 2 and done.stderr.startswith('piloop campaign stop: an agent on another machine runs only')
    assert 'idea status' in done.stderr


def test_a_relayed_call_that_runs_too_long_is_stopped(
    db: Database, campaign: Campaign, tmp_path: Path, runner: Runner, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(relay_module, 'RELAY_SECONDS', 0.2)
    monkeypatch.setattr(relay_module, 'PILOOP_ARGV', ('sh', '-c', 'sleep 5', 'piloop'))
    done = runner(agent_job(db, campaign, tmp_path), Call(argv=['sql', 'SELECT 1']))
    assert (done.exit_code, done.stderr) == (124, 'piloop: the call ran for 0.2 s on the server and was stopped\n')


class Server:
    """The server's half of the relay, as ``Remote.cli`` reaches it: what it got, and what it answers."""

    def __init__(self) -> None:
        self.calls: list[tuple[int, Call]] = []
        self.fail: Exception | None = None

    def __call__(self, lease: int, call: Call) -> Relayed:
        self.calls.append((lease, call))
        if self.fail is not None:
            raise self.fail
        return Relayed(stdout=' '.join(call.argv), stderr='', exit_code=0)


def post(relay: Relay, body: bytes | str, path: str = '/v1/cli') -> httpx.Response:
    """A request to ``relay``'s socket, as an agent's ``piloop`` sends it."""
    with httpx.Client(transport=httpx.HTTPTransport(uds=str(relay.path))) as client:
        return client.post(f'http://worker{path}', content=body)


def call(agent: int = AGENT, argv: list[str] | None = None) -> str:
    return AgentCall(agent=AgentSessionId(agent), argv=argv or []).model_dump_json()


def test_the_relay_takes_an_agents_calls_on_its_lease_while_its_job_runs_on_this_worker(tmp_path: Path) -> None:
    server = Server()
    relay = Relay(tmp_path, server)
    relay.start()
    assert relay.path.parent.stat().st_mode & 0o077 == 0, "only the worker's user reaches its socket"
    assert post(relay, call()).status_code == 404, 'no job of this agent runs here'
    relay.open({AGENT: 3})
    answered = post(relay, call(argv=['sql', 'SELECT 1']))
    assert Relayed.model_validate_json(answered.content) == Relayed(stdout='sql SELECT 1', exit_code=0)
    assert server.calls == [(3, Call(argv=['sql', 'SELECT 1']))]
    assert post(relay, '{"argv": 1}').status_code == 422
    assert post(relay, '{}', path='/v1/other').status_code == 404
    server.fail = StaleLease('stale_lease: lease#3 is reported')
    assert Relayed.model_validate_json(post(relay, call()).content) == Relayed(
        stderr='piloop: the lease of this agent ended: stale_lease: lease#3 is reported\n', exit_code=1
    )
    server.fail = httpx.ConnectError('refused')
    assert Relayed.model_validate_json(post(relay, call()).content).stderr == (
        'piloop: the call did not run on the server: refused\n'
    )
    server.fail = Dismissed('the server refused this worker (401): revoked')
    assert Relayed.model_validate_json(post(relay, call()).content).stderr == (
        'piloop: the call did not run on the server: the server refused this worker (401): revoked\n'
    )
    server.fail = httpx.ReadTimeout('timed out')
    assert Relayed.model_validate_json(post(relay, call()).content).stderr == (
        'piloop: the answer of the server was lost, and the call may have run there: timed out\n'
    )
    relay.close(3)
    assert post(relay, call()).status_code == 404, 'its job ended'
    relay.stop()
    assert not relay.path.exists()
    restarted = Relay(tmp_path, server)
    (tmp_path / SOCKET).touch()  # what a worker that was killed leaves
    restarted.start()
    restarted.open({AGENT: 4})
    assert post(restarted, call()).status_code == 200, 'a re-attached agent relays again, on the same path'
    restarted.stop()
    Relay(tmp_path, server).stop()


def test_the_piloop_of_an_agent_elsewhere_prints_what_the_call_printed_and_exits_with_its_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], mocker: MockerFixture
) -> None:
    server = Server()
    relay = Relay(tmp_path, server)
    relay.start()
    agent = Agent(role='implementer', session=AGENT)
    monkeypatch.setattr(door_module, 'settings', Settings(home=tmp_path, actor=agent))
    monkeypatch.setattr(sys, 'argv', ['piloop', 'sql', '-'])
    monkeypatch.setattr(sys, 'stdin', io.StringIO('SELECT 1'))
    with pytest.raises(SystemExit, match='the call did not run: agent_session#7 has no job on this worker'):
        main()
    relay.open({AGENT: 3})
    monkeypatch.setattr(sys, 'stdin', io.StringIO('SELECT 1'))
    with pytest.raises(SystemExit) as ended:
        main()
    assert (ended.value.code, capsys.readouterr().out) == (0, 'sql -')
    assert server.calls == [(3, Call(argv=['sql', '-'], stdin='SELECT 1'))]
    relay.stop()
    with pytest.raises(SystemExit, match='no worker relays on .*, and the call did not run'):
        main()
    with socket.socket(socket.AF_UNIX) as hung:  # a worker that dies while it reads the call
        hung.bind(str(tmp_path / SOCKET))
        hung.listen()
        threading.Thread(target=lambda: hung.accept()[0].close(), daemon=True).start()
        with pytest.raises(SystemExit, match='the relay of this worker did not answer, and the call did not run'):
            main()
    monkeypatch.setattr(door_module, 'settings', Settings(home=tmp_path, actor=System.HUMAN))
    with pytest.raises(SystemExit, match='relays the calls of an agent, and human is none'):
        main()
    execv = mocker.patch.object(door_module.os, 'execv', side_effect=SystemExit(0))
    monkeypatch.setattr(sys, 'argv', ['piloop', 'agent', 'messages', '--format', 'claude-hook'])
    with pytest.raises(SystemExit):
        main()
    assert execv.call_args.args[1][-4:] == ['agent', 'messages', '--format', 'claude-hook'], 'run here, not relayed'


def test_the_shim_of_a_worker_elsewhere_relays_a_call_and_reads_the_agents_queue_here(tmp_path: Path) -> None:
    from piloop.plugins.executor.kit.jobs import remote_site
    from tests.conftest import disk

    site = remote_site(tmp_path, disk(tmp_path), lambda repo, revisions, have: [], reserve=0)
    done = subprocess.run([site.cli, 'sql', 'SELECT 1'], capture_output=True, text=True, env={'PATH': '/usr/bin:/bin'})
    assert done.returncode == 1 and 'relays the calls of an agent, and human is none' in done.stderr
    log_dir = tmp_path / 'runs' / '1'
    log_dir.mkdir(parents=True)
    put(log_dir, 'try lzma')
    env = {'PATH': '/usr/bin:/bin', MESSAGES_ENV: str(log_dir / QUEUE), ACTOR_ENV: 'implementer#1'}
    taken = subprocess.run([site.cli, 'agent', 'messages'], capture_output=True, text=True, env=env)
    assert (taken.returncode, taken.stdout.strip()) == (0, 'try lzma'), 'its queue is on the worker: read here'
