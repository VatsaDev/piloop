"""Token usage: a spec names a reader and its parameters; the jsonl reader's encoders and tallies; the refill."""

import re
import shutil
from pathlib import Path

import pytest
from inline_snapshot import snapshot
from pydantic import ValidationError
from pytest_mock import MockerFixture
from sqlalchemy import select, text

from piloop.agents import refill as refill_module
from piloop.agents.refill import usage_of
from piloop.config import ConfigError, load_campaign_config
from piloop.db.models import AgentSession, AgentSessionState, Campaign, utcnow
from piloop.db.session import Database
from piloop.harnesses import DEFAULT_HARNESSES, Harness
from piloop.ids import IMPLEMENTER
from piloop.settings import env_names, expand_env
from piloop.usage import (
    READERS,
    Usage,
    UsageError,
    UsageSpec,
    human,
    jsonl as jsonl_module,
    read_usage,
    reader,
)
from piloop.usage.jsonl import CWD_ENCODERS, SAID, TALLIES, JsonlReader
from tests.conftest import WATCHDOG, bare_config

SESSIONS = Path(__file__).parent.parent / 'fixtures' / 'sessions'
"""Redacted heads of real session files: the records that carry usage, with the prompts and outputs cut."""
SAID_SESSIONS = Path(__file__).parent.parent / 'fixtures' / 'said'
"""Session files shaped like the real ones' ends: the agent's text, then tool calls and results after it."""
PI_ID = '01a084b5-ae5b-7113-9a8e-45c738e0ecfb'
CLAUDE_ID = 'd96a2121-355c-4608-b2f7-8bfc85bba1b5'
CODEX_ID = '01a09262-d574-7e10-9f75-23b0ce9c0a2f'
CWD = Path('/home/me/.piloop/runs/10/worktree')
PI_SPEC = {
    'file': '{home}/.pi/agent/sessions/{cwd}/*_{session_id}.jsonl',
    'cwd': 'dash_separators_wrapped',
    'tally': 'pi',
}
"""The built-in pi spec as a campaign would write it under ``[harness.<name>.usage]``."""


def spec_of(harness: str) -> UsageSpec:
    spec = DEFAULT_HARNESSES[harness].usage
    assert spec is not None
    return spec


def store(
    home: Path,
    harness: str,
    session_id: str,
    cwd: Path | None = CWD,
    name: str | None = None,
    fixtures: Path = SESSIONS,
) -> Path:
    """Copy the fixture for ``harness`` to where its CLI would have written the session of ``session_id``: under the
    harness's home, or at ``home / name``."""
    folder = JsonlReader(spec_of(harness)).encode_cwd(cwd) if cwd is not None else 'elsewhere'
    rel = {
        'pi': f'sessions/{folder}/2026-09-09T05-48-14-300Z_{session_id}.jsonl',
        'claude': f'projects/{folder}/{session_id}.jsonl',
        'codex': f'sessions/2026/09/11/rollout-2026-09-11T21-32-25-{session_id}.jsonl',
    }[harness]
    path = home / name if name else Path(expand_env(DEFAULT_HARNESSES[harness].home or '')) / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(fixtures / f'{harness}.jsonl', path)
    return path


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """``{home}`` in every spec: a fresh directory, with every built-in harness at its default home under it."""
    monkeypatch.setenv('HOME', str(tmp_path))
    for harness in DEFAULT_HARNESSES.values():
        for variable in env_names(harness.home or ''):
            monkeypatch.delenv(variable, raising=False)
    return tmp_path


def test_the_registries_hold_the_shipped_pieces() -> None:
    """``tests/test_registry_names.py`` checks that no module outside the leaves names one."""
    assert set(READERS) == {'jsonl'}
    assert set(CWD_ENCODERS) == {'none', 'dash_separators_wrapped', 'dash_non_alnum'}
    assert set(TALLIES) == {'pi', 'claude', 'codex'}
    assert set(SAID) == {'pi', 'claude', 'codex'}
    assert {name: h.usage for name, h in DEFAULT_HARNESSES.items()} == snapshot(
        {
            'pi': UsageSpec(
                reader='jsonl',
                file='{env:PI_CODING_AGENT_DIR:~/.pi/agent}/sessions/{cwd}/*_{session_id}.jsonl',
                cwd='dash_separators_wrapped',
                tally='pi',
            ),
            'claude': UsageSpec(
                reader='jsonl',
                file='{env:CLAUDE_CONFIG_DIR:~/.claude}/projects/{cwd}/{session_id}.jsonl',
                cwd='dash_non_alnum',
                tally='claude',
            ),
            'codex': UsageSpec(
                reader='jsonl',
                file='{env:CODEX_HOME:~/.codex}/sessions/*/*/*/rollout-*-{session_id}.jsonl',
                cwd='none',
                tally='codex',
            ),
        }
    ), 'the built-ins are data: what a piloop.toml override would say'


def test_cwd_encoders_match_the_stores_on_disk() -> None:
    """Folder names as pi 0.5x and Claude Code 2.1 wrote them for piloop worktrees and the campaign workspace."""
    wrapped, non_alnum, none = (
        CWD_ENCODERS['dash_separators_wrapped'],
        CWD_ENCODERS['dash_non_alnum'],
        CWD_ENCODERS['none'],
    )
    assert wrapped(Path('/home/me/.piloop/runs/10/worktree')) == '--home-me-.piloop-runs-10-worktree--'
    assert wrapped(Path('/home/me')) == '--home-me--'
    assert non_alnum(Path('/home/me/.piloop/runs/2/worktree')) == '-home-me--piloop-runs-2-worktree'
    assert non_alnum(Path('/tmp/piloop-j-e2e/home/runs/2/worktree')) == '-tmp-piloop-j-e2e-home-runs-2-worktree'
    assert none(CWD) == '*', 'a store not keyed by cwd: the session id in the file name is enough'


def test_pi_tally_sums_every_assistant_message(home: Path) -> None:
    store(home, 'pi', PI_ID)
    assert read_usage(spec_of('pi'), PI_ID, CWD, None) == snapshot(
        Usage(input=4, output=993, cache_read=6763, cache_write=8718, total=16478, turns=2)
    ), "totalTokens is pi's own sum: input + output + cacheRead + cacheWrite"


def test_claude_tally_counts_each_api_response_once(home: Path) -> None:
    """Streaming writes one assistant line per content block, each repeating the response's usage."""
    store(home, 'claude', CLAUDE_ID)
    usage = read_usage(spec_of('claude'), CLAUDE_ID, CWD, None)
    assert usage == snapshot(Usage(input=4, output=1750, cache_read=89160, cache_write=20075, total=110989, turns=2))
    assert usage is not None and usage.total == usage.input + usage.output + usage.cache_read + usage.cache_write, (
        'Anthropic reports uncached input, cache reads and cache writes apart; the total is the four summed'
    )


@pytest.mark.parametrize(('harness', 'session_id'), [('pi', PI_ID), ('claude', CLAUDE_ID), ('codex', CODEX_ID)])
def test_sessions_are_found_under_the_home_that_the_harness_variable_names(
    harness: str, session_id: str, home: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    [variable] = env_names(DEFAULT_HARNESSES[harness].home or '')
    store(home, harness, session_id)
    moved = tmp_path / 'elsewhere' / harness
    monkeypatch.setenv(variable, str(moved))
    with pytest.raises(UsageError, match='no session file'):
        read_usage(spec_of(harness), session_id, CWD, None)
    path = store(home, harness, session_id)
    assert path.is_relative_to(moved)
    assert read_usage(spec_of(harness), session_id, CWD, None) is not None
    assert spec_of(harness).resolved.session_file(session_id, CWD) == path


def test_codex_tally_takes_the_last_cumulative_token_count(home: Path) -> None:
    store(home, 'codex', CODEX_ID)
    assert read_usage(spec_of('codex'), CODEX_ID, CWD, None) == snapshot(
        Usage(input=16173, output=372, cache_read=23808, cache_write=0, total=40353, turns=2)
    ), (
        "input_tokens includes the cached part, which input leaves out as for every harness; total_tokens is Codex's"
        ' own input + output; a null info is not a turn'
    )


def test_the_file_is_found_by_the_encoded_cwd_then_by_a_scan_of_every_cwd_folder(home: Path) -> None:
    pi = JsonlReader(spec_of('pi'))
    assert pi.session_file(PI_ID, CWD) is None, 'no store yet'
    elsewhere = store(home, 'pi', PI_ID, cwd=Path('/somewhere/else'))
    assert pi.session_file(PI_ID, CWD) == elsewhere, 'the scan: one level of cwd folders, by session id'
    assert pi.session_file(PI_ID, None) == elsewhere, 'no cwd (the worktree is gone): straight to the scan'
    here = store(home, 'pi', PI_ID)
    assert pi.session_file(PI_ID, CWD) == here, "the agent's own cwd folder wins over the scan"
    store(home, 'pi', 'other-session', cwd=CWD)
    assert pi.session_file(PI_ID, CWD) == here, 'other sessions of the same cwd are not it'
    nested = store(home, 'pi', PI_ID, name=f'.pi/agent/sessions/a/b/x_{PI_ID}.jsonl')
    assert nested.is_file() and pi.session_file('unknown', None) is None, 'the scan never recurses'


def test_a_campaign_spells_a_spec_in_toml_for_an_override_or_a_custom_harness(home: Path, campaign_path: Path) -> None:
    toml = campaign_path / 'piloop.toml'
    toml.write_text(
        toml.read_text()
        + '\n[harness.claude]\nexecutable = "claude"\nheadless = ["--session-id", "{session_id}", "{prompt}"]\n'
        '[harness.claude.usage]\nfile = "{home}/claude-work/campaigns/{cwd}/{session_id}.jsonl"\n'
        'cwd = "dash_non_alnum"\ntally = "claude"\n'
        '\n[harness.local]\nexecutable = "local-agent"\nheadless = ["--session", "{session_id}", "{prompt}"]\n'
        '[harness.local.usage]\nreader = "jsonl"\nfile = "{home}/.local-agent/{session_id}.jsonl"\ntally = "pi"\n'
    )
    cfg = load_campaign_config(campaign_path)
    moved = cfg.harness_for('claude').usage
    assert moved == UsageSpec(
        file='{home}/claude-work/campaigns/{cwd}/{session_id}.jsonl', cwd='dash_non_alnum', tally='claude'
    ), 'an override of a built-in changes where the file is; the reader and tally stay'
    path = store(
        home, 'claude', CLAUDE_ID, name=f'claude-work/campaigns/{CWD_ENCODERS["dash_non_alnum"](CWD)}/{CLAUDE_ID}.jsonl'
    )
    usage = read_usage(moved, CLAUDE_ID, CWD, None)
    assert usage is not None and usage.total == 110989 and moved.resolved.session_file(CLAUDE_ID, CWD) == path
    local = cfg.harness_for('local').usage
    assert local == UsageSpec(file='{home}/.local-agent/{session_id}.jsonl', cwd='none', tally='pi'), (
        'a custom harness gets a whole spec: a pi-shaped file wherever it keeps it'
    )
    store(home, 'pi', 'abc', name='.local-agent/abc.jsonl')
    usage = read_usage(local, 'abc', CWD, None)
    assert usage is not None and usage.total == 16478
    assert cfg.harness_for('script').usage is None, 'no spec: no capture'
    toml.write_text(toml.read_text().replace('tally = "claude"', 'tally = "gpt"'))
    with pytest.raises(
        ConfigError, match=re.escape(f"harness.claude.usage: unknown usage tally 'gpt'; known: {sorted(TALLIES)}")
    ):
        load_campaign_config(campaign_path)


def test_spec_names_are_checked_against_the_registries() -> None:
    with pytest.raises(ValidationError, match=re.escape(f"unknown usage reader 'csv'; known: {sorted(READERS)}")):
        UsageSpec(reader='csv', file='x')
    with pytest.raises(ValidationError, match='the jsonl usage reader needs `file`'):
        UsageSpec(tally='pi')
    with pytest.raises(
        ValidationError, match=re.escape(f"unknown usage cwd encoder 'hex'; known: {sorted(CWD_ENCODERS)}")
    ):
        UsageSpec(file='x', cwd='hex', tally='pi')
    with pytest.raises(ValidationError, match='unknown usage tally None'):
        UsageSpec(file='x')
    with pytest.raises(ValueError, match=r'headless must pass \{session_id\}.*when usage is set'):
        Harness(executable='x', headless=['{prompt}'], usage=UsageSpec(**PI_SPEC))
    assert Harness(executable='x', headless=['{prompt}']).needs_session_id is False
    assert Harness(executable='x', headless=['{prompt}', '{session_id}'], usage=UsageSpec(**PI_SPEC)).needs_session_id


@pytest.mark.parametrize(
    ('harness', 'words'),
    [
        ('claude', 'Starting both training runs (16 processes each).'),
        ('codex', "I'm reusing the baseline predictions from another worker's shared workspace."),
        ('pi', "I'll launch sweeps when there's something new to read."),
    ],
)
def test_said_is_the_last_line_of_the_agents_last_text(harness: str, words: str) -> None:
    """Past the tool calls and results written after it; thinking, tool output and the brief are not the agent's words."""
    assert spec_of(harness).resolved.said((SAID_SESSIONS / f'{harness}.jsonl').read_bytes()) == [words]


def test_said_takes_as_many_lines_as_asked_across_messages() -> None:
    cut = b'{"type":"assistant","message":{"content":[{"type":"text","text":"cut short'  # a killed agent
    session = (SAID_SESSIONS / 'claude.jsonl').read_bytes() + cut
    claude = spec_of('claude').resolved
    assert claude.said(session, 3) == [
        'Reading the evaluator first.',
        'The smoke test passed (2 examples).',
        'Starting both training runs (16 processes each).',
    ], 'blank lines dropped, oldest first'
    assert claude.said(session, 60) == claude.said(session, 3), 'all there is'


def test_said_reads_only_the_end_of_the_session(monkeypatch: pytest.MonkeyPatch) -> None:
    record = '{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"Sweep %d done."}]}}\n'
    silent = '{"type":"message","message":{"role":"assistant","content":[]}}\n'
    monkeypatch.setattr(jsonl_module, 'TAIL_BYTES', len(record) + len(silent) + 5)
    session = (SAID_SESSIONS / 'pi.jsonl').read_text() + record % 1 + record % 2 + silent
    assert spec_of('pi').resolved.said(session.encode()) == ['Sweep 2 done.'], (
        'only the end is read; the line the cut goes through is skipped'
    )
    assert spec_of('pi').resolved.said(session.encode(), 2) == ['Sweep 2 done.'], 'nothing before the cut'


def test_said_is_none_when_there_is_nothing_to_say() -> None:
    claude = spec_of('claude').resolved
    assert claude.said(b'') == [], 'an empty session'
    session = (SAID_SESSIONS / 'claude.jsonl').read_text()
    no_text = '\n'.join(line for line in session.splitlines() if '"type":"text"' not in line)
    assert claude.said(no_text.encode()) == [], 'nothing but thinking and tool calls: the screen tells more'
    TALLIES['test-silent'] = TALLIES['pi']
    try:
        silent = JsonlReader(UsageSpec(file='{home}/x/{session_id}.jsonl', tally='test-silent'))
        assert silent.said((SAID_SESSIONS / 'pi.jsonl').read_bytes()) == [], 'a format without words says nothing'
    finally:
        del TALLIES['test-silent']


def test_a_new_kind_of_source_is_one_reader_registered_by_name() -> None:
    class FromStdout:
        def __init__(self, spec: UsageSpec) -> None:
            self.spec = spec

        def read(self, session_id: str | None, cwd: Path | None, log_dir: Path | None) -> Usage:
            return Usage(
                input=1,
                output=2,
                cache_read=0,
                cache_write=0,
                total=3,
                turns=1,
            )

        def said(self, session: bytes, lines: int = 1) -> list[str]:
            return []

        def session_file(self, session_id: str, cwd: Path | None) -> Path | None:
            return None

    reader('test-stdout')(FromStdout)
    try:
        spec = UsageSpec(reader='test-stdout')
        assert read_usage(spec, None, None, Path('/logs/7')) == Usage(
            input=1, output=2, cache_read=0, cache_write=0, total=3, turns=1
        ), 'no session id, no cwd: a reader decides what it needs'
    finally:
        del READERS['test-stdout']


def test_what_cannot_be_read_is_a_usage_error_or_nothing(home: Path) -> None:
    assert read_usage(None, PI_ID, CWD, None) is None, 'no spec: nothing to read'
    pi = spec_of('pi')
    with pytest.raises(UsageError, match='no session id'):
        read_usage(pi, None, CWD, None)
    with pytest.raises(
        UsageError,
        match=re.escape(f'no session file for {PI_ID}: nothing matches {home}/.pi/agent/sessions/*/*_{PI_ID}.jsonl'),
    ):
        read_usage(pi, PI_ID, CWD, None)
    path = store(home, 'pi', PI_ID)
    path.write_text('{"type":"session","id":"x"}\nnot json\n[1, 2]\n{"type":"message","message":{"role":"user"}}\n')
    with pytest.raises(UsageError, match=f'no usage records in {path}'):
        read_usage(pi, PI_ID, CWD, None)
    path.write_text(
        '{"type":"message","message":{"role":"assistant","usage":{"input":1,"output":2,"totalTokens":"3"}}}\n'
        '{"type":"message","message":{"role":"assistant"}}\n'
        '{"type":"message","message":{"role":"assistant","usage":{"input":1.0,"output":2,"totalTokens":3}}}\n'
        '{"type":"message","message":{"role":"assistant","usage":{"input":100,"out'
    )
    assert read_usage(pi, PI_ID, CWD, None) == Usage(
        input=2, output=4, cache_read=0, cache_write=0, total=3, turns=2
    ), 'a message without usage is not a turn; a non-numeric field counts nothing; the cut last line is skipped'


def test_usage_of_an_implementer_never_raises(
    db: Database, campaign: Campaign, home: Path, mocker: MockerFixture
) -> None:
    store(home, 'pi', PI_ID, cwd=Path('/gone/worktree'))
    with db.session() as s:
        p = s.get_one(Campaign, campaign.id)
        ended = dict(campaign_id=p.id, state=AgentSessionState.EXITED, ended_at=utcnow(), log_dir=str(home / 'logs'))
        found = AgentSession(kind=IMPLEMENTER, harness='pi', session_id=PI_ID, **ended)
        removed = AgentSession(kind=IMPLEMENTER, harness='local-agent', session_id='x', **ended)
        no_spec = AgentSession(kind=IMPLEMENTER, harness='script', session_id='x', **ended)
        orphan = AgentSession(kind=WATCHDOG, harness='pi', session_id=PI_ID, state=AgentSessionState.EXITED)
        s.add_all([found, removed, no_spec, orphan])
        s.flush()
        usage = usage_of(s, found)
        assert isinstance(usage, Usage) and usage.total == 16478
        assert str(usage_of(s, removed)) == "harness 'local-agent' is no longer configured for the campaign", (
            'a historical row keeps only the harness name; the campaign may have dropped it since'
        )
        assert usage_of(s, no_spec) is None and usage_of(s, orphan) is None, 'no spec; a row without a campaign'
        mocker.patch.object(refill_module, 'read_usage', side_effect=OSError('disk on fire'))
        assert str(usage_of(s, found)) == 'OSError: disk on fire', "a reader's failure is a UsageError, not a crash"


def test_human_tokens() -> None:
    assert [human(n) for n in (0, 987, 1_000, 340_499, 999_999, 1_000_000, 1_234_567, 23_400_000)] == snapshot(
        ['0', '987', '1k', '340k', '1000k', '1.0M', '1.2M', '23.4M']
    )


def test_the_usage_column_holds_a_usage_and_an_unknown_usage_is_null(db: Database) -> None:
    """``agent_sessions.usage`` loads as a ``Usage`` and saves its JSON. None is SQL NULL, so ``usage IS NULL`` finds
    the agents whose usage is unknown. A stored value of another shape fails the load of its row."""
    usage = Usage(input=1, output=2, cache_read=3, cache_write=4, total=10, turns=1)
    with db.session() as s:
        s.add(Campaign(name='p', path='/p', config=bare_config('p')))
        s.flush()
        s.add_all([AgentSession(kind=WATCHDOG, campaign_id=1, usage=usage), AgentSession(kind=WATCHDOG, campaign_id=1)])
        s.flush()
        s.get_one(AgentSession, 2).usage = None
        s.commit()
    with db.session() as s:
        assert [w.usage for w in s.scalars(select(AgentSession).order_by(AgentSession.id))] == [usage, None]
        raw = s.execute(text("SELECT json_extract(usage, '$.total'), usage IS NULL FROM agent_sessions ORDER BY id"))
        assert [tuple(row) for row in raw] == [(10, 0), (None, 1)]
        s.execute(text("""UPDATE agent_sessions SET usage = '{"total": "many"}' WHERE id = 1"""))
        s.commit()
    with db.session() as s, pytest.raises(ValidationError, match='input'):
        s.get_one(AgentSession, 1)
