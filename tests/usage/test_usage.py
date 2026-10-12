"""Token usage: a spec names a reader and its parameters; the jsonl reader's encoders and tallies; the refill."""

import shutil
from pathlib import Path

import pytest
from inline_snapshot import snapshot
from pydantic import ValidationError
from pytest_mock import MockerFixture

from piloop.agents import refill as refill_module
from piloop.agents.refill import usage_of
from piloop.config import ConfigError, load_project_config
from piloop.db.models import Project, Worker, WorkerKind, WorkerState, utcnow
from piloop.db.session import Database
from piloop.harnesses import DEFAULT_HARNESSES, Harness
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
from piloop.usage.jsonl import CWD_ENCODERS, SAID, TALLIES, JsonlReader, last_said

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
"""The built-in pi spec as a project would write it under ``[harness.<name>.usage]``."""


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
    """Copy the fixture for ``harness`` to where its CLI would have written the session of ``session_id`` under ``home``."""
    folder = JsonlReader(spec_of(harness)).encode_cwd(cwd) if cwd is not None else 'elsewhere'
    rel = {
        'pi': f'.pi/agent/sessions/{folder}/2026-09-09T05-48-14-300Z_{session_id}.jsonl',
        'claude': f'.claude/projects/{folder}/{session_id}.jsonl',
        'codex': f'.codex/sessions/2026/09/11/rollout-2026-09-11T21-32-25-{session_id}.jsonl',
    }[harness]
    path = home / (name or rel)
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(fixtures / f'{harness}.jsonl', path)
    return path


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """``{home}`` in every spec: a fresh directory."""
    monkeypatch.setenv('HOME', str(tmp_path))
    return tmp_path


def test_the_registries_hold_the_shipped_pieces_and_nothing_names_a_harness_outside_the_leaves() -> None:
    assert set(READERS) == {'jsonl'}
    assert set(CWD_ENCODERS) == {'none', 'dash_separators_wrapped', 'dash_non_alnum'}
    assert set(TALLIES) == {'pi', 'claude', 'codex'}
    assert set(SAID) == {'pi', 'claude', 'codex'}
    generic = (Path('piloop/usage/__init__.py').read_text() + Path('piloop/usage/jsonl.py').read_text()).lower()
    for name in ('pi', 'claude', 'codex'):
        assert f"'{name}'" not in generic, f'{name} belongs in piloop/usage/tallies/{name}.py and the harness spec'
    assert {name: h.usage for name, h in DEFAULT_HARNESSES.items()} == snapshot(
        {
            'pi': UsageSpec(
                reader='jsonl',
                file='{home}/.pi/agent/sessions/{cwd}/*_{session_id}.jsonl',
                cwd='dash_separators_wrapped',
                tally='pi',
            ),
            'claude': UsageSpec(
                reader='jsonl',
                file='{home}/.claude/projects/{cwd}/{session_id}.jsonl',
                cwd='dash_non_alnum',
                tally='claude',
            ),
            'codex': UsageSpec(
                reader='jsonl',
                file='{home}/.codex/sessions/*/*/*/rollout-*-{session_id}.jsonl',
                cwd='none',
                tally='codex',
            ),
        }
    ), 'the built-ins are data: what a piloop.toml override would say'


def test_cwd_encoders_match_the_stores_on_disk() -> None:
    """Folder names as pi 0.5x and Claude Code 2.1 wrote them for piloop worktrees and the project workspace."""
    wrapped, non_alnum, none = (
        CWD_ENCODERS['dash_separators_wrapped'],
        CWD_ENCODERS['dash_non_alnum'],
        CWD_ENCODERS['none'],
    )
    assert wrapped(Path('/home/me/.piloop/runs/10/worktree')) == '--home-me-.piloop-runs-10-worktree--'
    assert wrapped(Path('/home/me')) == '--home-me--'
    assert non_alnum(Path('/home/me/.piloop/runs/2/worktree')) == '-home-me--piloop-runs-2-worktree'
    assert non_alnum(Path('/tmp/entire-j-e2e/home/runs/2/worktree')) == '-tmp-entire-j-e2e-home-runs-2-worktree'
    assert none(CWD) == '*', 'a store not keyed by cwd: the session id in the file name is enough'


def test_pi_tally_sums_every_assistant_message(home: Path) -> None:
    path = store(home, 'pi', PI_ID)
    assert read_usage(spec_of('pi'), PI_ID, CWD, None) == snapshot(
        Usage(input=4, output=993, cache_read=6763, cache_write=8718, total=16478, turns=2, source=str(path))
    ), "totalTokens is pi's own sum: input + output + cacheRead + cacheWrite"


def test_claude_tally_counts_each_api_response_once(home: Path) -> None:
    """Streaming writes one assistant line per content block, each repeating the response's usage."""
    path = store(home, 'claude', CLAUDE_ID)
    usage = read_usage(spec_of('claude'), CLAUDE_ID, CWD, None)
    assert usage == snapshot(
        Usage(input=4, output=1750, cache_read=89160, cache_write=20075, total=110989, turns=2, source=str(path))
    )
    assert usage is not None and usage.total == usage.input + usage.output + usage.cache_read + usage.cache_write, (
        'Anthropic reports uncached input, cache reads and cache writes apart; the total is the four summed'
    )


def test_codex_tally_takes_the_last_cumulative_token_count(home: Path) -> None:
    path = store(home, 'codex', CODEX_ID)
    assert read_usage(spec_of('codex'), CODEX_ID, CWD, None) == snapshot(
        Usage(input=39981, output=372, cache_read=23808, cache_write=0, total=40353, turns=2, source=str(path))
    ), "input_tokens includes the cached part, so total_tokens is Codex's input + output; a null info is not a turn"


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


def test_a_project_spells_a_spec_in_toml_for_an_override_or_a_custom_harness(home: Path, project_path: Path) -> None:
    toml = project_path / 'piloop.toml'
    toml.write_text(
        toml.read_text()
        + '\n[harness.claude]\nexecutable = "claude"\nheadless = ["--session-id", "{session_id}", "{prompt}"]\n'
        '[harness.claude.usage]\nfile = "{home}/claude-work/projects/{cwd}/{session_id}.jsonl"\n'
        'cwd = "dash_non_alnum"\ntally = "claude"\n'
        '\n[harness.local]\nexecutable = "local-agent"\nheadless = ["--session", "{session_id}", "{prompt}"]\n'
        '[harness.local.usage]\nreader = "jsonl"\nfile = "{home}/.local-agent/{session_id}.jsonl"\ntally = "pi"\n'
    )
    cfg = load_project_config(project_path)
    moved = cfg.harness_for('claude').usage
    assert moved == UsageSpec(
        file='{home}/claude-work/projects/{cwd}/{session_id}.jsonl', cwd='dash_non_alnum', tally='claude'
    ), 'an override of a built-in changes where the file is; the reader and tally stay'
    path = store(
        home, 'claude', CLAUDE_ID, name=f'claude-work/projects/{CWD_ENCODERS["dash_non_alnum"](CWD)}/{CLAUDE_ID}.jsonl'
    )
    usage = read_usage(moved, CLAUDE_ID, CWD, None)
    assert usage is not None and (usage.total, usage.source) == (110989, str(path))
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
        ConfigError, match=r"harness.claude.usage: unknown usage tally 'gpt'; known: \['claude', 'codex', 'pi'\]"
    ):
        load_project_config(project_path)


def test_spec_names_are_checked_against_the_registries() -> None:
    with pytest.raises(ValidationError, match=r"unknown usage reader 'csv'; known: \['jsonl'\]"):
        UsageSpec(reader='csv', file='x')
    with pytest.raises(ValidationError, match='the jsonl usage reader needs `file`'):
        UsageSpec(tally='pi')
    with pytest.raises(ValidationError, match=r"unknown usage cwd encoder 'hex'; known: \['dash_non_alnum'"):
        UsageSpec(file='x', cwd='hex', tally='pi')
    with pytest.raises(ValidationError, match='unknown usage tally None'):
        UsageSpec(file='x')
    with pytest.raises(ValueError, match=r'headless must pass \{session_id\}.*when entire_agent or usage is set'):
        Harness(executable='x', headless=['{prompt}'], usage=UsageSpec(**PI_SPEC))
    assert Harness(executable='x', headless=['{prompt}']).captures_session is False
    assert Harness(executable='x', headless=['{prompt}', '{session_id}'], usage=UsageSpec(**PI_SPEC)).captures_session


@pytest.mark.parametrize(
    ('harness', 'words'),
    [
        ('claude', 'Starting both training runs (16 processes each).'),
        ('codex', "I'm reusing the baseline predictions from another worker's shared workspace."),
        ('pi', "I'll launch sweeps when there's something new to read."),
    ],
)
def test_said_is_the_last_line_of_the_agents_last_text(home: Path, harness: str, words: str) -> None:
    """Past the tool calls and results written after it; thinking, tool output and the brief are not the agent's words."""
    store(home, harness, CLAUDE_ID, cwd=None, fixtures=SAID_SESSIONS)
    assert spec_of(harness).resolved.said(CLAUDE_ID) == [words]


def test_said_takes_as_many_lines_as_asked_across_messages(home: Path) -> None:
    path = store(home, 'claude', CLAUDE_ID, fixtures=SAID_SESSIONS)
    with path.open('a') as f:
        f.write('{"type":"assistant","message":{"content":[{"type":"text","text":"cut short')  # a killed agent
    claude = spec_of('claude').resolved
    assert claude.said(CLAUDE_ID, 3) == [
        'Reading the evaluator first.',
        'The smoke test passed (2 examples).',
        'Starting both training runs (16 processes each).',
    ], 'blank lines dropped, oldest first'
    assert claude.said(CLAUDE_ID, 60) == claude.said(CLAUDE_ID, 3), 'all there is'


def test_said_reads_the_end_of_the_file_once_per_change(home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    pi = spec_of('pi').resolved
    path = store(home, 'pi', PI_ID, fixtures=SAID_SESSIONS)
    last_said.cache_clear()
    assert pi.said(PI_ID) == pi.said(PI_ID) == ["I'll launch sweeps when there's something new to read."]
    assert last_said.cache_info().hits == 1, 'an unchanged file is not read again'
    record = '{"type":"message","message":{"role":"assistant","content":[{"type":"text","text":"Sweep %d done."}]}}\n'
    with path.open('a') as f:
        f.write(record % 1)
    assert pi.said(PI_ID) == ['Sweep 1 done.'], 'a file that grew is read again'
    silent = '{"type":"message","message":{"role":"assistant","content":[]}}\n'
    monkeypatch.setattr(jsonl_module, 'TAIL_BYTES', len(record) + len(silent) + 5)
    with path.open('a') as f:
        f.write(record % 2 + silent)
    assert pi.said(PI_ID) == ['Sweep 2 done.'], 'only the end is read; the line the seek cut into is skipped'


def test_said_is_none_when_there_is_nothing_to_say(home: Path, mocker: MockerFixture) -> None:
    claude = spec_of('claude').resolved
    assert claude.said(None) == [], 'no session id yet'
    assert claude.said(CLAUDE_ID) == [], 'no session file yet'
    path = store(home, 'claude', CLAUDE_ID, fixtures=SAID_SESSIONS)
    path.write_text('\n'.join(line for line in path.read_text().splitlines() if '"type":"text"' not in line))
    assert claude.said(CLAUDE_ID) == [], 'nothing but thinking and tool calls: the screen tells more'
    store(home, 'claude', CLAUDE_ID, fixtures=SAID_SESSIONS)
    mocker.patch.object(jsonl_module, 'last_said', side_effect=FileNotFoundError('rotated'))
    assert claude.said(CLAUDE_ID) == [], 'a file gone between the glob and the read says nothing this frame'
    TALLIES['test-silent'] = TALLIES['pi']
    try:
        silent = JsonlReader(UsageSpec(file='{home}/x/{session_id}.jsonl', tally='test-silent'))
        assert silent.said(PI_ID) == [], 'a format without words registered says nothing'
    finally:
        del TALLIES['test-silent']


def test_a_new_kind_of_source_is_one_reader_registered_by_name() -> None:
    class FromStdout:
        def __init__(self, spec: UsageSpec) -> None:
            self.spec = spec

        def read(self, session_id: str | None, cwd: Path | None, log_dir: Path | None) -> Usage:
            return Usage(1, 2, 0, 0, 3, 1, f'{log_dir}/worker.stdout')

        def said(self, session_id: str | None, lines: int = 1) -> list[str]:
            return []

    reader('test-stdout')(FromStdout)
    try:
        spec = UsageSpec(reader='test-stdout')
        assert read_usage(spec, None, None, Path('/logs/7')) == Usage(1, 2, 0, 0, 3, 1, '/logs/7/worker.stdout'), (
            'no session id, no cwd: a reader decides what it needs'
        )
    finally:
        del READERS['test-stdout']


def test_what_cannot_be_read_is_a_usage_error_or_nothing(home: Path) -> None:
    assert read_usage(None, PI_ID, CWD, None) is None, 'no spec: nothing to read, like no entire_agent'
    pi = spec_of('pi')
    with pytest.raises(UsageError, match='no session id'):
        read_usage(pi, None, CWD, None)
    with pytest.raises(UsageError, match=f'no session file for {PI_ID} under {home}'):
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
    assert read_usage(pi, PI_ID, CWD, None) == Usage(2, 4, 0, 0, 3, 2, str(path)), (
        'a message without usage is not a turn; a non-numeric field counts nothing; the cut last line is skipped'
    )


def test_usage_of_a_worker_never_raises(db: Database, project: Project, home: Path, mocker: MockerFixture) -> None:
    store(home, 'pi', PI_ID, cwd=Path('/gone/worktree'))
    with db.session() as s:
        p = s.get_one(Project, project.id)
        ended = dict(project_id=p.id, state=WorkerState.EXITED, ended_at=utcnow(), log_dir=str(home / 'logs'))
        found = Worker(kind=WorkerKind.WORKER, harness='pi', session_id=PI_ID, **ended)
        removed = Worker(kind=WorkerKind.WORKER, harness='local-agent', session_id='x', **ended)
        no_spec = Worker(kind=WorkerKind.WORKER, harness='script', session_id='x', **ended)
        orphan = Worker(kind=WorkerKind.WATCHDOG, harness='pi', session_id=PI_ID, state=WorkerState.EXITED)
        s.add_all([found, removed, no_spec, orphan])
        s.flush()
        usage = usage_of(s, found)
        assert isinstance(usage, Usage) and usage.total == 16478
        assert str(usage_of(s, removed)) == "harness 'local-agent' is no longer configured for the project", (
            'a historical row keeps only the harness name; the project may have dropped it since'
        )
        assert usage_of(s, no_spec) is None and usage_of(s, orphan) is None, 'no spec; a row without a project'
        mocker.patch.object(refill_module, 'read_usage', side_effect=OSError('disk on fire'))
        assert str(usage_of(s, found)) == 'OSError: disk on fire', "a reader's failure is a UsageError, not a crash"


def test_human_tokens() -> None:
    assert [human(n) for n in (0, 987, 1_000, 340_499, 999_999, 1_000_000, 1_234_567, 23_400_000)] == snapshot(
        ['0', '987', '1k', '340k', '1000k', '1.0M', '1.2M', '23.4M']
    )
