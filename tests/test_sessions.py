"""An agent's session file goes into the CAS as it grows (``SessionFile``): debounced snapshots of new chunks only."""

from pathlib import Path

import pytest
from covalence import Value
from pytest_mock import MockerFixture

from piloop.plugins.executor.kit.sessions import FLUSH_SECONDS, QUIET_SECONDS, SessionFile
from piloop.store import chunks
from piloop.store.chunks import read
from piloop.usage import UsageSpec
from tests.conftest import disk
from tests.store.test_chunks import lines

SPEC = UsageSpec(file='{home}/sessions/{session_id}.jsonl', tally='pi')


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv('HOME', str(tmp_path))
    (tmp_path / 'sessions').mkdir()
    return tmp_path


def test_a_snapshot_is_due_once_the_file_grew_and_was_quiet_or_at_the_flush(home: Path) -> None:
    file = SessionFile(SPEC.resolved, disk(home / 'home'), home / 'worktree')
    assert not file.due(0), 'no session id yet'
    file.session_id = 's1'
    assert not file.due(0), 'no file yet'
    path = home / 'sessions' / 's1.jsonl'
    path.write_bytes(b'{"a":1}\n')
    assert not file.due(1), 'it just grew'
    assert file.due(1 + QUIET_SECONDS), 'quiet since'
    file.snapshot(1 + QUIET_SECONDS)
    assert not file.due(10), 'nothing new'
    for t in range(11, 11 + int(FLUSH_SECONDS)):
        path.write_bytes(path.read_bytes() + b'{"b":2}\n')
        if file.due(t):
            break
    assert t - (1 + QUIET_SECONDS) >= FLUSH_SECONDS, 'a file that keeps growing waits for the flush'
    path.unlink()
    assert not file.due(t + 100), 'a file that went is looked for again'


def test_a_snapshot_stores_only_the_new_whole_lines_and_reads_back_whole(home: Path, mocker: MockerFixture) -> None:
    cas = disk(home / 'home')
    file = SessionFile(SPEC.resolved, cas, home / 'worktree', 's1')
    assert file.snapshot(0) is None, 'no file yet'
    path = home / 'sessions' / 's1.jsonl'
    data = lines(800)
    path.write_bytes(data[:300_000] + b'{"cut')
    first = file.snapshot(1)
    assert first is not None and read(cas, first) == data[: data.rfind(b'\n', 0, 300_000) + 1], 'whole lines only'
    assert file.snapshot(2) == first, 'nothing new: the same snapshot'
    put = mocker.spy(cas, 'put')
    path.write_bytes(data)
    second = file.snapshot(3)
    assert second is not None and read(cas, second) == data
    new = sum(len(call.args[0]) for call in put.call_args_list)
    assert new < len(data) - 200_000, 'the closed chunks of the first snapshot are not read or put again'
    path.write_bytes(b'{"rewritten":1}\n')
    third = file.snapshot(4)
    assert third is not None and read(cas, third) == b'{"rewritten":1}\n', 'a shorter file is read from its start'


def test_a_long_file_keeps_its_closed_chunks_in_groups(home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr('piloop.plugins.executor.kit.sessions.GROUP', 4)
    cas = disk(home / 'home')
    file = SessionFile(SPEC.resolved, cas, home / 'worktree', 's1')
    data = lines(600, sizes=(4000,))
    (home / 'sessions' / 's1.jsonl').write_bytes(data)
    latest = file.snapshot(0)
    assert latest is not None and read(cas, latest) == data
    manifest = chunks.Chunked.model_validate(Value.decode(cas.get(latest) or b'').to_native())
    assert len(manifest.chunks) < len(list(chunks.cuts(data))) / 3, 'a group stands for its chunks'
