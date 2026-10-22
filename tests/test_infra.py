"""Settings, tracing, git helpers, database session."""

import threading
from pathlib import Path

import pytest
from git import Repo
from inline_snapshot import snapshot
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode
from pytest_mock import MockerFixture

from piloop import git, otel
from piloop.db.models import AgentSession, AgentSessionState
from piloop.ids import RESEARCH
from piloop.otel import Tracer
from piloop.runtime.factory import handle_for
from piloop.settings import Settings
from tests.conftest import console_lines


def test_settings_derived(tmp_path: Path) -> None:
    s = Settings(home=tmp_path)
    assert s.database_url == f'sqlite:///{tmp_path / "piloop.db"}'


def test_settings_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv('PILOOP_HOME', str(tmp_path))
    monkeypatch.setenv('OTEL_EXPORTER_OTLP_ENDPOINT', 'http://collector:4318')
    s = Settings()
    assert (s.home, s.otel_exporter_otlp_endpoint) == (tmp_path, 'http://collector:4318')


def test_tracer_console_lines(capsys: pytest.CaptureFixture[str], spans: InMemorySpanExporter, tracer: Tracer) -> None:
    with tracer.span('x', a=1, b=None) as span:
        span.set(c='v', d=None)
        span.event('happened', n=2)
        tracer.event('also', on='current span')
        span.error(ValueError('bad'))
    tracer.event('alone', k='v')
    tracer.shutdown()
    assert console_lines(capsys.readouterr().err) == snapshot(
        ['happened n=2', 'also on=current span', 'x Ns a=1 c=v error=ValueError: bad', 'alone Ns k=v']
    )
    x, alone = spans.get_finished_spans()
    assert [e.name for e in x.events] == ['happened', 'also', 'exception']
    assert (x.status.status_code, x.status.description) == (StatusCode.ERROR, 'ValueError: bad')
    assert (alone.name, dict(alone.attributes or {})) == ('alone', {'k': 'v'})


def test_tracer_otlp_export_is_optional(monkeypatch: pytest.MonkeyPatch, mocker: MockerFixture) -> None:
    Tracer(None).shutdown()
    exporter = mocker.patch('piloop.otel.OTLPSpanExporter')
    monkeypatch.setattr(otel, 'settings', Settings(otel_exporter_otlp_endpoint='http://collector:4318'))
    t = otel.tracer()
    with t.span('x', a=1) as span:
        span.set(c='v')
    t.shutdown()
    assert exporter.call_args.kwargs == {'endpoint': 'http://collector:4318/v1/traces'}


def test_git_links_and_worktree_listing(campaign_path: Path, tmp_path: Path) -> None:
    repo = Repo(campaign_path)
    sha = repo.head.commit.hexsha
    wt = tmp_path / 'wt' / 'a'
    links = {'data': campaign_path / 'data', 'missing': campaign_path / 'nope', 'nested/ws': tmp_path / 'ws'}
    (tmp_path / 'ws').mkdir()
    git.add_worktree(repo, wt, sha, links)
    assert (wt / 'data').is_symlink() and (wt / 'nested' / 'ws').is_symlink() and not (wt / 'missing').exists()
    git.add_worktree(repo, tmp_path / 'wt' / 'b', sha, {})
    assert git.worktrees_under(repo, tmp_path / 'wt') == [wt, tmp_path / 'wt' / 'b']
    assert git.worktrees_under(repo, tmp_path / 'elsewhere') == []
    git.remove_worktree(repo, wt)
    git.remove_worktree(repo, wt)  # idempotent
    assert git.worktrees_under(repo, tmp_path / 'wt') == [tmp_path / 'wt' / 'b']


def test_a_worktree_listing_waits_for_a_change_of_the_worktrees(campaign_path: Path, tmp_path: Path) -> None:
    """A ``git worktree list`` that overlaps a removal can fail in a recent git (``Invalid path``), so the listing
    takes the lock that every change of the worktrees holds."""
    repo = Repo(campaign_path)
    with git.WORKTREE_LOCK:  # a removal in flight
        listing = threading.Thread(target=git.worktrees_under, args=(repo, tmp_path))
        listing.start()
        listing.join(0.2)
        assert listing.is_alive(), 'the listing waits for the removal'
    listing.join(5)
    assert not listing.is_alive()


def test_worktrees_under_a_symlinked_root(campaign_path: Path, tmp_path: Path) -> None:
    real = tmp_path / 'real'
    real.mkdir()
    link = tmp_path / 'link'
    link.symlink_to(real)
    repo = Repo(campaign_path)
    git.add_worktree(repo, link / 'wt', repo.head.commit.hexsha, {})
    assert git.worktrees_under(repo, link) == [real / 'wt']


def test_stale_directories_do_not_block_worktrees(campaign_path: Path, tmp_path: Path) -> None:
    repo = Repo(campaign_path)
    sha = repo.head.commit.hexsha
    stale = tmp_path / 'wt' / 'stale'
    stale.mkdir(parents=True)
    (stale / 'leftover').write_text('x')
    git.add_worktree(repo, stale, sha, {})
    assert (stale / 'solve.txt').is_file() and not (stale / 'leftover').exists()
    git.remove_worktree(repo, stale)
    stale.mkdir()
    git.remove_worktree(repo, stale)  # a plain directory, unknown to git
    assert not stale.exists()


def test_handle_for_refuses_a_request() -> None:
    with pytest.raises(ValueError, match='agent_session#3 is requested: not launched yet'):
        handle_for(AgentSession(id=3, kind=RESEARCH, campaign_id=1, state=AgentSessionState.REQUESTED))
