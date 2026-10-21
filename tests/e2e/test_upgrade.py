"""An upgrade from the last release to this checkout loses nothing, with a run in flight, on a home with real work.

    uv run pytest -m e2e tests/e2e/test_upgrade.py -rA [--e2e-model <model>]

piloop ``RELEASE`` from PyPI runs a small enwik8 campaign: a keep, a discard, a run whose agent is killed (0.1.0 has
no ``run cancel``), notes and a ``research/`` loss table. While two agents of the release work, the user upgrades as
the upgrading guide says: ``uv tool install --reinstall <this checkout>``, then ``piloop upgrade``. Then every row,
ref, commit and file of the release is still there, in the new names (``RENAMES``), the backup is the database
before the migration, the new code cancels one agent and scores the other's run, and the store restores what it
archived.

Claude only: the release does not know the other harnesses' flags of today. About 6 minutes and 0.5 USD.
"""

import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import tempfile
import time
import tomllib
from collections.abc import Iterator, Mapping
from contextlib import suppress
from pathlib import Path

import psutil
import pytest
import tomli_w

from piloop.runtime.tags import tagged
from tests.conftest import wait_until
from tests.e2e.campaign import CHEAPEST_MODEL, OPEN, ROOT, Smoke, merged, needs
from tests.e2e.conftest import DROPPED_ENV
from tests.e2e.inventory import Changed, Inventory, Renames, differences, read_database, rows_differences, take

pytestmark = pytest.mark.e2e

RELEASE = '0.1.0'
RENAMES = Renames(
    tables={'projects': 'campaigns', 'workers': 'agent_sessions'},
    columns={'project_id': 'campaign_id'},
    target_kinds={'project': 'campaign', 'worker': 'agent_session'},
    # a role's own events take the role's new name, a session's the new target kind
    event_prefixes={
        'project.': 'campaign.',
        'worker.started': 'implementer.started',
        'worker.finished': 'implementer.finished',
        'worker.': 'agent_session.',
    },
    values={('agent_sessions', 'kind', 'worker'): 'implementer', ('runs', 'role', 'worker'): 'implementer'},
    roles={'worker': 'implementer'},
    payload_keys={'worker': 'agent_session'},
)
"""The renames since ``RELEASE`` that the upgrading guide documents. A rename of a later release goes here."""
CONFIG_REWRITES = {'roles.research.buffer', 'roles.research.context', 'roles.research.scout', 'context'}
"""The keys of the stored ``piloop.toml`` that a migration may move: ``buffer`` and ``context`` of research and its
``scout`` table become a trigger parameter, the top-level ``context`` and the ``scout`` role (revision 0011)."""
CONFIG = {
    'evaluate_timeout': 300,
    'stop': {'max_runs': 6, 'max_hours': 1, 'max_tokens': 20_000_000},
    'roles': {
        'worker': {'max_concurrent': 2, 'timeout': 600},
        'research': {'max_ideas': 0},
        'watchdog': {'timeout': 60, 'interval': 100_000},
    },
}
"""Over the release's enwik8 template: two workers, no research sweep (``max_ideas = 0``), a short watchdog (the
killed agent starts one) and no progress review."""
HERE = (
    'Work only in your current directory, your git worktree: edit ./compressor.py there and commit there, never in the'
    ' project repo that the brief names.'
)
"""0.1.0's brief names the project repo without saying it is read-only, and a small model then works there."""
NOTE = 'Then append one line on what you did and the bytes you measured to ./workspace/NOTES.md, and commit.'
IDEAS = {
    'killed': ('Wait for the data, then use lzma at preset 0', 'Run `sleep 600` first. Then use lzma preset 0.', 0),
    'keep': (
        'Replace zlib with lzma at preset 1',
        f'{HERE} In compressor.py, compress with lzma.compress(data, preset=1) and decompress with lzma.decompress.'
        f' Run `uv run python evaluate.py`. Change nothing else. {NOTE}',
        1,
    ),
    'discard': (
        'Lower the zlib level to 1',
        f'{HERE} In compressor.py, change the zlib level from 9 to 1. Run `uv run python evaluate.py`. Change nothing'
        f' else. {NOTE}',
        2,
    ),
}
IN_FLIGHT = (
    'Wait for the data, then use lzma at preset 2',
    f'{HERE} Run `sleep 150` first and wait for it to end: the data is being refreshed. Then, in compressor.py, use'
    ' lzma.compress(data, preset=2) and lzma.decompress. Run `uv run python evaluate.py`, then commit. Change nothing'
    ' else, and write nothing in ./workspace.',
    0,
)
"""The idea whose agent works through the upgrade: it waits in ``sleep``, so the upgrade happens while it runs."""
TO_CANCEL = (
    'Wait long for the data, then use lzma at preset 3',
    f'{HERE} Run `sleep 900` first and wait for it to end. Then use lzma preset 3, measure and commit.',
    0,
)
"""The idea of a second agent of the release, which the new code cancels after the upgrade: its kill finds the
release's processes by the names of the release."""
LIVE = ('NOTES.md', 'research')
LIVE_STATES = {'requested', 'starting', 'running'}
"""The states of an agent that is not done."""
"""Workspace entries that the store never archives (``ArchivePolicy.live``)."""


class Upgrade:
    """The release's campaign, and what the steps learn of it."""

    def __init__(self, smoke: Smoke, root: Path) -> None:
        self.smoke = smoke
        self.root = root
        self.before: Inventory | None = None
        self.after: Inventory | None = None
        self.output = ''
        self.in_flight: int | None = None
        """The id of the run that the upgrade happens in."""
        self.to_cancel: int | None = None
        """The id of the other run in flight, which the new code cancels."""
        self.leftovers: list[str] = []
        """The processes of the cancelled agent that were still alive 30 seconds after the cancel."""

    def transcripts(self) -> list[Path]:
        """The Claude Code session directories of the agents of this campaign: Claude Code names each after its cwd."""
        projects = Path(os.environ.get('CLAUDE_CONFIG_DIR', Path.home() / '.claude')) / 'projects'
        return sorted(projects.glob(re.sub(r'[^A-Za-z0-9]', '-', str(self.root)) + '*'))

    def take(self) -> Inventory:
        return take(self.smoke.home, self.smoke.repo, self.transcripts())


@pytest.fixture(scope='module')
def upgrade(request: pytest.FixtureRequest, e2e_summary: list[str]) -> Iterator[Upgrade]:
    """Install the release as a uv tool of its own, and start its campaign as its user would."""
    if (harness := request.config.getoption('e2e_harness')) != 'claude':
        pytest.skip(f'the release runs claude only, not {harness}')
    model = request.config.getoption('e2e_model') or CHEAPEST_MODEL['claude']
    root = Path(tempfile.mkdtemp(prefix='piloop-e2e-upgrade-'))
    base = {k: v for k, v in os.environ.items() if not DROPPED_ENV.match(k)}
    env = base | {
        'PATH': f'{root / "bin"}{os.pathsep}{base["PATH"]}',
        'UV_TOOL_DIR': str(root / 'uv-tools'),
        'UV_TOOL_BIN_DIR': str(root / 'bin'),
        'PILOOP_HOME': str(root / 'home'),
        'PILOOP_AGENT_SKILLS_DIR': str(root / 'skills' / 'agents'),
        'PILOOP_CLAUDE_SKILLS_DIR': str(root / 'skills' / 'claude'),
    }
    subprocess.run(['uv', 'tool', 'install', '--quiet', '--python', '3.14', f'piloop=={RELEASE}'], env=env, check=True)
    smoke = Smoke(root, 'enwik8', env)
    up = Upgrade(smoke, root)
    started = time.monotonic()
    try:
        assert smoke.piloop('--version').strip() == f'piloop {RELEASE}'
        tool_python = root / 'uv-tools' / 'piloop' / 'bin' / 'python'
        templates = subprocess.run(
            [
                str(tool_python),
                '-I',  # the release's piloop: in the cwd, the checkout's would come first on sys.path
                '-c',
                'import piloop.campaign, pathlib; print(pathlib.Path(piloop.campaign.__file__).parent)',
            ],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
        shutil.copytree(Path(templates) / 'templates' / 'enwik8', smoke.repo)
        toml = merged(
            merged(tomllib.loads((smoke.repo / 'piloop.toml').read_text()), CONFIG), {'model': {'claude': model}}
        )
        (smoke.repo / 'piloop.toml').write_text(tomli_w.dumps(toml))
        smoke.piloop('init', str(smoke.repo), '--example', 'enwik8')
        # The release installs Claude Code's skills into CLAUDE_CONFIG_DIR/skills (it has no PILOOP_CLAUDE_SKILLS_DIR):
        # a directory of this test for that one command, so that the real one stays untouched and the agents log in.
        claude = Smoke(root, smoke.name, env | {'CLAUDE_CONFIG_DIR': str(root / 'skills' / 'release-claude')})
        claude.piloop('install-skills')
        smoke.piloop('serve', '--detach')
        wait_until((smoke.home / 'serve.pid').is_file)
        yield up
    finally:
        try:
            smoke.shut_down()
        finally:
            e2e_summary.extend(
                [
                    f'test_upgrade: {RELEASE} → this checkout, model {model}, {(time.monotonic() - started) / 60:.1f} minutes',
                    *smoke.summary(),
                ]
            )


def test_the_release_does_real_work(upgrade: Upgrade) -> None:
    """A keep, a discard, a killed agent, notes, comments and a loss table, all written by the release."""
    smoke = upgrade.smoke
    for key, (title, plan, priority) in IDEAS.items():
        smoke.add_idea(key, title, plan, priority)
    wait_until(lambda: smoke.run_in('killed', {'running'}), timeout=180)
    time.sleep(15)  # into its sleep, so the kill ends an agent in the middle of its work
    [pgid] = [
        r['pgid']
        for r in smoke.sql(
            "SELECT json_extract(w.handle, '$.pgid') AS pgid FROM workers w JOIN runs r ON r.id = w.run_id"
            ' WHERE r.idea_id = ?',
            smoke.ideas['killed'],
        )
    ]
    os.killpg(pgid, signal.SIGTERM)
    smoke.piloop('sql', f"UPDATE ideas SET status = 'parked' WHERE id = {smoke.ideas['killed']}")
    for key in ('keep', 'discard'):
        # the release scores a run's commits after the run ends, and only then is its idea no longer scheduled
        wait_until(lambda key=key: idea_status(smoke, key) not in {'ready', 'scheduled'}, timeout=900)
    verdicts = {
        r['idea_id']: r['verdicts']
        for r in smoke.sql(
            'SELECT r.idea_id, group_concat(c.verdict) AS verdicts FROM candidates c JOIN runs r ON r.id = c.run_id'
            ' GROUP BY r.idea_id'
        )
    }
    assert 'keep' in verdicts[smoke.ideas['keep']] and 'discard' in verdicts[smoke.ideas['discard']], verdicts
    killed = smoke.run_of('killed')
    assert killed is not None and killed['status'] == 'failed', killed and dict(killed)
    smoke.piloop(
        'sql',
        'INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES'
        f" ('idea', {smoke.ideas['discard']}, 'human', 'note', 'a lower level loses bytes, as expected')",
    )
    smoke.piloop(
        'sql',
        'INSERT INTO comments (target_kind, target_id, author, kind, body, priority) VALUES'
        " ('project', 1, 'orchestrator', 'decision', 'decision: lzma before any context model', 3)",
    )
    workspace = smoke.home / 'projects' / '1' / 'workspace'
    (workspace / 'research').mkdir(parents=True, exist_ok=True)
    (workspace / 'research' / 'loss-table.md').write_text('| idea | bytes |\n|---|---|\n| lzma 1 | see candidate |\n')
    (workspace / 'research' / 'residuals.bin').write_bytes(os.urandom(200_000))
    with (workspace / 'NOTES.md').open('a') as notes:  # the workers may have written some already
        notes.write('- human: lzma beats zlib here; try the presets next\n')
    # what the store archives after the upgrade: an idea's own entry when the idea is done, the rest when it stops
    (workspace / f'idea{smoke.ideas["keep"]}-eval.txt').write_text('METRIC bytes=3342035\n')
    (workspace / 'scratch').mkdir(exist_ok=True)
    (workspace / 'scratch' / 'bulk.bin').write_bytes(os.urandom(300_000))
    log = subprocess.run(['git', 'log', '--format=%s', 'main'], cwd=smoke.repo, capture_output=True, text=True).stdout
    assert log.splitlines() == ['enwik8: baseline'], f'an agent committed in the project repo, not its worktree: {log}'
    print(f'verdicts by idea {verdicts}; killed run {killed["id"]}: {killed["error"]}')


@needs(test_the_release_does_real_work)
def test_upgrade_with_an_agent_in_flight(upgrade: Upgrade) -> None:
    """The user's upgrade, as the guide says, while an agent of the release works."""
    smoke = upgrade.smoke
    smoke.add_idea('in flight', *IN_FLIGHT)
    smoke.add_idea('to cancel', *TO_CANCEL)
    wait_until(lambda: smoke.run_in('in flight', {'running'}) and smoke.run_in('to cancel', {'running'}), timeout=180)
    time.sleep(20)  # the agents are in their sleep
    run, other = smoke.run_in('in flight', {'running'}), smoke.run_in('to cancel', {'running'})
    assert run is not None and other is not None, 'a run in flight ended before the upgrade'
    upgrade.in_flight, upgrade.to_cancel = run['id'], other['id']
    upgrade.before = upgrade.take()
    pid = int((smoke.home / 'serve.pid').read_text())
    install = subprocess.run(
        ['uv', 'tool', 'install', '--reinstall', '--quiet', '--python', '3.14', str(ROOT)],
        env=smoke.env,
        capture_output=True,
        text=True,
    )
    assert install.returncode == 0, install.stderr
    upgrade.output = smoke.piloop('upgrade', timeout=300)
    upgrade.after = upgrade.take()
    lines = upgrade.output.splitlines()
    assert f'daemon: pid {pid} runs the new code' in lines, upgrade.output
    assert [line for line in lines if line.startswith('FAIL')] == [], upgrade.output
    assert upgrade.before.revision == '0001' and upgrade.after.revision != '0001'
    assert smoke.run_in('in flight', {'running'}) is not None, 'the upgrade ended the in-flight run'
    print(upgrade.output)


@needs(test_upgrade_with_an_agent_in_flight)
def test_nothing_of_the_release_is_lost(upgrade: Upgrade) -> None:
    """Every row, ref, commit and file is still there; a cell differs only by a documented rename, or because the
    campaign wrote it: an open row, the stored config's documented rewrite, the files of the agent in flight."""
    assert upgrade.before is not None and upgrade.after is not None
    before, after = upgrade.before, upgrade.after
    found = differences(before, after, RENAMES)
    assert (after.integrity, after.foreign_key_violations) == ('ok', 0)
    open_rows = opened(before)
    unexpected = [c for c in found.changed if not allowed(c, open_rows)]
    assert unexpected == [], '\n'.join(map(str, unexpected))
    live = live_dirs(before)  # an agent at work changes its own directories, and they go when it ends
    files = [f for f in found.files if not any(f'/{n}/' in f or f'-{n.replace("/", "-")}-' in f for n in live)]
    lost = [d for d in found.lost() if d not in found.files] + files
    assert lost == [], '\n'.join(lost)
    candidates = {r['commit_sha'] for r in before.tables['candidates']}
    assert candidates <= after.commits, 'every candidate commit is still in the repo'
    worktree = next(w for p, w in before.worktrees.items() if p.endswith(f'runs/{upgrade.in_flight}/worktree'))
    kept = after.worktrees.get(worktree.path)
    assert kept is not None and kept.head is not None and worktree.head is not None, after.worktrees
    ancestor = subprocess.run(['git', 'merge-base', '--is-ancestor', worktree.head, kept.head], cwd=upgrade.smoke.repo)
    assert ancestor.returncode == 0, "the in-flight run's worktree stays, at its commit or one its agent made since"
    assert after.head == before.head
    rows = {t: len(r) for t, r in before.tables.items()}
    print(
        f'{sum(rows.values())} rows {rows}, {len(before.refs)} refs, {len(before.commits)} commits,'
        f' {len(before.files)} files: none lost; {len(found.changed)} cells changed, each allowed;'
        f' {len(found.files) - len(files)} files of the agents at work ({", ".join(live)}) changed or went'
    )


@needs(test_upgrade_with_an_agent_in_flight)
def test_the_backup_is_the_database_before_the_migration(upgrade: Upgrade) -> None:
    assert upgrade.before is not None
    [backup] = upgrade.smoke.home.glob(f'piloop.db.bak-{upgrade.before.revision}-*')
    assert f': {backup}' in upgrade.output, 'the upgrade says where the backup is'
    tables, revision, integrity, _ = read_database(backup)
    assert (revision, integrity) == (upgrade.before.revision, 'ok')
    found = rows_differences(upgrade.before.tables, tables, Renames())
    open_rows = opened(upgrade.before, Renames())
    assert found.lost() == [], 'the backup was taken after the snapshot, so it has every row of it'
    assert [c for c in found.changed if (c.table, c.rowid) not in open_rows] == []
    print(f'{backup.name}: {backup.stat().st_size} bytes, revision {revision}')


@needs(test_upgrade_with_an_agent_in_flight)
def test_the_new_code_cancels_an_agent_of_the_release(upgrade: Upgrade) -> None:
    """``piloop run cancel`` on the new code ends an agent that the release started."""
    smoke = upgrade.smoke
    assert upgrade.before is not None
    [session] = [r['id'] for r in upgrade.before.tables['workers'] if r['run_id'] == upgrade.to_cancel]
    marker = f'PILOOP_ACTOR=worker#{session}'  # what the release put in the environment of its agent
    assert tagged(marker), 'the agent of the release still runs'
    smoke.piloop('run', 'cancel', str(upgrade.to_cancel))
    wait_until(lambda: smoke.run_in('to cancel', OPEN, within=False), timeout=120)
    run = smoke.run_of('to cancel')
    assert run is not None and run['status'] == 'failed', run and dict(run)
    home = f'PILOOP_HOME={smoke.home}'

    def left() -> list[str]:
        """The processes of the cancelled agent: its signature and this home, so another campaign's never match."""
        ours = {pid for pid, _, _ in tagged(home)}
        return [f'{pid} {cmd[:100]}' for pid, _, cmd in tagged(marker) if pid in ours]

    with suppress(AssertionError):
        wait_until(lambda: not left(), timeout=30)
    upgrade.leftovers = left()
    for line in upgrade.leftovers:  # so that the steps after this one see only their own processes
        with suppress(psutil.Error):
            psutil.Process(int(line.split()[0])).kill()
    print(f'run {run["id"]} {run["status"]} ({run["error"]}); processes of worker#{session} left: {upgrade.leftovers}')


@needs(test_the_new_code_cancels_an_agent_of_the_release)
def test_the_cancel_ends_every_process_of_the_release_agent(upgrade: Upgrade) -> None:
    """Its tool calls run in sessions of their own (Claude Code's Bash), outside the process group of the agent."""
    assert upgrade.leftovers == []


@needs(test_upgrade_with_an_agent_in_flight)
def test_the_run_in_flight_is_scored_by_the_new_code(upgrade: Upgrade) -> None:
    smoke = upgrade.smoke

    def done() -> sqlite3.Row | None:
        rows = smoke.sql(
            'SELECT * FROM runs WHERE id = ? AND status NOT IN (?, ?) AND NOT scoring', upgrade.in_flight, *OPEN
        )
        return rows[0] if rows else None

    wait_until(done, timeout=600)
    run = done()
    assert run is not None and run['status'] == 'succeeded', run and dict(run)
    judged = smoke.sql('SELECT id, verdict FROM candidates WHERE run_id = ?', upgrade.in_flight)
    assert judged and all(r['verdict'] != 'pending' for r in judged), [dict(r) for r in judged]
    # What the agent wrote before the upgrade is renamed. What it writes after keeps the signature that the release
    # gave it, `worker#<id>`, which piloop reads as the implementer's until 0.3.0 (piloop.deprecated.signatures).
    assert upgrade.before is not None
    [session] = [r['id'] for r in upgrade.before.tables['workers'] if r['run_id'] == upgrade.in_flight]
    signed = {r['author'] for r in smoke.sql('SELECT author FROM comments WHERE author LIKE ?', f'%#{session}')}
    assert signed <= {RENAMES.value('comments', 'author', f'worker#{session}'), f'worker#{session}'}, signed
    print(
        f'run {upgrade.in_flight} {run["status"]}: '
        + ', '.join(f'candidate#{r["id"]} {r["verdict"]}' for r in judged)
        + f'; its agent signed {sorted(signed) or "nothing"}'
    )


@needs(test_the_run_in_flight_is_scored_by_the_new_code)
def test_the_store_archives_the_release_workspace_and_restores_it(upgrade: Upgrade) -> None:
    """Stopping the campaign archives what is left; ``piloop restore`` brings every archived file back, byte for byte."""
    smoke = upgrade.smoke
    smoke.stop_campaign('e2e: archive the workspace')
    wait_until(lambda: smoke.sql('SELECT id FROM archives WHERE idea_id IS NULL AND complete'), timeout=600)
    verified = smoke.piloop('store', 'verify').strip()
    assert re.search(r'(\d+) blob\(s\): \1 ok, 0 missing, 0 corrupt$', verified), verified
    workspace = smoke.home / 'projects' / '1' / 'workspace'
    aside = smoke.dir / 'workspace-aside'
    workspace.rename(aside)
    workspace.mkdir()
    for name in LIVE:  # never archived: they stay where they are
        if (aside / name).exists():
            shutil.move(aside / name, workspace / name)
    smoke.piloop('restore', smoke.name)
    archived = {str(p.relative_to(aside)): p.read_bytes() for p in aside.rglob('*') if p.is_file()}
    restored = {str(p.relative_to(workspace)): p.read_bytes() for p in workspace.rglob('*') if p.is_file()}
    assert {f'idea{smoke.ideas["keep"]}-eval.txt', 'scratch/bulk.bin'} <= set(archived), sorted(archived)
    assert {k: restored.get(k) for k in archived} == archived
    print(f'{verified}; restored {len(archived)} file(s) byte for byte')


@needs(test_upgrade_with_an_agent_in_flight)
def test_the_release_piloop_toml_is_unchanged_and_loads(upgrade: Upgrade) -> None:
    assert upgrade.before is not None and upgrade.after is not None
    assert upgrade.after.files['repo/piloop.toml'] == upgrade.before.files['repo/piloop.toml']
    proc = subprocess.run(['piloop', 'doctor'], env=upgrade.smoke.env, capture_output=True, text=True, timeout=300)
    assert [line for line in proc.stdout.splitlines() if line.startswith('FAIL')] == [], proc.stdout
    assert 'campaign enwik8: ' in proc.stdout and 'piloop.toml loads' in proc.stdout
    warnings = [line for line in proc.stderr.splitlines() if 'Warning' in line]
    assert all('PiloopDeprecationWarning' in w for w in warnings), proc.stderr
    print(f'{len(warnings)} deprecation warning(s)')


@needs(test_upgrade_with_an_agent_in_flight)
def test_no_process_of_the_release_is_left(upgrade: Upgrade) -> None:
    """``piloop stop`` on the new code ends every process of the campaign, those that the release started too."""
    smoke = upgrade.smoke
    smoke.piloop('stop', '--timeout', '60')
    try:
        wait_until(lambda: not smoke.processes(), timeout=10)
    except AssertionError as e:
        raise AssertionError(f'processes left: {smoke.processes()}') from e


def live_dirs(inventory: Inventory) -> list[str]:
    """``runs/<id>`` and ``workers/<id>`` of every agent at work in ``inventory``: its files under the home and its
    transcripts, by these names."""
    workers = inventory.tables['workers']
    return [f'workers/{r["id"]}' for r in workers if r['state'] in LIVE_STATES and r['run_id'] is None] + [
        f'runs/{r["run_id"]}' for r in workers if r['state'] in LIVE_STATES and r['run_id'] is not None
    ]


def idea_status(smoke: Smoke, key: str) -> str:
    return smoke.sql('SELECT status FROM ideas WHERE id = ?', smoke.ideas[key])[0]['status']


def opened(inventory: Inventory, renames: Renames = RENAMES) -> set[tuple[str, object]]:
    """``(table, rowid)`` of the rows that the campaign still changes, in the names of ``renames``: open runs, live
    agents, scheduled ideas, pending candidates, and the campaign row."""
    rows = {(renames.tables.get('projects', 'projects'), r['__rowid']) for r in inventory.tables['projects']}
    for table, column, values in (
        ('runs', 'status', OPEN),
        ('workers', 'state', LIVE_STATES),
        ('ideas', 'status', {'scheduled'}),
        ('candidates', 'verdict', {'pending'}),
    ):
        new = renames.tables.get(table, table)
        rows |= {(new, r['__rowid']) for r in inventory.tables[table] if r[column] in values}
    return rows


def allowed(change: Changed, open_rows: set[tuple[str, object]]) -> bool:
    """A change of a row the campaign still changes; for the campaign row, only its time, signer and the documented
    rewrite of its stored config."""
    if change.table == 'campaigns':
        if change.column == 'config':
            return config_keys(change.before, change.after) <= CONFIG_REWRITES
        return change.column in {'updated_at', 'updated_by'}
    return (change.table, change.rowid) in open_rows


def config_keys(before: object, after: object) -> set[str]:
    """The dotted keys whose values differ between two stored configs."""

    def flat(value: object, prefix: str = '') -> dict[str, object]:
        if isinstance(value, Mapping):
            return {k: v for key, item in value.items() for k, v in flat(item, f'{prefix}{key}.').items()}
        return {prefix.removesuffix('.'): value}

    old, new = (flat(json.loads(str(c))) for c in (before, after))
    return {k for k in old.keys() | new.keys() if old.get(k, ...) != new.get(k, ...)}
