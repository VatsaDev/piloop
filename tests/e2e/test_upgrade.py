"""An upgrade from the last release to this checkout loses nothing, with a run in flight, on a home with real work.

    uv run pytest -m e2e tests/e2e/test_upgrade.py -rA [--e2e-model <model>]

piloop ``RELEASE`` from PyPI runs a small enwik8 campaign: a keep, a discard, a cancelled run, notes, an archive of the
keep's workspace files and a ``research/`` loss table. Its ``piloop.toml`` has a role on the herdr runtime and keys
that this checkout removed. While two agents of the release work, the user upgrades as the upgrading guide says: to
``STEP`` first, with ``piloop restart``, then ``uv tool install --reinstall <this checkout>`` and ``piloop upgrade``.
The upgrade refuses the campaign on herdr and changes nothing; the user moves it off herdr with piloop 0.2, as the
refusal says, and upgrades again. Then every row,
ref, commit and file of the release is still there, the migrations rewrote only what the guide lists (event payloads,
run ends, archive manifests, stored configs), the backup is the database before the migration, the new code cancels
one agent and scores the other's run, the removed keys fail with a link to the guide, and the store restores what
both piloops archived.

A home of 0.1.0 is refused, untouched, with the release to install first.

Claude only: the release does not know the other harnesses' flags of today.
"""

import fnmatch
import importlib
import json
import os
import re
import shutil
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
from covalence import Cid

from piloop.db.models import EVENT, UnknownEvent
from piloop.plugins.runtime.kit.tags import tagged
from piloop.store.tree import DRISL
from tests.conftest import wait_until
from tests.e2e.campaign import CHEAPEST_MODEL, OPEN, ROOT, Smoke, merged, needs
from tests.e2e.conftest import Install, caller_env
from tests.e2e.inventory import Changed, Inventory, Renames, differences, read_database, take

pytestmark = pytest.mark.e2e

RELEASE = '0.2.0'
RELEASE_HEAD = '0018'
"""The schema revision that ``RELEASE`` shipped, and ``STEP`` too."""
STEP_VERSION = '0.2.1'
STEP = f'piloop=={STEP_VERSION}'
"""The release that the guide says to upgrade to first: its daemon keeps running while this checkout is installed."""
RENAMES = Renames(columns={'brief': 'brief_cid'}, blobs=frozenset({('runs', 'brief_cid')}))
"""The renames since ``RELEASE`` that the upgrading guide documents. A rename of a later release goes here."""
CONFIG_REWRITES = (
    *(f'harness.*.{key}' for key in ('interactive', 'startup_dialogs', 'busy_screens', 'failed_turn_screens')),
    'harness.*.queue_key',
    'harness.*.composer',
    'roles.*.workdir',
    'harness.*.entire_agent',
    'roles.*.triggers',
    'stop.*',
    'spend.limit',
    'confidence',
    'plugins.stop_rule.*',
    'plugins.verdict_rule.*',
)
"""The keys of the stored ``piloop.toml`` that a migration may change, as patterns: it deletes the harness keys that
only herdr read (revision 0034), ``workdir`` (0035) and ``entire_agent`` (0036), renames a trigger's ``kind`` to
``provider`` (0038), and moves ``[stop]``, ``[spend] limit`` and ``confidence`` into the rules' tables (0039)."""
DROPPED = {'agent_sessions.checkpoint_id', 'documents.commit_sha'}
"""The columns that this checkout dropped, as the upgrading guide lists them."""
QUEUE = re.compile(r'evaluations#\d+: |home/runs/\d+/eval-\d+/')
"""The evaluation queue, rows and worktrees: a daemon's start empties it, and what still needs a measurement asks
again (``Scheduler.reap``)."""
KIND_REWRITES = {'comment.delivered': 'comment.queued'}
"""The event kinds that a migration renames, besides a role's ``started`` and ``finished`` (0028, 0032)."""
CONFIG = {
    'evaluate_timeout': 300,
    'stop': {'max_runs': 6, 'max_hours': 1, 'max_tokens': 20_000_000},
    'roles': {
        'implementer': {'max_concurrent': 2, 'timeout': 600},
        'research': {'max_ideas': 0, 'workdir': 'head', 'buffer': 1},
        'watchdog': {'timeout': 60, 'interval': 100_000},
        'scout': {'runtime': 'herdr'},
    },
    'harness': {
        'idle': {'executable': 'true', 'headless': ['{session_id}', '{prompt}'], 'entire_agent': 'claude-code'}
    },
}
"""Over the release's enwik8 template: two implementers, no research sweep (``max_ideas = 0``) and no progress review.
Keys that this checkout removed: ``workdir``, ``entire_agent`` of a harness that no role runs, and research's
``buffer`` (deprecated by the release), which the upgrade leaves for the user to delete, and the scout on herdr, which
the upgrade refuses. The scout never runs: it has no ``interval``."""
REMOVED_KEYS = {
    ('roles', 'research', 'workdir'): 'upgrading#workdir',
    ('harness', 'idle', 'entire_agent'): 'upgrading#entire',
    ('roles', 'research', 'buffer'): 'roles.research.buffer',
}
"""Each removed key that the campaign's ``piloop.toml`` still has after the upgrade, and what its failure names, in the
order that ``piloop doctor`` names them: a key whose guard fails hides the other problems of its table."""
HERE = (
    'Work only in your current directory, your git worktree: edit ./compressor.py there and commit there, never in the'
    ' campaign repo that the brief names.'
)
NOTE = 'Then append one line on what you did and the bytes you measured to ./workspace/NOTES.md, and commit.'
WAIT = (
    "First wait for the data: run `python3 -c \"import os, time; [time.sleep(1) for _ in range(60) if os.path.exists('{hold}')];"
    " print('wait' if os.path.exists('{hold}') else 'go')\"` as a call of its own, never in the background, and run it"
    ' again each time it prints wait. Once it prints go,'
)
"""How a plan holds its agent until the test removes ``{hold}``: a sleep in Python, a minute at a time, because Claude
Code moves a long ``sleep`` to the background and goes on."""
IDEAS = {
    'cancelled': ('Wait for the data, then use lzma at preset 0', f'{WAIT} use lzma preset 0.', 0),
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
    f'{HERE} {WAIT} in compressor.py, use lzma.compress(data, preset=2) and lzma.decompress. Run'
    ' `uv run python evaluate.py`, then commit. Change nothing else, and write nothing in ./workspace.',
    0,
)
"""The idea whose agent works through both upgrades, the refused one and the real one: it waits until the test lets
it go, after the upgrade."""
TO_CANCEL = (
    'Wait long for the data, then use lzma at preset 3',
    f'{HERE} {WAIT} use lzma preset 3, measure and commit.',
    0,
)
"""The idea of a second agent of the release, which the new code cancels after the upgrade."""
LIVE = ('NOTES.md', 'research')
"""Workspace entries that the store never archives (``ArchivePolicy.live``)."""
LIVE_STATES = {'requested', 'starting', 'running'}
"""The states of an agent that is not done."""


class Upgrade:
    """The release's campaign, and what the steps learn of it."""

    def __init__(self, smoke: Smoke, root: Path) -> None:
        self.smoke = smoke
        self.root = root
        self.before: Inventory | None = None
        self.after: Inventory | None = None
        self.output = ''
        self.archive: int | None = None
        """The id of the archive that the release wrote, of the keep's workspace files."""
        self.in_flight: int | None = None
        """The id of the run that the upgrade happens in."""
        self.to_cancel: int | None = None
        """The id of the other run in flight, which the new code cancels."""
        self.leftovers: list[str] = []
        """The processes of the cancelled agent that were still alive 30 seconds after the cancel."""

    def transcripts(self) -> list[Path]:
        """The Claude Code session directories of the agents of this campaign: Claude Code names each after its cwd."""
        projects = (
            Path(self.smoke.env.get('CLAUDE_CONFIG_DIR') or Path(self.smoke.env['HOME']) / '.claude') / 'projects'
        )
        return sorted(projects.glob(re.sub(r'[^A-Za-z0-9]', '-', str(self.root)) + '*'))

    def take(self) -> Inventory:
        return take(self.smoke.home, self.smoke.repo, self.transcripts())

    def install(self, requirement: str) -> None:
        """``uv tool install --reinstall <requirement>``, as the user does, into this test's tool directory."""
        subprocess.run(
            ['uv', 'tool', 'install', '--reinstall', '--quiet', '--python', '3.14', requirement],
            env=self.smoke.env,
            check=True,
        )

    def run(self, *args: str) -> subprocess.CompletedProcess[str]:
        """``piloop <args>`` of the installed piloop, without a check: the step reads the exit status."""
        return subprocess.run(['piloop', *args], env=self.smoke.env, capture_output=True, text=True, timeout=300)

    def held(self, plan: str, key: str) -> str:
        """``plan`` with its wait on a hold file of its own, which ``release`` removes. A plan without a wait is as it is."""
        hold = self.root / 'holds' / key.replace(' ', '-')
        hold.parent.mkdir(exist_ok=True)
        hold.touch()
        return plan.format(hold=hold)

    def release(self, key: str) -> None:
        (self.root / 'holds' / key.replace(' ', '-')).unlink()

    @property
    def workspace(self) -> Path:
        return self.smoke.home / 'projects' / '1' / 'workspace'


@pytest.fixture(scope='module')
def upgrade(request: pytest.FixtureRequest, e2e_summary: list[str]) -> Iterator[Upgrade]:
    """Install the release as a uv tool of its own, and start its campaign as its user would."""
    if (harness := request.config.getoption('e2e_harness')) != 'claude':
        pytest.skip(f'the release runs claude only, not {harness}')
    model = request.config.getoption('e2e_model') or CHEAPEST_MODEL['claude']
    root = Path(tempfile.mkdtemp(prefix='piloop-e2e-upgrade-'))
    base = caller_env(root / 'home')
    env = base | {
        'PATH': f'{root / "bin"}{os.pathsep}{base["PATH"]}',
        'UV_TOOL_DIR': str(root / 'uv-tools'),
        'UV_TOOL_BIN_DIR': str(root / 'bin'),
        'PILOOP_AGENT_SKILLS_DIR': str(root / 'skills' / 'agents'),
        'PILOOP_CLAUDE_SKILLS_DIR': str(root / 'skills' / 'claude'),
    }
    smoke = Smoke(root, 'enwik8', env)
    up = Upgrade(smoke, root)
    up.install(f'piloop=={RELEASE}')
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
        smoke.piloop('install-skills')
        smoke.piloop('serve', '--detach')
        wait_until((smoke.home / 'serve.pid').is_file)
        yield up
    finally:
        try:
            smoke.shut_down()
        finally:
            e2e_summary.extend(
                [
                    f'test_upgrade: {RELEASE} → {STEP} → this checkout, model {model},'
                    f' {(time.monotonic() - started) / 60:.1f} minutes',
                    *smoke.summary(),
                ]
            )


def test_the_release_does_real_work(upgrade: Upgrade) -> None:
    """A keep, a discard, a cancelled run, notes, comments, an archive and a loss table, all written by the release."""
    smoke = upgrade.smoke
    for key, (title, plan, priority) in IDEAS.items():
        smoke.add_idea(key, title, upgrade.held(plan, key), priority)
    # the keep's own entry, which the release archives once the idea is done
    (upgrade.workspace / f'idea{smoke.ideas["keep"]}-eval.txt').write_text('METRIC bytes=3342035\n')
    wait_until(lambda: smoke.run_in('cancelled', {'running'}), timeout=180)
    time.sleep(15)  # into its sleep, so the cancel ends an agent in the middle of its work
    cancelled = smoke.run_of('cancelled')
    assert cancelled is not None
    smoke.piloop('run', 'cancel', str(cancelled['id']))
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
    wait_until(lambda: smoke.run_in('cancelled', OPEN, within=False), timeout=120)
    cancelled = smoke.run_of('cancelled')
    assert cancelled is not None and (cancelled['status'], cancelled['error']) == ('failed', 'cancelled'), dict(
        cancelled or {}
    )
    smoke.piloop(
        'sql',
        'INSERT INTO comments (target_kind, target_id, author, kind, body) VALUES'
        f" ('idea', {smoke.ideas['discard']}, 'human', 'note', 'a lower level loses bytes, as expected')",
    )
    smoke.piloop(
        'sql',
        'INSERT INTO comments (target_kind, target_id, author, kind, body, priority) VALUES'
        " ('campaign', 1, 'orchestrator', 'decision', 'decision: lzma before any context model', 3)",
    )
    workspace = upgrade.workspace
    (workspace / 'research').mkdir(parents=True, exist_ok=True)
    (workspace / 'research' / 'loss-table.md').write_text('| idea | bytes |\n|---|---|\n| lzma 1 | see candidate |\n')
    (workspace / 'research' / 'residuals.bin').write_bytes(os.urandom(200_000))
    with (workspace / 'NOTES.md').open('a') as notes:  # the implementers may have written some already
        notes.write('- human: lzma beats zlib here; try the presets next\n')
    # what the store archives after the upgrade, when the campaign stops
    (workspace / 'scratch').mkdir(exist_ok=True)
    (workspace / 'scratch' / 'bulk.bin').write_bytes(os.urandom(300_000))
    archived = 'SELECT id FROM archives WHERE idea_id = ? AND complete AND manifest IS NOT NULL'
    wait_until(lambda: smoke.sql(archived, smoke.ideas['keep']), timeout=300)
    upgrade.archive = smoke.sql(archived, smoke.ideas['keep'])[0]['id']
    log = subprocess.run(['git', 'log', '--format=%s', 'main'], cwd=smoke.repo, capture_output=True, text=True).stdout
    assert log.splitlines() == ['enwik8: baseline'], f'an agent committed in the campaign repo, not its worktree: {log}'
    print(f'verdicts by idea {verdicts}; cancelled run {cancelled["id"]}; archive#{upgrade.archive} of the keep')


@needs(test_the_release_does_real_work)
def test_the_upgrade_refuses_a_campaign_on_herdr_and_changes_nothing(upgrade: Upgrade) -> None:
    """First the step to ``STEP``, which keeps the daemon and its agents. Then the refused upgrade: the daemon keeps
    running ``STEP``, the agents keep working, and nothing is migrated or backed up. Then the user does what the
    refusal says, with piloop 0.2."""
    smoke = upgrade.smoke
    for key, (title, plan, priority) in (('in flight', IN_FLIGHT), ('to cancel', TO_CANCEL)):
        smoke.add_idea(key, title, upgrade.held(plan, key), priority)
    wait_until(lambda: smoke.run_in('in flight', {'running'}) and smoke.run_in('to cancel', {'running'}), timeout=180)
    time.sleep(20)  # the agents are in their sleep
    run, other = smoke.run_in('in flight', {'running'}), smoke.run_in('to cancel', {'running'})
    assert run is not None and other is not None, 'a run in flight ended before the upgrade'
    upgrade.in_flight, upgrade.to_cancel = run['id'], other['id']
    pid = int((smoke.home / 'serve.pid').read_text())
    upgrade.install(STEP)
    smoke.piloop('restart')
    assert smoke.piloop('--version').startswith(f'piloop {STEP_VERSION}')
    assert int((smoke.home / 'serve.pid').read_text()) == pid, 'the restart keeps the daemon process'
    assert smoke.run_in('in flight', {'running'}) and smoke.run_in('to cancel', {'running'}), 'the agents still work'
    upgrade.install(str(ROOT))
    refused = upgrade.run('upgrade')
    assert refused.returncode != 0, refused.stdout
    assert f'daemon: pid {pid} not restarted, and it keeps running the old code' in refused.stderr, refused.stderr
    assert 'work on the herdr runtime' in refused.stderr and 'campaign enwik8' in refused.stderr, refused.stderr
    assert int((smoke.home / 'serve.pid').read_text()) == pid
    [_, revision, _, _] = read_database(smoke.db)
    assert revision == RELEASE_HEAD, 'nothing is migrated'
    assert list(smoke.home.glob('piloop.db.bak-*')) == [], 'nothing is backed up'
    assert smoke.run_in('in flight', {'running'}) and smoke.run_in('to cancel', {'running'}), 'the agents still work'
    # what the refusal says: with piloop 0.2, move the campaign off herdr and re-read its piloop.toml
    upgrade.install(STEP)
    toml = tomllib.loads((smoke.repo / 'piloop.toml').read_text())
    del toml['roles']['scout']['runtime']
    (smoke.repo / 'piloop.toml').write_text(tomli_w.dumps(toml))
    smoke.piloop('init', str(smoke.repo))
    print(refused.stderr.strip())


@needs(test_the_upgrade_refuses_a_campaign_on_herdr_and_changes_nothing)
def test_upgrade_with_an_agent_in_flight(upgrade: Upgrade) -> None:
    """The user's upgrade, as the guide says, while two agents of the release work. Its last step, ``piloop doctor``,
    fails on the keys of ``piloop.toml`` that this checkout removed, and only on those."""
    smoke = upgrade.smoke
    assert smoke.run_in('in flight', {'running'}) is not None, 'the in-flight run ended before the upgrade'
    upgrade.before = upgrade.take()
    pid = int((smoke.home / 'serve.pid').read_text())
    upgrade.install(str(ROOT))
    done = upgrade.run('upgrade')
    upgrade.output = done.stdout
    upgrade.after = upgrade.take()
    lines = done.stdout.splitlines()
    assert f'daemon: pid {pid} runs the new code' in lines, done.stdout + done.stderr
    failed = [line for line in lines if line.startswith('FAIL')]
    assert len(failed) == 1 and 'piloop.toml' in failed[0], done.stdout
    assert next(iter(REMOVED_KEYS.values())) in failed[0] + done.stdout, done.stdout
    assert done.returncode != 0 and 'piloop doctor found problems' in done.stderr, done.stderr
    assert upgrade.before.revision == RELEASE_HEAD and upgrade.after.revision != RELEASE_HEAD
    assert smoke.run_in('in flight', {'running'}) is not None, 'the upgrade ended the in-flight run'
    upgrade.release('in flight')
    print(done.stdout)


@needs(test_upgrade_with_an_agent_in_flight)
def test_nothing_of_the_release_is_lost(upgrade: Upgrade) -> None:
    """Every row, ref, commit and file is still there; a cell differs only by a documented rewrite, or because the
    campaign wrote it: an open row, the stored config's documented rewrite, the files of the agents in flight."""
    assert upgrade.before is not None and upgrade.after is not None
    before, after = upgrade.before, upgrade.after
    found = differences(before, after, RENAMES)
    assert (after.integrity, after.foreign_key_violations) == ('ok', 0)
    open_rows = opened(before)
    unexpected = [c for c in found.changed if not allowed(c, open_rows)]
    assert unexpected == [], '\n'.join(map(str, unexpected))
    live = live_dirs(before)  # an agent at work changes its own directories, and they go when it ends
    files = [f for f in found.files if not any(f'/{n}/' in f or f'-{n.replace("/", "-")}-' in f for n in live)]
    lost = [d for d in found.lost() if d not in found.files and d not in DROPPED] + files
    lost = [d for d in lost if not QUEUE.match(d)]
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
def test_each_run_of_the_release_has_its_end(upgrade: Upgrade) -> None:
    """``runs.end`` of each finished run, from its status and the sentence in its ``error``, which stays."""
    smoke = upgrade.smoke
    ends = {key: (run['status'], run['error'], run['end']) for key in IDEAS if (run := smoke.run_of(key)) is not None}
    assert ends == {
        'cancelled': ('failed', 'cancelled', 'cancelled'),
        'keep': ('succeeded', None, 'finished'),
        'discard': ('succeeded', None, 'finished'),
    }, ends
    unended = smoke.sql('SELECT id FROM runs WHERE "end" IS NULL AND status NOT IN (?, ?)', *OPEN)
    assert unended == [], [dict(r) for r in unended]


@needs(test_upgrade_with_an_agent_in_flight)
def test_the_release_archive_manifest_is_stored_again_as_a_dasl_value(upgrade: Upgrade) -> None:
    """The daemon's start stores the release's JSON manifest again as a DASL value, and points the row at it."""
    smoke = upgrade.smoke

    def manifest() -> Cid:
        return Cid(smoke.sql('SELECT manifest FROM archives WHERE id = ?', upgrade.archive)[0]['manifest'])

    wait_until(lambda: manifest().codec == DRISL, timeout=120)
    print(f'archive#{upgrade.archive}: {manifest()}')


@needs(test_upgrade_with_an_agent_in_flight)
def test_the_new_code_cancels_an_agent_of_the_release(upgrade: Upgrade) -> None:
    """``piloop run cancel`` on the new code ends an agent that the release started.

    The run fails, unless the agent left its finding before the daemon ended it: a finding is the report that the run
    is done, and wins over the cancel. That happens. Claude Code runs the agent's `sleep 900` in the background, and a
    fast agent finishes its idea in about 40 seconds, about when the cancel comes.
    """
    smoke = upgrade.smoke
    assert upgrade.before is not None
    [session] = [r['id'] for r in upgrade.before.tables['agent_sessions'] if r['run_id'] == upgrade.to_cancel]
    marker = f'PILOOP_ACTOR=implementer#{session}'  # what the release put in the environment of its agent
    assert tagged(marker), 'the agent of the release still runs'
    smoke.piloop('run', 'cancel', str(upgrade.to_cancel))
    wait_until(lambda: smoke.run_in('to cancel', OPEN, within=False), timeout=120)
    run = smoke.run_of('to cancel')
    findings = smoke.sql(
        "SELECT created_at FROM comments WHERE kind = 'finding' AND author = ?", f'implementer#{session}'
    )
    [ended] = smoke.sql(
        "SELECT at FROM events WHERE kind = 'agent_session.stop_ended' AND target_kind = 'agent_session'"
        ' AND target_id = ?',
        session,
    )
    # the agent's raw INSERT stamps its finding to the second; one later than the end must have been refused
    assert all(str(f['created_at']) <= str(ended['at']) for f in findings), 'a finding came after the end'
    assert run is not None and run['status'] == ('succeeded' if findings else 'failed'), (run and dict(run), findings)
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
    finished = 'after its finding' if findings else 'cancelled'
    print(f'run {run["id"]} {run["status"]}, {finished}; processes of implementer#{session} left: {upgrade.leftovers}')


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
    print(
        f'run {upgrade.in_flight} {run["status"]}: ' + ', '.join(f'candidate#{r["id"]} {r["verdict"]}' for r in judged)
    )


@needs(test_the_run_in_flight_is_scored_by_the_new_code)
def test_the_store_restores_what_both_piloops_archived(upgrade: Upgrade) -> None:
    """Stopping the campaign archives what is left; ``piloop restore`` brings back every file that the release and the
    new code archived, byte for byte, the release's through its manifest stored again."""
    smoke = upgrade.smoke
    smoke.stop_campaign('e2e: archive the workspace')
    wait_until(lambda: smoke.sql('SELECT id FROM archives WHERE idea_id IS NULL AND complete'), timeout=600)
    verified = smoke.piloop('store', 'verify').strip()
    assert re.search(r'(\d+) blob\(s\): \1 ok, 0 missing, 0 corrupt$', verified), verified
    workspace = upgrade.workspace
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
def test_the_removed_keys_load_once_the_user_deletes_them(upgrade: Upgrade) -> None:
    """The upgrade leaves ``piloop.toml`` as it was. ``piloop doctor`` names each removed key, one at a time; without
    them the file loads, and ``piloop init`` re-reads it."""
    smoke = upgrade.smoke
    assert upgrade.before is not None and upgrade.after is not None
    assert upgrade.after.files['repo/piloop.toml'] == upgrade.before.files['repo/piloop.toml']
    for key, named in REMOVED_KEYS.items():
        proc = upgrade.run('doctor')
        # a warning, not a failure, once the campaign is stopped: the daemon reads the file only at `piloop init`
        found = [line for line in proc.stdout.splitlines() if line.startswith(('FAIL', 'WARN'))]
        assert len(found) == 1 and named in proc.stdout, proc.stdout
        toml = tomllib.loads((smoke.repo / 'piloop.toml').read_text())
        *tables, last = key
        table = toml
        for name in tables:
            table = table[name]
        del table[last]
        (smoke.repo / 'piloop.toml').write_text(tomli_w.dumps(toml))
        print(f'{key}: {next(line.strip() for line in proc.stdout.splitlines() if named in line)[:200]}')
    smoke.piloop('init', str(smoke.repo))
    proc = upgrade.run('doctor')
    assert [line for line in proc.stdout.splitlines() if line.startswith('FAIL')] == [], proc.stdout
    assert 'campaign enwik8: ' in proc.stdout and 'piloop.toml loads' in proc.stdout
    warnings = [line for line in proc.stderr.splitlines() if 'Warning' in line]
    assert all('PiloopDeprecationWarning' in w for w in warnings), proc.stderr  # the [stop] of 0.2, until 0.4.0


@needs(test_upgrade_with_an_agent_in_flight)
def test_no_process_of_the_release_is_left(upgrade: Upgrade) -> None:
    """``piloop stop`` on the new code ends every process of the campaign, those that the release started too."""
    smoke = upgrade.smoke
    smoke.piloop('stop', '--timeout', '60')
    try:
        wait_until(lambda: not smoke.processes(), timeout=10)
    except AssertionError as e:
        raise AssertionError(f'processes left: {smoke.processes()}') from e


def test_a_home_of_0_1_0_is_refused_and_left_as_it_was(piloop_install: Install, tmp_path: Path) -> None:
    """This piloop migrates from ``RELEASE``'s schema only: it names the release to install first, and changes
    nothing, on a database that piloop 0.1.0 created."""
    home = tmp_path / 'home'
    env = piloop_install.env(home) | {
        'UV_TOOL_DIR': str(tmp_path / 'uv-tools'),
        'UV_TOOL_BIN_DIR': str(tmp_path / 'bin'),
    }
    subprocess.run(['uv', 'tool', 'install', '--quiet', '--python', '3.14', 'piloop==0.1.0'], env=env, check=True)
    subprocess.run([str(tmp_path / 'bin' / 'piloop'), 'migrate'], env=env, check=True, capture_output=True)
    before = (home / 'piloop.db').read_bytes()
    proc = subprocess.run(['piloop', 'migrate'], env=env, capture_output=True, text=True, timeout=120)
    assert proc.returncode != 0
    assert 'is from piloop 0.1 (schema revision 0001)' in proc.stderr, proc.stderr
    assert "install piloop 0.2 (`uv tool install 'piloop==0.2.*'`)" in proc.stderr, proc.stderr
    assert (home / 'piloop.db').read_bytes() == before
    assert list(home.glob('piloop.db.bak-*')) == []


def live_dirs(inventory: Inventory) -> list[str]:
    """``runs/<id>`` and ``agent_sessions/<id>`` of every agent at work in ``inventory``: its files under the home and
    its transcripts, by these names."""
    sessions = inventory.tables['agent_sessions']
    return [f'agent_sessions/{r["id"]}' for r in sessions if r['state'] in LIVE_STATES and r['run_id'] is None] + [
        f'runs/{r["run_id"]}' for r in sessions if r['state'] in LIVE_STATES and r['run_id'] is not None
    ]


def idea_status(smoke: Smoke, key: str) -> str:
    return smoke.sql('SELECT status FROM ideas WHERE id = ?', smoke.ideas[key])[0]['status']


def opened(inventory: Inventory) -> set[tuple[str, object]]:
    """``(table, rowid)`` of the rows that the campaign still changes: open runs, live agents, scheduled ideas, pending
    candidates, and the campaign row."""
    rows = {('campaigns', r['__rowid']) for r in inventory.tables['campaigns']}
    for table, column, values in (
        ('runs', 'status', OPEN),
        ('agent_sessions', 'state', LIVE_STATES),
        ('ideas', 'status', {'scheduled'}),
        ('candidates', 'verdict', {'pending'}),
    ):
        rows |= {(table, r['__rowid']) for r in inventory.tables[table] if r[column] in values}
    return rows


def allowed(change: Changed, open_rows: set[tuple[str, object]]) -> bool:
    """A change of a row the campaign still changes; for the campaign row, only its time, signer and the documented
    rewrite of its stored config; for an event, the documented rewrite to its model; for an archive, its manifest's
    address as a CID."""
    match change.table, change.column:
        case 'campaigns', 'config':
            return all(
                any(fnmatch.fnmatch(k, p) for p in CONFIG_REWRITES) for k in config_keys(change.before, change.after)
            )
        case 'campaigns', column:
            return column in {'updated_at', 'updated_by'}
        case 'events', _:
            return typed(change)
        case 'archives', 'manifest':
            return is_cid(str(change.after))
        case table, _:
            return (table, change.rowid) in open_rows


def is_cid(text: str) -> bool:
    try:
        Cid(text)
    except ValueError:
        return False
    return True


def typed(change: Changed) -> bool:
    """An event that the migrations rewrote: a role's own events are the session's, the payload loads as its model,
    and the renamed kinds (``KIND_REWRITES``).

    ``tests/db/test_event_history.py`` checks each rewrite. This checks that the rows of a real campaign load, and that
    a kind that nothing registers only gained its ``kind``.
    """
    importlib.import_module('piloop.daemon.scheduler')  # registers the events of every layer
    before, after = str(change.before), str(change.after)
    match change.column:
        case 'kind':
            role_event = before.rpartition('.')[2] in {'started', 'finished'}
            return after == KIND_REWRITES.get(before) or (
                role_event and after == f'agent_session.{before.rpartition(".")[2]}'
            )
        case 'payload':
            event = EVENT.validate_json(after)
            return not isinstance(event, UnknownEvent) or json.loads(after) == {
                **json.loads(before),
                'kind': event.kind,
            }
        case _:
            return False


def config_keys(before: object, after: object) -> set[str]:
    """The dotted keys whose values differ between two stored configs."""

    def flat(value: object, prefix: str = '') -> dict[str, object]:
        if isinstance(value, Mapping):
            return {k: v for key, item in value.items() for k, v in flat(item, f'{prefix}{key}.').items()}
        return {prefix.removesuffix('.'): value}

    old, new = (flat(json.loads(str(c))) for c in (before, after))
    return {k for k in old.keys() | new.keys() if old.get(k, ...) != new.get(k, ...)}
