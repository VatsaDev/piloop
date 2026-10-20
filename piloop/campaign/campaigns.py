"""Create and register campaign repos: ``piloop init``, ``piloop campaign add`` and the bundled examples."""

import re
import shutil
import subprocess
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

import httpx2 as httpx
from git import Repo
from git.exc import InvalidGitRepositoryError, NoSuchPathError
from rich.console import Console
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop import git
from piloop.agents.capture import ENTIRE_DIR
from piloop.config import CONFIG_FILE, CampaignConfig, load_campaign_config
from piloop.db.events import emit
from piloop.db.models import Campaign, CampaignStatus, Measurement, TargetKind, Verdict
from piloop.proc import child_env
from piloop.scoring.evaluate import run_evaluate
from piloop.scoring.ledger import DAEMON, current_evaluator, resolve, save_units
from piloop.settings import settings

console = Console(stderr=True)

WORKSPACE = 'workspace'
"""A per-campaign directory, linked into every worktree, for files that runs share. It is never committed."""
GITIGNORE = 'data\n.venv\nworkspace\n__pycache__/\n'
TEMPLATES = Path(__file__).parent / 'templates'
BLANK = TEMPLATES / 'campaign'
"""The template ``piloop init`` writes into a repo without a ``piloop.toml``."""
EVALUATE_STUB = 'evaluate.py'
NAME_PLACEHOLDER = 'my-campaign'
"""The ``name`` in the blank template's ``piloop.toml``. ``piloop init`` replaces it with the directory name."""
EXAMPLE_DATA: dict[str, dict[str, str]] = {
    'enwik8': {'data/enwik8.zip': 'https://mattmahoney.net/dc/enwik8.zip'},
    'titanic-tracks': {
        'data/train.csv': 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv'
    },
}
"""Per example template: ``relative path → URL`` downloaded into the campaign (gitignored, not committed)."""
EVALUATE_CONTRACT = """The evaluate command is the referee. piloop runs it (the `evaluate` argv in piloop.toml) in a clean worktree of every
commit that a worker makes. It prints one `METRIC <name>=<value>` line for each metric under [[metrics]] and exits 0.
A non-zero exit is recorded as a crash, with the reason on stderr. Make it deterministic. Make sure that a worker
can change the score only through the code that it is supposed to change."""


class CampaignError(RuntimeError):
    """Registration failed. The message says why and what to do."""


def create_campaign_repo(dest: Path, template: Path, data: dict[str, str], *, ignore: str = GITIGNORE) -> list[str]:
    """Copy ``template`` to ``dest``, download ``data`` and make a git repo with a baseline commit.

    If ``dest`` already has a ``piloop.toml``, it is a campaign: only the missing ``.entire/`` files are copied.
    Files that already exist are never overwritten, so a second run is safe and keeps the user's edits.

    Args:
        dest: The campaign repo. It can already exist and can already be a git repo.
        template: The directory to copy. ``data/``, ``.git``, ``.venv`` and ``__pycache__`` are not copied.
        data: ``relative path → URL`` to download into ``dest``. These files are not committed, so ``ignore``
            must cover them.
        ignore: The ``.gitignore`` to write if the campaign has none.

    Returns:
        The repo-relative paths that this call wrote.

    Raises:
        ConfigError: ``piloop.toml`` is not a valid config. Nothing is committed then: fix it and run init again.
    """
    dest.mkdir(parents=True, exist_ok=True)
    written: list[str] = []

    def copy_missing(src: str, dst: str) -> None:
        if not Path(dst).exists():
            shutil.copy2(src, dst)
            written.append(str(Path(dst).relative_to(dest)))

    if not (dest / CONFIG_FILE).exists():
        shutil.copytree(
            template,
            dest,
            dirs_exist_ok=True,
            copy_function=copy_missing,
            ignore=shutil.ignore_patterns('data', '.git', '.venv', '__pycache__'),
        )
    shutil.copytree(BLANK / ENTIRE_DIR, dest / ENTIRE_DIR, dirs_exist_ok=True, copy_function=copy_missing)
    if not (dest / '.gitignore').exists():
        (dest / '.gitignore').write_text(ignore)
        written.append('.gitignore')
    if CONFIG_FILE in written:
        toml = dest / CONFIG_FILE
        toml.write_text(toml.read_text().replace(f'name = "{NAME_PLACEHOLDER}"', f'name = "{_slug(dest)}"', 1))
    for rel, url in data.items():
        target = dest / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            console.print(f'downloading {url}')
            target.write_bytes(httpx.get(url, follow_redirects=True, timeout=60).raise_for_status().content)
    if (template / 'pyproject.toml').is_file():
        # A new campaign has no lockfile yet, so an inherited UV_FROZEN (as in CI) must not apply.
        subprocess.run(['uv', 'sync', '--quiet'], cwd=dest, check=True, env=child_env('UV_FROZEN'))
    # Only a valid config is committed: it raises ConfigError, and the repo stays as it was, piloop.toml uncommitted.
    if (dest / CONFIG_FILE).is_file():
        load_campaign_config(dest)
    repo = Repo.init(dest, initial_branch='main') if not (dest / '.git').exists() else Repo(dest)
    if not repo.head.is_valid():
        repo.git.add('-A')
        repo.git.commit('-qm', f'{template.name}: baseline')
        return sorted(written)
    # Only init's own files: in a shared checkout, other dirty or staged files are someone else's work in progress.
    owned = sorted(
        {*written, '.gitignore', *_template_files(template), *_template_files(BLANK / ENTIRE_DIR, ENTIRE_DIR)}
    )
    # git status leaves out what the repo's ignore file ignores, which git add would refuse to take by name
    # and --no-renames reports a rename as its two paths, not as one "old -> new" record
    status = repo.git.status('--porcelain', '-z', '--no-renames', '--untracked-files=all', '--', *owned)
    entries = [entry for entry in status.split('\0') if entry]
    changed = sorted(entry[3:] for entry in entries)
    # only what the work tree changed: git add refuses a path whose deletion is staged already
    if unstaged := sorted(entry[3:] for entry in entries if entry[1] != ' '):
        repo.git.add('-A', '--', *unstaged)
    if changed:
        repo.git.commit('-qm', f'piloop init: {", ".join(changed)}', '--', *changed)
    return sorted(written)


def _template_files(template: Path, prefix: str = '') -> list[str]:
    """The repo-relative paths of the files that ``template`` provides, as ``create_campaign_repo`` copies them."""
    skipped = {'data', '.git', '.venv', '__pycache__'}
    return [
        str(Path(prefix) / path.relative_to(template))
        for path in template.rglob('*')
        if path.is_file() and not skipped & set(path.relative_to(template).parts)
    ]


def _slug(dest: Path) -> str:
    """Make a directory name safe to use as a campaign name. Git refs and Herdr workspaces use this name."""
    return re.sub(r'[^A-Za-z0-9._-]+', '-', dest.resolve().name).strip('-') or NAME_PLACEHOLDER


def stub_untouched(path: Path) -> bool:
    """Return True if the campaign still uses the placeholder ``evaluate.py`` from the template."""
    stub = path / EVALUATE_STUB
    return (
        stub.is_file()
        and stub.read_bytes() == (BLANK / EVALUATE_STUB).read_bytes()
        and load_campaign_config(path).evaluate == load_campaign_config(BLANK).evaluate
    )


def register(s: Session, path: Path) -> tuple[Campaign, Measurement]:
    """Score HEAD in a clean worktree, then insert the campaign row and its baseline measurement.

    The evaluation runs before any write, in a staging directory under ``campaigns_dir``, with no transaction open.
    An evaluation can take an hour, and an open write transaction locks all other clients out of the database.
    The rows are inserted afterwards in one short transaction, and the staging directory becomes the campaign's.

    Raises:
        CampaignError: Another repo uses the name, or the evaluate command failed on HEAD.
    """
    cfg = load_campaign_config(path)
    other = s.scalars(select(Campaign).where(Campaign.name == cfg.name)).first()
    s.rollback()
    if other is not None:
        raise CampaignError(f'campaign name {cfg.name!r} is already used by {other.path}; change `name` in piloop.toml')
    repo = git.open_repo(path)
    sha = repo.head.commit.hexsha
    staging = campaigns_dir(settings.home) / f'.registering-{uuid4().hex}'
    log_dir = staging / 'baseline'
    log_dir.mkdir(parents=True)
    wt = log_dir / 'worktree'
    try:
        git.add_worktree(repo, wt, sha, worktree_links(cfg, path.resolve(), staging))
        settings.units_dir.mkdir(parents=True, exist_ok=True)
        result = run_evaluate(
            cfg.evaluate,
            cwd=wt,
            log_dir=log_dir,
            timeout=cfg.evaluate_timeout,
            required={m.name for m in cfg.metrics},
            units_file=settings.units_dir / f'{uuid4().hex}.pending',
        )
    finally:
        git.remove_worktree(repo, wt)
    if result.error is not None:
        raise CampaignError(f'baseline failed to evaluate: {result.error}; its log is in {log_dir}')
    campaign = Campaign(
        name=cfg.name, path=str(path.resolve()), repo=git.identity(path), config=cfg.model_dump(mode='json')
    )
    s.add(campaign)
    s.flush()
    shutil.copytree(staging, campaign_dir(settings.home, campaign.id), symlinks=True, dirs_exist_ok=True)
    shutil.rmtree(staging)
    baseline, _ = resolve(s, campaign.id, repo, sha, parent=None, run_id=None)
    baseline.verdict, baseline.verdict_by = Verdict.KEEP, DAEMON
    measurement = Measurement(
        candidate_id=baseline.id,
        evaluator_id=current_evaluator(s, campaign, cfg).id,
        metrics=result.metrics,
        seconds=result.seconds,
    )
    s.add(measurement)
    s.flush()
    save_units(settings.units_dir, measurement.id, result.units)
    emit(s, 'campaign.added', TargetKind.CAMPAIGN, campaign.id, actor='cli', payload={'baseline': baseline.id})
    s.commit()
    return campaign, measurement


def registered(s: Session, path: Path) -> Campaign | None:
    """Return the campaign row for a repo path, or None."""
    return s.scalars(select(Campaign).where(Campaign.path == str(path.resolve()))).first()


def find(s: Session, ref: str) -> Campaign | None:
    """Find a campaign by name, else by a path inside its repo.

    The name is tried first, because a bare name can also be a directory in the current directory.
    """
    campaign = s.scalars(select(Campaign).where(Campaign.name == ref)).first()
    if campaign is None and Path(ref).exists():
        campaign = checked_out(s, Path(ref))
    return campaign


def checked_out(s: Session, path: Path) -> Campaign | None:
    """Return the campaign whose repo contains ``path``: the repo, a subdirectory or a linked git worktree.

    The nearest registered ancestor wins. The main worktree of the repo that contains ``path`` is tried last.
    """
    path = path.resolve()
    roots = [str(p) for p in (path, *path.parents)]
    with suppress(InvalidGitRepositoryError, NoSuchPathError):
        roots.append(str(Path(Repo(path, search_parent_directories=True).common_dir).resolve().parent))
    by_path = {p.path: p for p in s.scalars(select(Campaign).where(Campaign.path.in_(roots)))}
    return next((by_path[root] for root in roots if root in by_path), None)


def sync(s: Session, campaign: Campaign) -> CampaignConfig:
    """Read the repo's ``piloop.toml`` into the campaign row again.

    A change to ``evaluate``, ``metrics`` or ``link`` makes a new evaluator. A stopped campaign whose ``[stop]`` table
    changed becomes active again, because the human changed the stop conditions. The repo's identity is read again
    too, because a remote may have been added since.
    """
    cfg = load_campaign_config(Path(campaign.path))
    campaign.repo = git.identity(Path(campaign.path))
    config = cfg.model_dump(mode='json')
    if campaign.status is CampaignStatus.STOPPED and config['stop'] != campaign.config.get('stop'):
        campaign.status, campaign.stopped_reason = CampaignStatus.ACTIVE, None
    campaign.config = config
    current_evaluator(s, campaign, cfg)
    return cfg


def tree(s: Session) -> list[tuple[int, Campaign]]:
    """Return every campaign with its depth in the tree of campaigns (``campaigns.parent_id``), parents first.

    The roots and the children of each campaign come in the order of their ids.
    """
    children: dict[int | None, list[Campaign]] = {}
    for campaign in s.scalars(select(Campaign).order_by(Campaign.id)):
        children.setdefault(campaign.parent_id, []).append(campaign)
    ordered: list[tuple[int, Campaign]] = []

    def walk(parent_id: int | None, depth: int) -> None:
        for child in children.get(parent_id, []):
            ordered.append((depth, child))
            walk(child.id, depth + 1)

    walk(None, 0)
    return ordered


def campaigns_dir(home: Path) -> Path:
    """The parent of the directory of each campaign under the piloop home ``home``."""
    return home / 'projects'  # the directories keep their 0.1 name


def campaign_dir(home: Path, campaign_id: int) -> Path:
    """The directory of a campaign: its shared workspace, its baseline and the evaluations of its candidates."""
    return campaigns_dir(home) / str(campaign_id)


def workspace_dir(home: Path, campaign_id: int) -> Path:
    """The shared workspace of a campaign, where its agents leave notes and artifacts for each other."""
    return campaign_dir(home, campaign_id) / WORKSPACE


def worktree_links(cfg: CampaignConfig, campaign_path: Path, campaign_dir: Path) -> dict[str, Path]:
    """Return worktree-relative name → target for the linked paths, the shared workspace and Entire's settings.

    ``.entire`` is linked only into a worktree whose tree does not have it, for example a candidate made before
    ``piloop init`` added it. Entire reads its settings from the worktree it runs in. Without them, it silently
    falls back to a shadow branch.
    """
    workspace = campaign_dir / WORKSPACE
    workspace.mkdir(parents=True, exist_ok=True)
    return {
        **{rel: campaign_path / rel for rel in cfg.link},
        WORKSPACE: workspace,
        ENTIRE_DIR: campaign_path / ENTIRE_DIR,
    }
