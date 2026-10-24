"""Create and register campaign repos: ``piloop init``, ``piloop campaign add`` and the bundled examples."""

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Literal
from uuid import uuid4

import httpx2 as httpx
from covalence import Cid
from rich.console import Console
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.actor import Actor
from piloop.config import CONFIG_FILE, CampaignConfig, config_of, load_campaign_config
from piloop.db.events import emit
from piloop.db.models import Campaign, CampaignStatus, Candidate, EventModel, Measurement, Verdict
from piloop.db.session import Database
from piloop.ids import CampaignId, CandidateId, TargetKind, TargetRef
from piloop.plugins.cas.contract import CAS, Cas
from piloop.plugins.executor.contract import RepoId, Source, Workspace
from piloop.plugins.stop_rule.contract import STOP_RULES
from piloop.plugins.vcs.contract import VCS, Repository, Revision, Vcs
from piloop.proc import child_env
from piloop.scoring.evaluate import run_evaluate
from piloop.scoring.ledger import DAEMON, current_evaluator, resolve, save_units
from piloop.settings import INPUTS_DIR, settings
from piloop.store.tree import Copies, Manifest, Seen, materialise, put_tree

console = Console(stderr=True)


class CampaignAdded(EventModel):
    """A campaign was added (``piloop init``), with its baseline candidate."""

    kind: Literal['campaign.added'] = 'campaign.added'
    baseline: CandidateId


WORKSPACE = 'workspace'
"""A per-campaign directory, linked into every worktree, for files that runs share. It is never committed."""
deprecated.until('0.3.0', 'campaign directories keep their 0.1.0 name: move projects/ to campaigns/, delete PROJECTS')
PROJECTS = 'projects'
"""The directory under the piloop home that holds each campaign's directory."""
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
commit that an implementer makes. It prints one `METRIC <name>=<value>` line for each metric under [[metrics]] and exits 0.
A non-zero exit is recorded as a crash, with the reason on stderr. Make it deterministic. Make sure that an implementer
can change the score only through the code that it is supposed to change."""


class CampaignError(RuntimeError):
    """Registration failed. The message says why and what to do."""


def create_campaign_repo(dest: Path, template: Path, data: dict[str, str], *, ignore: str = GITIGNORE) -> list[str]:
    """Copy ``template`` to ``dest``, download ``data`` and make a repository with a baseline version (``Vcs.init``).

    If ``dest`` already has a ``piloop.toml``, it is a campaign: the template is not copied.
    Files that already exist are never overwritten, so a second run is safe and keeps the user's edits.

    Args:
        dest: The campaign repo. It can already exist and can already be a repository.
        template: The directory to copy. ``data/``, ``.git``, ``.venv`` and ``__pycache__`` are not copied.
        data: ``relative path → URL`` to download into ``dest``. These files are not saved, so ``ignore`` must cover
            them.
        ignore: The ``.gitignore`` to write if the campaign has none.

    Returns:
        The repo-relative paths that this call wrote.

    Raises:
        ConfigError: ``piloop.toml`` is not a valid config. Nothing is saved then: fix it and run init again.
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
    # Only a valid config is saved: it raises ConfigError, and the repo stays as it was, piloop.toml unsaved.
    plugins = load_campaign_config(dest).plugins if (dest / CONFIG_FILE).is_file() else {}
    # Only init's own files: in a shared checkout, other changed files are someone else's work in progress.
    owned = sorted({*written, '.gitignore', *_template_files(template)})
    VCS.selected(plugins).init(dest, template.name, owned)
    return sorted(written)


def _template_files(template: Path) -> list[str]:
    """The repo-relative paths of the files that ``template`` provides, as ``create_campaign_repo`` copies them."""
    skipped = {'data', '.git', '.venv', '__pycache__'}
    return [
        str(path.relative_to(template))
        for path in template.rglob('*')
        if path.is_file() and not skipped & set(path.relative_to(template).parts)
    ]


def _slug(dest: Path) -> str:
    """Make a directory name safe to use as a campaign name, which git refs use."""
    return re.sub(r'[^A-Za-z0-9._-]+', '-', dest.resolve().name).strip('-') or NAME_PLACEHOLDER


def stub_untouched(path: Path) -> bool:
    """Return True if the campaign still uses the placeholder ``evaluate.py`` from the template."""
    stub = path / EVALUATE_STUB
    return (
        stub.is_file()
        and stub.read_bytes() == (BLANK / EVALUATE_STUB).read_bytes()
        and load_campaign_config(path).evaluate == load_campaign_config(BLANK).evaluate
    )


def register(s: Session, path: Path, actor: Actor) -> tuple[Campaign, Measurement]:
    """Score the head version in a clean checkout, then insert the campaign row and its baseline measurement.

    The evaluation runs before any write, in a staging directory under ``campaigns_dir``, with no transaction open.
    An evaluation can take an hour, and an open write transaction locks all other clients out of the database.
    The rows are inserted afterwards in one short transaction, and the staging directory becomes the campaign's.
    ``actor`` signs the ``campaign.added`` event.

    Raises:
        CampaignError: Another repo uses the name, or the evaluate command failed on HEAD.
    """
    cfg = load_campaign_config(path)
    other = s.scalars(select(Campaign).where(Campaign.name == cfg.name)).first()
    s.rollback()
    if other is not None:
        raise CampaignError(f'campaign name {cfg.name!r} is already used by {other.path}; change `name` in piloop.toml')
    repo = VCS.selected(cfg.plugins).open(path)
    version = repo.head()
    staging = campaigns_dir(settings.home) / f'.registering-{uuid4().hex}'
    log_dir = staging / 'baseline'
    log_dir.mkdir(parents=True)
    wt = log_dir / 'worktree'
    cas = CAS.selected(settings.providers).open(settings.home)
    workspace = staging / WORKSPACE
    workspace.mkdir()
    try:
        repo.checkout(version.revision, wt)
        if not (wt / WORKSPACE).exists():
            (wt / WORKSPACE).symlink_to(workspace)
        copies = Copies(cas, settings.home / INPUTS_DIR)
        for blob in links_of(cas, cfg, path.resolve()).values():
            materialise(cas, Manifest.get(cas, blob), wt, into=copies)
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
        repo.discard(wt)
    if result.error is not None:
        raise CampaignError(f'baseline failed to evaluate: {result.error}; its log is in {log_dir}')
    campaign = Campaign(
        name=cfg.name,
        path=str(path.resolve()),
        repo=VCS.selected(cfg.plugins).identity(path),
        config=cfg.model_dump(mode='json'),
    )
    s.add(campaign)
    s.flush()
    shutil.copytree(staging, campaign_dir(settings.home, campaign.id), symlinks=True, dirs_exist_ok=True)
    shutil.rmtree(staging)
    baseline, _ = resolve(s, campaign.id, version, parent=None, run_id=None)
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
    emit(s, CampaignAdded(baseline=baseline.id), TargetRef(TargetKind.CAMPAIGN, campaign.id), actor)
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
    """Return the campaign whose repo contains ``path``: the repo, a subdirectory or a checkout that it made.

    The nearest registered ancestor wins. The main checkout that contains ``path``, as each vcs provider finds it
    (``Vcs.root``), is tried last.
    """
    path = path.resolve()
    roots = [str(p) for p in (path, *path.parents)]
    for provider in VCS.providers().values():
        if (root := provider.model.root(path)) is not None:
            roots.append(str(root))
    by_path = {p.path: p for p in s.scalars(select(Campaign).where(Campaign.path.in_(roots)))}
    return next((by_path[root] for root in roots if root in by_path), None)


def sync(s: Session, campaign: Campaign) -> CampaignConfig:
    """Read the repo's ``piloop.toml`` into the campaign row again.

    A change to ``evaluate``, ``metrics`` or ``link`` makes a new evaluator. A stopped campaign whose stop rule or its
    limits changed becomes active again, because the human changed the stop conditions. The repo's identity is read again
    too, because a remote may have been added since.
    """
    cfg = load_campaign_config(Path(campaign.path))
    campaign.repo = VCS.selected(cfg.plugins).identity(Path(campaign.path))
    config = cfg.model_dump(mode='json')
    stored = campaign.config.get('plugins')
    was = stored.get(STOP_RULES.name) if isinstance(stored, dict) else None
    if campaign.status is CampaignStatus.STOPPED and config['plugins'].get(STOP_RULES.name) != was:
        campaign.status, campaign.stopped_reason = CampaignStatus.ACTIVE, None
    campaign.config = config
    current_evaluator(s, campaign, cfg)
    return cfg


def tree(s: Session) -> list[tuple[int, Campaign]]:
    """Return every campaign with its depth in the tree of campaigns (``campaigns.parent_id``), parents first.

    The roots and the children of each campaign come in the order of their ids.
    """
    children: dict[CampaignId | None, list[Campaign]] = {}
    for campaign in s.scalars(select(Campaign).order_by(Campaign.id)):
        children.setdefault(campaign.parent_id, []).append(campaign)
    ordered: list[tuple[int, Campaign]] = []

    def walk(parent_id: CampaignId | None, depth: int) -> None:
        for child in children.get(parent_id, []):
            ordered.append((depth, child))
            walk(child.id, depth + 1)

    walk(None, 0)
    return ordered


def campaigns_dir(home: Path) -> Path:
    """The parent of the directory of each campaign under the piloop home ``home``."""
    return home / PROJECTS


def campaign_dir(home: Path, campaign_id: CampaignId) -> Path:
    """The directory of a campaign: its shared workspace, its baseline and the evaluations of its candidates."""
    return campaigns_dir(home) / str(campaign_id)


def workspace_dir(home: Path, campaign_id: CampaignId) -> Path:
    """The shared workspace of a campaign, where its agents leave notes and artifacts for each other."""
    return campaign_dir(home, campaign_id) / WORKSPACE


def links_of(cas: Cas, cfg: CampaignConfig, path: Path, seen: Seen | None = None) -> dict[str, Cid]:
    """Put the linked inputs of the checkout at ``path`` into ``cas``: each as the manifest of its tree, by name.

    They are the ``link`` paths that exist. With ``seen``, a file stored before is not read again.
    """
    return {rel: put_tree(cas, path, [rel], seen=seen).manifest.put(cas) for rel in cfg.link if (path / rel).exists()}


class Sources:
    """The artifacts of a campaign's jobs, by address: its repo of record, with its inputs and workspace in the CAS."""

    def __init__(self, cas: Cas, home: Path, db: Database) -> None:
        self._cas = cas
        self._home = home
        self._db = db
        self._seen: Seen = {}
        """The files stored so far: a job made later reads only those that changed."""

    def source(self, campaign: Campaign, revision: Revision) -> Source:
        """``revision`` of the campaign's repo, with its links: an evaluation's files."""
        cfg = config_of(campaign)
        return Source(
            vcs=VCS.selected(cfg.plugins),
            repo=RepoId(campaign.id),
            revision=revision,
            links=links_of(self._cas, cfg, Path(campaign.path), self._seen),
        )

    def agent(self, campaign: Campaign, revision: Revision) -> Source:
        """``source``, with a snapshot of the campaign's shared workspace and every candidate's version: an agent's."""
        workspace = workspace_dir(self._home, campaign.id)
        names = os.listdir(workspace) if workspace.is_dir() else []
        manifest = put_tree(self._cas, workspace, names, seen=self._seen).manifest.put(self._cas)
        place = workspace.relative_to(self._home).as_posix()
        with self._db.session() as s:
            history = [
                Revision(sha)
                for sha in s.scalars(
                    select(Candidate.commit_sha).where(Candidate.campaign_id == campaign.id).distinct()
                )
            ]
        update = {'workspace': Workspace(place=place, manifest=manifest), 'history': history}
        return self.source(campaign, revision).model_copy(update=update)


def vcs_of(campaign: Campaign) -> Vcs:
    """The campaign's vcs provider, with its parameters (``[plugins.vcs]``)."""
    return VCS.selected(config_of(campaign).plugins)


def repository(campaign: Campaign) -> Repository:
    """The campaign's repo of record, opened with its vcs provider.

    Raises:
        NotARepository: Its checkout is gone, or is no checkout of the provider.
    """
    return vcs_of(campaign).open(Path(campaign.path))


def checkout_of(db: Database, repo: RepoId) -> Path:
    """The checkout of the repo of record ``repo``: its campaign's.

    Raises:
        KeyError: The campaign is gone.
    """
    with db.session() as s:
        return Path(_of_record(s, repo).path)


def record_of(db: Database, repo: RepoId) -> Repository:
    """The repo of record ``repo``, opened with its campaign's vcs provider.

    Raises:
        KeyError: The campaign is gone.
        NotARepository: Its checkout is gone, or is no checkout of the provider.
    """
    with db.session() as s:
        return repository(_of_record(s, repo))


def _of_record(s: Session, repo: RepoId) -> Campaign:
    if (campaign := s.get(Campaign, CampaignId(repo))) is None:
        raise KeyError(f'no campaign#{repo}: its repo of record is gone')
    return campaign
