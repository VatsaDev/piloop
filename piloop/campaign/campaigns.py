"""Create and register campaign repos: ``piloop init``, ``piloop campaign add`` and the bundled examples."""

import os
import re
import shutil
import subprocess
from collections.abc import Collection
from pathlib import Path
from typing import Literal
from uuid import uuid4

import httpx2 as httpx
from covalence import Cid
from pydantic import SecretStr, ValidationError
from rich.console import Console
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.actor import Actor, System
from piloop.config import CONFIG_FILE, CampaignConfig, ConfigError, load_campaign_config
from piloop.db.events import emit
from piloop.db.models import NOT_RETIRED, Campaign, Candidate, EventModel, Origin
from piloop.db.session import Database
from piloop.db.sql import sql_error
from piloop.ids import (
    LISTED,
    CampaignId,
    CampaignName,
    CampaignStatus,
    CandidateId,
    Revision,
    TargetKind,
    TargetRef,
    checked_campaign_name,
)
from piloop.plugins.cas.contract import Cas
from piloop.plugins.executor.contract import RepoId, Source, Workspace
from piloop.plugins.role.contract import ideas_taken
from piloop.plugins.stop_rule.contract import STOP_RULES
from piloop.plugins.vcs.contract import VCS, Repository, Vcs
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.proc import child_env
from piloop.scoring.ledger import current_evaluator, resolve
from piloop.settings import settings
from piloop.store.tree import Seen, put_tree

console = Console(stderr=True)


class CampaignAdded(EventModel):
    """A campaign was added (``piloop init``), with its baseline candidate."""

    kind: Literal['campaign.added'] = 'campaign.added'
    baseline: CandidateId


WORKSPACE = 'workspace'
"""A per-campaign directory, linked into every worktree, for files that runs share. It is never committed."""
deprecated.until('0.4.0', 'campaign directories keep their 0.1.0 name: move projects/ to campaigns/, delete PROJECTS')
PROJECTS = 'projects'
"""The directory under the piloop home that holds each campaign's directory."""
REPOS = 'repos'
"""The directory under the piloop home that holds the checkout of each campaign that the daemon cloned, by its id
(``register_clone``)."""
IGNORED = ('data', '.venv', WORKSPACE, '__pycache__')
"""What no version of a new campaign holds (``Vcs.init``)."""
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
"""Per example template: ``relative path → URL`` downloaded into the campaign (in no version: ``IGNORED``)."""
EVALUATE_CONTRACT = """The evaluate command is the referee. piloop runs it (the `evaluate` argv in piloop.toml) in a clean checkout of every
version that an implementer saves. It writes one JSON line with each metric under [[metrics]],
`{"metrics": {"<name>": <value>}}`, to $PILOOP_RESULT_FILE and exits 0. A non-zero exit is recorded as a crash, with
the reason on stderr. Make it deterministic. Make sure that an implementer can change the score only through the code
that it is supposed to change."""


class CampaignError(RuntimeError):
    """Registration failed. The message says why and what to do."""


def create_campaign_repo(dest: Path, template: Path, data: dict[str, str]) -> list[str]:
    """Copy ``template`` to ``dest``, download ``data`` and make a repository with a baseline version (``Vcs.init``).

    If ``dest`` already has a ``piloop.toml``, it is a campaign: the template is not copied.
    Files that already exist are never overwritten, so a second run is safe and keeps the user's edits.

    Args:
        dest: The campaign repo. It can already exist and can already be a repository.
        template: The directory to copy. ``data/``, ``.git``, ``.venv`` and ``__pycache__`` are not copied.
        data: ``relative path → URL`` to download into ``dest``. These files are not saved, so ``IGNORED`` must cover
            them.

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
    if CONFIG_FILE in written:
        toml = dest / CONFIG_FILE
        toml.write_text(
            toml.read_text().replace(f'name = "{NAME_PLACEHOLDER}"', f'name = "{_slug(dest.resolve().name)}"', 1)
        )
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
    owned = sorted({*written, *_template_files(template)})
    written += VCS.selected(plugins).init(dest, f'{template.name}: baseline', owned, IGNORED)
    return sorted(written)


def _template_files(template: Path) -> list[str]:
    """The repo-relative paths of the files that ``template`` provides, as ``create_campaign_repo`` copies them."""
    skipped = {'data', '.git', '.venv', '__pycache__'}
    return [
        str(path.relative_to(template))
        for path in template.rglob('*')
        if path.is_file() and not skipped & set(path.relative_to(template).parts)
    ]


def _slug(name: str) -> str:
    """Make a directory's name a campaign name (``CampaignName``)."""
    slug = re.sub(r'[^A-Za-z0-9._-]+', '-', name).strip('-') or NAME_PLACEHOLDER
    return f'campaign-{slug}' if slug.isdigit() else slug


def register_clone(
    s: Session,
    home: Path,
    vcs: Vcs,
    url: str,
    credential: SecretStr | None,
    actor: Actor,
    name: CampaignName | None = None,
) -> tuple[Campaign, Candidate]:
    """Clone ``url`` into the home's ``REPOS``, register the clone (``register``), and return what that returns.

    The checkout is ``REPOS/<campaign id>``, so a repository registers as many campaigns as asked. It is cloned under
    a new name first, because the id comes with the row. A clone that does not register is deleted. The caller
    commits.

    Raises:
        CampaignError: The campaign's directory exists, or ``register`` refuses the clone.
        CloneFailed: The clone failed.
    """
    (home / REPOS).mkdir(parents=True, exist_ok=True)
    cloning = home / REPOS / f'.cloning-{uuid4().hex}'
    try:
        vcs.clone(url, cloning, credential)
        campaign, baseline = register(s, cloning, actor, name)
        checkout = home / REPOS / str(campaign.id)
        if checkout.exists():
            raise CampaignError(f'{checkout} exists: move it away, then register again')
        cloning.rename(checkout)
    except Exception:
        if cloning.exists():
            shutil.rmtree(cloning)
        raise
    campaign.path = str(checkout.resolve())
    return campaign, baseline


def stub_untouched(path: Path) -> bool:
    """Return True if the campaign still uses the placeholder ``evaluate.py`` from the template."""
    stub = path / EVALUATE_STUB
    return (
        stub.is_file()
        and stub.read_bytes() == (BLANK / EVALUATE_STUB).read_bytes()
        and load_campaign_config(path).evaluate == load_campaign_config(BLANK).evaluate
    )


def check_takes(path: Path, cfg: CampaignConfig) -> None:
    """Refuse the ``takes`` condition of a role that does not run on the ``ideas`` table.

    The check reads the schema, whose models type ``campaigns.config`` with the config models, so it runs here and
    not in the role's model.

    Raises:
        ConfigError: one line for each bad condition, as ``load_campaign_config`` gives one for each bad key.
    """
    bad = [
        f'  roles.{name}.takes = {role.takes!r}: the condition does not run on the ideas table: {error}'
        for name, role in cfg.roles.doing_runs().items()
        if role.takes is not None and (error := sql_error(ideas_taken(role.takes))) is not None
    ]
    if bad:
        raise ConfigError(
            '\n'.join([f'{path / CONFIG_FILE} is not a valid piloop config ({len(bad)} problem(s)):', *bad])
        )


def register(s: Session, path: Path, actor: Actor, name: CampaignName | None = None) -> tuple[Campaign, Candidate]:
    """Insert the campaign of the checkout at ``path``, its evaluator and the baseline candidate of its head version.

    The campaign is called ``name``, else the ``name`` of its ``piloop.toml``. The baseline is a keep by the daemon,
    which measures it next (``ledger.due``: the head first). ``actor`` signs the ``campaign.added`` event. The caller
    commits.

    Raises:
        CampaignError: The name is no ``CampaignName``, or another campaign has it or the checkout.
    """
    cfg = load_campaign_config(path)
    check_takes(path, cfg)
    if name is None:
        try:
            name = checked_campaign_name(cfg.name)
        except ValueError as e:
            raise CampaignError(f'{e}; change `name` in piloop.toml, or name the campaign') from e
    held = s.scalars(select(Campaign).where(Campaign.path == str(path.resolve()), NOT_RETIRED)).first()
    if held is not None:
        raise CampaignError(f'{path} is the checkout of campaign {held.name} #{held.id} already')
    other = s.scalars(select(Campaign).where(Campaign.name == name, NOT_RETIRED)).first()
    if other is not None:
        raise CampaignError(
            f'campaign name {name!r} is already used by {other.path}; name the campaign another way, or retire'
            f' campaign #{other.id}'
        )
    vcs = VCS.selected(cfg.plugins)
    version = vcs.open(path).head()
    campaign = Campaign(name=name, path=str(path.resolve()), repo=vcs.identity(path), config=cfg)
    s.add(campaign)
    s.flush()
    current_evaluator(s, campaign, cfg)
    baseline, _ = resolve(s, campaign.id, version, parent=None, run_id=None, origin=Origin.BASELINE)
    baseline.verdict, baseline.verdict_by = Outcome.KEEP, System.DAEMON.signature
    emit(s, CampaignAdded(baseline=baseline.id), TargetRef(TargetKind.CAMPAIGN, campaign.id), actor)
    return campaign, baseline


def referenced(s: Session, ref: str) -> Campaign | None:
    """The campaign that ``ref`` names: any campaign by its id, else one that is not retired by its name.

    A name is never all digits (``CampaignName``), so ``ref`` of digits is an id.
    """
    if ref.isdigit():
        return s.get(Campaign, CampaignId(int(ref)))
    return s.scalars(select(Campaign).where(Campaign.name == ref, NOT_RETIRED)).one_or_none()


def find(s: Session, ref: str, cwd: Path | None = None) -> Campaign | None:
    """Find a campaign by id or name (``referenced``), else by a path inside its repo, relative to ``cwd``.

    ``cwd`` is this process's by default. The id and the name are tried first, because they can also be directories in
    the current directory.
    """
    campaign = referenced(s, ref)
    if campaign is None and (path := _path(ref, cwd)).exists():
        campaign = checked_out(s, path)
    return campaign


def _path(ref: str, cwd: Path | None) -> Path:
    return Path(ref) if cwd is None else cwd / ref


class UnknownCampaign(LookupError):
    """No campaign of the home has that name or holds that path."""

    def __init__(self, ref: str, cwd: Path | None = None) -> None:
        where = f'at {ref}' if _path(ref, cwd).exists() else repr(ref)
        super().__init__(
            f'no campaign {where} in {settings.home}; is PILOOP_HOME right? `piloop init <path>` registers one'
        )


def named(s: Session, ref: str, cwd: Path | None = None) -> Campaign:
    """The campaign that ``ref`` names (``find``). Raises ``UnknownCampaign``."""
    if (found := find(s, ref, cwd)) is None:
        raise UnknownCampaign(ref, cwd)
    return found


def selected(
    s: Session, ref: str | None, cwd: Path | None, statuses: Collection[CampaignStatus] = LISTED
) -> list[Campaign]:
    """The campaign that ``ref`` names, else the one ``cwd`` is in, else every campaign of one of ``statuses``.

    Raises:
        UnknownCampaign: ``ref`` names none.
    """
    if ref:
        return [named(s, ref, cwd)]
    if cwd is not None and (here := checked_out(s, cwd)):
        return [here]
    return list(s.scalars(select(Campaign).where(Campaign.status.in_(statuses))).all())


def checked_out(s: Session, path: Path) -> Campaign | None:
    """Return the campaign whose repo contains ``path``: the repo, a subdirectory or a checkout that it made.

    A retired campaign holds no path. The nearest registered ancestor wins. The main checkout that contains ``path``,
    as each vcs provider finds it (``Vcs.root``), is tried last.
    """
    path = path.resolve()
    roots = [str(p) for p in (path, *path.parents)]
    for provider in VCS.providers().values():
        if (root := provider.model.root(path)) is not None:
            roots.append(str(root))
    by_path = {p.path: p for p in s.scalars(select(Campaign).where(Campaign.path.in_(roots), NOT_RETIRED))}
    return next((by_path[root] for root in roots if root in by_path), None)


def sync(s: Session, campaign: Campaign) -> CampaignConfig:
    """Read the repo's ``piloop.toml`` into the campaign row again.

    A change to ``evaluate``, ``metrics`` or ``link`` makes a new evaluator. A stopped campaign whose stop rule or its
    limits changed becomes active again, because the human changed the stop conditions. A stored copy that no longer
    validates is replaced, and its stop rule counts as unchanged. The repo's identity is read again too, because a
    remote may have been added since.
    """
    cfg = load_campaign_config(Path(campaign.path))
    check_takes(Path(campaign.path), cfg)
    campaign.repo = VCS.selected(cfg.plugins).identity(Path(campaign.path))
    try:
        changed = cfg.plugins.get(STOP_RULES.name) != campaign.config.plugins.get(STOP_RULES.name)
    except ValidationError:
        changed = False  # the copy is replaced below; what it said about the stop rule is unknown
    if campaign.status is CampaignStatus.STOPPED and changed:
        campaign.status, campaign.stopped_reason = CampaignStatus.ACTIVE, None
    campaign.config = cfg
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
    """The directory of a campaign: its shared workspace and the evaluations of its candidates."""
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
        cfg = campaign.config
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
            query = select(Candidate.revision).where(Candidate.campaign_id == campaign.id).distinct()
            history = list(s.scalars(query))
        update = {'workspace': Workspace(place=place, manifest=manifest), 'history': history}
        return self.source(campaign, revision).model_copy(update=update)


def vcs_of(campaign: Campaign) -> Vcs:
    """The campaign's vcs provider, with its parameters (``[plugins.vcs]``)."""
    return VCS.selected(campaign.config.plugins)


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
