"""Create and register project repos: ``piloop init``, ``piloop project add`` and the bundled examples."""

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
from piloop.config import CONFIG_FILE, ProjectConfig, load_project_config
from piloop.db.events import emit
from piloop.db.models import Measurement, Project, ProjectStatus, TargetKind, Verdict
from piloop.proc import child_env
from piloop.scoring.evaluate import run_evaluate
from piloop.scoring.ledger import DAEMON, current_evaluator, resolve, save_units
from piloop.settings import settings

console = Console(stderr=True)

WORKSPACE = 'workspace'
"""A per-project directory, linked into every worktree, for files that runs share. It is never committed."""
GITIGNORE = 'data\n.venv\nworkspace\n__pycache__/\n'
TEMPLATES = Path(__file__).parent / 'templates'
BLANK = TEMPLATES / 'project'
"""The template ``piloop init`` writes into a repo without a ``piloop.toml``."""
EVALUATE_STUB = 'evaluate.py'
NAME_PLACEHOLDER = 'my-project'
"""The ``name`` in the blank template's ``piloop.toml``. ``piloop init`` replaces it with the directory name."""
EXAMPLE_DATA: dict[str, dict[str, str]] = {
    'enwik8': {'data/enwik8.zip': 'https://mattmahoney.net/dc/enwik8.zip'},
    'titanic-tracks': {
        'data/train.csv': 'https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv'
    },
}
"""Per example template: ``relative path → URL`` downloaded into the project (gitignored, not committed)."""
EVALUATE_CONTRACT = """The evaluate command is the referee. piloop runs it (the `evaluate` argv in piloop.toml) in a clean worktree of every
commit that a worker makes. It prints one `METRIC <name>=<value>` line for each metric under [[metrics]] and exits 0.
A non-zero exit is recorded as a crash, with the reason on stderr. Make it deterministic. Make sure that a worker
can change the score only through the code that it is supposed to change."""


class ProjectError(RuntimeError):
    """Registration failed. The message says why and what to do."""


def create_project_repo(dest: Path, template: Path, data: dict[str, str], *, ignore: str = GITIGNORE) -> list[str]:
    """Copy ``template`` to ``dest``, download ``data`` and make a git repo with a baseline commit.

    If ``dest`` already has a ``piloop.toml``, it is a project: only the missing ``.entire/`` files are copied.
    Files that already exist are never overwritten, so a second run is safe and keeps the user's edits.

    Args:
        dest: The project repo. It can already exist and can already be a git repo.
        template: The directory to copy. ``data/``, ``.git``, ``.venv`` and ``__pycache__`` are not copied.
        data: ``relative path → URL`` to download into ``dest``. These files are not committed, so ``ignore``
            must cover them.
        ignore: The ``.gitignore`` to write if the project has none.

    Returns:
        The repo-relative paths that this call wrote.
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
        # A new project has no lockfile yet, so an inherited UV_FROZEN (as in CI) must not apply.
        subprocess.run(['uv', 'sync', '--quiet'], cwd=dest, check=True, env=child_env('UV_FROZEN'))
    repo = Repo.init(dest, initial_branch='main') if not (dest / '.git').exists() else Repo(dest)
    repo.git.add('-A')
    if not repo.head.is_valid():
        repo.git.commit('-qm', f'{template.name}: baseline')
    elif repo.is_dirty(index=True):
        staged = repo.git.diff('--cached', '--name-only').splitlines()
        repo.git.commit('-qm', f'piloop init: {", ".join(staged)}')
    return sorted(written)


def _slug(dest: Path) -> str:
    """Make a directory name safe to use as a project name. Git refs and Herdr workspaces use this name."""
    return re.sub(r'[^A-Za-z0-9._-]+', '-', dest.resolve().name).strip('-') or NAME_PLACEHOLDER


def stub_untouched(path: Path) -> bool:
    """Return True if the project still uses the placeholder ``evaluate.py`` from the template."""
    stub = path / EVALUATE_STUB
    return (
        stub.is_file()
        and stub.read_bytes() == (BLANK / EVALUATE_STUB).read_bytes()
        and load_project_config(path).evaluate == load_project_config(BLANK).evaluate
    )


def register(s: Session, path: Path) -> tuple[Project, Measurement]:
    """Score HEAD in a clean worktree, then insert the project row and its baseline measurement.

    The evaluation runs before any write, in a staging directory under ``projects_dir``, with no transaction open.
    An evaluation can take an hour, and an open write transaction locks all other clients out of the database.
    The rows are inserted afterwards in one short transaction, and the staging directory becomes the project's.

    Raises:
        ProjectError: Another repo uses the name, or the evaluate command failed on HEAD.
    """
    cfg = load_project_config(path)
    other = s.scalars(select(Project).where(Project.name == cfg.name)).first()
    s.rollback()
    if other is not None:
        raise ProjectError(f'project name {cfg.name!r} is already used by {other.path}; change `name` in piloop.toml')
    repo = git.open_repo(path)
    sha = repo.head.commit.hexsha
    staging = settings.projects_dir / f'.registering-{uuid4().hex}'
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
        raise ProjectError(f'baseline failed to evaluate: {result.error}; its log is in {log_dir}')
    project = Project(
        name=cfg.name, path=str(path.resolve()), repo=git.identity(path), config=cfg.model_dump(mode='json')
    )
    s.add(project)
    s.flush()
    shutil.copytree(staging, settings.projects_dir / str(project.id), symlinks=True, dirs_exist_ok=True)
    shutil.rmtree(staging)
    baseline, _ = resolve(s, project.id, repo, sha, parent=None, run_id=None)
    baseline.verdict, baseline.verdict_by = Verdict.KEEP, DAEMON
    measurement = Measurement(
        candidate_id=baseline.id,
        evaluator_id=current_evaluator(s, project, cfg).id,
        metrics=result.metrics,
        seconds=result.seconds,
    )
    s.add(measurement)
    s.flush()
    save_units(settings.units_dir, measurement.id, result.units)
    emit(s, 'project.added', TargetKind.PROJECT, project.id, actor='cli', payload={'baseline': baseline.id})
    s.commit()
    return project, measurement


def registered(s: Session, path: Path) -> Project | None:
    """Return the project row for a repo path, or None."""
    return s.scalars(select(Project).where(Project.path == str(path.resolve()))).first()


def find(s: Session, ref: str) -> Project | None:
    """Find a project by name, else by a path inside its repo.

    The name is tried first, because a bare name can also be a directory in the current directory.
    """
    project = s.scalars(select(Project).where(Project.name == ref)).first()
    if project is None and Path(ref).exists():
        project = checked_out(s, Path(ref))
    return project


def checked_out(s: Session, path: Path) -> Project | None:
    """Return the project whose repo contains ``path``: the repo, a subdirectory or a linked git worktree.

    The nearest registered ancestor wins. The main worktree of the repo that contains ``path`` is tried last.
    """
    path = path.resolve()
    roots = [str(p) for p in (path, *path.parents)]
    with suppress(InvalidGitRepositoryError, NoSuchPathError):
        roots.append(str(Path(Repo(path, search_parent_directories=True).common_dir).resolve().parent))
    by_path = {p.path: p for p in s.scalars(select(Project).where(Project.path.in_(roots)))}
    return next((by_path[root] for root in roots if root in by_path), None)


def sync(s: Session, project: Project) -> ProjectConfig:
    """Read the repo's ``piloop.toml`` into the project row again.

    A change to ``evaluate``, ``metrics`` or ``link`` makes a new evaluator. A stopped project whose ``[stop]`` table
    changed becomes active again, because the human changed the stop conditions. The repo's identity is read again
    too, because a remote may have been added since.
    """
    cfg = load_project_config(Path(project.path))
    project.repo = git.identity(Path(project.path))
    config = cfg.model_dump(mode='json')
    if project.status is ProjectStatus.STOPPED and config['stop'] != project.config.get('stop'):
        project.status, project.stopped_reason = ProjectStatus.ACTIVE, None
    project.config = config
    current_evaluator(s, project, cfg)
    return cfg


def worktree_links(cfg: ProjectConfig, project_path: Path, project_dir: Path) -> dict[str, Path]:
    """Return worktree-relative name → target for the linked paths, the shared workspace and Entire's settings.

    ``.entire`` is linked only into a worktree whose tree does not have it, for example a candidate made before
    ``piloop init`` added it. Entire reads its settings from the worktree it runs in. Without them, it silently
    falls back to a shadow branch.
    """
    workspace = project_dir / WORKSPACE
    workspace.mkdir(parents=True, exist_ok=True)
    return {
        **{rel: project_path / rel for rel in cfg.link},
        WORKSPACE: workspace,
        ENTIRE_DIR: project_path / ENTIRE_DIR,
    }
