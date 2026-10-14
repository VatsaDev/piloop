"""Research memory: copy the repo documents that ``[memory] documents`` names into the ``documents`` table.

``piloop init`` and the stop of a campaign run the sync. Agents then search the documents with ``documents_fts``,
next to the ideas, comments and lessons of every project. A sync inserts the text of each file once for each distinct
content (``UNIQUE (project_id, path, hash)``), so running it again does nothing, and an edited file is a new version.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path

from git import Repo
from git.exc import InvalidGitRepositoryError, NoSuchPathError
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from piloop.config import ProjectConfig
from piloop.db.models import Document, Project
from piloop.db.sql import digest

HEADING = re.compile(r'^#{1,6}\s+(.+?)\s*#*\s*$', re.MULTILINE)


def title_of(text: str, path: str) -> str:
    """The first markdown heading, or else the file name without its suffix."""
    match = HEADING.search(text)
    return match.group(1) if match else Path(path).stem


@dataclass
class Synced:
    """What one sync did: the files it added as a new version, the files already there, the files it could not read."""

    added: list[str] = field(default_factory=list)
    unchanged: list[str] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)
    """Repo-relative path → why the sync did not read it (not UTF-8 text, unreadable, outside the repo)."""

    def line(self) -> str:
        """One line for the console or a span event."""
        skipped = ''.join(f'; skipped {path}: {why}' for path, why in self.skipped.items())
        return f'documents: {len(self.added)} added, {len(self.unchanged)} unchanged{skipped}'


def sync_documents(s: Session, project: Project, cfg: ProjectConfig, actor: str) -> Synced:
    """Insert every file under the repo that ``cfg.memory.documents`` matches and that is not in yet. It only flushes."""
    root = Path(project.path).resolve()
    repo = _repo(root)
    synced = Synced()
    for rel in _matches(root, cfg.memory.documents):
        path = root / rel
        if not path.resolve().is_relative_to(root):
            synced.skipped[rel] = 'resolves outside the repo'
            continue
        try:
            text = path.read_text(encoding='utf-8')
        except UnicodeDecodeError as e:
            synced.skipped[rel] = f'not UTF-8 text ({e.reason} at byte {e.start})'
            continue
        except OSError as e:
            synced.skipped[rel] = f'unreadable ({e.strerror})'
            continue
        values = {
            'project_id': project.id,
            'path': rel,
            'hash': digest(text),
            'title': title_of(text, rel),
            'body': text,
            'commit_sha': _commit(repo, rel) if repo is not None else None,
            'created_by': actor,
        }
        inserted = s.scalar(insert(Document).values(values).on_conflict_do_nothing().returning(Document.id))
        (synced.added if inserted is not None else synced.unchanged).append(rel)
    return synced


def _matches(root: Path, globs: list[str]) -> list[str]:
    """The repo-relative paths of the files that the globs match, each once, sorted."""
    return sorted({str(p.relative_to(root)) for pattern in globs for p in root.glob(pattern) if p.is_file()})


def _repo(path: Path) -> Repo | None:
    try:
        return Repo(path)
    except InvalidGitRepositoryError, NoSuchPathError:
        return None


def _commit(repo: Repo, rel: str) -> str | None:
    """The commit that last changed ``rel``, when the file is tracked and clean. None otherwise."""
    if repo.is_dirty(path=rel) or not repo.git.ls_files('--', rel):
        return None
    return repo.git.log('-1', '--format=%H', '--', rel) or None
