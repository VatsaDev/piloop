"""The sync of ``local``: copy the repo files that ``documents`` names into the ``documents`` table.

Agents then search them with ``documents_fts``, next to the ideas, comments and lessons of every campaign. A sync
inserts the text of each file once for each distinct content (``UNIQUE (campaign_id, path, hash)``), so running it
again does nothing, and an edited file is a new version.
"""

from pathlib import Path

from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from piloop.actor import Actor
from piloop.db.models import Campaign
from piloop.db.sql import digest
from piloop.plugins.memory.contract import Synced
from piloop.plugins.memory.local.tables import Document
from piloop.text import HEADING


def synced(s: Session, campaign: Campaign, globs: list[str], actor: Actor) -> Synced:
    """Insert every file under the repo that ``globs`` match and that is not in yet. It only flushes."""
    root = Path(campaign.path).resolve()
    added: list[str] = []
    unchanged: list[str] = []
    skipped: dict[str, str] = {}
    for rel in _matches(root, globs):
        path = root / rel
        if not path.resolve().is_relative_to(root):
            skipped[rel] = 'resolves outside the repo'
            continue
        try:
            text = path.read_text(encoding='utf-8')
        except UnicodeDecodeError as e:
            skipped[rel] = f'not UTF-8 text ({e.reason} at byte {e.start})'
            continue
        except OSError as e:
            skipped[rel] = f'unreadable ({e.strerror})'
            continue
        values = {
            'campaign_id': campaign.id,
            'path': rel,
            'hash': digest(text),
            'title': title_of(text, rel),
            'body': text,
            'created_by': actor.signature,
        }
        inserted = s.scalar(insert(Document).values(values).on_conflict_do_nothing().returning(Document.id))
        (added if inserted is not None else unchanged).append(rel)
    return Synced(added=added, unchanged=unchanged, skipped=skipped)


def title_of(text: str, path: str) -> str:
    """The first markdown heading, or else the file name without its suffix."""
    return next((m[2] for line in text.splitlines() if (m := HEADING.match(line))), Path(path).stem)


def _matches(root: Path, globs: list[str]) -> list[str]:
    """The repo-relative paths of the files that the globs match, each once, sorted."""
    return sorted({str(p.relative_to(root)) for pattern in globs for p in root.glob(pattern) if p.is_file()})
