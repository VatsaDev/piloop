"""Research memory: copy the repo documents that ``[memory] documents`` names into the ``documents`` table.

``piloop init`` and the stop of a campaign run the sync. Agents then search the documents with ``documents_fts``,
next to the ideas, comments and lessons of every campaign. A sync inserts the text of each file once for each distinct
content (``UNIQUE (campaign_id, path, hash)``), so running it again does nothing, and an edited file is a new version.
"""

from pathlib import Path

from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import Session

from piloop.actor import Actor
from piloop.api.views import Synced
from piloop.config import CampaignConfig
from piloop.db.models import Campaign, Document
from piloop.db.sql import digest
from piloop.text import HEADING


def title_of(text: str, path: str) -> str:
    """The first markdown heading, or else the file name without its suffix."""
    return next((m[2] for line in text.splitlines() if (m := HEADING.match(line))), Path(path).stem)


def sync_documents(s: Session, campaign: Campaign, cfg: CampaignConfig, actor: Actor) -> Synced:
    """Insert every file under the repo that ``cfg.memory.documents`` matches and that is not in yet. It only flushes."""
    root = Path(campaign.path).resolve()
    added: list[str] = []
    unchanged: list[str] = []
    skipped: dict[str, str] = {}
    for rel in _matches(root, cfg.memory.documents):
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


def _matches(root: Path, globs: list[str]) -> list[str]:
    """The repo-relative paths of the files that the globs match, each once, sorted."""
    return sorted({str(p.relative_to(root)) for pattern in globs for p in root.glob(pattern) if p.is_file()})
