"""``local``: the default memory provider, the lessons, documents and journal tables of the home's database."""

from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from pydantic import field_validator

from piloop.actor import Actor
from piloop.plugins.memory.contract import MemoryProvider, Synced

if TYPE_CHECKING:  # piloop journal imports this package: only the annotations name the database and the server
    from sqlalchemy.orm import Session

    from piloop.db.models import Campaign
    from piloop.plugins.contribution import Contribution


class LocalMemory(MemoryProvider):
    """``[plugins.memory] provider = "local"``: the memory in the home's database, which every campaign searches."""

    documents: list[str] = []
    """Repo-relative globs of text documents, for example ``["docs/evidence/**/*.md"]``: campaign reports, orchestrator
    logs, design notes. ``piloop init`` and the stop of the campaign copy them into the ``documents`` table, where
    every agent of every campaign can search them. Never name a document that is derived from validation or test
    data."""

    @field_validator('documents')
    @classmethod
    def _inside_the_repo(cls, globs: list[str]) -> list[str]:
        for pattern in globs:
            path = PurePosixPath(pattern)
            if not path.parts or path.is_absolute() or '..' in path.parts:
                raise ValueError(f'{pattern!r} is not a glob inside the repo: name files relative to its root, no ..')
        return globs

    def sync(self, s: Session, campaign: Campaign, actor: Actor) -> Synced:
        """Insert every file under the repo that ``documents`` matches and that is not in yet. It only flushes."""
        from piloop.plugins.memory.local.documents import synced  # here: piloop journal imports this package

        return synced(s, campaign, self.documents, actor)

    def contribution(self) -> Contribution:
        """The journal's route, ``POST /v1/journal``."""
        from piloop.plugins.contribution import Contribution  # here: piloop journal imports this package
        from piloop.plugins.memory.local.api import router

        return Contribution(router=router)
