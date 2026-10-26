"""``local``: the default archive provider, which records each archiving in the home's database."""

from pathlib import Path

from sqlalchemy.orm import Session

from piloop.db.models import Campaign
from piloop.ids import TargetRef
from piloop.plugins.archive.contract import ArchiveConfig, ArchiveHost, Restored, Summary
from piloop.plugins.archive.local import store
from piloop.plugins.archive.local.archiver import ArchivePolicy, LocalArchiver
from piloop.plugins.cas.contract import Cas


class LocalArchive(ArchiveConfig):
    """``[plugins.archive] provider = "local"``: the archivings in the home's database."""

    def open(self, host: ArchiveHost) -> LocalArchiver:
        """The archiver, with the default policy and the host's reserve."""
        return LocalArchiver(host, ArchivePolicy(reserve=host.reserve))

    def restore(self, s: Session, cas: Cas, home: Path, campaign: Campaign, target: TargetRef | str | None) -> Restored:
        """Write the latest archived copy of each matching file back (``store.restore``)."""
        return store.restore(s, cas, home, campaign, target)

    def summary(self, s: Session) -> Summary:
        """The archivings of each campaign, and what is due (``store.summary``)."""
        return store.summary(s)
