"""The table of ``notes``, its schema as a first release declares it (``SCHEMA``), and as a later one (``LATER``)."""

from pathlib import Path

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from piloop.db.models import Base
from piloop.db.schema import Schema


class Note(Base):
    """A note on a campaign, which goes with it."""

    __tablename__ = 'notes'

    id: Mapped[int] = mapped_column(primary_key=True)
    campaign_id: Mapped[int] = mapped_column(ForeignKey('campaigns.id', ondelete='CASCADE'))
    body: Mapped[str]


SCHEMA = Schema(tables=(Note,), versions=Path(__file__).parent / 'migrations')
LATER = Schema(tables=(Note,), versions=Path(__file__).parent / 'later')
