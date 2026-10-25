"""``local``: the default secrets provider, the ``secrets`` table of the home's database.

The values are plain text in the database. ``piloop sql`` cannot read them (``piloop.db.sql``), and its backups hold
them as it does. A provider that seals them wraps this one.
"""

from collections.abc import Collection

from pydantic import SecretStr
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from piloop.db.models import Secret
from piloop.mcp import EnvName
from piloop.plugins.secrets.contract import Owner, SecretKey, SecretsProvider


class LocalSecrets(SecretsProvider):
    """``[plugins.secrets] provider = "local"``: the values in the home's database."""

    def put(self, s: Session, owner: Owner, name: EnvName, value: SecretStr) -> None:
        row = s.scalars(select(Secret).where(Secret.campaign_id.is_(owner), Secret.name == name)).first()
        row = row or Secret(campaign_id=owner, name=name)
        row.value = value.get_secret_value()
        s.add(row)

    def delete(self, s: Session, owner: Owner, name: EnvName) -> bool:
        gone = delete(Secret).where(Secret.campaign_id.is_(owner), Secret.name == name).returning(Secret.id)
        return s.execute(gone).first() is not None

    def names(self, s: Session) -> list[SecretKey]:
        rows = s.execute(
            select(Secret.campaign_id, Secret.name).order_by(
                Secret.campaign_id.is_not(None), Secret.campaign_id, Secret.name
            )
        )
        return [SecretKey(owner, name) for owner, name in rows]

    def get(self, s: Session, owner: Owner, names: Collection[EnvName]) -> dict[EnvName, SecretStr]:
        rows = s.execute(select(Secret.name, Secret.value).where(Secret.campaign_id.is_(owner), Secret.name.in_(names)))
        return {name: SecretStr(value) for name, value in rows}
