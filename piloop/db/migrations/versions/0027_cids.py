"""``archives.manifest`` holds a CID: a 52-character base32 BLAKE3 digest becomes the text of its raw CID.

The digest is the same, so the store finds the same bytes. The manifest it names is still JSON: ``piloop migrate``
rewrites each as a DASL value once the schema is at head (``piloop.campaign.archive.migrate_manifests``).

The downgrade turns each raw CID back into its digest. It refuses a DRISL CID, a manifest already rewritten: restore
the database backup that the upgrade wrote, whose JSON manifests stay in the store.

Revision ID: 0027
Revises: 0026
"""

import base64
from collections.abc import Sequence

from alembic import op
from covalence import Cid, Codec
from sqlalchemy import text

revision: str = '0027'
down_revision: str | None = '0026'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ROWS = text('SELECT id, manifest FROM archives WHERE manifest IS NOT NULL')
SET = text('UPDATE archives SET manifest = :manifest WHERE id = :id')


def upgrade() -> None:
    con = op.get_bind()
    for id, digest in con.execute(ROWS).all():
        digest_bytes = base64.b32decode(digest.upper() + '====')
        con.execute(SET, {'id': id, 'manifest': str(Cid.from_parts('raw', 'blake3', digest_bytes))})


def downgrade() -> None:
    con = op.get_bind()
    for id, manifest in con.execute(ROWS).all():
        cid = Cid(manifest)
        if cid.codec != Codec('raw'):
            raise RuntimeError(
                f'archive {id} names a DASL manifest ({manifest}), which piloop before 0027 cannot read: restore the'
                ' database backup that the upgrade wrote (piloop.db.bak-0026-…) instead of downgrading'
            )
        con.execute(SET, {'id': id, 'manifest': base64.b32encode(cid.digest).decode().rstrip('=').lower()})
