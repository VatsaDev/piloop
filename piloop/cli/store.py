"""The commands of the CAS that archived files live in: ``piloop restore``, and ``piloop store`` ``status``, ``evict``, ``verify``."""

from collections import Counter
from typing import Annotated

import cyclopts
from rich import box
from rich.filesize import decimal
from rich.table import Table
from sqlalchemy import select

from piloop.campaign.campaigns import workspace_dir
from piloop.cli import common, local
from piloop.cli.common import CampaignRef
from piloop.ids import REF_PATTERN, TargetKind, TargetRef
from piloop.plugins.archive.contract import ARCHIVES
from piloop.plugins.archive.local.tables import Archive
from piloop.plugins.cas.contract import CAS, Integrity, Upkeep, integrity
from piloop.store.tree import Manifest

app = cyclopts.App(
    name='store', help='The artifact store that archived workspace files live in: what it holds, eviction, checks.'
)


def _target(given: str | None) -> TargetRef | str | None:
    """What ``piloop restore`` writes back: a subject (``idea#12``, or ``12`` for ``idea#12``), else a path, else all."""
    spelled = f'{TargetKind.IDEA}#{given}' if given is not None and given.isdigit() else given
    ref = REF_PATTERN.fullmatch(spelled or '')
    return TargetRef(TargetKind(ref['kind']), int(ref['id'])) if ref and ref['kind'] in TargetKind else given


def restore(
    ref: CampaignRef,
    target: Annotated[
        str | None,
        cyclopts.Parameter(
            help='A subject (`idea#12`, or `12` for an idea): its files; else a workspace-relative path: it or'
            ' everything under it. Left out: every archived file.'
        ),
    ] = None,
    /,
) -> None:
    """Write archived files back into the campaign's shared workspace: agents that start after get them in `./workspace/`.

    The daemon archives a settled subject's workspace files, such as a done idea's, and what is left when a campaign
    stops, into the artifact store and removes the local copies. This brings them back from the local store or its
    remote. A file that is already there with other content is kept, never overwritten.
    """
    parsed = _target(target)
    with local.session() as s:
        campaign = local.campaign(s, ref)
        restored = ARCHIVES.selected(common.settings.providers).restore(
            s, local.cas(), common.settings.home, campaign, parsed
        )
    done = restored.done
    if done.written + done.same + len(done.kept) == 0:
        what = parsed or 'anything'
        raise SystemExit(f'nothing archived matches {what} in {restored.archives} archive(s) of {campaign.name}')
    workspace = workspace_dir(common.settings.home, campaign.id)
    common.console.print(
        f'restored {done.written} file(s), {decimal(done.bytes)}, into {workspace}; {done.same} already there',
        markup=False,
        highlight=False,
        soft_wrap=True,
    )
    if done.kept:
        shown = ', '.join(done.kept[:10]) + (f' and {len(done.kept) - 10} more' if len(done.kept) > 10 else '')
        common.console.print(
            f'kept {len(done.kept)} path(s) that hold other content or sit below a symlink: {shown}',
            markup=False,
            soft_wrap=True,
        )


@app.command(name='status')
def store_status() -> None:
    """What the store holds, where, and how much of each campaign's workspace was archived into it."""
    cas = local.cas()
    line = f'cas: {CAS.name_of(CAS.selected(common.settings.providers))}'
    if isinstance(cas, Upkeep):
        usage = cas.usage()
        line += (
            f'; {usage.blobs} blob(s), {usage.local} local ({decimal(usage.local_bytes)}), {usage.remote} on'
            f' {usage.where}'
        )
    common.console.print(line, markup=False, highlight=False, soft_wrap=True)
    table = Table('campaign', 'archives', 'files', 'archived', box=box.SIMPLE_HEAD)
    with local.session() as s:
        summary = ARCHIVES.selected(common.settings.providers).summary(s)
    for each in summary.campaigns:
        table.add_row(each.name, str(each.archivings), str(each.files), decimal(each.bytes))
    common.console.print(table)
    latest = summary.latest
    since = f'; latest archiving {latest:%Y-%m-%d %H:%M} UTC' if latest is not None else ''
    common.console.print(
        f'to archive: {summary.subjects_due} settled subject(s), {summary.campaigns_due} stopped campaign(s){since}'
        ' (the daemon works through them in the background, one at a time)',
        markup=False,
    )


@app.command(name='evict')
def store_evict() -> None:
    """Upload what the remote lacks, then drop local copies as `[plugins.cas]` says; the daemon does it hourly too."""
    cas = local.cas()
    if not isinstance(cas, Upkeep):
        raise SystemExit('the CAS keeps no local copies to evict: only a cache over a remote does')
    done = cas.flush(evict=True)
    common.console.print(f'uploaded {done.uploaded} blob(s), evicted {done.evicted} ({decimal(done.freed)})')


@app.command(name='verify')
def store_verify(ref: CampaignRef | None = None, /) -> None:
    """Check that every blob of every archive is in the store: local copies are re-hashed, evicted ones asked of the remote.

    Exits non-zero when one is missing or corrupt.
    """
    cas = local.cas()
    with local.session() as s:
        query = select(Archive.manifest).where(Archive.manifest.is_not(None))
        if ref is not None:
            query = query.where(Archive.campaign_id == local.campaign(s, ref).id)
        manifests = [m for m in s.scalars(query).all() if m is not None]
    cids = set(manifests)
    for manifest in manifests:
        if integrity(cas, manifest) is Integrity.OK:
            cids |= Manifest.get(cas, manifest).blobs
    found = Counter(integrity(cas, cid) for cid in cids)
    common.console.print(
        f'{len(manifests)} manifest(s), {len(cids)} blob(s): {found[Integrity.OK]} ok, {found[Integrity.MISSING]} missing,'
        f' {found[Integrity.CORRUPT]} corrupt'
    )
    if found[Integrity.MISSING] or found[Integrity.CORRUPT]:
        raise SystemExit(1)
