"""The artifact store's commands: ``piloop restore``, and ``piloop store status``, ``evict`` and ``verify``."""

from collections import Counter
from typing import Annotated

import cyclopts
from rich import box
from rich.filesize import decimal
from rich.table import Table
from sqlalchemy import func, select

from piloop.campaign import archive
from piloop.campaign.project import workspace_dir
from piloop.cli import common
from piloop.cli.common import ProjectRef
from piloop.db.models import Archive, Project
from piloop.store.config import open_store

app = cyclopts.App(
    name='store', help='The artifact store that archived workspace files live in: what it holds, eviction, checks.'
)


def restore(
    ref: ProjectRef,
    target: Annotated[
        str | None,
        cyclopts.Parameter(
            help='An idea (`12` or `idea#12`): its files; else a workspace-relative path: it or everything under it.'
            ' Left out: every archived file.'
        ),
    ] = None,
    /,
) -> None:
    """Write archived files back into the project's workspace (`./workspace/` in every agent's worktree).

    The daemon archives a done idea's workspace files, and what is left when a campaign stops, into the artifact store
    and removes the local copies. This brings them back from the local store or its remote. A file that is already
    there with other content is kept, never overwritten.
    """
    idea = target.removeprefix('idea#') if target is not None else None
    parsed: int | str | None = int(idea) if idea is not None and idea.isdigit() else target
    with common.session() as s:
        project = common.project(s, ref)
        restored = archive.restore(s, open_store(common.settings), common.settings.home, project, parsed)
    done = restored.done
    if done.written + done.same + len(done.kept) == 0:
        what = f'idea#{parsed}' if isinstance(parsed, int) else parsed or 'anything'
        raise SystemExit(f'nothing archived matches {what} in {restored.archives} archive(s) of {project.name}')
    workspace = workspace_dir(common.settings.home, project.id)
    common.console.print(
        f'restored {done.written} file(s), {decimal(done.bytes)}, into {workspace}; {done.same} already there',
        markup=False,
    )
    if done.kept:
        shown = ', '.join(done.kept[:10]) + (f' and {len(done.kept) - 10} more' if len(done.kept) > 10 else '')
        common.console.print(f'kept {len(done.kept)} existing file(s) whose content differs: {shown}', markup=False)


@app.command(name='status')
def store_status() -> None:
    """What the store holds, where, and how much of each project's workspace was archived into it."""
    store = open_store(common.settings)
    usage = store.usage()
    remote = f'{usage.remote} on {store.remote.url}' if store.remote is not None else 'no remote ([storage.remote])'
    common.console.print(
        f'{store.root}: {usage.blobs} blob(s), {usage.local} local ({decimal(usage.local_bytes)}), {remote}',
        markup=False,
    )
    table = Table('project', 'archives', 'files', 'archived', box=box.SIMPLE_HEAD)
    with common.session() as s:
        rows = s.execute(
            select(Project.name, func.count(Archive.id), func.sum(Archive.files), func.sum(Archive.bytes))
            .join(Archive, Archive.project_id == Project.id)
            .group_by(Project.id)
            .order_by(Project.id)
        ).all()
        owed = archive.backlog(s)
        latest = s.scalar(select(func.max(Archive.created_at)))
    for name, count, files, size in rows:
        table.add_row(name, str(count), str(files), decimal(size))
    common.console.print(table)
    since = f'; latest archiving {latest:%Y-%m-%d %H:%M} UTC' if latest is not None else ''
    common.console.print(
        f'to archive: {owed.ideas} done idea(s), {owed.projects} stopped project(s){since}'
        ' (the daemon works through them in the background, one at a time)',
        markup=False,
    )


@app.command(name='evict')
def store_evict() -> None:
    """Upload what the remote lacks, then drop local copies as `[storage]` says; the daemon does it hourly too."""
    done = open_store(common.settings).evict()
    common.console.print(f'uploaded {done.uploaded} blob(s), evicted {done.evicted} ({decimal(done.freed)})')


@app.command(name='verify')
def store_verify(ref: ProjectRef | None = None, /) -> None:
    """Check that every blob of every archive is in the store: local copies are re-hashed, evicted ones asked of the remote.

    Exits non-zero when one is missing or corrupt.
    """
    store = open_store(common.settings)
    with common.session() as s:
        query = select(Archive.manifest).where(Archive.manifest.is_not(None))
        if ref is not None:
            query = query.where(Archive.project_id == common.project(s, ref).id)
        manifests = [m for m in s.scalars(query).all() if m is not None]
    hashes = set(manifests)
    for manifest in manifests:
        if store.check(manifest) == 'ok':
            hashes |= store.manifest(manifest).blobs
    found = Counter(store.check(h) for h in sorted(hashes))
    common.console.print(
        f'{len(manifests)} manifest(s), {len(hashes)} blob(s): {found["ok"]} ok, {found["missing"]} missing,'
        f' {found["corrupt"]} corrupt'
    )
    if found['missing'] or found['corrupt']:
        raise SystemExit(1)
