"""The research pack: the context that the daemon gives each research sweep.

Ideas written with broad context produce more keeps. The daemon builds this pack from the database and the campaign
repo when a research sweep or a progress review starts. ``piloop history`` prints it too. It contains:

- the metrics with their roles, tracks, margins and floors, and the start and the head on each;
- the analysis artifacts (``context``) and the shared workspace, newest first, as file pointers
  with an outline;
- what was tried, by family (``ideas.family``), and the seeds that other roles filed for research to develop;
- the history (``piloop.reports.history``);
- the latest implementer findings, and the digests and decisions of the orchestrator and the human, by id and first
  line.

The ideas by source and the open escalations are in the campaign overview, which every sweep also reads.

The size is bounded. Each list keeps its latest rows, a comment is its first line, and files are pointers, not inlined
content. On top of the history, the pack adds at most about 15 KB (about 4k tokens).
"""

import heapq
import io
import os
from collections import defaultdict
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path

from rich import box
from rich.console import Console, Group, RenderableType
from rich.table import Table
from rich.text import Text
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from piloop.campaign.inbox import campaign_of
from piloop.config import CampaignConfig, MetricRole
from piloop.db.models import Campaign, Candidate, Comment, CommentKind, Idea, IdeaStatus, Verdict
from piloop.db.triggers import DAEMON_ACTOR
from piloop.ids import TargetKind
from piloop.reports.history import BRIEF_WIDTH, comment_line, renderables as history, text_row, titled_table
from piloop.scoring.ledger import head, metrics
from piloop.text import HEADING, NONE, cell, metric

ARTIFACTS_SHOWN = 12
WORKSPACE_SHOWN = 10
WALK_ENTRIES = 20_000
"""How much of the workspace the pack lists files from: a sweep's brief never waits for a walk of all of it."""
OUTLINE_CHARS = 600
"""The maximum length of an artifact outline: its markdown headings down to level 3, on one line."""
OUTLINE_LEVELS = 3
MARKDOWN = ('.md', '.markdown')
"""The suffixes of the files that the pack outlines."""
SEEDS_SHOWN = 20
POINTERS_CHARS = 400
FINDINGS_SHOWN = 8
DIGESTS_SHOWN = 5
OUTCOMES = (
    Verdict.KEEP,
    Verdict.INTERESTING,
    Verdict.DISCARD,
    Verdict.CRASH,
    Verdict.RECOVERED,
    Verdict.UNMEASURED,
    Verdict.PENDING,
)
"""Verdicts, best first. The outcome of an idea is the first verdict in this list that one of its candidates has."""


def pack(s: Session, campaign: Campaign, cfg: CampaignConfig, *, workspace: Path | None) -> str:
    """Render the pack as markdown for the brief of a sweep, with markdown tables and no colour."""
    out = io.StringIO()
    console = Console(file=out, width=BRIEF_WIDTH, color_system=None, force_terminal=False, legacy_windows=False)
    console.print(Group(*renderables(s, campaign, cfg, table_box=box.MARKDOWN, heading='## ', workspace=workspace)))
    return '\n'.join(line.rstrip() for line in out.getvalue().splitlines()).strip() + '\n'


def renderables(
    s: Session,
    campaign: Campaign,
    cfg: CampaignConfig,
    *,
    table_box: box.Box,
    heading: str = '',
    workspace: Path | None,
) -> list[RenderableType]:
    """Build every section in reading order. ``heading='## '`` makes the titles markdown headings."""
    return [
        _metrics(s, campaign, cfg, table_box, heading),
        Text(),
        *_artifacts(campaign, cfg, workspace, heading),
        _families(s, campaign, table_box, heading),
        Text(),
        *_seeds(s, campaign, heading),
        *history(s, campaign, cfg, table_box=table_box, heading=heading),
        Text(),
        *_agent_session_findings(s, campaign, cfg, heading),
        *_digests(s, campaign, heading),
    ]


def _metrics(s: Session, campaign: Campaign, cfg: CampaignConfig, table_box: box.Box, heading: str) -> Table:
    """Show the definition of each metric, with the value of the start and of the head."""
    evaluator_id = campaign.evaluator_id
    start = s.scalars(
        select(Candidate)
        .where(Candidate.campaign_id == campaign.id, Candidate.verdict != Verdict.REFERENCE)
        .order_by(Candidate.id)
        .limit(1)
    ).first()
    trunk = head(s, campaign)
    at_start = metrics(start, evaluator_id) if start is not None else {}
    at_head = metrics(trunk, evaluator_id) if trunk is not None else {}
    start_ref = f'candidate#{start.id}' if start is not None else 'no candidate'
    head_ref = f'candidate#{trunk.id}' if trunk is not None else 'no trunk keep'
    tracks = {t.primary.name: t.name for t in cfg.all_tracks}
    table = titled_table(
        f'{heading}Research pack: metrics (confidence {cfg.confidence:g}; start {start_ref}, head {head_ref})',
        [
            'metric',
            'role',
            *(['track'] if cfg.multitrack else []),
            'better',
            'margin',
            'floor',
            'start',
            'head',
            'gain',
        ],
        table_box,
    )
    for m in cfg.metrics:
        a, b = at_start.get(m.name), at_head.get(m.name)
        text_row(
            table,
            m.name,
            str(m.role),
            *(
                [tracks.get(m.name, 'every track' if m.role is MetricRole.GUARDRAIL else NONE)]
                if cfg.multitrack
                else []
            ),
            str(m.direction),
            _num(m.margin),
            _num(m.floor),
            _num(a),
            _num(b),
            NONE if a is None or b is None else f'{m.gain(b, a):+g}',
        )
    return table


def _artifacts(campaign: Campaign, cfg: CampaignConfig, workspace: Path | None, heading: str) -> list[RenderableType]:
    """List the analysis artifacts in ``context``, then the shared workspace, newest first.

    A pattern that matches no file is listed, so that a renamed report shows as missing instead of disappearing.
    """
    repo = Path(campaign.path)
    found: dict[str, Path] = {}
    missing: list[str] = []
    for pattern in cfg.context:
        matches = [p for p in sorted(repo.glob(pattern)) if p.is_file()]
        missing += [] if matches else [pattern]
        found |= {str(p): p for p in matches}
    lines = _file_lines(found, ARTIFACTS_SHOWN)
    lines += [Text(f'- {pattern}: matches no file in {repo}') for pattern in missing]
    if not cfg.context:
        lines = [Text('- None configured: `context` in piloop.toml lists them.')]
    title = f'{heading}Research pack: analysis artifacts (read before proposing; newest first)'
    sections: list[RenderableType] = [Text(title, style='bold'), *lines, Text()]
    if workspace is not None and workspace.is_dir():
        files, cut = _recent(workspace, WALK_ENTRIES)
        title = f'{heading}Research pack: shared workspace (what agents left in ./workspace/; newest first)'
        lines = _file_lines({f'./workspace/{p.relative_to(workspace)}': p for p in files}, WORKSPACE_SHOWN)
        if cut:
            lines.append(Text(f'- (listed from the {WALK_ENTRIES:,} entries of the most recently changed directories)'))
        sections += [Text(title, style='bold'), *(lines or [Text('- Empty.')]), Text()]
    return sections


def _recent(root: Path, budget: int) -> tuple[list[Path], bool]:
    """Files from a bounded walk under ``root``, and whether it stopped short of the end.

    The walk opens the most recently changed directory next and stops after ``budget`` entries. A new file changes its
    own directory's modification time (not its parents'), so recent files are mostly found early, and a workspace of a
    million files costs a sweep's brief ``budget`` entries. A directory that goes meanwhile (the archiver prunes) is
    skipped.
    """
    queue = [(-root.stat().st_mtime, str(root))]
    files: list[Path] = []
    seen = 0
    while queue:
        _, directory = heapq.heappop(queue)
        try:
            listing = os.scandir(directory)
        except FileNotFoundError:
            continue
        with listing as entries:
            for entry in entries:  # read as it goes: a directory of a million entries costs only the budget
                if seen == budget:
                    return files, True
                seen += 1
                if entry.is_dir(follow_symlinks=False):
                    with suppress(FileNotFoundError):
                        heapq.heappush(queue, (-entry.stat(follow_symlinks=False).st_mtime, entry.path))
                elif entry.is_file():
                    files.append(Path(entry.path))
    return files, False


def _file_lines(files: dict[str, Path], shown: int) -> list[RenderableType]:
    """Make one line per file, newest first, with its size, its modification time and its markdown outline."""
    stats = {}
    for name, path in files.items():
        with suppress(FileNotFoundError):  # archived meanwhile
            stats[name] = path.stat()
    newest = sorted(stats.items(), key=lambda kv: kv[1].st_mtime, reverse=True)
    lines: list[RenderableType] = []
    for name, stat in newest[:shown]:
        path = files[name]
        modified = datetime.fromtimestamp(stat.st_mtime, UTC).strftime('%Y-%m-%d %H:%M')
        size, outline = f'{stat.st_size} bytes', ''
        if path.suffix.lower() in MARKDOWN:
            try:
                text = path.read_text(errors='replace')
            except FileNotFoundError:  # archived and pruned since its stat
                continue
            size, outline = f'{len(text.splitlines())} lines', _outline(text)
        lines.append(Text(f'- {name} ({size}, modified {modified} UTC)' + (f'\n  {outline}' if outline else '')))
    if len(newest) > shown:
        lines.append(Text(f'- … and {len(newest) - shown} older files.'))
    return lines


def _outline(text: str) -> str:
    """Join the headings of a markdown text, down to level 3 and outside code fences, on one line."""
    headings: list[str] = []
    fenced = False
    for line in text.splitlines():
        if line.startswith('```'):
            fenced = not fenced
        elif not fenced and (m := HEADING.match(line)) and len(m[1]) <= OUTLINE_LEVELS:
            headings.append(line.strip())
    joined = ' · '.join(headings) or '(no headings)'
    return joined if len(joined) <= OUTLINE_CHARS else joined[: OUTLINE_CHARS - 1] + '…'


def _families(s: Session, campaign: Campaign, table_box: box.Box, heading: str) -> Table:
    """Show what was tried, by family: how many ideas, and their outcomes. The history's ideas table lists the ideas.

    The outcome of an idea is the best verdict of the candidates of its runs. An idea that never ran shows its status.
    Ideas without a family share the ``—`` row. Families with a keep come first, then the families with more ideas.
    """
    ideas = s.scalars(select(Idea).where(Idea.campaign_id == campaign.id).order_by(Idea.id)).all()
    by_family: defaultdict[str, list[tuple[Idea, str]]] = defaultdict(list)
    for idea in ideas:
        by_family[idea.family or NONE].append((idea, _outcome(idea)))
    table = titled_table(
        f'{heading}Research pack: what was tried, by mechanism family (ideas.family)',
        ['family', 'ideas', 'keep', 'interesting', 'discard/crash', 'not run'],
        table_box,
    )

    def count(rows: list[tuple[Idea, str]], *outcomes: str) -> int:
        return sum(outcome in outcomes for _, outcome in rows)

    order = sorted(by_family.items(), key=lambda kv: (-count(kv[1], Verdict.KEEP), -len(kv[1]), kv[0]))
    for family, rows in order:
        text_row(
            table,
            cell(family, 40),
            str(len(rows)),
            str(count(rows, Verdict.KEEP)),
            str(count(rows, Verdict.INTERESTING)),
            str(count(rows, Verdict.DISCARD, Verdict.CRASH)),
            str(sum(not i.runs for i, _ in rows)),
        )
    return table


def _outcome(idea: Idea) -> str:
    verdicts = {c.verdict for run in idea.runs for c in run.candidates}
    return next((str(v) for v in OUTCOMES if v in verdicts), str(idea.status))


def _seeds(s: Session, campaign: Campaign, heading: str) -> list[RenderableType]:
    """List the seeds that wait for research, oldest first, with their author, base candidate and pointers."""
    rows = s.scalars(
        select(Idea)
        .where(Idea.campaign_id == campaign.id, Idea.status == IdeaStatus.SEED)
        .order_by(Idea.id)
        .limit(SEEDS_SHOWN)
    ).all()
    title = Text(f'{heading}Research pack: seeds to develop (oldest first)', style='bold')
    if not rows:
        return [title, Text('- None.'), Text()]
    lines = [
        Text(
            f'- idea#{i.id} by {i.created_by}'
            + (f' on candidate#{i.base_candidate_id}' if i.base_candidate_id is not None else '')
            + f': {cell(i.title)} — pointers: {cell(i.provenance or NONE, POINTERS_CHARS)}'
        )
        for i in rows
    ]
    return [title, *lines, Text()]


def _agent_session_findings(s: Session, campaign: Campaign, cfg: CampaignConfig, heading: str) -> list[RenderableType]:
    """List the latest findings of the agents that did runs."""
    by_runs = or_(*(Comment.author.startswith(f'{name}#', autoescape=True) for name in cfg.roles.doing_runs()))
    rows = s.scalars(
        select(Comment)
        .join(Idea, (Comment.target_kind == TargetKind.IDEA) & (Comment.target_id == Idea.id))
        .where(Idea.campaign_id == campaign.id, Comment.kind == CommentKind.FINDING, by_runs)
        .order_by(Comment.id.desc())
        .limit(FINDINGS_SHOWN)
    ).all()
    title = Text(f'{heading}Research pack: latest implementer findings (newest first)', style='bold')
    lines = [comment_line(c, f'{c.author} on idea#{c.target_id}') for c in rows]
    return [title, *(lines or [Text('- None yet.')]), Text()]


def _digests(s: Session, campaign: Campaign, heading: str) -> list[RenderableType]:
    """List the latest findings and decisions of the orchestrator or a human."""
    rows = s.scalars(
        select(Comment)
        .where(
            campaign_of(Comment.target_kind, Comment.target_id) == campaign.id,
            Comment.kind.in_((CommentKind.FINDING, CommentKind.DECISION)),
            Comment.author.not_like('%#%'),
            Comment.author != DAEMON_ACTOR,
        )
        .order_by(Comment.id.desc())
        .limit(DIGESTS_SHOWN)
    ).all()
    title = Text(f'{heading}Research pack: orchestrator and human digests and decisions (newest first)', style='bold')
    lines = [comment_line(c, f'{c.kind} by {c.author} on {c.target}') for c in rows]
    return [title, *(lines or [Text('- None yet.')])]


def _num(value: float | None) -> str:
    return NONE if value is None else metric(value)
