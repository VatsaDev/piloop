"""The research pack: the context that the daemon gives each research sweep.

Ideas written with broad context produce more keeps. The daemon builds this pack from the database and the project
repo when a research sweep or a progress review starts. ``piloop history`` prints it too. It contains:

- the metrics with their roles, tracks, margins and floors, and the start and the head on each;
- the keeps that the ideas of each source produced (``piloop.campaign.ideas.yields``);
- the analysis artifacts (``[roles.research] context``) and the shared workspace, newest first, as file pointers
  with an outline;
- what was tried, by family (``ideas.family``), and the seeds that other roles filed for research to develop;
- the history (``piloop.reports.history``);
- the latest worker findings, the digests and decisions of the orchestrator and the human, and the open escalations.

The size is bounded. Each list keeps its latest rows, each body keeps a fixed number of characters, and files are
pointers, not inlined content. On top of the history, the pack adds at most about 45 KB (about 11k tokens).
"""

import io
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from rich import box
from rich.console import Console, Group, RenderableType
from rich.table import Table
from rich.text import Text
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.campaign.ideas import yields
from piloop.campaign.inbox import project_of
from piloop.config import MetricRole, ProjectConfig
from piloop.db.models import Candidate, Comment, CommentKind, Idea, IdeaStatus, Project, TargetKind, Verdict
from piloop.db.triggers import DAEMON_ACTOR
from piloop.reports.history import BRIEF_WIDTH, block, renderables as history, text_row, titled_table
from piloop.scoring.ledger import head, metrics
from piloop.text import NONE, cell, metric

ARTIFACTS_SHOWN = 12
WORKSPACE_SHOWN = 10
OUTLINE_CHARS = 600
"""The maximum length of an artifact outline: its markdown headings down to level 3, on one line."""
HEADING = re.compile(r'#{1,3} ')
SEEDS_SHOWN = 20
POINTERS_CHARS = 400
FINDINGS_SHOWN = 8
FINDING_CHARS = 800
DIGESTS_SHOWN = 5
DIGEST_CHARS = 1000
ESCALATIONS_SHOWN = 8
ESCALATION_CHARS = 300
FAMILY_IDEAS_CHARS = 200
OUTCOMES = (Verdict.KEEP, Verdict.INTERESTING, Verdict.DISCARD, Verdict.CRASH, Verdict.UNMEASURED, Verdict.PENDING)
"""Verdicts, best first. The outcome of an idea is the first verdict in this list that one of its candidates has."""


def pack(s: Session, project: Project, cfg: ProjectConfig, *, workspace: Path | None) -> str:
    """Render the pack as markdown for the brief of a sweep, with markdown tables and no colour."""
    out = io.StringIO()
    console = Console(file=out, width=BRIEF_WIDTH, color_system=None, force_terminal=False, legacy_windows=False)
    console.print(Group(*renderables(s, project, cfg, table_box=box.MARKDOWN, heading='## ', workspace=workspace)))
    return '\n'.join(line.rstrip() for line in out.getvalue().splitlines()).strip() + '\n'


def renderables(
    s: Session, project: Project, cfg: ProjectConfig, *, table_box: box.Box, heading: str = '', workspace: Path | None
) -> list[RenderableType]:
    """Build every section in reading order. ``heading='## '`` makes the titles markdown headings."""
    return [
        _metrics(s, project, cfg, table_box, heading),
        Text(),
        _yields(s, project, table_box, heading),
        Text(),
        *_artifacts(project, cfg, workspace, heading),
        _families(s, project, table_box, heading),
        Text(),
        *_seeds(s, project, heading),
        *history(s, project, cfg, table_box=table_box, heading=heading),
        Text(),
        *_worker_findings(s, project, heading),
        *_digests(s, project, heading),
        *_escalations(s, project, heading),
    ]


def _metrics(s: Session, project: Project, cfg: ProjectConfig, table_box: box.Box, heading: str) -> Table:
    """Show the definition of each metric, with the value of the start and of the head."""
    evaluator_id = project.evaluator_id or 0
    start = s.scalars(
        select(Candidate)
        .where(Candidate.project_id == project.id, Candidate.verdict != Verdict.REFERENCE)
        .order_by(Candidate.id)
        .limit(1)
    ).first()
    trunk = head(s, project)
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


def _yields(s: Session, project: Project, table_box: box.Box, heading: str) -> Table:
    """Show the ideas of each source and their outcomes, so research sees which sources find gains."""
    table = titled_table(
        f'{heading}Research pack: ideas by source (keeps, not ideas, measure a source)',
        ['source', 'ideas', 'ran', 'keep', 'interesting'],
        table_box,
    )
    for y in yields(s, project.id):
        text_row(table, y.source, str(y.ideas), str(y.ran), str(y.keeps), str(y.interesting))
    return table


def _artifacts(project: Project, cfg: ProjectConfig, workspace: Path | None, heading: str) -> list[RenderableType]:
    """List the analysis artifacts in ``[roles.research] context``, then the shared workspace, newest first.

    A pattern that matches no file is listed, so that a renamed report shows as missing instead of disappearing.
    """
    repo = Path(project.path)
    found: dict[str, Path] = {}
    missing: list[str] = []
    for pattern in cfg.roles.research.context:
        matches = [p for p in sorted(repo.glob(pattern)) if p.is_file()]
        missing += [] if matches else [pattern]
        found |= {str(p): p for p in matches}
    lines = _file_lines(found, ARTIFACTS_SHOWN)
    lines += [Text(f'- {pattern}: matches no file in {repo}') for pattern in missing]
    if not cfg.roles.research.context:
        lines = [Text('- None configured: `[roles.research] context` in piloop.toml lists them.')]
    title = f'{heading}Research pack: analysis artifacts (read before proposing; newest first)'
    sections: list[RenderableType] = [Text(title, style='bold'), *lines, Text()]
    if workspace is not None and workspace.is_dir():
        files = {f'./workspace/{p.relative_to(workspace)}': p for p in workspace.rglob('*') if p.is_file()}
        title = f'{heading}Research pack: shared workspace (what agents left in ./workspace/; newest first)'
        sections += [Text(title, style='bold'), *(_file_lines(files, WORKSPACE_SHOWN) or [Text('- Empty.')]), Text()]
    return sections


def _file_lines(files: dict[str, Path], shown: int) -> list[RenderableType]:
    """Make one line per file, newest first, with its size, its modification time and its markdown outline."""
    newest = sorted(files.items(), key=lambda kv: kv[1].stat().st_mtime, reverse=True)
    lines: list[RenderableType] = []
    for name, path in newest[:shown]:
        stat = path.stat()
        modified = datetime.fromtimestamp(stat.st_mtime, UTC).strftime('%Y-%m-%d %H:%M')
        outline = _outline(path)
        size = f'{len(path.read_text(errors="replace").splitlines())} lines' if outline else f'{stat.st_size} bytes'
        lines.append(Text(f'- {name} ({size}, modified {modified} UTC)' + (f'\n  {outline}' if outline else '')))
    if len(newest) > shown:
        lines.append(Text(f'- … and {len(newest) - shown} older files.'))
    return lines


def _outline(path: Path) -> str:
    """Join the headings of a markdown file, down to level 3 and outside code fences, on one line.

    Returns:
        The outline, or an empty string for a file that is not markdown.
    """
    if path.suffix.lower() not in ('.md', '.markdown'):
        return ''
    headings: list[str] = []
    fenced = False
    for line in path.read_text(errors='replace').splitlines():
        if line.startswith('```'):
            fenced = not fenced
        elif not fenced and HEADING.match(line):
            headings.append(line.strip())
    joined = ' · '.join(headings) or '(no headings)'
    return joined if len(joined) <= OUTLINE_CHARS else joined[: OUTLINE_CHARS - 1] + '…'


def _families(s: Session, project: Project, table_box: box.Box, heading: str) -> Table:
    """Show what was tried, by family: how many ideas, their outcomes, and which ideas.

    The outcome of an idea is the best verdict of the candidates of its runs. An idea that never ran shows its status.
    Ideas without a family share the ``—`` row. Families with a keep come first, then the families with more ideas.
    """
    ideas = s.scalars(select(Idea).where(Idea.project_id == project.id).order_by(Idea.id)).all()
    by_family: defaultdict[str, list[tuple[Idea, str]]] = defaultdict(list)
    for idea in ideas:
        by_family[idea.family or NONE].append((idea, _outcome(idea)))
    table = titled_table(
        f'{heading}Research pack: what was tried, by mechanism family (ideas.family)',
        ['family', 'ideas', 'keep', 'interesting', 'discard/crash', 'not run', 'ideas and outcomes'],
        table_box,
    )

    def count(rows: list[tuple[Idea, str]], *outcomes: str) -> int:
        return sum(outcome in outcomes for _, outcome in rows)

    order = sorted(by_family.items(), key=lambda kv: (-count(kv[1], Verdict.KEEP), -len(kv[1]), kv[0]))
    for family, rows in order:
        listed = ', '.join(f'#{i.id} {outcome}' for i, outcome in reversed(rows))
        text_row(
            table,
            cell(family, 40),
            str(len(rows)),
            str(count(rows, Verdict.KEEP)),
            str(count(rows, Verdict.INTERESTING)),
            str(count(rows, Verdict.DISCARD, Verdict.CRASH)),
            str(sum(not i.runs for i, _ in rows)),
            cell(listed, FAMILY_IDEAS_CHARS),
        )
    return table


def _outcome(idea: Idea) -> str:
    verdicts = {c.verdict for run in idea.runs for c in run.candidates}
    return next((str(v) for v in OUTCOMES if v in verdicts), str(idea.status))


def _seeds(s: Session, project: Project, heading: str) -> list[RenderableType]:
    """List the seeds that wait for research, oldest first, with their author, base candidate and pointers."""
    rows = s.scalars(
        select(Idea)
        .where(Idea.project_id == project.id, Idea.status == IdeaStatus.SEED)
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


def _worker_findings(s: Session, project: Project, heading: str) -> list[RenderableType]:
    """List the latest worker findings, with more text than the ideas table shows."""
    rows = s.scalars(
        select(Comment)
        .join(Idea, (Comment.target_kind == TargetKind.IDEA) & (Comment.target_id == Idea.id))
        .where(Idea.project_id == project.id, Comment.kind == CommentKind.FINDING, Comment.author.like('worker#%'))
        .order_by(Comment.id.desc())
        .limit(FINDINGS_SHOWN)
    ).all()
    title = Text(f'{heading}Research pack: latest worker findings (newest first)', style='bold')
    lines = [_body_line(f'comment#{c.id} {c.author} on idea#{c.target_id}', c.body, FINDING_CHARS) for c in rows]
    return [title, *(lines or [Text('- None yet.')]), Text()]


def _digests(s: Session, project: Project, heading: str) -> list[RenderableType]:
    """List the latest findings and decisions of the orchestrator or a human."""
    rows = s.scalars(
        select(Comment)
        .where(
            project_of(Comment.target_kind, Comment.target_id) == project.id,
            Comment.kind.in_((CommentKind.FINDING, CommentKind.DECISION)),
            Comment.author.not_like('%#%'),
            Comment.author != DAEMON_ACTOR,
        )
        .order_by(Comment.id.desc())
        .limit(DIGESTS_SHOWN)
    ).all()
    title = Text(f'{heading}Research pack: orchestrator and human digests and decisions (newest first)', style='bold')
    lines = [
        _body_line(f'comment#{c.id} {c.kind} by {c.author} on {c.target_kind}#{c.target_id}', c.body, DIGEST_CHARS)
        for c in rows
    ]
    return [title, *(lines or [Text('- None yet.')]), Text()]


def _escalations(s: Session, project: Project, heading: str) -> list[RenderableType]:
    """List the unacknowledged escalations: open questions that an idea must not assume an answer to."""
    rows = s.scalars(
        select(Comment)
        .where(
            project_of(Comment.target_kind, Comment.target_id) == project.id,
            Comment.kind == CommentKind.ESCALATION,
            Comment.acked_at.is_(None),
        )
        .order_by(Comment.id.desc())
        .limit(ESCALATIONS_SHOWN)
    ).all()
    title = Text(f'{heading}Research pack: open escalations (unacknowledged, newest first)', style='bold')
    lines = [
        _body_line(
            f'comment#{c.id} p{c.priority} by {c.author} on {c.target_kind}#{c.target_id}', c.body, ESCALATION_CHARS
        )
        for c in rows
    ]
    return [title, *(lines or [Text('- None.')])]


def _body_line(head: str, body: str, chars: int) -> Text:
    return Text(f'- {head}: ' + block(body, chars).replace('\n', '\n  '))


def _num(value: float | None) -> str:
    return NONE if value is None else metric(value)
