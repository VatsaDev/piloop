"""The project history, rendered as markdown for an agent's brief and as tables for ``piloop history``.

The history has four parts:

- The ideas: who proposed each one, its family, its outcome, and its links to other ideas.
- The kept lineage: the gain of each keep over its parent, its idea, its commit, the Entire checkpoint of the
  session that made it, and the tokens that session used. An agent can run ``git show`` and
  ``entire checkpoint explain`` on these.
- The starting points: imported references that a pinned idea can start from.
- The latest research findings.

The size is bounded. 60 idea rows of about 560 characters and 10 research findings of about 850 characters come to
about 42 KB (about 11k tokens). Each kept candidate adds one line of about 230 characters.
"""

import io

from rich import box
from rich.console import Console, Group, RenderableType
from rich.table import Table
from rich.text import Text
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.campaign.ideas import links, superseded_by
from piloop.config import Metric, ProjectConfig
from piloop.db.models import Candidate, Comment, CommentKind, Idea, Project, TargetKind, count
from piloop.scoring.ledger import fallback_track, keeps, references, score, session_checkpoint, session_tokens, track_of
from piloop.text import NONE, cell
from piloop.usage import human

IDEAS_IN_HISTORY = 60
FINDINGS_IN_HISTORY = 10
HYPOTHESIS_CHARS = 140
FAMILY_CHARS = 30
FINDING_CHARS = 800
BRIEF_WIDTH = 1000
"""Wide enough that no bounded cell wraps. A wrapped cell breaks the markdown table."""
SHORT_SHA = 7
"""Enough of a commit SHA for ``git show``. Entire checkpoint ids (26-character ULIDs) are shown in full."""


def history(s: Session, project: Project, cfg: ProjectConfig) -> str:
    """Render the history as markdown for an agent's brief, with markdown tables and no colour."""
    out = io.StringIO()
    console = Console(file=out, width=BRIEF_WIDTH, color_system=None, force_terminal=False, legacy_windows=False)
    console.print(Group(*renderables(s, project, cfg, table_box=box.MARKDOWN, heading='## ')))
    text = out.getvalue()
    return '\n'.join(line.rstrip() for line in text.splitlines()).strip() + '\n'


def renderables(
    s: Session, project: Project, cfg: ProjectConfig, *, table_box: box.Box, heading: str = ''
) -> list[RenderableType]:
    """Build the ideas table, the kept lineage, the starting points and the last research findings.

    The agent brief (``history``, with ``heading='## '``) and ``piloop history`` use the same renderables.

    A research finding keeps up to ``FINDING_CHARS`` of its body. The lines after the first list the directions that
    the sweep rejected, and the next sweep must read them. Watchdog findings on the project are not shown. Watchdog
    findings on ideas show in the table. The ``links`` column shows what blocks an idea and which idea replaces
    which. This stops a sweep from proposing a superseded idea again or waiting on an idea that will never run.
    """
    tracks = cfg.all_tracks
    ideas = s.scalars(
        select(Idea).where(Idea.project_id == project.id).order_by(Idea.id.desc()).limit(IDEAS_IN_HISTORY)
    ).all()[::-1]
    total = count(s, select(Idea).where(Idea.project_id == project.id))
    findings = _last_findings(s, [i.id for i in ideas])
    newer = superseded_by(s, [i.id for i in ideas])
    evaluator_id = project.evaluator_id or 0
    kept = s.scalars(keeps(project).order_by(Candidate.id)).all()
    research = s.scalars(
        select(Comment)
        .where(Comment.target_kind == TargetKind.PROJECT, Comment.target_id == project.id)
        .where(Comment.kind == CommentKind.FINDING, Comment.author.like('research#%') | Comment.author.like('scout#%'))
        .order_by(Comment.id.desc())
        .limit(FINDINGS_IN_HISTORY)
    ).all()[::-1]
    older = f'; {total - len(ideas)} older omitted' if total > len(ideas) else ''
    track_column = ['track'] if cfg.multitrack else []
    on = "its track's primary" if cfg.multitrack else tracks[0].primary.name
    ideas_table = titled_table(
        f'{heading}History: ideas ({len(ideas)} of {total}{older})',
        [
            'id',
            'status',
            *track_column,
            'by',
            'family',
            'links',
            'prio',
            'title',
            'hypothesis',
            f'best candidate ({on})',
            'worker finding',
        ],
        table_box,
    )
    for i in ideas:
        ideas_table.add_row(
            str(i.id),
            str(i.status),
            *([i.track] if cfg.multitrack else []),
            i.created_by,
            cell(i.family or NONE, FAMILY_CHARS),
            links(i, newer),
            str(i.priority),
            cell(i.title),
            cell(i.hypothesis, HYPOTHESIS_CHARS),
            _outcome(s, i, fallback_track(cfg, i.track).primary, evaluator_id),
            cell(findings[i.id].body) if i.id in findings else NONE,
        )
    lineage = titled_table(
        f'{heading}History: kept lineage',
        [
            'candidate',
            'parent',
            *track_column,
            *(t.primary.name for t in tracks),
            "vs parent (its track's primary)" if cfg.multitrack else 'vs parent',
            'idea (by)',
            'commit',
            'checkpoint',
            'tokens',
            'summary',
        ],
        table_box,
    )
    for c in kept:
        tokens = session_tokens(s, c)
        track = track_of(cfg, c)
        lineage.add_row(
            f'#{c.id}',
            f'#{c.parent_id}' if c.parent_id else NONE,
            *([track.name] if cfg.multitrack else []),
            *(_value(c, t.primary, evaluator_id) for t in tracks),
            _delta(c, track.primary, evaluator_id),
            f'idea#{c.run.idea.id} ({c.run.idea.created_by})' if c.run is not None else NONE,
            c.commit_sha[:SHORT_SHA],
            session_checkpoint(s, c) or NONE,
            human(tokens) if tokens is not None else NONE,
            cell(c.summary or NONE),
        )
    title = Text(f'{heading}History: research findings (last {FINDINGS_IN_HISTORY})', style='bold')
    bodies = [Text(f'- {c.author}: {block(c.body).replace(chr(10), chr(10) + "  ")}') for c in research]
    starts = _starting_points(s, project, cfg, table_box, heading)
    return [ideas_table, Text(), lineage, Text(), *starts, title, *bodies]


def _starting_points(
    s: Session, project: Project, cfg: ProjectConfig, table_box: box.Box, heading: str
) -> list[RenderableType]:
    """List the imported references that a pinned idea can start from, with their deciding metrics."""
    found = references(s, project)
    if not found:
        return []
    deciding = [*(t.primary for t in cfg.all_tracks), *cfg.guardrails]
    table = titled_table(
        f'{heading}History: starting points (pin one with base_candidate_id; never kept)',
        ['candidate', 'commit', *(m.name for m in deciding), 'summary'],
        table_box,
    )
    evaluator_id = project.evaluator_id or 0
    for c in found:
        table.add_row(
            f'#{c.id}',
            c.commit_sha[:SHORT_SHA],
            *(_value(c, m, evaluator_id) for m in deciding),
            cell(c.summary or NONE),
        )
    return [table, Text()]


def titled_table(title: str, columns: list[str], table_box: box.Box) -> Table:
    """Make a table with a left-aligned title whose cells fold on a narrow terminal.

    The title never wraps to the width of the table. On a narrow table, a wrapped title splits a markdown heading over
    two lines.
    """
    heading = Text(title, no_wrap=True, overflow='ignore')
    table = Table(*columns, title=heading, title_justify='left', title_style='bold', box=table_box)
    for column in table.columns:
        column.overflow = 'fold'
    return table


def text_row(table: Table, *cells: str, style: str | None = None) -> None:
    """Add a row of plain-text cells, so that text such as ``[wip]`` in a title is not read as Rich markup."""
    table.add_row(*map(Text, cells), style=style)


def block(text: str, chars: int = FINDING_CHARS) -> str:
    """Return whole lines of ``text`` up to ``chars`` characters, then ``… (N more lines)`` for the rest.

    If the first line is longer than ``chars``, it is cut short, so that a one-line finding always shows.
    """
    lines = text.strip().splitlines() or [NONE]
    kept: list[str] = []
    used = 0
    for line in lines:
        if used + len(line) > chars:
            break
        kept.append(line)
        used += len(line) + 1
    if not kept:
        kept = [lines[0][: chars - 1] + '…']
    omitted = len(lines) - len(kept)
    return '\n'.join([*kept, f'… ({omitted} more lines)'] if omitted else kept)


def _value(c: Candidate, primary: Metric, evaluator_id: int) -> str:
    """Format the score as a short number, or ``—`` if the current evaluator did not measure the candidate."""
    value = score(c, evaluator_id, primary)
    return NONE if value is None else f'{value:g}'


def _delta(c: Candidate, primary: Metric, evaluator_id: int) -> str:
    """Format the signed gain of the primary metric over the parent, or ``—`` without a parent or a score."""
    value = score(c, evaluator_id, primary)
    base = score(c.parent, evaluator_id, primary) if c.parent is not None else None
    return NONE if value is None or base is None else f'{value - base:+g}'


def _last_findings(s: Session, idea_ids: list[int]) -> dict[int, Comment]:
    """Return the latest worker ``finding`` for each idea."""
    rows = s.scalars(
        select(Comment)
        .where(Comment.target_kind == TargetKind.IDEA, Comment.kind == CommentKind.FINDING)
        .where(Comment.target_id.in_(idea_ids))
        .order_by(Comment.id)
    )
    return {c.target_id: c for c in rows}


def _outcome(s: Session, idea: Idea, primary: Metric, evaluator_id: int) -> str:
    """Format the best candidate of the idea with its verdict and its delta against the run base."""
    produced = [(c, run) for run in idea.runs for c in run.candidates]
    if not produced:
        return NONE
    best: tuple[float, Candidate, int] | None = None
    for c, run in produced:
        value = score(c, evaluator_id, primary)
        if value is not None and (best is None or primary.better(value, best[0])):
            best = (value, c, run.base_candidate_id)
    if best is None:  # Every commit crashed in measurement or was skipped (Piloop-Eval: skip).
        c, _ = produced[-1]
        return f'{c.verdict} (candidate#{c.id})'
    value, c, base_id = best
    base = score(s.get_one(Candidate, base_id), evaluator_id, primary)
    delta = f' ({value - base:+g} vs candidate#{base_id})' if base is not None else ''
    return f'{c.verdict} {value:g}{delta} (candidate#{c.id})'
