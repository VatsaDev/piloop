"""The campaign history, rendered as markdown for an agent's brief and as tables for ``piloop history``.

The history has four parts:

- The ideas: who proposed each one, its family, its outcome, and its links to other ideas. Their hypotheses and the
  implementers' findings on them are a query away (``ideas_fts``, ``comments``), so the table leaves them out.
- The kept lineage: the gain of each keep over its parent, its idea, its commit, the Entire checkpoint of the
  session that made it, and the tokens that session used. An agent can run ``git show`` and
  ``entire checkpoint explain`` on these.
- The starting points: imported references that a pinned idea can start from.
- The latest findings on the campaign, of every role, by id and first line.

The size is bounded. 60 idea rows of about 250 characters and 10 findings of up to about 250 characters come to
about 18 KB (about 4.5k tokens). Each kept candidate adds one line of about 230 characters.
"""

import io

from rich import box
from rich.console import Console, Group, RenderableType
from rich.table import Table
from rich.text import Text
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.campaign.ideas import links, superseded_by
from piloop.config import CampaignConfig, Metric
from piloop.db.models import Campaign, Candidate, Comment, CommentKind, Idea, count
from piloop.ids import EvaluatorId, TargetKind, TargetRef
from piloop.scoring.ledger import fallback_track, keeps, references, score, session_checkpoint, session_tokens, track_of
from piloop.text import NONE, cell, metric
from piloop.usage import human

IDEAS_IN_HISTORY = 60
FINDINGS_IN_HISTORY = 10
FAMILY_CHARS = 30
FIRST_LINE_CHARS = 200
"""How much of a comment's first line a brief shows. The rest is a query away: ``comment#<id>`` names the row."""
BRIEF_WIDTH = 1000
"""Wide enough that no bounded cell wraps. A wrapped cell breaks the markdown table."""
SHORT_SHA = 7
"""Enough of a commit SHA for ``git show``. Entire checkpoint ids (26-character ULIDs) are shown in full."""


def history(s: Session, campaign: Campaign, cfg: CampaignConfig) -> str:
    """Render the history as markdown for an agent's brief, with markdown tables and no colour."""
    out = io.StringIO()
    console = Console(file=out, width=BRIEF_WIDTH, color_system=None, force_terminal=False, legacy_windows=False)
    console.print(Group(*renderables(s, campaign, cfg, table_box=box.MARKDOWN, heading='## ')))
    text = out.getvalue()
    return '\n'.join(line.rstrip() for line in text.splitlines()).strip() + '\n'


def renderables(
    s: Session, campaign: Campaign, cfg: CampaignConfig, *, table_box: box.Box, heading: str = ''
) -> list[RenderableType]:
    """Build the ideas table, the kept lineage, the starting points and the last findings on the campaign.

    The agent brief (``history``, with ``heading='## '``) and ``piloop history`` use the same renderables.

    The findings are those on the campaign of every agent, whatever its role (an author ``<role>#<id>``), each by its id
    and its first line. The findings of the orchestrator and the human are in the pack's digests, and the latest
    implementer findings in the pack's findings. The ``links`` column shows what blocks an idea
    and which idea replaces which. This stops a sweep from proposing a superseded idea again or waiting on an idea that
    will never run.
    """
    tracks = cfg.all_tracks
    ideas = s.scalars(
        select(Idea).where(Idea.campaign_id == campaign.id).order_by(Idea.id.desc()).limit(IDEAS_IN_HISTORY)
    ).all()[::-1]
    total = count(s, select(Idea).where(Idea.campaign_id == campaign.id))
    newer = superseded_by(s, [i.id for i in ideas])
    evaluator_id = campaign.evaluator_id
    kept = s.scalars(keeps(campaign).order_by(Candidate.id)).all()
    campaign_findings = s.scalars(
        select(Comment)
        .where(Comment.target == TargetRef(TargetKind.CAMPAIGN, campaign.id))
        .where(Comment.kind == CommentKind.FINDING, Comment.author.like('%#%'))
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
            f'best candidate ({on})',
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
            _outcome(s, i, fallback_track(cfg, i.track).primary, evaluator_id),
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
    title = Text(f'{heading}History: findings on the campaign (last {FINDINGS_IN_HISTORY})', style='bold')
    bodies = [comment_line(c, c.author) for c in campaign_findings]
    starts = _starting_points(s, campaign, cfg, table_box, heading)
    return [ideas_table, Text(), lineage, Text(), *starts, title, *bodies]


def _starting_points(
    s: Session, campaign: Campaign, cfg: CampaignConfig, table_box: box.Box, heading: str
) -> list[RenderableType]:
    """List the imported references that a pinned idea can start from, with their deciding metrics."""
    found = references(s, campaign)
    if not found:
        return []
    deciding = [*(t.primary for t in cfg.all_tracks), *cfg.guardrails]
    table = titled_table(
        f'{heading}History: starting points (pin one with base_candidate_id; never kept)',
        ['candidate', 'commit', *(m.name for m in deciding), 'summary'],
        table_box,
    )
    evaluator_id = campaign.evaluator_id
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


def comment_line(c: Comment, head: str) -> Text:
    """One line for a comment in a brief: ``- comment#<id> <head>: <first line>``, and how many lines it leaves out."""
    lines = c.body.strip().splitlines() or [NONE]
    more = f' (+{len(lines) - 1} lines)' if len(lines) > 1 else ''
    return Text(f'- comment#{c.id} {head}: {cell(lines[0], FIRST_LINE_CHARS)}{more}')


def _value(c: Candidate, primary: Metric, evaluator_id: EvaluatorId | None) -> str:
    """Format the score as a short number, or ``—`` if the current evaluator did not measure the candidate."""
    value = score(c, evaluator_id, primary)
    return NONE if value is None else metric(value)


def _delta(c: Candidate, primary: Metric, evaluator_id: EvaluatorId | None) -> str:
    """Format the signed gain of the primary metric over the parent, or ``—`` without a parent or a score."""
    value = score(c, evaluator_id, primary)
    base = score(c.parent, evaluator_id, primary) if c.parent is not None else None
    return NONE if value is None or base is None else f'{value - base:+g}'


def _outcome(s: Session, idea: Idea, primary: Metric, evaluator_id: EvaluatorId | None) -> str:
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
    return f'{c.verdict} {metric(value)}{delta} (candidate#{c.id})'
