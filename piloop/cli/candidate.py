"""The ``piloop candidate`` commands: import, measure again, note a regressed result, or override a verdict."""

from typing import Annotated

import cyclopts
from cyclopts import validators

from piloop.campaign import ops
from piloop.campaign.campaigns import repository
from piloop.cli import common
from piloop.cli.common import CampaignRef
from piloop.config import config_of
from piloop.db.models import Campaign, Candidate
from piloop.ids import CandidateId, TrackName
from piloop.plugins.verdict_rule.contract import Outcome
from piloop.scoring.ledger import (
    REPEATS,
    ImportRefused,
    RemeasureRefused,
    bar_of,
    import_reference,
    request_remeasure,
)
from piloop.scoring.verdict import label, rejudgeable

app = cyclopts.App(
    name='candidate',
    help='Import a starting point, measure a candidate again, keep a regressed result in view, or override a verdict.',
)


@app.command(name='import')
def candidate_import(
    ref: CampaignRef,
    commit: Annotated[str, cyclopts.Parameter(help="A commit, branch or tag in the campaign's repo.")],
    /,
    *,
    summary: Annotated[
        str | None, cyclopts.Parameter(help="A summary of the candidate. Default: the commit's subject line.")
    ] = None,
) -> None:
    """Register the tree of a commit as a reference candidate: a starting point for an idea.

    The daemon measures it with the current evaluator. A reference is never kept, and it is never the head or a bar.
    It does not count in the kept total, the sparkline or a stop condition. To import a candidate of another campaign,
    fetch its `commit_sha` into this repo first (`git fetch <its repo> <sha>`).
    """
    with common.session() as s:
        campaign = common.campaign(s, ref)
        cfg = config_of(campaign)
        try:
            candidate = import_reference(
                s,
                campaign,
                cfg,
                repository(campaign),
                commit,
                summary=summary,
                actor=common.settings.actor,
            )
        except ImportRefused as e:
            raise SystemExit(str(e)) from e
        s.commit()
        common.console.print(
            f'candidate#{candidate.id} reference: {candidate.summary}; the daemon measures it; start an idea from it'
            f' with `piloop idea add {campaign.name} "<title>" --base {candidate.id}`',
            markup=False,
        )


@app.command(name='remeasure')
def candidate_remeasure(
    candidate_id: Annotated[CandidateId, cyclopts.Parameter(help='The candidate to measure again.')],
    /,
    *,
    repeats: Annotated[
        int, cyclopts.Parameter(help='How many pairs of the candidate and its bar.', validator=validators.Number(gte=1))
    ] = REPEATS,
) -> None:
    """Ask the daemon to measure a candidate again, next to its bar, with the current evaluator.

    Use it when a fault of the host spoiled a measurement (a full disk, a lost GPU), or when load made it noisy. The
    daemon runs the evaluate command on the candidate and on its bar, one after the other, `--repeats` times, and judges
    a `crash`, `discard` or `interesting` that it gave again on these pairs. A keep, and a verdict that someone set,
    stay. It measures soon, after the head and the bars that runs need, also when the campaign is stopped: not while it
    is paused with `--evals`, or held by its stop rule. A commit that the daemon never measured, `skipped` or
    `recovered`, is measured and judged. A pending candidate is refused: its run measures and judges it.
    """
    with common.session() as s:
        candidate = s.get(Candidate, candidate_id)
        if candidate is None:
            raise SystemExit(f'no candidate#{candidate_id}')
        try:
            request_remeasure(s, candidate, common.settings.actor, repeats)
        except RemeasureRefused as e:
            raise SystemExit(str(e)) from e
        s.commit()
        campaign = s.get_one(Campaign, candidate.campaign_id)
        later = ops.measuring_held(s, campaign, config_of(campaign))
        bar = f'next to its bar candidate#{bar_of(candidate.parent).id}' if candidate.parent is not None else 'alone'
        effect = 'then judges it again' if rejudgeable(candidate) else 'its verdict stays'
        common.console.print(
            f'candidate#{candidate.id} {label(candidate)}: the daemon measures it {repeats} times {bar}{later}; {effect}',
            markup=False,
        )


@app.command(name='note')
def candidate_note(
    candidate_id: Annotated[
        CandidateId, cyclopts.Parameter(help='The candidate: a measured discard, or one marked interesting already.')
    ],
    reason: Annotated[str, cyclopts.Parameter(help='Why the tree is worth starting from although it regressed.')],
    /,
    *,
    follow_ups: Annotated[
        tuple[str, ...],
        cyclopts.Parameter(
            name='--follow-up', help='A seed pinned to the candidate, for research to develop. Repeatable.'
        ),
    ] = (),
    track: Annotated[
        TrackName | None,
        cyclopts.Parameter(
            help="The track of the follow-ups. Default: the track of the candidate's run, else the first track."
        ),
    ] = None,
) -> None:
    """Keep a regressed result in view as a lead, with follow-up seeds pinned to it.

    The verdict becomes `interesting`, with the cause `note` and your reason before the daemon's. The candidate is not
    kept and sets no bar, but it stays listed and ideas can start from it. Each `--follow-up` is a seed whose `base_candidate_id` is the
    candidate. An implementer files no follow-ups. When its candidate does not exist yet, an implementer ends its commit message
    with a `Piloop-Note: <reason>` trailer instead, and the daemon applies the note when it judges the commit.
    """
    with common.refused(), common.session() as s:
        candidate, ideas = ops.note_candidate(
            s, candidate_id, reason, actor=common.settings.actor, follow_ups=follow_ups, track=track
        )
        s.commit()
        common.console.print(
            f'candidate#{candidate.id} interesting ({candidate.cause}): {candidate.verdict_reason}', markup=False
        )
        for idea in ideas:
            common.console.print(f'idea#{idea.id} seed, pinned to candidate#{candidate.id}: {idea.title}', markup=False)


@app.command(name='verdict')
def candidate_verdict(
    candidate_id: Annotated[CandidateId, cyclopts.Parameter(help='The candidate.')],
    verdict: Annotated[Outcome, cyclopts.Parameter(help='keep, interesting, discard or crash.')],
    /,
    *,
    reason: Annotated[str, cyclopts.Parameter(help='Why, with ids: what the number does not say.')],
) -> None:
    """Override a candidate's judgement, with the reason.

    The verdict is then yours, signed as PILOOP_ACTOR, so the daemon never changes it. The `candidate.verdict` event
    records the old and new verdict with the reason. Only a judgement is overridden: pending, reference, recovered and
    skipped candidates are refused. A keep of a trunk run must build on the head, or it would drop what the head
    kept.
    """
    with common.refused(), common.session() as s:
        old = ops.set_verdict(s, candidate_id, verdict, reason=reason, actor=common.settings.actor)
        s.commit()
        common.console.print(f'candidate#{candidate_id} {old} → {verdict}: {reason.strip()}', markup=False)
