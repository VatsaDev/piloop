"""The ``piloop candidate`` commands: import, measure again, note a regressed result, or override a verdict."""

from pathlib import Path
from typing import Annotated

import cyclopts

from piloop import git
from piloop.campaign import ops
from piloop.cli import common
from piloop.cli.common import ProjectRef
from piloop.config import config_of
from piloop.db.models import Candidate, Verdict
from piloop.scoring.ledger import (
    ImportRefused,
    RemeasureRefused,
    import_reference,
    request_remeasure,
)

app = cyclopts.App(
    name='candidate',
    help='Import a starting point, measure a candidate again, keep a regressed result in view, or override a verdict.',
)


@app.command(name='import')
def candidate_import(
    ref: ProjectRef,
    commit: Annotated[str, cyclopts.Parameter(help="A commit, branch or tag in the project's repo.")],
    /,
    *,
    summary: Annotated[
        str | None, cyclopts.Parameter(help="A summary of the candidate. Default: the commit's subject line.")
    ] = None,
) -> None:
    """Register the tree of a commit as a reference candidate: a starting point for an idea.

    The daemon measures it with the current evaluator. A reference is never kept, and it is never the head or a bar.
    It does not count in the kept total, the sparkline or a stop condition. To import a candidate of another project,
    fetch its `commit_sha` into this repo first (`git fetch <its repo> <sha>`).
    """
    with common.session() as s:
        project = common.project(s, ref)
        cfg = config_of(project)
        try:
            candidate = import_reference(
                s, project, cfg, git.open_repo(Path(project.path)), commit, summary=summary, actor=common.settings.actor
            )
        except ImportRefused as e:
            raise SystemExit(str(e)) from e
        s.commit()
        common.console.print(
            f'candidate#{candidate.id} reference: {candidate.summary}; the daemon measures it; start an idea from it'
            f' with `piloop idea add {project.name} "<title>" --base {candidate.id}`',
            markup=False,
        )


@app.command(name='remeasure')
def candidate_remeasure(
    candidate_id: Annotated[int, cyclopts.Parameter(help='The candidate to measure again.')],
    /,
) -> None:
    """Ask the daemon to measure a candidate again with the current evaluator.

    Use it when a fault of the host spoiled a measurement (a full disk, a lost GPU), or for one more repeat. The daemon
    measures it soon, after the head and the bars that runs need. A `crash` that the daemon judged is judged again with
    the new measurement. Any other verdict stays. A pending, unmeasured or recovered candidate is never measured.
    """
    with common.session() as s:
        candidate = s.get(Candidate, candidate_id)
        if candidate is None:
            raise SystemExit(f'no candidate#{candidate_id}')
        try:
            request_remeasure(s, candidate, common.settings.actor)
        except RemeasureRefused as e:
            raise SystemExit(str(e)) from e
        s.commit()
        common.console.print(
            f'candidate#{candidate.id} {candidate.verdict}: the daemon measures it again with the current evaluator;'
            ' a crash is judged again',
            markup=False,
        )


@app.command(name='note')
def candidate_note(
    candidate_id: Annotated[
        int, cyclopts.Parameter(help='The candidate: a measured discard, or one marked interesting already.')
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
        str | None,
        cyclopts.Parameter(
            help="The track of the follow-ups. Default: the track of the candidate's run, else the first track."
        ),
    ] = None,
) -> None:
    """Keep a regressed result in view as a lead, with follow-up seeds pinned to it.

    The verdict becomes `interesting`, with the reason `note: <reason>`. The candidate is not kept and sets no bar,
    but it stays listed and ideas can start from it. Each `--follow-up` is a seed whose `base_candidate_id` is the
    candidate. A worker files no follow-ups. When its candidate does not exist yet, a worker ends its commit message
    with a `Piloop-Note: <reason>` trailer instead, and the daemon applies the note when it judges the commit.
    """
    with common.refused(), common.session() as s:
        candidate, ideas = ops.note_candidate(
            s, candidate_id, reason, actor=common.settings.actor, follow_ups=follow_ups, track=track
        )
        s.commit()
        common.console.print(f'candidate#{candidate.id} interesting: {candidate.verdict_reason}', markup=False)
        for idea in ideas:
            common.console.print(f'idea#{idea.id} seed, pinned to candidate#{candidate.id}: {idea.title}', markup=False)


@app.command(name='verdict')
def candidate_verdict(
    candidate_id: Annotated[int, cyclopts.Parameter(help='The candidate.')],
    verdict: Annotated[Verdict, cyclopts.Parameter(help='keep, interesting, discard or crash.')],
    /,
    *,
    reason: Annotated[str, cyclopts.Parameter(help='Why, with ids: what the number does not say.')],
) -> None:
    """Override a candidate's judgement, with the reason.

    The verdict is then yours, signed as PILOOP_ACTOR, so the daemon never changes it. The `candidate.verdict` event
    records the old and new verdict with the reason. Only a judgement is overridden: pending, reference, unmeasured and
    recovered candidates are refused. A keep of a trunk run must build on the head, or it would drop what the head
    kept.
    """
    with common.refused(), common.session() as s:
        old = ops.set_verdict(s, candidate_id, verdict, reason=reason, actor=common.settings.actor)
        s.commit()
        common.console.print(f'candidate#{candidate_id} {old} → {verdict}: {reason.strip()}', markup=False)
