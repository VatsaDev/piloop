"""The ``piloop candidate`` commands: import a starting point, or note a regressed result as a lead."""

from pathlib import Path
from typing import Annotated

import cyclopts

from piloop import git
from piloop.cli import common
from piloop.cli.common import ProjectRef
from piloop.cli.idea import filing, track_for
from piloop.config import config_of
from piloop.db.models import Candidate, Project
from piloop.scoring.ledger import ImportRefused, NoteRefused, follow_up, import_reference, note, track_of

app = cyclopts.App(name='candidate', help='Import a starting point, or keep a regressed result in view as a lead.')


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
    with common.session() as s:
        candidate = s.get(Candidate, candidate_id)
        if candidate is None:
            raise SystemExit(f'no candidate#{candidate_id}')
        if follow_ups:
            filing(s.get_one(Project, candidate.project_id))
        try:
            note(candidate, reason, common.settings.actor)
        except NoteRefused as e:
            raise SystemExit(str(e)) from e
        project = s.get_one(Project, candidate.project_id)
        on = track_for(project, track) if track is not None else track_of(config_of(project), candidate).name
        ideas = follow_up(s, candidate, follow_ups, reason, common.settings.actor, track=on)
        s.commit()
        common.console.print(f'candidate#{candidate.id} interesting: {candidate.verdict_reason}', markup=False)
        for idea in ideas:
            common.console.print(f'idea#{idea.id} seed, pinned to candidate#{candidate.id}: {idea.title}', markup=False)
