"""The ``piloop idea`` commands: file a seed, develop it, or add a defined idea."""

from typing import Annotated

import cyclopts
from sqlalchemy import select
from sqlalchemy.orm import Session

from piloop.agents.roles import files_of
from piloop.campaign.ideas import Definition, NotASeed, develop
from piloop.cli import common
from piloop.cli.common import HARNESS_HELP, ProjectRef
from piloop.config import Files, config_of
from piloop.db.events import emit
from piloop.db.models import Candidate, Idea, IdeaLink, IdeaStatus, LinkKind, Project, TargetKind

app = cyclopts.App(name='idea', help='Propose work: file a seed for research to develop, or add a defined idea.')
PRIORITY_HELP = 'From 0 (urgent) to 4 (someday).'
FAMILY_HELP = 'The mechanism family. Ideas that try the same mechanism share it.'
BASE_HELP = 'The candidate id to start from instead of the head. This makes a branch.'
TRACK_HELP = 'The track it works on. Required when piloop.toml has more than one track.'
AFTER_HELP = 'An idea id to wait for. Repeatable. The idea runs from the head when all are done.'
SUPERSEDES_HELP = 'An idea id that this idea refines. Repeatable. The daemon rejects each one that never ran.'


@app.command(name='add')
def idea_add(
    ref: ProjectRef,
    title: Annotated[str, cyclopts.Parameter(help='A short title for the idea.')],
    /,
    *,
    hypothesis: Annotated[
        str, cyclopts.Parameter(help='Why this should help, and by how much. Default: the title.', show_default=False)
    ] = '',
    plan: Annotated[str | None, cyclopts.Parameter(help='The steps for the worker.')] = None,
    from_: Annotated[
        str | None,
        cyclopts.Parameter(
            name='--from', help='What prompted the idea, with ids or a URL. Example: "finding comment#881: …".'
        ),
    ] = None,
    family: Annotated[str | None, cyclopts.Parameter(help=FAMILY_HELP)] = None,
    priority: Annotated[int, cyclopts.Parameter(help=PRIORITY_HELP)] = 2,
    harness: Annotated[str | None, cyclopts.Parameter(help=HARNESS_HELP)] = None,
    base: Annotated[int | None, cyclopts.Parameter(help=BASE_HELP)] = None,
    proposed: Annotated[
        bool, cyclopts.Parameter(help='Wait for a review before the daemon schedules the idea.')
    ] = False,
    track: Annotated[str | None, cyclopts.Parameter(help=TRACK_HELP)] = None,
    after: Annotated[tuple[int, ...], cyclopts.Parameter(help=AFTER_HELP)] = (),
    supersedes: Annotated[tuple[int, ...], cyclopts.Parameter(help=SUPERSEDES_HELP)] = (),
) -> None:
    """Add a defined idea, which the daemon schedules in priority order once it is ready.

    The `files` key of a role says whether its agents may add ideas, only file seeds (`piloop idea seed`), or
    neither. The orchestrator and humans may add ideas. While the `max_ideas` of the actor's role is 0, the idea is
    proposed: it waits for a review. The idea is signed as PILOOP_ACTOR. The idea and its links are written together,
    so the daemon never sees the idea without them.
    """
    with common.session() as s:
        project = common.project(s, ref)
        if filing(project) is Files.SEEDS:
            raise SystemExit(
                f'{common.settings.actor} files seeds, not ideas: `piloop idea seed {ref} "<hunch>" --from "<pointers>"`.'
                ' Research develops each seed with the whole campaign in view.'
            )
        check_base(s, project, base)
        # A sweep reads its max_ideas once, in its brief; the operator may have set it to 0 since, to pause the role.
        name = common.settings.actor.partition('#')[0]
        role = config_of(project).roles.sweeping().get(name)
        paused = role is not None and role.max_ideas == 0
        idea = Idea(
            project_id=project.id,
            track=track_for(project, track),
            title=title,
            hypothesis=hypothesis or title,
            plan=plan,
            provenance=from_,
            family=family,
            priority=priority,
            harness=harness,
            base_candidate_id=base,
            status=IdeaStatus.PROPOSED if proposed or paused else IdeaStatus.READY,
            created_by=common.settings.actor,
            links=links_to(after, supersedes),
        )
        s.add(idea)
        s.flush()
        emit(s, 'idea.created', TargetKind.IDEA, idea.id, actor=common.settings.actor)
        s.commit()
        why = f' ([roles.{name}] max_ideas = 0: it waits for a review)' if paused else ''
        common.console.print(f'idea#{idea.id} {idea.status} p{priority}: {title}{why}', markup=False)


@app.command(name='seed')
def idea_seed(
    ref: ProjectRef,
    hunch: Annotated[str, cyclopts.Parameter(help='The hunch, in one line: what might move the metric.')],
    /,
    *,
    from_: Annotated[
        str,
        cyclopts.Parameter(
            name='--from',
            help='Pointers to what prompted the hunch: ids, paths or numbers. Example: "run#7 finding comment#81: …".',
        ),
    ],
    base: Annotated[
        int | None, cyclopts.Parameter(help='The candidate id to start from, when the hunch is about that tree.')
    ] = None,
    track: Annotated[str | None, cyclopts.Parameter(help=TRACK_HELP)] = None,
) -> None:
    """File a seed: a hunch with pointers, for research to develop into a defined idea or to reject.

    An agent may file a seed when the `files` key of its role allows it. Research reads the earlier work and checks for duplicates and
    negative results. Then it defines the seed (`piloop idea develop`) or rejects it with the reason. The seed is
    signed as PILOOP_ACTOR.
    """
    with common.session() as s:
        project = common.project(s, ref)
        filing(project)
        if not from_.strip():
            raise SystemExit('a seed needs pointers: --from "<ids, paths, numbers>"')
        check_base(s, project, base)
        idea = Idea(
            project_id=project.id,
            track=track_for(project, track),
            title=hunch,
            hypothesis=hunch,
            provenance=from_,
            base_candidate_id=base,
            status=IdeaStatus.SEED,
            created_by=common.settings.actor,
        )
        s.add(idea)
        s.flush()
        emit(s, 'idea.created', TargetKind.IDEA, idea.id, actor=common.settings.actor)
        s.commit()
        common.console.print(f'idea#{idea.id} seed: {hunch}; research develops it next', markup=False)


@app.command(name='develop')
def idea_develop(
    idea_id: Annotated[int, cyclopts.Parameter(help='The id of the seed.')],
    /,
    *,
    hypothesis: Annotated[str, cyclopts.Parameter(help='The mechanism, and why it should move the metric.')],
    evidence: Annotated[
        str, cyclopts.Parameter(help='What supports it, with ids and paths: runs, findings, candidates, loss tables.')
    ],
    check: Annotated[
        str,
        cyclopts.Parameter(help='The cheapest measurement that can prove it wrong, and the result that stops the run.'),
    ],
    metric: Annotated[
        str, cyclopts.Parameter(help='The metric that decides success: the primary metric, or the slice it targets.')
    ],
    effect: Annotated[str, cyclopts.Parameter(help="The expected size of the effect in the metric's units, and why.")],
    plan: Annotated[str | None, cyclopts.Parameter(help='More steps for the worker, after the check.')] = None,
    title: Annotated[str | None, cyclopts.Parameter(help="A clearer title. Default: the seed's title.")] = None,
    family: Annotated[str | None, cyclopts.Parameter(help=FAMILY_HELP)] = None,
    base: Annotated[int | None, cyclopts.Parameter(help=BASE_HELP)] = None,
    priority: Annotated[int | None, cyclopts.Parameter(help=f"{PRIORITY_HELP} Default: the seed's priority.")] = None,
    proposed: Annotated[
        bool, cyclopts.Parameter(help='Wait for a human review before the daemon schedules the idea.')
    ] = False,
    after: Annotated[tuple[int, ...], cyclopts.Parameter(help=AFTER_HELP)] = (),
    supersedes: Annotated[tuple[int, ...], cyclopts.Parameter(help=SUPERSEDES_HELP)] = (),
) -> None:
    """Develop a seed into a defined idea that the daemon can schedule.

    The hypothesis, the evidence, the check, the metric and the effect are all required, because the worker needs
    each one. Reject a seed that does not hold up instead, with a note that names the earlier work (see
    `piloop schema`). The idea is signed as PILOOP_ACTOR. The idea and its links are written together, so the daemon
    never sees the ready idea without them.
    """
    with common.session() as s:
        idea = s.get(Idea, idea_id)
        if idea is None:
            raise SystemExit(f'no idea#{idea_id}')
        if filing(idea.project) is Files.SEEDS:
            raise SystemExit(f'{common.settings.actor} files seeds; research develops them')
        check_base(s, idea.project, base)
        definition = Definition(hypothesis, evidence, check, metric, effect, plan)
        try:
            develop(idea, definition, title=title, family=family, base=base, priority=priority, proposed=proposed)
        except (NotASeed, ValueError) as e:
            raise SystemExit(str(e)) from e
        idea.links.extend(links_to(after, supersedes))
        s.commit()
        common.console.print(f'idea#{idea.id} {idea.status} p{idea.priority}: {idea.title}', markup=False)


def links_to(after: tuple[int, ...], supersedes: tuple[int, ...]) -> list[IdeaLink]:
    """Return the links of a new or developed idea, which the caller writes in the transaction of the idea.

    An ``after`` link inserted once the idea is ``ready`` can come too late: the daemon may queue a run in between.
    """
    return [
        *(IdeaLink(target_id=t, kind=LinkKind.AFTER) for t in after),
        *(IdeaLink(target_id=t, kind=LinkKind.SUPERSEDES) for t in supersedes),
    ]


def filing(project: Project) -> Files | None:
    """Return what the actor may file in ``project`` (``[roles.<name>] files``). Exit if it may file nothing."""
    files = files_of(config_of(project), common.settings.actor)
    if files is Files.NOTHING:
        raise SystemExit(
            f"{common.settings.actor} files no ideas or seeds: say the hunch in your run's finding, with pointers."
            ' Research reads the findings of the workers.'
        )
    return files


def track_for(project: Project, track: str | None) -> str:
    """The track for an idea: ``track`` when the config names it, else the only track. Exits otherwise."""
    tracks = [t.name for t in config_of(project).all_tracks]
    if track is None and len(tracks) == 1:
        return tracks[0]
    if track not in tracks:
        given = 'name one with --track' if track is None else f'there is no track {track!r}'
        raise SystemExit(f'{project.name} has tracks {", ".join(tracks)}: {given}')
    return track


def check_base(s: Session, project: Project, base: int | None) -> None:
    """Exit unless ``base`` is unset or a candidate of ``project``."""
    if base is not None and s.scalar(select(Candidate.project_id).where(Candidate.id == base)) != project.id:
        raise SystemExit(f'no candidate#{base} in project {project.name}')
