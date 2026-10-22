"""The ``piloop idea`` commands: propose work, and change an idea's status, links, base or track."""

from typing import Annotated, Literal

import cyclopts

from piloop.campaign import ops
from piloop.campaign.ideas import Definition
from piloop.cli import common
from piloop.cli.common import HARNESS_HELP, CampaignRef
from piloop.db.models import IdeaStatus, LinkKind

app = cyclopts.App(
    name='idea', help='Propose work (a seed, a defined idea) and change an idea: its status, links, base or track.'
)
PRIORITY_HELP = 'From 0 (urgent) to 4 (someday).'
FAMILY_HELP = 'The mechanism family. Ideas that try the same mechanism share it.'
BASE_HELP = 'The candidate id to start from instead of the head. This makes a branch.'
TRACK_HELP = 'The track it works on. Required when piloop.toml has more than one track.'
AFTER_HELP = 'An idea id to wait for. Repeatable. The idea runs from the head when all are done.'
SUPERSEDES_HELP = 'An idea id that this idea refines. Repeatable. The daemon rejects each one that never ran.'
REASON_HELP = 'Why, with ids. It goes into the event or a note on the idea.'
IdeaId = Annotated[int, cyclopts.Parameter(help='The idea.')]
Reason = Annotated[str, cyclopts.Parameter(help=REASON_HELP)]


@app.command(name='add')
def idea_add(
    ref: CampaignRef,
    title: Annotated[str, cyclopts.Parameter(help='A short title for the idea.')],
    /,
    *,
    hypothesis: Annotated[
        str, cyclopts.Parameter(help='Why this should help, and by how much. Default: the title.', show_default=False)
    ] = '',
    plan: Annotated[str | None, cyclopts.Parameter(help='The steps for the implementer.')] = None,
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
    with common.refused(), common.session() as s:
        idea, held = ops.add_idea(
            s,
            common.campaign(s, ref).id,
            title,
            actor=common.settings.actor,
            hypothesis=hypothesis,
            plan=plan,
            provenance=from_,
            family=family,
            priority=priority,
            harness=harness,
            base=base,
            proposed=proposed,
            track=track,
            after=after,
            supersedes=supersedes,
        )
        s.commit()
        common.console.print(f'idea#{idea.id} {idea.status} p{priority}: {title}{waits(held)}', markup=False)


@app.command(name='seed')
def idea_seed(
    ref: CampaignRef,
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
    with common.refused(), common.session() as s:
        idea = ops.file_seed(
            s,
            common.campaign(s, ref).id,
            hunch,
            actor=common.settings.actor,
            provenance=from_,
            base=base,
            track=track,
        )
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
    plan: Annotated[str | None, cyclopts.Parameter(help='More steps for the implementer, after the check.')] = None,
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

    The hypothesis, the evidence, the check, the metric and the effect are all required, because the implementer needs
    each one. Reject a seed that does not hold up instead, with a note that names the earlier work (see
    `piloop schema`). The idea is signed as PILOOP_ACTOR. The idea and its links are written together, so the daemon
    never sees the ready idea without them.
    """
    with common.refused(), common.session() as s:
        idea, held = ops.develop_seed(
            s,
            idea_id,
            Definition(hypothesis, evidence, check, metric, effect, plan),
            actor=common.settings.actor,
            title=title,
            family=family,
            base=base,
            priority=priority,
            proposed=proposed,
            after=after,
            supersedes=supersedes,
        )
        s.commit()
        common.console.print(f'idea#{idea.id} {idea.status} p{idea.priority}: {idea.title}{waits(held)}', markup=False)


@app.command(name='status')
def idea_status(
    idea_id: IdeaId,
    status: Annotated[IdeaStatus, cyclopts.Parameter(help='proposed, ready, parked, rejected or done.')],
    /,
    *,
    reason: Reason,
) -> None:
    """Set an idea's status, with a note on the idea that says why.

    `ready` queues it, `proposed` waits for a review, `parked` is for now, `rejected` for good, and `done` settles it
    without a run, so the ideas after it start. A seed is developed first (`piloop idea develop`), and a scheduled
    idea has an open run: either one can only be parked or rejected. A parked or rejected idea's open run finishes.
    """
    with common.refused(), common.session() as s:
        old = ops.set_idea_status(s, idea_id, status, reason=reason, actor=common.settings.actor)
        s.commit()
        common.console.print(f'idea#{idea_id} {old} → {status}: {reason.strip()}', markup=False)


@app.command(name='link')
def idea_link(
    idea_id: IdeaId,
    kind: Annotated[LinkKind, cyclopts.Parameter(help='after: wait until the target is done. supersedes: refine it.')],
    target_id: Annotated[int, cyclopts.Parameter(help='The idea it links to.')],
    /,
    *,
    reason: Reason,
) -> None:
    """Link an idea that exists to another idea of its campaign: it runs after the target, or supersedes it.

    The link is written in one transaction, so the daemon never queues the idea in between. A scheduled idea takes no
    `after` link: its run started already. A link that closes a cycle of its kind is refused.
    """
    with common.refused(), common.session() as s:
        ops.link(s, idea_id, kind, target_id, reason=reason, actor=common.settings.actor)
        s.commit()
        common.console.print(f'idea#{idea_id} {kind} idea#{target_id}', markup=False)


@app.command(name='unlink')
def idea_unlink(
    idea_id: IdeaId,
    kind: Annotated[LinkKind, cyclopts.Parameter(help='after or supersedes.')],
    target_id: Annotated[int, cyclopts.Parameter(help='The idea it links to.')],
    /,
    *,
    reason: Reason,
) -> None:
    """Delete an idea's link, for example an `after` link to a prerequisite that was rejected and blocks it."""
    with common.refused(), common.session() as s:
        ops.unlink(s, idea_id, kind, target_id, reason=reason, actor=common.settings.actor)
        s.commit()
        common.console.print(f'idea#{idea_id} no longer {kind} idea#{target_id}', markup=False)


@app.command(name='base')
def idea_base(
    idea_id: IdeaId,
    base: Annotated[
        int | Literal['head'],
        cyclopts.Parameter(help='A candidate id to start from, or head: no pin, the head when the run starts.'),
    ],
    /,
    *,
    reason: Reason,
) -> None:
    """Set the candidate that an idea's runs start from, or clear the pin with `head`.

    A pin to another candidate than the head makes a branch. A run already open keeps its base; the next attempt
    takes the new one.
    """
    with common.refused(), common.session() as s:
        ops.set_base(s, idea_id, None if base == 'head' else base, reason=reason, actor=common.settings.actor)
        s.commit()
        start = 'the head' if base == 'head' else f'candidate#{base}'
        common.console.print(f'idea#{idea_id} starts from {start}', markup=False)


@app.command(name='track')
def idea_track(
    idea_id: IdeaId,
    track: Annotated[str, cyclopts.Parameter(help='A track of piloop.toml.')],
    /,
    *,
    reason: Reason,
) -> None:
    """Move an idea to another track of its campaign, with a note on the idea that says why.

    Its runs are judged on that track's primary. A scheduled idea keeps its track: its run is judged on it.
    """
    with common.refused(), common.session() as s:
        old = ops.set_track(s, idea_id, track, reason=reason, actor=common.settings.actor)
        s.commit()
        common.console.print(f'idea#{idea_id} track {old} → {track}', markup=False)


def waits(held: str | None) -> str:
    """Return `` (<why>: it waits for a review)`` for an idea that ``ops.review_first`` holds, else nothing."""
    return f' ({held}: it waits for a review)' if held is not None else ''
