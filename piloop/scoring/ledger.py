"""What the ledger means: the current evaluator, a candidate's score under it, the head, the verdict and the rebase.

All code that reads candidates and measurements to answer "how good is this" uses this module. So the CLI, the
scheduler, the runner and the history block agree.
"""

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from statistics import fmean

from git import Repo
from git.exc import BadName
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from piloop import git
from piloop.config import Metric, ProjectConfig, Track
from piloop.db.events import emit
from piloop.db.models import (
    Candidate,
    Evaluator,
    Idea,
    IdeaStatus,
    Project,
    Run,
    TargetKind,
    Verdict,
    Worker,
    latest_worker,
)
from piloop.scoring.evaluate import UNITS, Units
from piloop.scoring.verdict import decide, same

DAEMON = 'daemon'
"""The ``verdict_by`` of a verdict that the daemon set. The daemon never overwrites a verdict by anyone else."""
SKIPPED = 'not evaluated: Piloop-Eval: skip'
"""The ``verdict_reason`` of the daemon's ``discard`` on a commit that asked not to be evaluated.

No measurement backs this verdict. So when a commit without the trailer reaches the same tree, the daemon measures and
judges it (``skipped``).
"""


def current_evaluator(s: Session, project: Project, cfg: ProjectConfig) -> Evaluator:
    """Return the evaluator row that ``cfg`` describes, and make it the project's current one. Create it if needed."""
    key = cfg.evaluator_key
    evaluator = s.scalar(select(Evaluator).where(Evaluator.project_id == project.id, Evaluator.key == key))
    if evaluator is None:
        evaluator = Evaluator(project_id=project.id, key=key, definition=cfg.evaluator_definition)
        s.add(evaluator)
        s.flush()
    if project.evaluator_id != evaluator.id:
        project.evaluator_id = evaluator.id
        s.flush()
    return evaluator


def metrics(candidate: Candidate, evaluator_id: int) -> dict[str, float]:
    """Return the mean of each metric over the candidate's successful measurements under ``evaluator_id``."""
    rows = [m.metrics for m in candidate.measurements if m.evaluator_id == evaluator_id and m.error is None]
    names = {name for row in rows for name in row}
    return {name: fmean(values) for name in names if (values := [r[name] for r in rows if name in r])}


def score(candidate: Candidate, evaluator_id: int, primary: Metric) -> float | None:
    """Return the primary metric under ``evaluator_id``, or None when no successful measurement has it."""
    return metrics(candidate, evaluator_id).get(primary.name)


def measured(candidate: Candidate, evaluator_id: int) -> bool:
    """Return whether any measurement under ``evaluator_id`` exists. A crashed measurement counts too."""
    return any(m.evaluator_id == evaluator_id for m in candidate.measurements)


def head(s: Session, project: Project, *, excluding: Candidate | None = None) -> Candidate | None:
    """Return the head: the latest trunk keep, a kept candidate that no branch run made. The baseline is the first.

    Every run that pins no base starts from the head. Research and the watchdog get the head checked out, and
    ``target`` is read on it. "Latest" is by id: the runner keeps a run's candidate only while its bar is the head, and
    a candidate is always newer than its bar. So each trunk keep is newer than the one before. Scores do not matter:
    after an evaluator change, the head stays where it was.

    ``excluding`` leaves out one candidate: the one being judged, which is never its own bar.
    """
    query = trunk_keeps(project)
    if excluding is not None:
        query = query.where(Candidate.id != excluding.id)
    return s.scalars(query.order_by(Candidate.id.desc()).limit(1)).first()


def trunk_keeps(project: Project) -> Select[Candidate]:
    """Select the project's trunk keeps: its kept candidates except those that a branch run (``runs.branch``) made."""
    return keeps(project).outerjoin(Run, Candidate.run_id == Run.id).where(Run.id.is_(None) | ~Run.branch)


def bar_of(candidate: Candidate) -> Candidate:
    """Return the bar of a run that starts from ``candidate``.

    The bar is ``candidate`` when it is a keep or a reference, else its nearest ancestor that is one. With neither, it
    is the root of the ancestry. A non-keep is never a bar, pinned or not. Otherwise a run pinned to an ``interesting``
    candidate would be held to that candidate's numbers, which a fix to it could never beat. A run pinned to a
    non-keep starts from its tree and must beat the latest keep of its lineage.
    """
    while candidate.verdict not in (Verdict.KEEP, Verdict.REFERENCE) and candidate.parent is not None:
        candidate = candidate.parent
    return candidate


def track_of(cfg: ProjectConfig, candidate: Candidate) -> Track:
    """Return the track of the candidate's run (its idea's track). A renderer shows the candidate's score on it.

    The baseline and a reference get the first track (``fallback_track``).
    """
    return fallback_track(cfg, candidate.run.idea.track if candidate.run is not None else None)


def fallback_track(cfg: ProjectConfig, name: str | None) -> Track:
    """Return the track called ``name``, else the first track. An idea can name a track that ``piloop.toml`` removed."""
    return next((t for t in cfg.all_tracks if t.name == name), cfg.all_tracks[0])


def unmeasured(s: Session, project: Project) -> list[Candidate]:
    """Return the keeps, the baseline included, then the references, that the current evaluator did not measure.

    Each group is newest first. A new evaluator has measured none of them, and an imported reference starts
    unmeasured too. The daemon measures these, so the head, every bar and every starting point a run can pin have
    numbers under the current evaluator. Their verdicts do not change.
    """
    rows = select(Candidate).where(
        Candidate.project_id == project.id, Candidate.verdict.in_((Verdict.KEEP, Verdict.REFERENCE))
    )
    evaluator_id = project.evaluator_id or 0
    found = [c for c in s.scalars(rows.order_by(Candidate.id.desc())) if not measured(c, evaluator_id)]
    return sorted(found, key=lambda c: c.verdict is Verdict.REFERENCE)


def references(s: Session, project: Project) -> list[Candidate]:
    """Return the project's imported starting points (``piloop candidate import``), oldest first."""
    query = select(Candidate).where(Candidate.project_id == project.id, Candidate.verdict == Verdict.REFERENCE)
    return list(s.scalars(query.order_by(Candidate.id)))


def keeps(project: Project) -> Select[Candidate]:
    """Select the project's kept candidates. The baseline is one while its verdict is ``keep``."""
    return select(Candidate).where(Candidate.project_id == project.id, Candidate.verdict == Verdict.KEEP)


def baseline(s: Session, project: Project) -> Candidate:
    """Return the project's first candidate: the repo's HEAD when the project was registered."""
    return s.scalars(select(Candidate).where(Candidate.project_id == project.id).order_by(Candidate.id).limit(1)).one()


def checkout_candidate(s: Session, project: Project) -> Candidate:
    """Return what a sweeping agent gets checked out: the head, or the baseline when its verdict was overridden."""
    return head(s, project) or baseline(s, project)


def units_file(units_dir: Path, measurement_id: int) -> Path:
    """Return the path of a measurement's per-unit results, under ``PILOOP_HOME``.

    The file is outside every worktree and the database. The results are not a column, because every agent can read
    every table through ``piloop sql``, and a worker must not tune against per-unit results. A file is not a sandbox,
    because an agent's shell can read ``PILOOP_HOME``. But the file is not in the schema, the briefs or any renderer.
    """
    return units_dir / f'{measurement_id}.json'


def save_units(units_dir: Path, measurement_id: int, units: Units) -> None:
    """Store a measurement's per-unit results. With none, remove any old file that a reused id could find."""
    path = units_file(units_dir, measurement_id)
    if not units:
        path.unlink(missing_ok=True)
        return
    units_dir.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(units))


def units(candidate: Candidate, evaluator_id: int, units_dir: Path) -> Units:
    """Return the candidate's per-unit results under ``evaluator_id``: each unit's mean over its measurements."""
    rows = [
        UNITS.validate_json(path.read_bytes())
        for m in candidate.measurements
        if m.evaluator_id == evaluator_id and m.error is None and (path := units_file(units_dir, m.id)).is_file()
    ]
    per_metric: dict[str, dict[str, list[float]]] = {}
    for row in rows:
        for name, values in row.items():
            for unit, value in values.items():
                per_metric.setdefault(name, {}).setdefault(unit, []).append(value)
    return {name: {unit: fmean(v) for unit, v in values.items()} for name, values in per_metric.items()}


def judge(
    candidate: Candidate,
    own: Candidate,
    evaluator_id: int,
    cfg: ProjectConfig,
    track: Track,
    units_dir: Path,
    *,
    head: Candidate | None,
    branch: bool,
) -> tuple[Verdict, str]:
    """Return the daemon's verdict on ``candidate``, a candidate of ``track``, against ``own``, and the reason.

    ``own`` is the run's bar: its last keep, or its last candidate that kept against its bar and was rebased. It
    starts as the run's base. The runner keeps a run's candidate on the trunk only while ``own`` is the head. A keep
    against an ``own`` that the head has moved past is rebased onto the head instead (``rebase``).

    A branch run (``runs.branch``: its idea pinned another starting point) keeps what beats ``own``: a branch keep.
    Its reason also says whether it is above ``head`` on the track's primary.

    When the deciding metrics equal the parent's (``piloop.scoring.verdict.same``), the reason says so first: the
    change did not affect what the evaluation measures, whatever the verdict against the bar.
    """
    mine = metrics(candidate, evaluator_id)
    verdict, reason = decide(
        track,
        cfg.confidence,
        mine,
        metrics(own, evaluator_id),
        units(candidate, evaluator_id, units_dir),
        units(own, evaluator_id, units_dir),
        bar=f'candidate#{own.id}',
    )
    primary = track.primary
    leader = metrics(head, evaluator_id).get(primary.name) if head is not None else None
    if branch and verdict is Verdict.KEEP and head is not None and leader is not None:
        where = 'above' if primary.better(mine[primary.name], leader) else 'not above'
        reason += f'; a branch keep, {where} the head candidate#{head.id} ({primary.name} {leader:g})'
    parent = candidate.parent
    if parent is not None and same(track, mine, metrics(parent, evaluator_id)):
        which = 'primary and guardrails' if track.guardrails else primary.name
        reason = f'no-op: {which} identical to parent candidate#{parent.id}; {reason}'
    return verdict, reason


REBASED = 'candidate.rebased'
"""The event on a candidate that kept against its own bar after the head moved on.

Its payload has ``head``, the head it was merged onto, and one of: ``rebased`` (the merged candidate, which is
measured and judged against that head next), ``known`` (the merged tree is already a candidate) or ``conflicts`` (the
paths).
"""
REBASED_SUFFIX = re.compile(r' \(rebased onto candidate#\d+\)$')
"""How the subject of a merge commit ends. A merge that is rebased again keeps the run commit's subject, with the
new head."""
MOVED = 'keeps against its own bar, but the head moved on to candidate#{head}: '
"""How the reason of a candidate to rebase starts. The candidate is ``interesting``, never a keep off the trunk."""


def rebase(s: Session, cfg: ProjectConfig, repo: Repo, candidate: Candidate, onto: Candidate) -> Candidate | None:
    """Merge ``candidate``'s commit onto the head ``onto``. Return the merged candidate to measure and judge, or None.

    ``candidate`` kept against its own bar after the head moved on. So it is ``interesting`` (``MOVED``), and its
    reason ends with the result of the merge. The merge is a commit with both as parents, made without a worktree
    (``git.merge``) and pinned at ``refs/piloop/<project>/rebase/<candidate id>``. A new tree becomes a pending
    candidate of the same run, with ``onto`` as its parent. A merge that conflicts files a seed on the run's track to
    compose the two by hand. The seed is not pinned, so it runs from the head (``follow_up``). A merged tree that is
    already a candidate (the head had the change) gives no new candidate. Emits ``REBASED``.
    """
    subject = REBASED_SUFFIX.sub('', candidate.summary or '')
    merged = git.merge(repo, onto.commit_sha, candidate.commit_sha, f'{subject} (rebased onto candidate#{onto.id})')
    payload: dict[str, object] = {'head': onto.id}
    found: Candidate | None = None
    if isinstance(merged, list):
        paths = ', '.join(merged)
        why = (
            f'it kept against its own bar but its merge onto the head candidate#{onto.id} conflicts in {paths}:'
            f' from the head, git merge {candidate.commit_sha} and resolve the conflicts'
        )
        assert candidate.run is not None  # only a run's candidate is rebased
        seeds = follow_up(
            s,
            candidate,
            [f'Compose candidate#{candidate.id} onto the head'],
            why,
            DAEMON,
            track=candidate.run.idea.track,
            pin=False,
        )
        filed = f': seed idea#{seeds[0].id} composes it by hand' if seeds else ''
        candidate.verdict_reason = f'{candidate.verdict_reason}; its merge onto the head conflicts in {paths}{filed}'
        payload['conflicts'] = merged
    else:
        git.pin(repo, f'refs/piloop/{cfg.name}/rebase/{candidate.id}', merged)
        found, created = resolve(s, candidate.project_id, repo, merged, parent=onto, run_id=candidate.run_id)
        if created:
            candidate.verdict_reason = f'{candidate.verdict_reason}; rebased onto the head as candidate#{found.id}'
            payload['rebased'] = found.id
        else:
            candidate.verdict_reason = (
                f'{candidate.verdict_reason}; its merge onto the head is candidate#{found.id} already ({found.verdict})'
            )
            payload['known'] = found.id
            found = None
    emit(s, REBASED, TargetKind.CANDIDATE, candidate.id, payload=payload)
    return found


def skipped(candidate: Candidate) -> bool:
    """Return whether the candidate has the daemon's ``SKIPPED`` verdict: a discard that no measurement backs."""
    return candidate.verdict_by == DAEMON and candidate.verdict_reason == SKIPPED


def resolve(
    s: Session, project_id: int, repo: Repo, sha: str, *, parent: Candidate | None, run_id: int | None
) -> tuple[Candidate, bool]:
    """Return the candidate for the tree of ``sha``: the existing one, or a new row with ``parent`` as its parent.

    Returns ``(candidate, created)``. An existing candidate is returned unchanged: a commit that reverts to a known
    state is that state, whatever run or commit reached it.
    """
    tree = git.tree_sha(repo, sha)
    found = s.scalar(select(Candidate).where(Candidate.project_id == project_id, Candidate.tree_sha == tree))
    if found is not None:
        return found, False
    candidate = Candidate(
        project_id=project_id,
        run_id=run_id,
        parent_id=parent.id if parent else None,
        tree_sha=tree,
        commit_sha=sha,
        summary=git.subject_line(repo, sha),
    )
    s.add(candidate)
    s.flush()
    return candidate, True


class ImportRefused(Exception):
    """``import_reference`` cannot register the commit. The message says why."""


def import_reference(
    s: Session, project: Project, cfg: ProjectConfig, repo: Repo, ref: str, *, summary: str | None, actor: str
) -> Candidate:
    """Register the tree of ``ref`` (a commit, branch or tag in the project's repo) as a ``reference`` candidate.

    Like the baseline, it has no run and no parent. The commit is pinned at ``refs/piloop/<project>/reference/<id>``,
    so it outlives its branch. The daemon measures it under the current evaluator (``unmeasured``).

    Raises:
        ImportRefused: ``ref`` names no commit, or its tree is already a candidate of the project.
    """
    try:
        sha = repo.commit(ref).hexsha
    except (BadName, ValueError) as e:
        raise ImportRefused(f'{ref!r} names no commit in {repo.working_dir}: {e}') from e
    candidate, created = resolve(s, project.id, repo, sha, parent=None, run_id=None)
    if not created:
        raise ImportRefused(f'the tree of {ref!r} already is candidate#{candidate.id} ({candidate.verdict})')
    candidate.summary = summary or candidate.summary
    candidate.verdict, candidate.verdict_by = Verdict.REFERENCE, actor
    candidate.verdict_reason = f'imported from {ref} ({sha[:7]})'
    git.pin(repo, f'refs/piloop/{cfg.name}/reference/{candidate.id}', sha)
    emit(s, 'candidate.imported', TargetKind.CANDIDATE, candidate.id, actor=actor, payload={'ref': ref, 'commit': sha})
    return candidate


NOTE = 'note: '
"""How the reason of a noted candidate starts. An agent marked a regressed result as a lead (``note``)."""


class NoteRefused(Exception):
    """``note`` cannot mark the candidate. The message says why."""


def note(candidate: Candidate, reason: str, actor: str) -> None:
    """Mark a measured ``discard`` or ``interesting`` as a lead: ``interesting``, with ``reason`` before the daemon's.

    The result regressed, but the tree is worth starting from. It stays pinnable and in view. Like any
    ``interesting``, it is not a keep and moves no bar. The verdict is now ``actor``'s, so the daemon never overwrites
    it.

    Raises:
        NoteRefused: the reason is empty, or the candidate is not a measured discard or interesting. A keep needs no
            note. A pending, crashed, skipped or reference candidate has no result to note.
    """
    if not reason.strip():
        raise NoteRefused('a note needs a reason: what the follow-ups from this tree are')
    if candidate.verdict not in (Verdict.DISCARD, Verdict.INTERESTING) or skipped(candidate):
        raise NoteRefused(
            f'candidate#{candidate.id} is {candidate.verdict} ({candidate.verdict_reason or "no reason"}):'
            ' only a measured discard or interesting takes a note'
        )
    candidate.verdict, candidate.verdict_by = Verdict.INTERESTING, actor
    candidate.verdict_reason = (
        NOTE + reason.strip() + (f'; {candidate.verdict_reason}' if candidate.verdict_reason else '')
    )


def follow_up(
    s: Session,
    candidate: Candidate,
    titles: Sequence[str],
    why: str,
    actor: str,
    *,
    track: str,
    pin: bool = True,
) -> list[Idea]:
    """File one seed per title on ``track``, pinned to ``candidate`` unless ``pin`` is False. Return the new seeds.

    A title that the project already has gets `` (from candidate#<id>)``. A follow-up already filed from this candidate
    with that title is not filed twice. These are seeds, not ready ideas: research develops them before they run. A
    pinned one runs as a branch run when its candidate is not the computed base (``[roles.worker] explore_every``). An
    unpinned one runs from the head, as a trunk run.
    """
    ideas: list[Idea] = []
    hypothesis = f'Follow-up of candidate#{candidate.id} ({candidate.summary or "no summary"})'
    for title in titles:
        free = (
            n for n in (title, f'{title} (from candidate#{candidate.id})') if not _titled(s, candidate.project_id, n)
        )
        name = next(free, None)
        if name is None:
            continue
        idea = Idea(
            project_id=candidate.project_id,
            title=name,
            hypothesis=f'{hypothesis}: {why}',
            status=IdeaStatus.SEED,
            track=track,
            base_candidate_id=candidate.id if pin else None,
            provenance=f'candidate#{candidate.id}',
            created_by=actor,
        )
        s.add(idea)
        s.flush()
        emit(s, 'idea.created', TargetKind.IDEA, idea.id, actor=actor)
        ideas.append(idea)
    return ideas


def _titled(s: Session, project_id: int, title: str) -> bool:
    return s.scalar(select(Idea.id).where(Idea.project_id == project_id, Idea.title == title)) is not None


@dataclass(frozen=True)
class Branch:
    """One lineage of keeps: the starting point it grew from, and its latest keep."""

    root: Candidate
    branched: bool
    """True when a branch run started the lineage at ``root``. False for the trunk."""
    latest: Candidate
    keeps: int


def branch_root(candidate: Candidate) -> tuple[Candidate, bool]:
    """Return the starting point of ``candidate``'s lineage, and whether a branch run started the lineage there.

    The starting point is the base of the nearest branch run among its ancestors. Without one, it is the first
    ancestor without a run: the baseline, or a reference that a trunk run started from. A branch from the baseline is
    a different lineage from the trunk that grew from it, so the flag tells them apart.
    """
    while (run := candidate.run) is not None:
        if run.branch:
            return run.base, True
        candidate = run.base
    return candidate, False


def branches(s: Session, project: Project) -> list[Branch]:
    """Return every lineage of keeps: the trunk first (its latest keep is the head), then the branches, newest first."""
    found: dict[tuple[int, bool], Branch] = {}
    for c in s.scalars(keeps(project).order_by(Candidate.id)):
        root, branched = branch_root(c)
        known = found.get((root.id, branched))
        found[root.id, branched] = Branch(root, branched, c, (known.keeps if known else 0) + 1)
    return sorted(found.values(), key=lambda b: (b.branched, -b.latest.id))


def session_worker(s: Session, candidate: Candidate) -> Worker | None:
    """Return the worker whose session made ``candidate`` (through ``workers.run_id``). None for the baseline."""
    return latest_worker(s, candidate.run_id) if candidate.run_id is not None else None


def session_checkpoint(s: Session, candidate: Candidate) -> str | None:
    """Return the Entire checkpoint of the session that made ``candidate`` (``workers.checkpoint_id``).

    None for the baseline, which has no run, and for a run whose worker session was not attached.
    """
    worker = session_worker(s, candidate)
    return worker.checkpoint_id if worker is not None else None


def session_tokens(s: Session, candidate: Candidate) -> int | None:
    """Return the tokens that the session that made ``candidate`` spent (``workers.usage`` total). None when unknown."""
    worker = session_worker(s, candidate)
    return worker.usage['total'] if worker is not None and worker.usage else None
