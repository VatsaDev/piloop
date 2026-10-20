"""The briefs of the roles: the prompt, the context and what the agent may write (https://piloop.autoresearch.inc/concepts/roles).

Every role shares one lifecycle (``piloop.agents.launcher``). A role differs in its ``[roles.<name>]`` table
(``piloop.config``), its triggers (``piloop.triggers``) and the content of its brief, which this module composes.
"""

from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.orm import Session

from piloop.config import BRIEF_FILE, Files, ProjectConfig, SweepRole, Track, config_of
from piloop.db.models import Project, Worker

PROMPTS = Path(__file__).resolve().parent / 'prompts'
PROCEDURES = 'sql_procedures.md'
PILOOP_HINT = '"$PILOOP_BIN"'
"""How a brief runs piloop: the daemon sets ``PILOOP_BIN`` for every agent (``piloop.agents.launcher.own_cli``)."""
SQL_HINT = f'{PILOOP_HINT} sql'


@dataclass(frozen=True)
class Role:
    """The part of a brief that changes with the role. ``Launcher.launch`` does everything else.

    It comes from the role's ``[roles.<kind>]`` table (``role``).
    """

    kind: str
    """The role's name in ``piloop.toml``, and so ``workers.kind``, the signature of its agents
    (``<kind>#<worker id>``) and the source of the ideas they file."""
    prompt: Path
    """The file that starts the brief."""
    incident: Path | None
    """The file that follows ``prompt`` in an incident sweep, one that got anomalies. None: nothing follows."""
    routine: Path | None
    """The file that follows ``prompt`` in every other sweep. None for a role that does runs, or a role without one."""
    writes: tuple[str, ...]
    """What the role may write. The brief states it."""
    files: Files
    """What the role may file with ``piloop idea``. The CLI enforces it."""
    ideas: str
    """How this role gives work to the workers, as the context block of the brief says it.

    ``{piloop}`` is the CLI and ``{project}`` is the project's name.
    """


SEED_HINT = (
    '- File a seed: `{piloop} idea seed {project} "<hunch, one line>" --from "<pointers: ids, paths, numbers>"`'
    ' (`--base <candidate>` when it is about that tree): research checks it against the existing work and develops it'
    ' into an idea, or rejects it with the reason.'
)
IDEAS: dict[Files, str] = {
    Files.NOTHING: (
        '- A hunch worth a run of its own goes in your finding, with pointers (ids, paths, the number): research'
        " reads the workers' findings. You file no ideas or seeds."
    ),
    Files.SEEDS: SEED_HINT,
    Files.IDEAS: (
        '- Develop a seed: `{piloop} idea develop <seed id> --hypothesis "…" --evidence "<ids, paths>" --check'
        ' "<cheapest measurement that kills it, and the result that stops the run>" --metric "<what decides'
        ' success>" --effect "<expected size, in the metric\'s units, and why>" [--plan "…"] [--family "…"]'
        ' [--title "…"] [--base <candidate>]`.\n'
        '- Add an idea of your own: `{piloop} idea add {project} "<title>" --hypothesis "…" --plan "…" --family'
        ' "<mechanism>" --from "<the evidence, with ids>"` (`--help` for links, a base, a priority).'
    ),
}
"""The ideas line of the brief for each value of ``files``."""


def role(cfg: ProjectConfig, kind: str, repo: Path) -> Role:
    """Return the role of ``[roles.<kind>]`` in a project at ``repo``.

    Raises:
        KeyError: ``piloop.toml`` has no such role.
    """
    table = cfg.role(kind)
    prompt = _prompt(repo, table.prompt, table.PROMPT)
    assert prompt is not None  # a table without its own prompt is a built-in role, which has one
    incident = routine = None
    if isinstance(table, SweepRole):
        incident = _prompt(repo, table.incident_prompt, table.INCIDENT_PROMPT)
        routine = _prompt(repo, table.routine_prompt, table.ROUTINE_PROMPT)
    ideas = table.IDEAS or IDEAS[table.files]
    return Role(kind, prompt, incident, routine, tuple(table.writes), table.files, ideas)


def _prompt(repo: Path, configured: str | None, built_in: str) -> Path | None:
    """Return the file of a prompt key: the configured file in the repo, else the built-in one, else None."""
    if configured is not None:
        return repo / configured
    return PROMPTS / built_in if built_in else None


def role_of(s: Session, worker: Worker) -> Role:
    """Return the role of a row: the role of its kind in its project."""
    project = s.get_one(Project, worker.project_id)
    return role(config_of(project), worker.kind, Path(project.path))


def files_of(cfg: ProjectConfig, actor: str) -> Files | None:
    """Return what ``actor`` may file with ``piloop idea``, by the role in its signature (``watchdog#4``).

    None for an actor that is not a role, such as ``orchestrator`` or ``human``: it may file anything.
    """
    table = cfg.roles.all().get(actor.partition('#')[0])
    return None if table is None else table.files


def compose_brief(
    role: Role,
    project: Project,
    worker_id: int,
    *,
    reason: str,
    context: str,
    incident: bool = False,
    track: Track | None = None,
    wake: str = '',
) -> str:
    """Compose a brief: the role prompt, the project brief, the role's addendum, the database procedures, the context.

    A sweep also gets its role's incident or routine prompt after the role prompt: the incident prompt when it got
    anomalies (``incident``). ``wake``, the lessons overview of the repo (``piloop.reports.wake``), follows them. A
    worker also gets its track's addendum after the role's addendum. With several tracks, the context lists them.
    """
    author = f'{role.kind}#{worker_id}'
    repo = Path(project.path)
    cfg = config_of(project)
    addendum = cfg.role(role.kind).addendum
    follows = role.incident if incident else role.routine
    parts = [
        role.prompt.read_text().rstrip(),
        follows.read_text().rstrip() if follows is not None else '',
        wake.rstrip(),
        (repo / BRIEF_FILE).read_text().rstrip(),
        (repo / addendum).read_text().rstrip() if addendum else '',
        (repo / track.addendum).read_text().rstrip() if track is not None and track.addendum else '',
        (PROMPTS / PROCEDURES).read_text().rstrip(),
        '\n'.join(
            [
                '# Context',
                f'- You are worker#{worker_id}, the {role.kind} for project#{project.id} ({project.name}).',
                f'- Project repo: {project.path} (every candidate commit and Entire checkpoint is in it).',
                f"- Sign what you write as '{author}' (comments.author, ideas.created_by).",
                f'- Trigger: {reason}',
                f"- piloop: `{PILOOP_HINT}` is this daemon's piloop. Run every `piloop` command of this brief with it.",
                f'- SQL tool: `{SQL_HINT} "<sql>"`',
                f'- Sweeping roles (`workers.kind` of a request): {", ".join(cfg.roles.sweeping())}.',
                *([tracks_line(cfg, track)] if cfg.multitrack else []),
                role.ideas.format(piloop=PILOOP_HINT, project=project.name),
                *([f'- You may write: {", ".join(sorted(role.writes))}.'] if role.writes else []),
                context.rstrip(),
            ]
        ),
    ]
    return '\n\n'.join(p for p in parts if p) + '\n'


def tracks_line(cfg: ProjectConfig, track: Track | None) -> str:
    """Return the project's tracks in one line for a brief. Every idea and seed names one. A worker learns its own."""
    tracks = '; '.join(
        f'{t.name} (primary {t.primary.name}, {t.primary.direction} is better; share {t.share:g})'
        for t in cfg.all_tracks
    )
    mine = f' Your run is on track {track.name}.' if track is not None else ''
    return (
        f'- Tracks: {tracks}. Every idea and seed names its track (`--track <name>`); every keep must also hold the'
        f" other tracks' primaries within their margins and floors.{mine}"
    )
