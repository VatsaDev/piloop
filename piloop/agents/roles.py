"""The briefs of the roles: the prompt, the context and what the agent may write (https://piloop.autoresearch.inc/concepts/roles).

Every role shares one lifecycle (``piloop.agents.launcher``). A role differs in its ``[roles.<name>]`` table
(``piloop.plugins.role``), its triggers (``piloop.plugins.trigger``) and the content of its brief, which this module composes.
"""

from dataclasses import dataclass
from importlib.resources.abc import Traversable
from pathlib import Path

from covalence import Cid
from sqlalchemy.orm import Session

from piloop.actor import Actor, Agent
from piloop.config import BRIEF_FILE, CampaignConfig, Track
from piloop.db.models import AgentSession, Campaign
from piloop.ids import RoleName
from piloop.plugins.cas.contract import Cas
from piloop.plugins.harness.contract import Harness
from piloop.plugins.role.contract import Files, SweepRole
from piloop.plugins.vcs.contract import VCS, prompted

PROMPTS = Path(__file__).resolve().parent / 'prompts'
PROCEDURES = 'sql_procedures.md'
"""The database procedures of every role's brief."""
PILOOP_HINT = '"$PILOOP_BIN"'
"""How a brief runs piloop: the worker sets ``PILOOP_BIN`` for every agent (``piloop.plugins.executor.kit.jobs.own_cli``)."""
SQL_HINT = f'{PILOOP_HINT} sql'


@dataclass(frozen=True)
class Role:
    """The part of a brief that changes with the role. ``Launcher.launch`` does everything else.

    It comes from the role's ``[roles.<kind>]`` table (``role``).
    """

    kind: RoleName
    """The role's name in ``piloop.toml``, and so ``agent_sessions.kind``, the signature of its agents
    (``<kind>#<agent session id>``) and the source of the ideas they file."""
    prompt: Traversable
    """The file that starts the brief: in the repo, or a resource of the role's package."""
    incident: Traversable | None
    """The file that follows ``prompt`` in an incident sweep, one that got anomalies. None: nothing follows."""
    routine: Traversable | None
    """The file that follows ``prompt`` in every other sweep. None for a role that does runs, or a role without one."""
    writes: tuple[str, ...]
    """What the role may write. The brief states it."""
    files: Files
    """What the role may file with ``piloop idea``. The CLI enforces it."""
    ideas: str
    """How this role gives work to the implementers, as the context block of the brief says it.

    ``{piloop}`` is the CLI and ``{campaign}`` is the campaign's name.
    """


SEED_HINT = (
    "- File a seed: `{piloop} idea seed {campaign} '<hunch, one line>' --from '<pointers: ids, paths, numbers>'`"
    ' (`--base <candidate>` when it is about that tree): research checks it against the existing work and develops it'
    ' into an idea, or rejects it with the reason.'
)
IDEAS: dict[Files, str] = {
    Files.NOTHING: (
        '- A hunch worth a run of its own goes in your finding, with pointers (ids, paths, the number): research'
        " reads the implementers' findings. You file no ideas or seeds."
    ),
    Files.SEEDS: SEED_HINT,
    Files.IDEAS: (
        "- Develop a seed: `{piloop} idea develop <seed id> --hypothesis '…' --evidence '<ids, paths>' --check"
        " '<cheapest measurement that kills it, and the result that stops the run>' --metric '<what decides"
        " success>' --effect '<expected size, in the units of the metric, and why>' [--plan '…'] [--family '…']"
        " [--title '…'] [--base <candidate>]`.\n"
        "- Add an idea of your own: `{piloop} idea add {campaign} '<title>' --hypothesis '…' --plan '…' --family"
        " '<mechanism>' --from '<the evidence, with ids>'` (`--help` for links, a base, a priority)."
    ),
}
"""The ideas line of the brief for each value of ``files``."""


def role(cfg: CampaignConfig, kind: RoleName, repo: Path) -> Role:
    """Return the role of ``[roles.<kind>]`` in a campaign at ``repo``.

    Raises:
        KeyError: ``piloop.toml`` has no such role.
    """
    table = cfg.role(kind)
    prompt = _prompt(repo, table.prompt, table.PROMPT)
    assert prompt is not None  # RoleConfig refuses a role with neither
    incident = routine = None
    if isinstance(table, SweepRole):
        incident = _prompt(repo, table.incident_prompt, table.INCIDENT_PROMPT)
        routine = _prompt(repo, table.routine_prompt, table.ROUTINE_PROMPT)
    ideas = table.IDEAS or IDEAS[table.files]
    return Role(kind, prompt, incident, routine, tuple(table.writes), table.files, ideas)


def _prompt(repo: Path, configured: str | None, own: Traversable | None) -> Traversable | None:
    """Return the file of a prompt key: the configured file in the repo, else the role's own, else None."""
    return repo / configured if configured is not None else own


def role_of(s: Session, agent_session: AgentSession) -> Role:
    """Return the role of a row: the role of its kind in its campaign."""
    campaign = s.get_one(Campaign, agent_session.campaign_id)
    return role(campaign.config, agent_session.kind, Path(campaign.path))


def harness_of(s: Session, agent_session: AgentSession) -> Harness | None:
    """Return the definition of the harness that a row runs, from its campaign's config.

    None before the launcher picks a harness, and when ``piloop.toml`` no longer names it (a row keeps only the name).
    """
    if agent_session.harness is None or agent_session.campaign_id is None:
        return None
    try:
        return s.get_one(Campaign, agent_session.campaign_id).config.harness_for(agent_session.harness)
    except KeyError:
        return None


def files_of(cfg: CampaignConfig, actor: Actor) -> Files | None:
    """Return what ``actor`` may file with ``piloop idea``, by the role in its signature (``watchdog#4``).

    None for an actor that is not a role, such as ``orchestrator`` or ``human``: it may file anything.
    """
    if (agent := actor.launched) is None:
        return None
    table = cfg.roles.all().get(agent.role)
    return None if table is None else table.files


def read_brief(cas: Cas, brief: Cid) -> str:
    """The text of the brief at ``brief`` in ``cas``.

    Raises:
        LookupError: the CAS lacks it.
    """
    if (data := cas.get(brief)) is None:
        raise LookupError(f'the CAS lacks the brief {brief}')
    return data.decode()


def compose_brief(
    role: Role,
    campaign: Campaign,
    agent: Agent,
    *,
    reason: str,
    context: str,
    incident: bool = False,
    track: Track | None = None,
) -> str:
    """Compose a brief: the role prompt, the campaign brief, the role's addendum, the database procedures, the context.

    A sweep also gets its role's incident or routine prompt after the role prompt: the incident prompt when it got
    anomalies (``incident``). An implementer also gets its track's addendum after the role's addendum. With several
    tracks, the context lists them. ``context`` ends with the role's sections.
    """
    repo = Path(campaign.path)
    cfg = campaign.config
    addendum = cfg.role(role.kind).addendum
    follows = role.incident if incident else role.routine
    vcs = VCS.selected(cfg.plugins)
    parts = [
        prompted(role.prompt.read_text().rstrip(), vcs),
        prompted(follows.read_text().rstrip(), vcs) if follows is not None else '',
        (repo / BRIEF_FILE).read_text().rstrip(),
        (repo / addendum).read_text().rstrip() if addendum else '',
        (repo / track.addendum).read_text().rstrip() if track is not None and track.addendum else '',
        prompted((PROMPTS / PROCEDURES).read_text().rstrip(), vcs),
        _context(role, campaign, agent, reason, context, track),
    ]
    return '\n\n'.join(p for p in parts if p) + '\n'


def compose_resumed(role: Role, campaign: Campaign, agent: Agent, *, reason: str, context: str) -> str:
    """Compose the brief of an agent that resumes its role's last session: only its context, which is new to it.

    The session holds the rest of the brief already, from the agent that started it.
    """
    lead = (
        f'You continue this session as a new agent, {agent.signature}. Your earlier briefs and work are above.'
        ' Follow your role as before, with this new context.'
    )
    return f'{lead}\n\n{_context(role, campaign, agent, reason, context, None)}\n'


def _context(role: Role, campaign: Campaign, agent: Agent, reason: str, context: str, track: Track | None) -> str:
    author = agent.signature
    cfg = campaign.config
    return '\n'.join(
        [
            '# Context',
            f'- You are {author}, the {role.kind} for campaign#{campaign.id} ({campaign.name}).',
            f"- Campaign repo: {campaign.path}, on the daemon's machine. Your checkout can read every candidate's version.",
            f"- Sign what you write as '{author}' (comments.author, ideas.created_by).",
            f'- Why this agent started (context, not a scope): {reason}',
            f"- piloop: `{PILOOP_HINT}` is this daemon's piloop. Run every `piloop` command of this brief with it.",
            f"- SQL tool: `{SQL_HINT} -`, with the statement on stdin from a quoted heredoc (`<<'EOF'`).",
            *([tracks_line(cfg, track)] if cfg.multitrack else []),
            role.ideas.format(piloop=PILOOP_HINT, campaign=campaign.name),
            *([f'- You may write: {", ".join(sorted(role.writes))}.'] if role.writes else []),
            context.rstrip(),
        ]
    )


def tracks_line(cfg: CampaignConfig, track: Track | None) -> str:
    """Return the campaign's tracks in one line for a brief. Every idea and seed names one. An implementer learns its own."""
    tracks = '; '.join(
        f'{t.name} (primary {t.primary.name}, {t.primary.direction} is better; share {t.share:g})'
        for t in cfg.all_tracks
    )
    mine = f' Your run is on track {track.name}.' if track is not None else ''
    return (
        f'- Tracks: {tracks}. Every idea and seed names its track (`--track <name>`); every keep must also hold the'
        f" other tracks' primaries within their margins and floors.{mine}"
    )
