from datetime import timedelta

from sqlalchemy import select

from piloop.api.views import Lines, Rendered
from piloop.campaign.ideas import defined_by, yields
from piloop.db.models import ENDED, LIVE, AgentSession, AgentSessionState
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.reports.overview import agent_output, defining, on
from piloop.text import NONE, age, cell
from piloop.usage import human


class Agents(Section):
    """The agents: the live ones with their last line, the count of each role, the yield of each source, the tokens."""

    SHOWN_IN = frozenset({Surface.STATUS, Surface.SWEEP})
    ORDER = 160

    def render(self, r: Rendering) -> Rendered:
        s, campaign, now = r.s, r.campaign, r.now
        cfg = campaign.config
        rows = s.scalars(
            select(AgentSession).where(AgentSession.campaign_id == campaign.id).order_by(AgentSession.id)
        ).all()
        live = [
            f'  - {w.kind}#{w.id} {w.harness or NONE}/{w.model or NONE} {age(w.started_at, now)}, on {on(s, w)}: '
            + (cell(output[-1]) if (output := agent_output(s, w, 1)) else NONE)
            for w in rows
            if w.state in LIVE
        ]
        requested = [f'{w.kind}#{w.id}' for w in rows if w.state is AgentSessionState.REQUESTED]
        per_kind: list[str] = []
        spend: list[str] = []
        for kind in dict.fromkeys([*cfg.roles.all(), *(w.kind for w in rows)]):
            mine = [w for w in rows if w.kind == kind]
            killed = sum(w.state is AgentSessionState.KILLED for w in mine)
            per_kind.append(f'{len(mine)} {kind}' + (f' ({killed} killed)' if killed else ''))
            spend.append(f'{kind} {human(sum(_tokens(w) for w in mine))}')
        defined = defined_by(s, campaign.id)
        barren = []
        for kind in defining(cfg):
            sweeps = [w for w in rows if w.kind == kind and w.state in ENDED]
            empty = sum(f'{kind}#{w.id}' not in defined for w in sweeps)
            barren.append(f'; {empty} of {len(sweeps)} ended {kind} sweeps defined no idea')
        hour = sum(_tokens(w) for w in rows if w.ended_at is not None and now - w.ended_at <= timedelta(hours=1))
        by_source = '; '.join(y.line() for y in yields(s, campaign.id)) or 'none yet'
        lines = (
            f'- Agents live: {len(live) or "none"}' + (f'; requested: {", ".join(requested)}' if requested else ''),
            *live,
            f'- Agents so far: {", ".join(per_kind)}{"".join(barren)}.',
            f'- Ideas by source: {by_source}.',
            f'- Tokens: {human(sum(_tokens(w) for w in rows))} ({", ".join(spend)}); {human(hour)} by agents that'
            ' ended in the last hour.',
        )
        return Rendered(title='Agents', blocks=(Lines(lines=lines),))


def _tokens(w: AgentSession) -> int:
    return w.usage.total if w.usage else 0
