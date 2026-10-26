"""``[roles.orchestrator]``: piloop launches the campaign's orchestrator when news arrives for it.

Off by default: with ``triggers = []``, the person runs an orchestrator of their own (the ``piloop-operate`` skill), and
piloop launches none. ``triggers = [{ provider = "news" }]`` turns it on. Each agent of the role resumes the session of
the one before (``continuity = "resume"``). The news comes in notes on its row (the ``news`` trigger).
"""

from importlib.resources import files
from typing import ClassVar

from pydantic import Field

from piloop.plugins.role.contract import Continuity, Files, SweepRole, Verb
from piloop.plugins.section.contract import SectionEntry
from piloop.plugins.trigger.contract import TriggerEntry


class OrchestratorRole(SweepRole):
    """``[roles.orchestrator]``: decides escalations, answers messages, steers the work and records each decision."""

    PROMPT = files(__package__) / 'prompts' / 'orchestrator.md'
    RELAYED: ClassVar[frozenset[Verb]] = frozenset(
        {
            ('sql',),
            ('status',),
            ('history',),
            ('journal',),
            ('cas', 'get'),
            ('restore',),
            ('inbox',),
            ('inbox', 'show'),
            ('inbox', 'ack'),
            ('inbox', 'reply'),
            ('idea', 'add'),
            ('idea', 'seed'),
            ('idea', 'develop'),
            ('idea', 'status'),
            ('idea', 'link'),
            ('idea', 'unlink'),
            ('idea', 'base'),
            ('idea', 'track'),
            ('candidate', 'verdict'),
            ('candidate', 'remeasure'),
            ('candidate', 'note'),
            ('candidate', 'import'),
            ('measurement', 'add'),
            ('eval', 'cancel'),
            ('prompt',),
            ('launch',),
            ('run', 'cancel'),
            ('agent', 'cancel'),
            ('pause',),
            ('resume',),
            ('campaign', 'stop'),
        }
    )

    triggers: list[TriggerEntry] = []
    """Off by default. ``[{ provider = "news" }]``: an agent starts when news arrives for the orchestrator."""
    continuity: Continuity = Continuity.RESUME
    files: Files = Files.IDEAS
    timeout: float = 900
    max_concurrent: int = Field(default=1, ge=1, le=1)
    """One orchestrator at a time: two would answer the same news twice."""
    writes: list[str] = [
        'comments(decision) addressed to the human, with a priority',
        'answers to messages via inbox reply',
        'journal lines',
        'ideas, seeds, verdicts and the campaign changes of the piloop commands in this brief',
    ]
    sections: list[SectionEntry] | None = Field(
        default=[
            {'provider': 'headline'},
            {'provider': 'trajectory'},
            {'provider': 'runs'},
            {'provider': 'queue'},
            {'provider': 'agents'},
            {'provider': 'candidates'},
        ],
        validate_default=True,
    )
    """The status that each brief shows after the news: lean, because each one stays in the resumed session."""
