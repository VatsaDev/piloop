"""``by_priority``: pick the ready idea of the highest priority, the one tried least first. No state, no I/O."""

from piloop_pick_by_priority.contract import Idea, Pick, Wait


class ByPriority:
    """The highest priority first; among equals, the idea tried least, then the oldest."""

    def pick(self, ideas: list[Idea], free_slots: int) -> Pick | Wait:
        if free_slots < 1:
            return Wait('no free slot')
        if not ideas:
            return Wait('no idea is ready')
        best = max(ideas, key=lambda i: (i.priority, -i.attempts, -i.id))
        return Pick(best.id, f'priority {best.priority}, {best.attempts} attempt(s)')


BY_PRIORITY = ByPriority()
"""The provider: an instance, as the entry point of a pure provider names."""
