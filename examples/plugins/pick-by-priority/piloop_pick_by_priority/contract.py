"""The contract of an idea picker: what the host passes and what a picker returns. Data only, so it fits WIT.

piloop has no pure capability yet, so this module stands in for the contract module that piloop will ship with one.
"""

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Idea:
    """An idea that is ready to run."""

    id: int
    priority: int
    track: str
    attempts: int


@dataclass(frozen=True)
class Pick:
    """Run this idea now."""

    id: int
    reason: str


@dataclass(frozen=True)
class Wait:
    """Run nothing now: the error case, returned as a value."""

    reason: str


class Picker(Protocol):
    """Pick the next idea to run, from the ready ones and the free slots."""

    def pick(self, ideas: list[Idea], free_slots: int) -> Pick | Wait: ...
