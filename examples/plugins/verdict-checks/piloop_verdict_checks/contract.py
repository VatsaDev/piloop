"""The capability ``verdict check``, which this plugin declares, and its contract.

A verdict check looks at a candidate that the built-in verdict rule ``bootstrap`` keeps. It returns a reason to
refuse the keep, or None. The verdict rule ``checked`` of this plugin calls the checks. Other plugins add checks with
an entry point in ``piloop_verdict_checks.checks``, and import this module for the types.
"""

from typing import Protocol

from piloop.capabilities import Case, PureCapability


class VerdictCheck(Protocol):
    """Look at a case that the built-in rule keeps."""

    def check(self, case: Case) -> str | None:
        """Return why the candidate must not be kept, or None."""
        ...


VERDICT_CHECKS = PureCapability('verdict check', group='piloop_verdict_checks.checks', contract=VerdictCheck)
"""Every verdict check by name. The verdict rule ``checked`` calls the ones that its ``checks`` parameter names."""
