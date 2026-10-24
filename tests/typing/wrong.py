"""Providers that ty refuses in their own package: ``tests/test_typing.py`` expects one error on each ``# wrong`` line.

ty models pydantic's lax mode, which turns a string into a float, so the wrong parameter here is None.
"""

from piloop_min_gain import MinGain

from piloop.capabilities import Bootstrap, Case, Limits, VerdictRule


class Late(Bootstrap):
    def decide(self, case: Case) -> str:  # wrong
        return 'keep'


MinGain(min_gain=None)  # wrong
rule: VerdictRule = Limits()  # wrong
