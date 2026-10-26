"""The stops of the evaluation ladder: a floor whatever the bar, a regression from the bar at the same rung."""

import pytest

from piloop.config import Rung, Tolerance
from piloop.ids import RungName
from piloop.plugins.metric import Direction
from piloop.scoring.ladder import Gate, stop_reason

DIRECTIONS = {'accuracy': Direction.HIGHER, 'latency_ms': Direction.LOWER}
SCREEN = Rung(
    name=RungName('screen'),
    stop={'accuracy': Tolerance(floor=0.5, margin=0.03), 'latency_ms': Tolerance(fraction=0.1)},
)
BAR = {'accuracy': 0.702, 'latency_ms': 38.4}


@pytest.mark.parametrize(
    ('mine', 'reason'),
    [
        ({'accuracy': 0.69, 'latency_ms': 40}, None),
        ({'accuracy': 0.664, 'latency_ms': 38.4}, 'accuracy 0.664 vs 0.702 (-0.038; margin 0.03)'),
        ({'accuracy': 0.4, 'latency_ms': 38.4}, 'accuracy 0.4 past its floor 0.5'),
        ({'accuracy': 0.75, 'latency_ms': 45}, 'latency_ms 45 vs 38.4 (+6.6; fraction 0.1)'),
        ({'accuracy': 0.75, 'latency_ms': 30}, None),
    ],
)
def test_a_rung_stops_at_its_first_stop_that_holds(mine: dict[str, float], reason: str | None) -> None:
    assert stop_reason(SCREEN, mine, BAR, DIRECTIONS) == reason


def test_without_the_bars_value_only_a_floor_stops() -> None:
    assert stop_reason(SCREEN, {'accuracy': 0.1, 'latency_ms': 99}, {}, DIRECTIONS) == 'accuracy 0.1 past its floor 0.5'
    assert stop_reason(SCREEN, {'accuracy': 0.6, 'latency_ms': 99}, None, DIRECTIONS) is None


def test_a_gate_reads_the_bar_at_the_same_rung() -> None:
    gate = Gate(bar={RungName('screen'): BAR, RungName('full'): {'accuracy': 0.1}}, directions=DIRECTIONS)
    assert (
        gate.stop_reason(SCREEN, {'accuracy': 0.6, 'latency_ms': 38.4}) == 'accuracy 0.6 vs 0.702 (-0.102; margin 0.03)'
    )


def test_a_floor_alone_lets_a_value_past_the_bar_go_on() -> None:
    smoke = Rung(name=RungName('smoke'), stop={'accuracy': Tolerance(floor=0.5)})
    assert stop_reason(smoke, {'accuracy': 0.6}, {'accuracy': 0.9}, DIRECTIONS) is None
