"""The evaluate command: score the checkout it runs in and print one ``METRIC <name>=<value>`` line per metric.

piloop runs this command (``evaluate`` in ``piloop.toml``) in a clean worktree of every commit that a worker makes.
Print every metric under ``[[metrics]]`` in ``piloop.toml`` to stdout and exit 0. If the candidate cannot be scored,
exit non-zero with the reason on stderr, and piloop records a crash. Be deterministic: use fixed seeds and fixed
splits. Make sure the candidate can change the score only through what it is supposed to change.

Per-unit results (optional): when ``PILOOP_UNITS_FILE`` is set, also write each metric's contribution per unit (one
record, example or test case) to that file as ``{"<metric>": {"<unit id>": <number>, ...}}``. The contributions of a
metric must add up to its METRIC value. For a mean, each unit's contribution is its value divided by the count. piloop
then tests the primary metric's gain with a paired bootstrap over the units. Leave out metrics that are not sums, such
as a latency. piloop sets the variable only for its own measurements.

Replace the body of ``main`` with the real measurement. ``piloop init`` registers the project when this file is
different from the template.
"""


def main() -> None:
    """Run the candidate, measure it and print the metrics."""
    print('METRIC score=0.0')


if __name__ == '__main__':
    main()
