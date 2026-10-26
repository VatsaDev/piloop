"""The evaluate command: score the checkout it runs in, and report one JSON line of metrics.

piloop runs this command (``evaluate`` in ``piloop.toml``) in a clean checkout of every version that an implementer saves.
Report a value for every metric under ``[[metrics]]`` and exit 0: piloop reads the line from the pipe that
``PILOOP_RESULT_FILE`` names. If the candidate cannot be scored, exit non-zero with the reason on stderr, and piloop
records a crash. Be deterministic: use fixed seeds and fixed splits. Make sure the candidate can change the score only
through what it is supposed to change.

Per-unit results (optional): add ``units``, each metric's contribution per unit (one record, example or test case), as
``{"<metric>": {"<unit id>": <number>, ...}}``. The contributions of a metric must add up to its value. For a mean,
each unit's contribution is its value divided by the count. piloop then tests the primary metric's gain with a paired
bootstrap over the units. Leave out metrics that are not sums, such as a latency.

Replace the body of ``main`` with the real measurement. ``piloop init`` registers the campaign when this file is
different from the template.
"""

import json
import os


def report(result: dict[str, object]) -> None:
    """Append the result to ``PILOOP_RESULT_FILE`` when piloop runs this command, else print it."""
    line = json.dumps(result)
    if path := os.environ.get('PILOOP_RESULT_FILE'):
        with open(path, 'a') as f:
            f.write(line + '\n')
    else:
        print(line)


def main() -> None:
    """Run the candidate, measure it and report the metrics."""
    report({'metrics': {'score': 0.0}})


if __name__ == '__main__':
    main()
