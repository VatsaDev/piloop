"""The ``piloop measurement`` command: record a measurement made outside the daemon."""

from pathlib import Path
from typing import Annotated

import cyclopts
from pydantic import ValidationError

from piloop.campaign import ops
from piloop.cli import common, local
from piloop.ids import CandidateId

app = cyclopts.App(name='measurement')  # its help is where piloop.cli.main registers it


@app.command(name='add')
def measurement_add(
    candidate_id: Annotated[CandidateId, cyclopts.Parameter(help='The candidate that was measured.')],
    /,
    *,
    report: Annotated[
        Path,
        cyclopts.Parameter(help='A JSON file: {"metrics": {"<name>": <value>}, "seconds": <s>, "error": "<why>"}.'),
    ],
) -> None:
    """Record a measurement of a candidate under the campaign's current evaluator, and have its verdict judged again.

    Use it for an evaluation that you ran yourself, for example the evaluate command on the candidate's commit after
    the daemon's run timed out. `metrics` takes the metrics of piloop.toml, all of them unless `error` says why the
    evaluation failed. `seconds` and `error` are optional. A pending candidate is refused. When the verdict is the
    daemon's crash, discard or interesting, the daemon judges the candidate again with every measurement under the
    current evaluator, the next time it measures the campaign. A keep, and a verdict that someone set, stay. The output
    says which. The measurement is signed as PILOOP_ACTOR.
    """
    try:
        parsed = ops.Report.model_validate_json(report.read_text())
    except ValidationError as e:
        raise SystemExit(f'the report is not {{"metrics": {{…}}, "seconds": …, "error": …}}: {e}') from e
    with local.refused(), local.session() as s:
        added = ops.add_measurement(s, candidate_id, parsed, actor=common.settings.actor)
        s.commit()
        common.console.print(
            f'measurement#{added.measurement.id} of candidate#{candidate_id}: {added.effect}', markup=False
        )
