from piloop.api.views import Lines, Rendered
from piloop.config import TRANSCRIPT
from piloop.db.schema import tables
from piloop.plugins.section.contract import Rendering, Section, Surface
from piloop.plugins.vcs.contract import VCS, prompted
from piloop.reports.history import IDEAS_IN_HISTORY

TOOLS = [
    '- `piloop sql "<one statement>"` (the full command is under Context) takes nothing else: no flags, no --help.'
    ' It prints a table of at most 500 rows, and wraps no cell when its output is not a terminal.',
    "- Scores: `json_extract(m.metrics, '$.<metric>')`; a candidate's score is the mean over its measurements with"
    ' `error IS NULL AND rung IS NULL AND evaluator_id = campaigns.evaluator_id` (a `rung` is a step of the ladder).'
    f' `piloop history` prints the latest {IDEAS_IN_HISTORY} ideas with their outcomes.',
    f'- Transcripts: `<runs.log_dir>/{TRANSCRIPT}` (an implementer), `<agent_sessions.log_dir>/{TRANSCRIPT}` and'
    " `brief.md` beside it (any agent); the whole session, the harness's own file:"
    ' `piloop cas get <agent_sessions.session_file>`.',
    '{vcs:code}',
    "- Who changed what: `SELECT at, actor, kind, payload FROM events WHERE target_kind='idea' AND target_id=12`.",
    '- Full-text search: `ideas_fts` (title, hypothesis, plan), `comments_fts` (body); see the procedures above.',
    '- Tables and columns:',
]


class Tools(Section):
    """How an agent queries the database, the transcripts and the history, with every table and its columns.

    With the tables listed, an agent does not query the schema.
    """

    SHOWN_IN = frozenset({Surface.SWEEP})
    ORDER = 900

    def render(self, r: Rendering) -> Rendered:
        vcs = VCS.selected(r.campaign.config.plugins)
        schema = [f'  - {t.name}({", ".join(c.name for c in t.columns)})' for t in tables()]
        lines = (*(prompted(line, vcs) for line in TOOLS), *schema)
        return Rendered(title='Tools', blocks=(Lines(lines=lines),))
