"""Every agent has a role of its project: the persistent trigger on ``workers``."""

from piloop.db.models import WorkerKind
from piloop.db.triggers.ddl import Trigger


def known_role() -> Trigger:
    """Persistent: refuse a ``workers`` row whose ``kind`` is neither a built-in role nor a role of its project.

    The roles of a project are the ``roles`` keys of the config that ``piloop init`` stored. A CHECK cannot read
    another row, so this is a trigger. The daemon never changes the kind of a row, so the trigger checks INSERT only.
    """
    built_in = ', '.join(f"'{kind}'" for kind in WorkerKind)
    names = ', '.join(WorkerKind)
    return Trigger(
        'workers_kind',
        'workers',
        'BEFORE INSERT',
        when=(
            f"NEW.kind NOT IN ({built_in}) AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind))"
            ' FROM projects WHERE id = NEW.project_id) IS NULL'
        ),
        body=(
            f"SELECT RAISE(ABORT, 'workers.kind must be a role of the project: {names} or a [roles.<name>] table of"
            " its piloop.toml')"
        ),
    )
