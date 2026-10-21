"""Every agent has a role of its campaign: the persistent trigger on ``agent_sessions``."""

from piloop.db.models import RoleName
from piloop.db.triggers.ddl import Trigger


def known_role() -> Trigger:
    """Persistent: refuse a ``agent_sessions`` row whose ``kind`` is neither a built-in role nor a role of its campaign.

    The roles of a campaign are the ``roles`` keys of the config that ``piloop init`` stored. A CHECK cannot read
    another row, so this is a trigger. The daemon never changes the kind of a row, so the trigger checks INSERT only.
    """
    built_in = ', '.join(f"'{kind}'" for kind in RoleName)
    names = ', '.join(RoleName)
    return Trigger(
        'agent_sessions_kind',
        'agent_sessions',
        'BEFORE INSERT',
        when=(
            f"NEW.kind NOT IN ({built_in}) AND (SELECT json_type(config, '$.roles.' || json_quote(NEW.kind))"
            ' FROM campaigns WHERE id = NEW.campaign_id) IS NULL'
        ),
        body=(
            f"SELECT RAISE(ABORT, 'agent_sessions.kind must be a role of the campaign: {names} or a [roles.<name>] table of"
            " its piloop.toml')"
        ),
    )
