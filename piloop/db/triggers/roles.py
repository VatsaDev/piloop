"""Every agent has a role of its campaign: the persistent trigger on ``agent_sessions``."""

from piloop.db.triggers.ddl import Trigger


def known_role() -> Trigger:
    """Persistent: refuse a ``agent_sessions`` row whose ``kind`` is not a role of its campaign, if it has one.

    The roles of a campaign are the ``plugins.role`` keys of the config that ``piloop init`` stored: every role of a
    package, piloop's included, and every ``[plugins.role.<name>]`` table of its piloop.toml. A CHECK cannot read another row, so this
    is a trigger. The daemon never changes the kind of a row, so the trigger checks INSERT only.
    """
    return Trigger(
        'agent_sessions_kind',
        'agent_sessions',
        'BEFORE INSERT',
        when=(
            "NEW.campaign_id IS NOT NULL AND (SELECT json_type(config, '$.plugins.role.' || json_quote(NEW.kind))"
            ' FROM campaigns WHERE id = NEW.campaign_id) IS NULL'
        ),
        body=(
            "SELECT RAISE(ABORT, 'agent_sessions.kind must be a role of the campaign: a key of plugins.role in its"
            " config')"
        ),
    )
