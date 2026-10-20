"""Idea links stay inside one campaign: the persistent trigger on ``idea_links``."""

from piloop.db.triggers.ddl import Trigger


def same_campaign() -> Trigger:
    """Persistent: refuse an ``idea_links`` row whose two ideas belong to different campaigns.

    A prerequisite or a superseded idea in another campaign would block or reject ideas across campaigns. A CHECK
    cannot read another row, so this is a trigger. Rows are never updated, so the trigger checks INSERT only.
    ``IS NOT`` also refuses an idea id that does not exist (the foreign key would too, with a less clear message).
    """
    return Trigger(
        'idea_links_campaign',
        'idea_links',
        'BEFORE INSERT',
        when=(
            '(SELECT campaign_id FROM ideas WHERE id = NEW.idea_id)'
            ' IS NOT (SELECT campaign_id FROM ideas WHERE id = NEW.target_id)'
        ),
        body="SELECT RAISE(ABORT, 'idea_links: idea_id and target_id must be ideas of the same campaign')",
    )
