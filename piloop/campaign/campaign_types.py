"""Campaign types: what each type of campaign declares about itself, and the rules of the campaign tree.

A type describes itself with a ``CampaignType``: plain data, so that a plugin of any kind can provide it later.
``installed`` is the one place that lists the types. It has the built-in ``optimization`` only, until the plugin
registry provides the others.
"""

from pydantic import BaseModel, ConfigDict

from piloop.db.models import OPTIMIZATION


class CampaignType(BaseModel):
    """What a campaign type declares: its name, its parameters and where it may be in the campaign tree."""

    model_config = ConfigDict(frozen=True, extra='forbid')

    name: str
    """The name that ``campaigns.type`` holds."""
    parameters: dict[str, object] = {}
    """The JSON Schema of the type's own configuration, which the type validates. Empty: the type takes none."""
    leaf: bool = False
    """The type may have no child campaign."""
    child_types: list[str] | None = None
    """The types that a child campaign may have. None: any type."""
    parent_types: list[str] | None = None
    """The types that the parent campaign may have. None: any type."""


BUILT_IN = (CampaignType(name=OPTIMIZATION),)
"""``optimization``: improve the code against its evaluate command. It may have children of any type, for example a
campaign that optimizes one component of the larger system."""


def installed() -> dict[str, CampaignType]:
    """Return the installed campaign types by name.

    TODO: add the campaign-type plugins from the plugin registry when it lands; nothing else changes here.
    """
    return {t.name: t for t in BUILT_IN}


def misplaced(parent: CampaignType, child: CampaignType) -> str | None:
    """Return why a campaign of type ``child`` cannot be under one of type ``parent``, or None when it can."""
    if parent.leaf:
        return f'campaign type {parent.name} is leaf-only'
    if parent.child_types is not None and child.name not in parent.child_types:
        return (
            f"type {child.name} can't be under type {parent.name}: it takes {', '.join(parent.child_types) or 'none'}"
        )
    if child.parent_types is not None and parent.name not in child.parent_types:
        return f"type {child.name} can't be under type {parent.name}: it goes under {', '.join(child.parent_types)}"
    return None
