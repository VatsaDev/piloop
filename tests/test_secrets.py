"""The secrets capability: a campaign's secret wins over the home's, and the events say who set one, never its value."""

from pydantic import SecretStr

from piloop.actor import System
from piloop.db.models import AgentSession, Campaign
from piloop.db.session import Database
from piloop.ids import AgentSessionId
from piloop.plugins.secrets.contract import SECRETS, SecretKey, listed, remove_secret, set_secret, values
from piloop.plugins.secrets.local import LocalSecrets


def test_an_agent_gets_its_campaigns_secret_over_the_homes_and_a_listing_says_who_set_each(
    db: Database, campaign: Campaign
) -> None:
    local = SECRETS.selected({})
    assert isinstance(local, LocalSecrets)
    with db.session() as s:
        set_secret(s, local, System.HUMAN, None, 'KEY', SecretStr('home'))
        set_secret(s, local, System.ORCHESTRATOR, None, 'KEY', SecretStr('home, again'))
        set_secret(s, local, System.HUMAN, campaign.id, 'KEY', SecretStr('campaign'))
        local.put(s, None, 'ELSEWHERE', SecretStr('not through piloop'))
        session = AgentSession(kind='watchdog', campaign_id=campaign.id)
        orphan = AgentSession(kind='watchdog', campaign_id=None)
        s.add_all([session, orphan])
        s.commit()
        assert {k: v.get_secret_value() for k, v in values(s, local, session.id, ['KEY', 'NONE']).items()} == {
            'KEY': 'campaign'
        }
        assert values(s, local, orphan.id, ['KEY'])['KEY'].get_secret_value() == 'home, again'
        assert values(s, local, AgentSessionId(99), ['KEY'])['KEY'].get_secret_value() == 'home, again'
        found = [(f.secret, f.set_by) for f in listed(s, local)]
        assert found == [
            (SecretKey(None, 'ELSEWHERE'), None),
            (SecretKey(None, 'KEY'), 'orchestrator'),
            (SecretKey(campaign.id, 'KEY'), 'human'),
        ]
        assert remove_secret(s, local, System.HUMAN, campaign.id, 'KEY')
        assert not remove_secret(s, local, System.HUMAN, campaign.id, 'KEY')
        s.commit()
        assert values(s, local, session.id, ['KEY'])['KEY'].get_secret_value() == 'home, again'
