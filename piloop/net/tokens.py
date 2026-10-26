"""Bearer tokens of ``piloop serve`` and ``piloop cas serve``: ``piloop token create|list|revoke``.

A secret is ``plt_`` and 32 random bytes. It is shown once, when it is made: the ``tokens`` table keeps its SHA-256
only. A token's name is the caller's name, and its access says which routes it may call. Core verifies a token on
every network, before the network identifies a caller that sends none (``piloop.net.server.authorized``).

The daemon also mints a token for each agent that it launches (``mint_agent``), which its worker hands it at its start
as ``PILOOP_AGENT_TOKEN``, as a Kubernetes pod gets its projected service-account token. The agent acts only as itself
with it, and only while its session is live.
"""

import hashlib
import secrets
from dataclasses import dataclass

from pydantic import SecretStr
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from piloop import deprecated
from piloop.actor import Agent, CallerKind
from piloop.api.contract import Access
from piloop.api.views import TokenList, TokenMade, TokenView
from piloop.db.models import LIVE, AgentSession, Token
from piloop.db.session import Database

PREFIX = 'plt_'
AGENT_ACCESS = Access.ACT
"""The access of a launched agent's token: every operation on a campaign, and none of the home's own settings."""


@dataclass(frozen=True)
class Caller:
    """Who calls the server: the name of the token's caller, what it may call, and who it is."""

    name: str
    access: Access
    identity: Agent | CallerKind = CallerKind.PERSON
    """The launched agent whose token this is, which acts as that agent and as nobody else, or what the network named:
    a person, who signs as ``human:<name>``, or a service, which signs as ``service:<name>``."""


class NameTaken(ValueError):
    """A token has that name already: revoke it first."""


class NoToken(LookupError):
    """No token has that name."""


def _sha256(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def create(s: Session, name: str, access: Access) -> str:
    """Add a token called ``name`` and return its secret, which nothing keeps. Raises ``NameTaken``."""
    return _added(s, Token(name=name, access=access))


def mint_agent(s: Session, agent: Agent) -> SecretStr:
    """Add a token of ``AGENT_ACCESS`` bound to ``agent``'s session, in place of any that it had. The caller commits.

    Raises:
        NameTaken: A token that is no agent's has the agent's signature as its name.
    """
    s.execute(delete(Token).where(Token.agent_session_id == agent.session))
    token = Token(name=agent.signature, access=AGENT_ACCESS, agent_session_id=agent.session)
    return SecretStr(_added(s, token))


def _added(s: Session, token: Token) -> str:
    secret = PREFIX + secrets.token_urlsafe(32)
    token.sha256 = _sha256(secret)
    s.add(token)
    try:
        s.flush()
    except IntegrityError as e:
        raise NameTaken(f'a token is called {token.name!r} already: `piloop token revoke {token.name}` first') from e
    return secret


def revoke(s: Session, name: str) -> None:
    """Delete the token called ``name``: its next call gets 401. Raises ``NoToken``."""
    if s.execute(delete(Token).where(Token.name == name).returning(Token.id)).first() is None:
        raise NoToken(f'no token is called {name!r}')


def listing(s: Session) -> TokenList:
    """The tokens by name, as the API answers them: never their secrets, which nothing keeps, nor launched agents'."""
    people = select(Token).where(Token.agent_session_id.is_(None)).order_by(Token.name)
    return TokenList(tokens=[_view(t) for t in s.scalars(people)])


def made(s: Session, name: str, access: Access) -> TokenMade:
    """``create``, as the API answers it: with the secret. Raises ``NameTaken``."""
    secret = create(s, name, access)
    token = s.scalars(select(Token).where(Token.name == name)).one()
    return TokenMade(name=token.name, access=token.access, created_at=token.created_at, secret=SecretStr(secret))


def _view(token: Token) -> TokenView:
    return TokenView(name=token.name, access=token.access, created_at=token.created_at)


def verify(s: Session, secret: str) -> Caller | None:
    """The caller that ``secret`` is the token of, or None for an unknown or revoked one, or an agent's that ended."""
    token = s.scalar(select(Token).where(Token.sha256 == _sha256(secret)))
    match token:
        case None:
            return None
        case Token(agent_session_id=None):
            return Caller(token.name, token.access)
        case _:
            agent = s.get(AgentSession, token.agent_session_id)
            if agent is None or agent.state not in LIVE:
                return None
            return Caller(token.name, token.access, Agent(role=agent.kind, session=agent.id))


@deprecated.until('0.5.0', 'an agent that a daemon before agent tokens launched: delete tokenless and its callers')
def tokenless(s: Session, agent: Agent) -> bool:
    """Whether ``agent``'s session is live and was launched with no token: by a daemon before agent tokens."""
    session = s.get(AgentSession, agent.session)
    if session is None or session.state not in LIVE or session.kind != agent.role:
        return False
    return s.scalar(select(Token.id).where(Token.agent_session_id == agent.session)) is None


@dataclass(frozen=True)
class Tokens:
    """The tokens of a database, which a network provider verifies a caller's secret against."""

    db: Database

    def verify(self, secret: str) -> Caller | None:
        """The caller that ``secret`` is the token of, or None for an unknown or revoked one."""
        with self.db.session() as s:
            return verify(s, secret)

    def mint(self, name: str, access: Access) -> str:
        """Add a token called ``name`` in place of the one that has that name, and return its secret."""
        with self.db.session() as s:
            s.execute(delete(Token).where(Token.name == name))
            secret = create(s, name, access)
            s.commit()
        return secret

    @deprecated.until('0.5.0', 'an agent that a daemon before agent tokens launched: delete Tokens.tokenless')
    def tokenless(self, agent: Agent) -> bool:
        """``tokenless``, in a session of its own."""
        with self.db.session() as s:
            return tokenless(s, agent)

    def revoke(self, name: str) -> None:
        """Delete the token called ``name``, if there is one: its next call gets 401."""
        with self.db.session() as s:
            s.execute(delete(Token).where(Token.name == name))
            s.commit()
