"""Bearer tokens of ``piloop serve`` and ``piloop cas serve``: ``piloop token create|list|revoke``.

A secret is ``plt_`` and 32 random bytes. It is shown once, when it is made: the ``tokens`` table keeps its SHA-256
only. A token's name is the caller's name, and its access says which routes it may call. Core verifies a token on
every network, before the network identifies a caller that sends none (``piloop.net.server.authorized``).
"""

import hashlib
import secrets
from dataclasses import dataclass

from pydantic import SecretStr
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from piloop.api.contract import Access
from piloop.api.views import TokenList, TokenMade, TokenView
from piloop.db.models import Token
from piloop.db.session import Database

PREFIX = 'plt_'


@dataclass(frozen=True)
class Caller:
    """Who calls the server: the name of the token's caller, and what it may call."""

    name: str
    access: Access


class NameTaken(ValueError):
    """A token has that name already: revoke it first."""


class NoToken(LookupError):
    """No token has that name."""


def _sha256(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def create(s: Session, name: str, access: Access) -> str:
    """Add a token called ``name`` and return its secret, which nothing keeps. Raises ``NameTaken``."""
    secret = PREFIX + secrets.token_urlsafe(32)
    s.add(Token(name=name, access=access, sha256=_sha256(secret)))
    try:
        s.flush()
    except IntegrityError as e:
        raise NameTaken(f'a token is called {name!r} already: `piloop token revoke {name}` first') from e
    return secret


def revoke(s: Session, name: str) -> None:
    """Delete the token called ``name``: its next call gets 401. Raises ``NoToken``."""
    if s.execute(delete(Token).where(Token.name == name).returning(Token.id)).first() is None:
        raise NoToken(f'no token is called {name!r}')


def listing(s: Session) -> TokenList:
    """The tokens by name, as the API answers them: never their secrets, which nothing keeps."""
    return TokenList(tokens=[_view(t) for t in s.scalars(select(Token).order_by(Token.name))])


def made(s: Session, name: str, access: Access) -> TokenMade:
    """``create``, as the API answers it: with the secret. Raises ``NameTaken``."""
    secret = create(s, name, access)
    token = s.scalars(select(Token).where(Token.name == name)).one()
    return TokenMade(name=token.name, access=token.access, created_at=token.created_at, secret=SecretStr(secret))


def _view(token: Token) -> TokenView:
    return TokenView(name=token.name, access=token.access, created_at=token.created_at)


def verify(s: Session, secret: str) -> Caller | None:
    """The caller that ``secret`` is the token of, or None for an unknown or revoked one."""
    token = s.scalar(select(Token).where(Token.sha256 == _sha256(secret)))
    return Caller(token.name, token.access) if token is not None else None


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

    def revoke(self, name: str) -> None:
        """Delete the token called ``name``, if there is one: its next call gets 401."""
        with self.db.session() as s:
            s.execute(delete(Token).where(Token.name == name))
            s.commit()
