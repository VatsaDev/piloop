"""Bearer tokens of the server that ``piloop serve --listen`` runs: ``piloop token create|list|revoke``.

A secret is ``plt_`` and 32 random bytes. It is shown once, when it is made: the ``tokens`` table keeps its SHA-256
only. A token's name is the caller's name, and its scope says which routes it may call.
"""

import hashlib
import secrets
from dataclasses import dataclass

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from piloop.db.models import Token, TokenScope
from piloop.db.session import Database

PREFIX = 'plt_'


@dataclass(frozen=True)
class Caller:
    """Who calls the server: the name of the token's caller, and what it may call."""

    name: str
    scope: TokenScope


class NameTaken(ValueError):
    """A token has that name already: revoke it first."""


class NoToken(LookupError):
    """No token has that name."""


def _sha256(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


def create(s: Session, name: str, scope: TokenScope) -> str:
    """Add a token called ``name`` and return its secret, which nothing keeps. Raises ``NameTaken``."""
    secret = PREFIX + secrets.token_urlsafe(32)
    s.add(Token(name=name, scope=scope, sha256=_sha256(secret)))
    try:
        s.flush()
    except IntegrityError as e:
        raise NameTaken(f'a token is called {name!r} already: `piloop token revoke {name}` first') from e
    return secret


def revoke(s: Session, name: str) -> None:
    """Delete the token called ``name``: its next call gets 401. Raises ``NoToken``."""
    if s.execute(delete(Token).where(Token.name == name).returning(Token.id)).first() is None:
        raise NoToken(f'no token is called {name!r}')


def verify(s: Session, secret: str) -> Caller | None:
    """The caller that ``secret`` is the token of, or None for an unknown or revoked one."""
    token = s.scalar(select(Token).where(Token.sha256 == _sha256(secret)))
    return Caller(token.name, token.scope) if token is not None else None


@dataclass(frozen=True)
class Tokens:
    """The tokens of a database, which a network provider verifies a caller's secret against."""

    db: Database

    def verify(self, secret: str) -> Caller | None:
        """The caller that ``secret`` is the token of, or None for an unknown or revoked one."""
        with self.db.session() as s:
            return verify(s, secret)
