"""Whom a caller acts as: one table, which the API (``piloop.daemon.api.acting``) and ``piloop sql`` both read.

A caller asks to act as an actor (``x-piloop-actor``, ``Network.actor`` or ``PILOOP_ACTOR``), or as itself. Acting as an
actor grants nothing: core checks the access of every route as before.
"""

from collections.abc import Callable

from pydantic import ValidationError

from piloop.actor import Actor, Agent, Orchestrating, Person, System
from piloop.api.contract import Access
from piloop.net.tokens import Caller

OWNER = Caller('owner', Access.ADMIN)
"""Who calls on the home's Unix socket without a token: only the home's user can open it (Docker's socket)."""


class MayNotAct(Exception):
    """The caller may not act as the actor that it asks for: the API answers 403 with the message."""


def itself(caller: Caller) -> Actor:
    """Who ``caller`` is: its agent, the person at the terminal on the socket, else the person whom it is named for.

    Raises:
        MayNotAct: Its name is no name that signs a write.
    """
    match caller.agent:
        case Agent() as agent:
            return agent
        case None if caller is OWNER:
            return System.HUMAN
        case _:
            try:
                return Person(name=caller.name)
            except ValidationError as e:
                raise MayNotAct(f'{caller.name!r} is no name that signs a write: name the caller another way') from e


def acts_as(caller: Caller, asked: Actor | None, tokenless: Callable[[Agent], bool]) -> Actor:
    """Whom ``caller`` acts as when it asks for ``asked``: ``None`` and ``human`` are itself.

    An agent's token acts only as its agent, and only an agent's token acts as an agent. Any other caller may act as the
    orchestrator, which records the person behind it when the network named one (``Orchestrating``), and ``admin`` as
    any other actor. ``tokenless`` says whether an agent was launched before agent tokens
    (``piloop.net.tokens.tokenless``).

    Raises:
        MayNotAct: Why it may not.
    """
    me = itself(caller)
    match caller.agent, asked:
        case _, None | System.HUMAN:
            return me
        case _, actor if actor == me:
            return me
        case Agent() as bound, actor:
            raise MayNotAct(f'{bound} acts only as itself, not as {actor}')
        case None, Agent() as agent if caller is OWNER and tokenless(agent):
            return agent
        case None, Agent() as agent:
            raise MayNotAct(f'{caller.name} may not act as {agent}: only the token of {agent} acts as it')
        case None, System.ORCHESTRATOR:
            return _orchestrator(me)
        case None, actor if caller.access is Access.ADMIN:
            return actor
        case _:
            raise MayNotAct(f'{caller.name} has {caller.access} access, which may not act as {asked}')


def _orchestrator(me: Actor) -> Actor:
    """The orchestrator as ``me`` acts as it: a person whom the network named signs as both, the owner as it alone."""
    match me:
        case Person() as person:
            return Orchestrating(person=person)
        case _:
            return System.ORCHESTRATOR
