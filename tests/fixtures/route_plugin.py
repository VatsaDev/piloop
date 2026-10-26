"""A memory provider that contributes an authenticated route, with ``piloop.capabilities`` alone, as a package would."""

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from piloop.capabilities import REFUSALS_DOCUMENTED, Acting, Addressed, Contribution, LocalMemory, actor_access, reader


class Greeting(BaseModel):
    campaign: str
    to: str


router = APIRouter(prefix='/v1', dependencies=[Depends(reader)], responses=REFUSALS_DOCUMENTED)


@router.post('/campaigns/{campaign}/greeting', operation_id='greeting', dependencies=[Depends(actor_access)])
def greeting(me: Acting, campaign: Addressed) -> Greeting:
    return Greeting(campaign=campaign.name, to=me.signature)


class Greeter(LocalMemory):
    """``local``, and a route that greets the caller by the actor it signs as."""

    def contribution(self) -> Contribution:
        return Contribution(router=router)
