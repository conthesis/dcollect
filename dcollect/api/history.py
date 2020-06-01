from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse

import dcollect.deps as deps
from dcollect.util import pointer_as_str

router = APIRouter()


@router.get("/entity/{entity}/history", response_class=ORJSONResponse)
async def read_item_history(entity: str, model=Depends(deps.model)):
    history = [
        {"vsn": int(vsn.decode("utf-8")), "pointer": pointer_as_str(pointer)}
        for (vsn, pointer) in await model.get_history(entity)
    ]
    return {"history": history}
