from typing import Any, Dict

from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse

import dcollect.deps as deps
from dcollect.util import pointer_as_str

router = APIRouter()


@router.get("/entity/{entity}/history", response_class=ORJSONResponse)
async def read_item_history(entity: str, model=Depends(deps.model)) -> Dict[str, Any]:
    history = [
        {"pointer": pointer_as_str(pointer)}
        for pointer in await model.get_history(entity)
    ]
    return {"history": history}
