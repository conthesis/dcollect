from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse, Response

import dcollect.cas as cas
import dcollect.deps as deps
from dcollect.util import guess_media_type

router = APIRouter()


async def read_versioned(model, entity):
    ptr = await model.get_latest_pointer(entity)
    if ptr is None:
        return None
    data = await model.get_ca(ptr)
    return data


@router.get("/entity/{entity}")
async def read_item(entity: str, model=Depends(deps.model)):
    data = await read_versioned(model, entity)
    if data is None:
        return Response(status_code=404)
    return Response(data, media_type=guess_media_type(data))
