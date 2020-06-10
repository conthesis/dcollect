from typing import Any, Dict

from fastapi import APIRouter, Depends, Request
from fastapi.responses import ORJSONResponse

import dcollect.deps as deps
from dcollect.util import pointer_as_str

router = APIRouter()


@router.post("/entity/{entity}", response_class=ORJSONResponse)
async def ingest(
    entity: str, data: Dict[str, Any], model=Depends(deps.model), cas=Depends(deps.cas),
) -> Dict[str, Any]:
    pointer = await cas.store(data)
    version = await model.store_vsn(entity, pointer)
    return {"pointer": pointer_as_str(pointer), "version": version}


@router.post("/entity-ptr/{entity}", response_class=ORJSONResponse)
async def ingest_ptr(
        request: Request,
        entity: str, model=Depends(deps.model), cas=Depends(deps.cas),
) -> Dict[str, Any]:
    pointer = await request.body()
    if pointer == b'':
        return ORJSONResponse({"err": "Empty ptr input"}, status_code=400)
    version = await model.store_vsn(entity, pointer)
    return {"pointer": pointer_as_str(pointer), "version": version}
