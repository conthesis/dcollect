from fastapi import APIRouter, Depends
from fastapi.responses import Response

import dcollect.deps as deps

router = APIRouter()


@router.get("/entity-ptr/{entity}")
async def read_ptr(entity: str, model=Depends(deps.model)):
    ptr = await model.get_latest_pointer(entity)
    if ptr is None:
        return Response(status_code=404)
    return Response(ptr)
