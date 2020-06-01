from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse

import dcollect.cas as cas
import dcollect.deps as deps
from dcollect.util import pointer_as_str

router = APIRouter()


async def internal_ingest(
    model, entity: str, data: Dict[str, Any]
) -> Tuple[bytes, int]:
    pointer = await cas.store(model, data)
    version = await model.store_vsn(entity, pointer)
    return pointer, version


@router.post("/entity/{entity}", response_class=ORJSONResponse)
async def ingest(
    entity: str,
    data: Dict[str, Any],
    model=Depends(deps.model),
    notify=Depends(deps.notify),
):
    (pointer, version) = await internal_ingest(model, entity, data)
    await notify.schedule(entity)
    return {"pointer": pointer_as_str(pointer), "version": version}
