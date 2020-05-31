from typing import Any, Dict, Optional, Tuple

from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse

import dcollect.cas as cas
import dcollect.deps as deps
from dcollect.util import now, pointer_as_str

router = APIRouter()


async def internal_ingest(
    model, entity: str, version: Optional[int], data: Dict[str, Any]
) -> Tuple[bytes, int]:
    if version is None:
        version = now()
    pointer = await cas.store(model, data)
    await model.store_vsn(entity, version, pointer)
    return pointer, version


@router.post("/entity/{entity}", response_class=ORJSONResponse)
async def ingest(
    entity: str,
    data: Dict[str, Any],
    model=Depends(deps.model),
    notify=Depends(deps.notify),
):
    (pointer, version) = await internal_ingest(model, entity, None, data)
    await notify.schedule(entity)
    return {"version": version, "pointer": pointer_as_str(pointer)}
