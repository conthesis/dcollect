from typing import Any, Dict, Tuple

from fastapi import APIRouter, Depends
from fastapi.responses import ORJSONResponse

import dcollect.deps as deps
from dcollect.util import pointer_as_str

router = APIRouter()

@router.post("/entity/{entity}", response_class=ORJSONResponse)
async def ingest(
    entity: str,
    data: Dict[str, Any],
    model=Depends(deps.model),
        cas=Depends(deps.cas),

) -> Dict[str, Any]:
    pointer = await cas.store(data)
    version = await model.store_vsn(entity, pointer)
    return {"pointer": pointer_as_str(pointer), "version": version}
