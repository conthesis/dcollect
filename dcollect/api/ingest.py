from fastapi import APIRouter, Depends, Request
from fastapi.responses import ORJSONResponse

import dcollect.deps as deps
from dcollect.model import Notification
from dcollect.notify import Notify
from dcollect.util import pointer_as_str

router = APIRouter()


@router.post("/entity-ptr/{entity}", response_class=ORJSONResponse)
async def ingest_ptr(
    request: Request,
    entity: str,
    model=Depends(deps.model),
    notify: Notify = Depends(deps.notify),
):
    pointer = await request.body()
    if pointer == b"":
        return ORJSONResponse({"err": "Empty ptr input"}, status_code=400)
    version = await model.store_vsn(entity, pointer)
    await notify.send_notification(Notification(entity=entity, version=version))
    return {"pointer": pointer_as_str(pointer), "version": version}
