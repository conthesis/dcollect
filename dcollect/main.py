import asyncio
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import orjson
from fastapi import BackgroundTasks, Depends, FastAPI, Response
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel

import dcollect.cas as cas
import dcollect.deps as deps
import dcollect.status as status
from dcollect.util import guess_media_type, now, pointer_as_str

app = FastAPI()

app.include_router(status.router, prefix="/status", tags=["status"])


@app.on_event("startup")
async def startup():
    await deps.model().setup()
    await deps.mq().startup()
    await deps.notify().setup()


@app.on_event("shutdown")
async def shutdown():
    await deps.http_client().aclose()
    await deps.model().teardown()
    await deps.mq().shutdown()


class StoreRequest(BaseModel):
    entity: str
    version: Optional[int] = None
    data: Dict[str, Any]


class WatchRequest(BaseModel):
    url: str


class UnwatchRequest(BaseModel):
    url: str


class WatchMultipleItem(BaseModel):
    entity: str
    url: str


class WatchMultipleRequest(BaseModel):
    to_watch: List[WatchMultipleItem]


async def read_versioned(model, entity):
    ptr = await model.get_latest_pointer(entity)
    if ptr is None:
        return None
    data = await model.get_ca(ptr)
    return data


@app.get("/entity/{entity}")
async def read_item(entity: str, model=Depends(deps.model)):
    data = await read_versioned(model, entity)
    if data is None:
        return Response(status_code=404)
    return Response(data, media_type=guess_media_type(data))


@app.get("/entity/{entity}/history", response_class=ORJSONResponse)
async def read_item_history(entity: str, model=Depends(deps.model)):
    history = [
        {"vsn": vsn, "pointer": pointer_as_str(pointer)}
        async for (vsn, pointer) in model.get_history(entity)
    ]
    return {"history": history}


async def internal_ingest(
    model, entity: str, version: Optional[int], data: Dict[str, Any]
) -> Tuple[bytes, int]:
    if version is None:
        version = now()
    pointer = await cas.store(data)
    await model.store_vsn(entity, version, pointer)
    return pointer, version


@app.post("/entity/{entity}/watch")
async def watch(entity: str, watch_request: WatchRequest, model=Depends(deps.model)):
    await model.watch_store(entity, watch_request.url)


@app.post("/watchMultiple")
async def watch_multiple(
    watch_multiple: WatchMultipleRequest, model=Depends(deps.model)
):
    for x in watch_multiple.to_watch:
        await model.watch_store(x.entity, x.url)


@app.post("/unwatchMultiple")
async def unwatch_multiple(
    unwatch_multiple: WatchMultipleRequest, model=Depends(deps.model)
):
    for x in unwatch_multiple.to_watch:
        await model.watch_delete(x.entity, x.url)


@app.post("/entity/{entity}/unwatch")
async def unwatch(
    entity: str, unwatch_request: UnwatchRequest, model=Depends(deps.model)
):
    await model.watch_delete(entity, unwatch_request.url)


@app.post("/entity/{entity}", response_class=ORJSONResponse)
async def ingest(
    entity: str,
    data: Dict[str, Any],
    model=Depends(deps.model),
    notify=Depends(deps.notify),
):
    (pointer, version) = await internal_ingest(model, entity, None, data)
    await notify.schedule(entity)
    return {"version": version, "pointer": pointer_as_str(pointer)}
