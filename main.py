import asyncio
import base64
import datetime
import hashlib
from typing import Any, Dict, Optional, Tuple

import filetype  # type: ignore
import httpx
import orjson
from fastapi import BackgroundTasks, FastAPI, Response
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel

import cas
import model


def now() -> int:
    return int(
        datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).timestamp() * 1000
    )


def pointer_as_str(pointer: bytes):
    return base64.b64encode(pointer)


app = FastAPI()

http_client = None


@app.on_event("startup")
async def startup():
    http_client = httpx.AsyncClient()
    await model.setup()


@app.on_event("shutdown")
async def shutdown():
    await http_client.aclose()
    await model.teardown()


class StoreRequest(BaseModel):
    entity: str
    version: Optional[int] = None
    data: Dict[str, Any]


class WatchRequest(BaseModel):
    url: str


class UnwatchRequest(BaseModel):
    url: str


def guess_media_type(data: bytes):
    kind = filetype.match(data)
    if kind is not None:
        return kind.type
    elif data[0] == b"{"[0]:
        return "appliction/json"
    else:
        return None


async def read_versioned(entity):
    ptr = await model.get_latest_pointer(entity)
    if ptr is None:
        return None
    data = await model.get_ca(ptr)
    return data


@app.get("/entity/{entity}")
async def read_item(entity: str):
    data = await read_versioned(entity)
    if data is None:
        return Response(status_code=404)
    return Response(data, media_type=guess_media_type(data))


@app.get("/entity/{entity}/history", response_class=ORJSONResponse)
async def read_item_history(entity: str):
    history = [
        {"vsn": vsn, "pointer": pointer_as_str(pointer)}
        async for (vsn, pointer) in model.get_history(entity)
    ]
    return {"history": history}


async def internal_ingest(
    entity: str, version: Optional[int], data: Dict[str, Any]
) -> Tuple[bytes, int]:
    if version is None:
        version = now()
    pointer = await cas.store(data)
    await model.store_vsn(entity, version, pointer)
    return pointer, version


async def send_notification(url: str, entity: str):
    async with httpx.AsyncClient() as client:
        if client is None:
            raise RuntimeError("Trying to send notification before ready")
        body = {"entity": entity}
        resp = await client.post(url=url, json=body)
        return resp.status_code == 200


async def notify_watcher(entity: str, url: str, version: int):
    if await send_notification(url, entity):
        model.update_watch(entity, url, version)


async def notify_watchers(entity: str):
    update_promises = []
    async for (url, version) in model.get_trailing_watches_for_entity(entity):
        update_promises.append(notify_watcher(entity, url, version))
    await asyncio.gather(*update_promises)


@app.post("/entity/{entity}/watch")
async def watch(entity: str, watch_request: WatchRequest):
    await model.watch_store(entity, watch_request.url)


@app.post("/entity/{entity}/unwatch")
async def unwatch(entity: str, unwatch_request: UnwatchRequest):
    await model.watch_delete(entity, unwatch_request.url)


@app.post("/entity/{entity}", response_class=ORJSONResponse)
async def ingest(entity: str, data: Dict[str, Any], background_tasks: BackgroundTasks):
    (pointer, version) = await internal_ingest(entity, None, data)
    background_tasks.add_task(notify_watchers, entity=entity)
    return {"version": version, "pointer": pointer_as_str(pointer)}


@app.get("/healthz", response_class=ORJSONResponse)
def healthz():
    return {"health": True}


@app.get("/readyz", response_class=ORJSONResponse)
def readyz():
    return {"ready": True}
