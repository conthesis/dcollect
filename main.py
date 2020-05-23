from typing import Optional, Awaitable, Dict, Any, Tuple
import base64
from fastapi import FastAPI, Response, BackgroundTasks
import aiohttp
from pydantic import BaseModel
import json
import databases
import hashlib
import filetype  # type: ignore
import datetime
import model


def hs(data):
    h = hashlib.shake_128()
    h.update(data)
    d = h.digest(8)
    return d


def now() -> int:
    return int(datetime.datetime.now().replace(
        tzinfo=datetime.timezone.utc).timestamp() * 1000)


def to_json(x) -> bytes:
    return json.dumps(x, sort_keys=True, separators=(',', ':')).encode('utf-8')


def from_json(x: bytes) -> Dict[str, Any]:
    return json.loads(x.decode("utf-8"))


async def store_ca(data: Dict[Any, Any]) -> bytes:
    blob = to_json(data)
    h = hs(blob)
    await model.cas_insert(h, blob)
    return h


app = FastAPI()

http_session = None


@app.on_event("startup")
async def startup():
    http_session = aiohttp.ClientSession()
    await model.setup()


@app.on_event("shutdown")
async def shutdown():
    await http_session.close()
    await model.teardown()


class StoreRequest(BaseModel):
    entity: str
    version: Optional[int] = None
    data: Dict[str, Any]


def guess_media_type(data: bytes):
    kind = filetype.match(data)
    if kind is not None:
        return kind.type
    elif data[0] == b"{"[0]:
        return "appliction/json"
    else:
        return None


async def read_versioned(entity):
    ptr = await get_latest_pointer(entity)
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


async def internal_ingest(entity: str, version: Optional[int],
                          data: Dict[str, Any]) -> Tuple[bytes, int]:
    if version is None:
        version = now()
    pointer = await store_ca(data)
    await model.store_vsn(entity, version, pointer)
    return pointer, version


async def send_notification(url: str, entity: str):
    if http_session is None:
        raise RuntimeError("Trying to send notification before ready")
    async with http_session.post(url, body=to_json({"entity": entity})) as resp:
        if resp.status == 200:
            return True
        else:
            return False


async def notify_watchers(entity: str):
    oks = []
    async for (url, version) in model.get_trailing_watches_for_entity(entity):
        if await send_notification(url, entity):
            oks.append(url)


@app.post("/entity/{entity}")
async def ingest(entity: str, data: Dict[str, Any],
                 background_tasks: BackgroundTasks):
    (pointer, version) = await internal_ingest(entity, None, data)
    background_tasks.add_task(notify_watchers, entity=entity)
    return {"version": version, "pointer": base64.b64encode(pointer)}


@app.get("/healthz")
def healthz():
    return {"health": True}


@app.get("/readyz")
def readyz():
    return {"ready": True}
