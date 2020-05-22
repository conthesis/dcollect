from typing import Optional, Awaitable, Dict, Any, Tuple
import base64
from fastapi import FastAPI, Response
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


@app.on_event("startup")
async def startup():
    await model.setup()


@app.on_event("shutdown")
async def shutdown():
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


@app.post("/ingest")
async def ingest_item(store_req: StoreRequest):
    (pointer, version) = await internal_ingest(store_req.entity,
                                               store_req.version,
                                               store_req.data)
    return {"version": version, "pointer": base64.b64encode(pointer)}


@app.post("/entity/{entity}")
async def ingest(entity: str, data: Dict[str, Any]):
    (pointer, version) = await internal_ingest(entity, None, data)
    return {"version": version, "pointer": base64.b64encode(pointer)}


@app.get("/healthz")
def healthz():
    return {"health": True}


@app.get("/readyz")
def readyz():
    return {"ready": True}
