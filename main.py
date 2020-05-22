from typing import Optional, Awaitable, Dict, Any
import base64
from fastapi import FastAPI, Response
from pydantic import BaseModel
import json
import databases
import hashlib
import filetype  # type: ignore
import datetime


def now() -> int:
    return int(datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).timestamp() * 1000) 


DATABASE_URL = "sqlite:///./test.db"
database = databases.Database(DATABASE_URL)

SETUP_QUERIES = [
    """CREATE TABLE IF NOT EXISTS cas (hash BLOB(8) PRIMARY KEY, data BLOB) """,
    """CREATE TABLE IF NOT EXISTS vsn (entity TEXT, vsn INTEGER, pointer BLOB(8), PRIMARY KEY (entity, vsn))"""
]


def hs(data):
    h = hashlib.shake_128()
    h.update(data)
    d = h.digest(8)
    return d


def to_json(x) -> bytes:
    return json.dumps(x, sort_keys=True, separators=(',', ':')).encode('utf-8')


def from_json(x: bytes) -> Dict[str, Any]:
    return json.loads(x.decode("utf-8"))


async def store_ca(data: Dict[Any, Any]) -> bytes:
    blob = to_json(data)
    h = hs(blob)
    CAS_STORE = "INSERT OR REPLACE INTO cas (hash, data) VALUES (:hash, :data)"
    await database.execute(CAS_STORE, values={"hash": h, "data": blob})
    return h


async def get_ca(hs: bytes) -> Optional[bytes]:
    CAS_GET = "SELECT data FROM cas WHERE hash = :hash"
    res = await database.fetch_one(CAS_GET, values={"hash": hs})
    if res is None:
        return None
    (data, ) = res
    return data


def store_vsn(entity: str, vsn: Optional[int], pointer: bytes) -> Awaitable[int]:
    STORE = "INSERT OR REPLACE INTO vsn (entity, vsn, pointer) VALUES (:entity, :vsn, :pointer)"
    return database.execute(STORE,
                            values={
                                "entity": entity,
                                "vsn": vsn,
                                "pointer": pointer
                            })


async def get_latest_pointer(entity: str) -> Optional[bytes]:
    Q = "SELECT pointer FROM vsn WHERE entity = :entity ORDER BY vsn DESC LIMIT 1"
    res = await database.fetch_one(Q, values={"entity": entity})
    if res is None:
        return None
    (ptr, ) = res
    return ptr


app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()
    for query in SETUP_QUERIES:
        await database.execute(query=query)


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


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


@app.get("/entity/{entity}")
async def read_item(entity: str):
    ptr = await get_latest_pointer(entity)
    if ptr is None:
        return Response(status_code=404)
    data = await get_ca(ptr)
    if data is None:
        return Response(status_code=404)
    return Response(data, media_type=guess_media_type(data))


async def internal_ingest(entity: str, version: Optional[int], data: Dict[str, Any]) -> (bytes, int):
    if version is None:
        version = now()
    pointer = await store_ca(data)
    await store_vsn(entity, version, pointer)
    return pointer, version

@app.post("/ingest")
async def ingest_item(store_req: StoreRequest):
    (pointer, version) = await internal_ingest(store_req.entity, store_req.version, store_req.data)
    return {
        "entity": store_req.entity,
        "version": version,
        "pointer": base64.b64encode(pointer)
    }

@app.post("/entity/{entity}")
async def ingest(entity: str, data: Dict[str, Any]):
    (pointer, version) = await internal_ingest(entity, None, data)
    return {
        "entity": entity,
        "version": version,
        "pointer": base64.b64encode(pointer)
    }
    
    
    
    

@app.get("/healthz")
def healthz():
    return { "health": True }

@app.get("/readyz")
def readyz():
    return { "ready": True }
