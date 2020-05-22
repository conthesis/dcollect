import databases
from typing import Awaitable, Optional, Dict
import datetime
DATABASE_URL = "sqlite:///./test.db"
database = databases.Database(DATABASE_URL)

SETUP_QUERIES = [
    """CREATE TABLE IF NOT EXISTS cas (hash BLOB(8) PRIMARY KEY, data BLOB) """,
    """CREATE TABLE IF NOT EXISTS vsn (entity TEXT, vsn INTEGER, pointer BLOB(8), PRIMARY KEY (entity, vsn))"""
]


async def cas_insert(hash_: bytes, data: bytes):
    CAS_STORE = "INSERT OR REPLACE INTO cas (hash, data) VALUES (:hash, :data)"
    await database.execute(CAS_STORE, values={"hash": hash_, "data": data})


async def get_ca(hs: bytes) -> Optional[bytes]:
    CAS_GET = "SELECT data FROM cas WHERE hash = :hash"
    res = await database.fetch_one(CAS_GET, values={"hash": hs})
    if res is None:
        return None
    (data, ) = res
    return data


def store_vsn(entity: str, vsn: Optional[int],
              pointer: bytes) -> Awaitable[int]:
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


async def setup():
    await database.connect()
    for query in SETUP_QUERIES:
        await database.execute(query=query)


async def teardown():
    await database.disconnect()
