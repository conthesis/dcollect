import os
from typing import AsyncGenerator, Iterator, Optional, Tuple

import dcollect.redis as redis

def _cas_key(hash_: bytes) -> bytes:
    return b"dcollect_cas:" + hash_

def _vsn_ptr_key(entity: str) -> bytes:
    return b"dcollect_vsn_ptr:" + entity.encode("utf-8")

def _vsn_key(entity: str) -> bytes:
    return b"dcollect_vsn_vsn:" + entity.encode("utf-8")

def _watch_key(entity: str) -> bytes:
        return b"dcollect_watch:" + entity.encode("utf-8")


class Model:

    def __init__(self):
        self.redis = redis.from_url(os.environ["REDIS_URL"])

    async def teardown(self):
        self.disconnect()

    async def cas_insert(self, hash_: bytes, data: bytes):
        await self.redis.set(_cas_key(hash_), data)

    async def get_ca(self, hs: bytes) -> Optional[bytes]:
        return await self.redis.get(_cas_key(hs))

    async def watch_store(self, entity: str, url: str):
        return await self.redis.zadd(_watch_key(entity), 0, url)

    async def watch_delete(self, entity: str, url: str):
        return await self.redis.zrem(_watch_key(entity), url)

    async def store_vsn(
        self, entity: str, vsn: int, pointer: bytes
    ):
        await self.redis.lpush(_vsn_ptr_key(entity), pointer)
        await self.redis.lpush(_vsn_key(entity), vsn)


    async def get_latest_pointer(self, entity: str) -> Optional[bytes]:
        res = await self.redis.lrange(_vsn_ptr_key(entity), 0, 0)
        if len(res) == 0:
            return None
        else:
            return res[0]

    async def get_trailing_watches_for_entity(
        self, entity: str,
    ) -> AsyncGenerator[Tuple[str, int], None]:
        ptr = await self.get_latest_pointer(entity)
        if ptr is None:
            return
        vsns = await self.redis.lrange(_vsn_key(entity), 0, 0)
        if len(vsns) == 0:
            return
        vsn = int(vsns[0])

        for url in await self.redis.zrangebyscore(_watch_key(entity), "-inf", f"({str(vsn)}"):
            yield (url.decode("utf-8"), vsn)

    async def update_watch(self, entity: str, url: str, version: int):
        await self.redis.zadd(_watch_key(entity), version, url)

    async def get_history(self, entity: str) -> Iterator[Tuple[int, bytes]]:
        vsns = await self.redis.lrange(_vsn_key(entity), 0, 100)
        pointers = await self.redis.lrange(_vsn_ptr_key(entity), 0, 100)
        return zip(vsns, pointers)
