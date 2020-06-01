import os
from typing import AsyncGenerator, List, Optional, Tuple

import dcollect.redis as redis


def _cas_key(hash_: bytes) -> bytes:
    return b"dcollect_cas:" + hash_


def _vsn_ptr_key(entity: str) -> bytes:
    return b"dcollect_vsn_ptr:" + entity.encode("utf-8")


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

    async def store_vsn(self, entity: str, pointer: bytes) -> int:
        return await self.redis.lpush(_vsn_ptr_key(entity), pointer)

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
        vsn = await self.redis.llen(_vsn_ptr_key(entity))

        for url in await self.redis.zrangebyscore(
            _watch_key(entity), "-inf", f"({str(vsn)}"
        ):
            yield (url.decode("utf-8"), vsn)

    async def update_watch(self, entity: str, url: str, version: int) -> None:
        await self.redis.zadd(_watch_key(entity), version, url)

    async def get_history(self, entity: str) -> List[bytes]:
        pointers = await self.redis.lrange(_vsn_ptr_key(entity), 0, 100)
        return pointers
