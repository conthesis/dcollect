import dataclasses
import os
from typing import Any, AsyncGenerator, List, Optional, Tuple

import orjson

import dcollect.redis as redis


def _cas_key(hash_: bytes) -> bytes:
    return b"dcollect_cas:" + hash_


def _vsn_ptr_key(entity: str) -> bytes:
    return b"dcollect_vsn_ptr:" + entity.encode("utf-8")


def _watch_key(entity: str) -> bytes:
    return b"dcollect_watch:" + entity.encode("utf-8")


NOTIFY_QUEUE_KEY = "dcollect_notify"
NOTIFY_QUEUE_TIMEOUT = 5


@dataclasses.dataclass
class Notification:
    entity: str
    version: str

    @classmethod
    def from_bytes(cls, x: bytes) -> "Notification":
        data = orjson.loads(x)
        return cls(**data)


class Model:
    def __init__(self):
        self.redis = redis.from_url(os.environ["REDIS_URL"])

    async def teardown(self):
        self.disconnect()

    async def store_vsn(self, entity: str, pointer: bytes) -> int:
        version = await self.redis.lpush(_vsn_ptr_key(entity), pointer)
        msg = orjson.dumps(
            {
                "entity": entity,
                "version": version,
            }
        )
        await self.redis.rpush(NOTIFY_QUEUE_KEY, msg)
        return version

    async def get_notifications(self) -> AsyncGenerator[Notification, Any]:
        current = None
        while True:
            current = await self.redis.lpop(NOTIFY_QUEUE_KEY)
            if current is not None:
                yield Notification.from_bytes(current)
            else:
                break
        res = await self.redis.blpop([NOTIFY_QUEUE_KEY], 5)
        if res:
            yield Notification.from_bytes(res[1])

    async def get_latest_pointer(self, entity: str) -> Optional[bytes]:
        res = await self.redis.lrange(_vsn_ptr_key(entity), 0, 0)
        if len(res) == 0:
            return None
        else:
            return res[0]

    async def get_history(self, entity: str) -> List[bytes]:
        pointers = await self.redis.lrange(_vsn_ptr_key(entity), 0, 100)
        return pointers
