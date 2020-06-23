import dataclasses
import os
from typing import Any, AsyncGenerator, List, Optional


import dcollect.redis as redis


def _cas_key(hash_: bytes) -> bytes:
    return b"dcollect_cas:" + hash_


def _vsn_ptr_key(entity: str) -> bytes:
    return b"dcollect_vsn_ptr:" + entity.encode("utf-8")


def _watch_key(entity: str) -> bytes:
    return b"dcollect_watch:" + entity.encode("utf-8")


NOTIFY_SET_KEY = "dcollect_notify"


@dataclasses.dataclass
class Notification:
    entity: str
    version: str

    @classmethod
    def from_bytes(cls, x: bytes) -> "Notification":
        fields = x.split(b"\0")
        if len(fields) != 2:
            raise RuntimeError("invalid format")

        entity = fields[0].decode("utf-8")
        version = int(fields[1].decode("utf-8"))
        return cls(entity=entity, version=version)

    def to_bytes(self) -> bytes:
        return self.entity.encode("utf-8") + b"\0" + str(self.version).encode("utf-8")


class Model:
    def __init__(self):
        self.redis = redis.from_url(os.environ["REDIS_URL"])

    async def teardown(self):
        self.disconnect()

    async def store_vsn(self, entity: str, pointer: bytes) -> int:
        version = await self.redis.lpush(_vsn_ptr_key(entity), pointer)
        await self.redis.sadd(
            NOTIFY_SET_KEY, Notification(entity=entity, version=version).to_bytes()
        )
        return version

    async def get_notifications(self) -> AsyncGenerator[Notification, Any]:
        ents = await self.redis.srandmember(NOTIFY_SET_KEY, 10)
        for e in ents:
            yield Notification.from_bytes(e)

    async def get_latest_pointer(self, entity: str) -> Optional[bytes]:
        res = await self.redis.lrange(_vsn_ptr_key(entity), 0, 0)
        if len(res) == 0:
            return None
        else:
            return res[0]

    async def remove_notification(self, notification_data: bytes) -> bool:
        return await self.redis.srem(NOTIFY_SET_KEY, notification_data) == 1

    async def get_history(self, entity: str) -> List[bytes]:
        pointers = await self.redis.lrange(_vsn_ptr_key(entity), 0, 100)
        return pointers
