import os
from typing import Callable, Any
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrNoServers, ErrTimeout

NATS_URL = os.environ.get("NATS_URL", None)


class MQ:
    def __init__(self):
        self.nc = NATS()

    async def startup(self):
        await self.nc.connect(NATS_URL)

    async def shutdown(self):
        await self.nc.close()

    async def subscribe(self, topic: str, cb: Callable[[Any], None]):
        return await self.nc.subscribe(topic, cb=cb)

    async def publish(self, topic: str, data: bytes):
        return await self.nc.publish(topic, data)
