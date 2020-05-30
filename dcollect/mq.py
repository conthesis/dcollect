import os
from typing import Any, Callable, Union

from nats.aio.client import Client as NATS  # type: ignore
from nats.aio.errors import (  # type: ignore
    ErrConnectionClosed,
    ErrNoServers,
    ErrTimeout,
)

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

    async def publish(self, topic: str, data: Union[str, bytes]):
        return await self.nc.publish(topic, data)
