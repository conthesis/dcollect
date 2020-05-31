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
        if NATS_URL is not None:
            self.nc = NATS()
        else:
            self.nc = None

    async def startup(self):
        if NATS_URL is not None:
            await self.nc.connect(NATS_URL)

    async def shutdown(self):
        if self.nc is not None:
            await self.nc.close()

    async def subscribe(
        self,
        topic: str,
        cb: Callable[[Any], None],
        manual_acks: bool = True,
        ack_wat: int = 30,
    ):
        if self.nc is not None:
            return await self.nc.subscribe(topic, cb=cb)

    async def ack(self, msg):
        return await self.nc.ack(msg)

    async def publish(self, topic: str, data: Union[str, bytes]):
        if self.nc is not None:
            return await self.nc.publish(topic, data)
