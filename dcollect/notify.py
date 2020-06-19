import asyncio
from nats.aio.client import Client as NATS
import logging
import os

import httpx

from dcollect.model import Model

NOTIFY_TOPIC = "entity-updates-v1"

logger = logging.getLogger("dcollect.notify")


class Notify:
    nc: NATS
    model: Model
    run: bool
    fut_done: asyncio.Future

    def __init__(self, http_client: httpx.AsyncClient, model: Model):
        self.model = model
        self.http_client = http_client
        self.run = True
        self.nc = NATS()
        loop = asyncio.get_event_loop()
        self.fut_done = loop.create_future()

    async def setup(self) -> None:
        if os.environ.get("NO_SUBSCRIBE") == "1":
            self.fut_done.set_result(True)
            return
        await self.nc.connect("nats://nats:4222")
        self.notify_task = asyncio.create_task(self.notify_loop())
        logging.info("Setup done")

    async def shutdown(self):
        self.run = False
        try:
            await self.nc.drain()
            await asyncio.wait_for(self.fut_done, timeout=7.0)
        except asyncio.TimeoutError:
            self.notify_task.cancel()

    async def notify_loop(self):
        logging.info("Starting notify loop")
        try:
            while self.run:
                futures = []
                async for notification in self.model.get_notifications():
                    futures.append(asyncio.create_task(self.send_notification(notification.entity)))
                asyncio.gather(*futures)

            self.fut_done.set_result(True)
        except asyncio.CancelledError:
            return

    async def send_notification(self, entity: str) -> bool:
        response = await self.nc.request(NOTIFY_TOPIC, entity.encode("utf-8"), timeout=5)
        return response == b"OK"
