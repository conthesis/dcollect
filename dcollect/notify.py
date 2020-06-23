import asyncio
import logging
import os

import httpx
from nats.aio.client import Client as NATS

from dcollect.model import Model, Notification

NOTIFY_TOPIC = "entity-updates-v1"
NOTIFY_UPDATE_ACCEPTED = "entity-updates-v1.accepted"
NOTIFY_UPDATE_ACCEPTED_QUEUE = "dcollect-entity-updates-v1.accepted"

logger = logging.getLogger("dcollect.notify")


class Notify:
    nc: NATS
    model: Model
    run: bool
    fut_done: asyncio.Future
    no_subscribe: bool

    def __init__(self, http_client: httpx.AsyncClient, model: Model):
        self.model = model
        self.http_client = http_client
        self.run = True
        self.nc = NATS()
        loop = asyncio.get_event_loop()
        self.fut_done = loop.create_future()
        self.no_subscribe = os.environ.get("NO_SUBSCRIBE") == "1"

    async def setup(self) -> None:
        if self.no_subscribe:
            return

        await self.nc.connect("nats://nats:4222")
        await self.nc.subscribe(
            NOTIFY_UPDATE_ACCEPTED,
            queue=NOTIFY_UPDATE_ACCEPTED_QUEUE,
            cb=self.on_accepted,
        )
        self.notify_task = asyncio.create_task(self.notify_loop())
        logging.info("Setup done")

    async def shutdown(self):
        self.run = False
        try:
            await self.nc.drain()
            await asyncio.wait_for(self.fut_done, timeout=7.0)
        except asyncio.TimeoutError:
            self.notify_task.cancel()

    async def on_accepted(self, msg):
        try:
            await self.model.remove_notification(msg.data)
        except Exception as ex:
            logger.error(ex)

    async def notify_loop(self):
        if self.no_subscribe:
            return
        logging.info("Starting notify loop")
        try:
            while self.run:
                await asyncio.gather(
                    *[
                        self.send_notification(ntf)
                        async for ntf in self.model.get_notifications()
                    ]
                )
                # Ugly... But makes sure we are responsive when terminating
                for _ in range(5):
                    await asyncio.sleep(1)
                    if not self.run:
                        break

            self.fut_done.set_result(True)
        except asyncio.CancelledError:
            return

    async def send_notification(self, notification: Notification):
        if self.no_subscribe:
            return
        logger.info("Sending notificaition %s", notification)
        await self.nc.publish(NOTIFY_TOPIC, notification.to_bytes())
