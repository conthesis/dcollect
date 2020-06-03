import os
import asyncio
import logging

import httpx

from dcollect.model import Model

NOTIFY_TOPIC = "dcollect-notify-v1"

logger = logging.getLogger("dcollect.notify")


class Notify:
    http_client: httpx.AsyncClient
    model: Model
    run: bool
    fut_done: asyncio.Future

    def __init__(self, http_client: httpx.AsyncClient, model: Model):
        self.model = model
        self.http_client = http_client
        self.run = True
        loop = asyncio.get_event_loop()
        self.fut_done = loop.create_future()

    async def setup(self) -> None:
        if os.environ.get("NO_SUBSCRIBE") == "1":
            self.fut_done.set_result(True)
            return
        self.notify_task = asyncio.create_task(self.notify_loop())

    async def shutdown(self):
        self.run = False
        try:
            await asyncio.wait_for(self.fut_done, timeout=5.0)
        except asyncio.TimeoutError:
            self.notify_task.cancel()


    async def notify_loop(self):
        try:
            while self.run:
                async for notification in self.model.get_notifications():
                    tasks = []
                    for watcher in notification.watchers:
                        tasks.append(self.send_notification(watcher, notification.entity))
                    res = await asyncio.gather(*tasks)
            self.fut_done.set_result(True)
        except asyncio.CancelledError:
            return

    async def send_notification(self, url: str, entity: str) -> bool:
        body = {"entity": entity}
        resp = await self.http_client.post(url=url, json=body)
        return resp.status_code == 200
