import asyncio
import os
import traceback

from fastapi import FastAPI
from nats.aio.client import Client as NATS

from dcollect.model import Model, Notification
from dcollect.notify import Notify

app = FastAPI()

STORE_TOPIC = "conthesis.dcollect.store"
READ_TOPIC = "conthesis.dcollect.get"


async def main():
    dc = DCollect()
    try:
        await dc.setup()
        await dc.wait_for_shutdown()
    finally:
        await dc.shutdown()


class Service:
    def __init__(self, model: Model, notify: Notify):
        self.model = model
        self.notify = notify

    async def store(self, entity: str, pointer: bytes):
        version = await self.model.store_vsn(entity, pointer)
        await self.notify.send_notification(
            Notification(entity=entity, version=version)
        )
        return version

    async def get(self, entity: str) -> bytes:
        return await self.model.get_latest_pointer(entity)


class DCollect:
    def __init__(self):
        self.nc = NATS()
        self.model = Model()
        self.notify = Notify(self.model, self.nc)
        self.service = Service(self.model, self.notify)
        self.shutdown_f = asyncio.get_running_loop().create_future()

    async def wait_for_shutdown(self):
        await self.shutdown_f

    async def setup(self):
        await self.nc.connect(os.environ["NATS_URL"])
        await self.nc.subscribe(
            STORE_TOPIC, cb=self.handle_store,
        )
        await self.nc.subscribe(
            READ_TOPIC, cb=self.handle_get,
        )
        await self.notify.setup()

    async def reply(self, msg, data):
        reply = msg.reply
        if reply:
            await self.nc.publish(reply, data)

    async def handle_store(self, msg):
        try:
            data = msg.data
            nl = data.index(b"\n")
            if nl == -1:
                await self.reply(msg, b"ERR")
                return
            entity = data[:nl].decode("utf-8")
            data = data[nl + 1 :]
            if len(data) > 64:
                await self.reply(msg, b"ERR")
            vsn = await self.service.store(entity, data)
            await self.reply(msg, b"OK " + str(vsn).encode("utf-8"))
        except:
            traceback.print_exc()

    async def handle_get(self, msg):
        try:
            entity = msg.data.decode("utf-8")
            ptr = await self.service.get(entity)
            await self.reply(msg, ptr)
        except:
            traceback.print_exc()

    async def shutdown(self):
        self.shutdown_f.set_result(True)
        await self.notify.shutdown()
        if self.nc.is_connected:
            await self.nc.drain()
        await self.model.shutdown()
