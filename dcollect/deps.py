from typing import Optional

import httpx

from dcollect.model import Model
from dcollect.mq import MQ
from dcollect.notify import Notify

http_client_: Optional[httpx.AsyncClient] = None
mq_: Optional[MQ] = None
notify_: Optional[Notify] = None
model_: Optional[Model] = None


async def model() -> Model:
    global model_
    if model_ is None:
        model_ = Model()
        await model_.setup()
    return model_


async def notify() -> Notify:
    global notify_
    if notify_ is None:
        notify_ = Notify(await http_client(), await mq(), await model())
        await notify_.setup()

    return notify_


async def http_client() -> httpx.AsyncClient:
    global http_client_
    if http_client_ is None:
        http_client_ = httpx.AsyncClient()
    return http_client_


async def mq() -> MQ:
    global mq_
    if mq_ is None:
        mq_ = MQ()
        await mq_.startup()
    return mq_
