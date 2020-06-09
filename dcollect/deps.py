from typing import Optional

import httpx
from fastapi import Depends

from dcollect.model import Model
from dcollect.notify import Notify
from dcollect.cas import CAS

http_client_: Optional[httpx.AsyncClient] = None
notify_: Optional[Notify] = None
model_: Optional[Model] = None
cas_: Optional[CAS] = None


async def model() -> Model:
    global model_
    if model_ is None:
        model_ = Model()
    return model_


async def notify() -> Notify:
    global notify_
    if notify_ is None:
        notify_ = Notify(await http_client(), await model())
        await notify_.setup()

    return notify_


async def http_client() -> httpx.AsyncClient:
    global http_client_
    if http_client_ is None:
        http_client_ = httpx.AsyncClient()
    return http_client_


async def cas(http_client: httpx.AsyncClient = Depends(http_client)) -> CAS:
    global cas_
    if cas_ is None:
        cas_ = CAS(http_client)
    return cas_
