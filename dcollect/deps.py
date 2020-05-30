import httpx

import dcollect.model as model_mod
from dcollect.mq import MQ
from dcollect.notify import Notify

http_client_ = httpx.AsyncClient()
mq_ = MQ()
notify_ = Notify(http_client_, mq_)


def model():
    # HACK: The model module is not a class yet lets pretend it is
    return model_mod


def notify():
    return notify_


def http_client():
    return http_client_


def mq():
    return mq_
