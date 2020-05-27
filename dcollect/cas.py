import hashlib
from typing import Any, Dict

import orjson

import dcollect.model as model


def to_json(x) -> bytes:
    return orjson.dumps(x, option=orjson.OPT_SORT_KEYS)


def hs(data):
    h = hashlib.shake_128()
    h.update(data)
    d = h.digest(8)
    return d


async def store(data: Dict[Any, Any]) -> bytes:
    blob = to_json(data)
    h = hs(blob)
    await model.cas_insert(h, blob)
    return h
