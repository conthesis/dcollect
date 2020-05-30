import base64
import datetime
from typing import Optional

import filetype  # type: ignore


def now() -> int:
    return int(
        datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).timestamp() * 1000
    )


def pointer_as_str(pointer: bytes) -> str:
    return base64.b64encode(pointer)


def guess_media_type(data: bytes) -> Optional[str]:
    kind = filetype.match(data)
    if kind is not None:
        return kind.type
    elif data[0] == b"{"[0]:
        return "appliction/json"
    else:
        return None
