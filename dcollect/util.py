import base64


def pointer_as_str(pointer: bytes) -> bytes:
    return base64.b64encode(pointer)
