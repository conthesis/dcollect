def now() -> int:
    return int(
        datetime.datetime.now().replace(tzinfo=datetime.timezone.utc).timestamp() * 1000
    )


def pointer_as_str(pointer: bytes):
    return base64.b64encode(pointer)
