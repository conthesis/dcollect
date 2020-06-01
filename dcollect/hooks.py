import dcollect.deps as deps


async def startup() -> None:
    """Hook to be run when the server starts"""


async def shutdown() -> None:
    await (await deps.http_client()).aclose()
    await (await deps.model()).teardown()
    await (await deps.mq()).shutdown()
