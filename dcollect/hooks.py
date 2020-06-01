import dcollect.deps as deps


async def startup():
    """Hook to be run when the server starts"""

async def shutdown():
    await (await deps.http_client()).aclose()
    await (await deps.model()).teardown()
    await (await deps.mq()).shutdown()
