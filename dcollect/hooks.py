import dcollect.deps as deps


async def startup() -> None:
    """Hook to be run when the server starts"""
    await deps.notify()


async def shutdown() -> None:
    await (await deps.notify()).shutdown()
    await (await deps.http_client()).aclose()
    await (await deps.model()).teardown()
