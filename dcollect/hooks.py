import dcollect.deps as deps


async def startup():
    await deps.model().setup()
    await deps.mq().startup()
    await deps.notify().setup()


async def shutdown():
    await deps.http_client().aclose()
    await deps.model().teardown()
    await deps.mq().shutdown()
