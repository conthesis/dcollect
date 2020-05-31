from fastapi import FastAPI

import dcollect.api as api
import dcollect.hooks as hooks

app = FastAPI()

app.include_router(api.status, tags=["status"])
app.include_router(api.watch, tags=["watch"])
app.include_router(api.ingest, tags=["ingest"])
app.include_router(api.read, tags=["read"])
app.include_router(api.history, tags=["history"])

app.on_event("startup")(hooks.startup)
app.on_event("shutdown")(hooks.shutdown)
