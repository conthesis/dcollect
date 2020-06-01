from typing import List

from fastapi import APIRouter, Depends
from pydantic import BaseModel

import dcollect.deps as deps


class WatchRequest(BaseModel):
    url: str


class UnwatchRequest(BaseModel):
    url: str


class WatchMultipleItem(BaseModel):
    entity: str
    url: str


class WatchMultipleRequest(BaseModel):
    to_watch: List[WatchMultipleItem]


router = APIRouter()


@router.post("/entity/{entity}/watch")
async def watch(
    entity: str, watch_request: WatchRequest, model=Depends(deps.model)
) -> None:
    await model.watch_store(entity, watch_request.url)


@router.post("/watchMultiple")
async def watch_multiple(
    watch_multiple: WatchMultipleRequest, model=Depends(deps.model)
) -> None:
    for x in watch_multiple.to_watch:
        await model.watch_store(x.entity, x.url)


@router.post("/unwatchMultiple")
async def unwatch_multiple(
    unwatch_multiple: WatchMultipleRequest, model=Depends(deps.model)
) -> None:
    for x in unwatch_multiple.to_watch:
        await model.watch_delete(x.entity, x.url)


@router.post("/entity/{entity}/unwatch")
async def unwatch(
    entity: str, unwatch_request: UnwatchRequest, model=Depends(deps.model)
) -> None:
    await model.watch_delete(entity, unwatch_request.url)
