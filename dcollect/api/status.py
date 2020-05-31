from fastapi import APIRouter
from fastapi.responses import ORJSONResponse

router = APIRouter()


@router.get("/healthz", response_class=ORJSONResponse)
def healthz():
    return {"health": True}


@router.get("/readyz", response_class=ORJSONResponse)
def readyz():
    return {"ready": True}
